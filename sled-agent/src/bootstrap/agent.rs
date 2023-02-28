// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::client::Client as BootstrapAgentClient;
use super::config::{Config, BOOTSTRAP_AGENT_PORT};
use super::ddm_admin_client::{DdmAdminClient, DdmError};
use super::hardware::HardwareMonitor;
use super::params::SledAgentRequest;
use super::rss_handle::RssHandle;
use super::server::TrustQuorumMembership;
use super::trust_quorum::{
    RackSecret, SerializableShareDistribution, ShareDistribution,
    TrustQuorumError,
};
use super::views::SledAgentResponse;
use crate::config::Config as SledConfig;
use crate::server::Server as SledServer;
use crate::services::ServiceManager;
use crate::sp::SpHandle;
use futures::stream::{self, StreamExt, TryStreamExt};
use illumos_utils::dladm::{self, Dladm, GetMacError, PhysicalLink};
use illumos_utils::link::LinkKind;
use illumos_utils::zfs::{
    Mountpoint, Zfs, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT,
};
use illumos_utils::zone::Zones;
use omicron_common::address::Ipv6Subnet;
use omicron_common::api::external::{Error as ExternalError, MacAddr};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use serde::{Deserialize, Serialize};
use sled_hardware::HardwareManager;
use slog::Logger;
use std::borrow::Cow;
use std::collections::HashSet;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// Initial octet of IPv6 for bootstrap addresses.
pub(crate) const BOOTSTRAP_PREFIX: u16 = 0xfdb0;

/// IPv6 prefix mask for bootstrap addresses.
pub(crate) const BOOTSTRAP_MASK: u8 = 64;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(Error, Debug)]
pub enum BootstrapError {
    #[error("IO error: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Error monitoring hardware: {0}")]
    Hardware(#[from] crate::bootstrap::hardware::Error),

    #[error("Error starting sled agent: {0}")]
    SledError(String),

    #[error("Error deserializing toml from {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error(transparent)]
    TrustQuorum(#[from] TrustQuorumError),

    #[error("Error collecting peer addresses: {0}")]
    PeerAddresses(String),

    #[error("Failed to initialize bootstrap address: {err}")]
    BootstrapAddress { err: illumos_utils::zone::EnsureGzAddressError },

    #[error(transparent)]
    GetMacError(#[from] GetMacError),

    #[error("Failed to lookup VNICs on boot: {0}")]
    GetVnics(#[from] illumos_utils::dladm::GetVnicError),

    #[error("Failed to delete VNIC on boot: {0}")]
    DeleteVnic(#[from] illumos_utils::dladm::DeleteVnicError),

    #[error("Failed to ensure ZFS filesystem: {0}")]
    ZfsEnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),

    #[error("Failed to perform Zone operation: {0}")]
    ZoneOperation(#[from] illumos_utils::zone::AdmError),

    #[error("Error managing guest networking: {0}")]
    Opte(#[from] crate::opte::Error),
}

impl From<BootstrapError> for ExternalError {
    fn from(err: BootstrapError) -> Self {
        Self::internal_error(&err.to_string())
    }
}

// Describes the view of the sled agent from the perspective of the bootstrap
// agent.
enum SledAgentState {
    // Either we're in the "before" stage, and we're monitoring for hardware,
    // waiting for the sled agent to be requested...
    Before(Option<HardwareMonitor>),
    // ... or we're in the "after" stage, and the sled agent is running. In this
    // case, the responsibility for monitoring hardware should be transferred to
    // the sled agent.
    After(SledServer),
}

/// The entity responsible for bootstrapping an Oxide rack.
pub(crate) struct Agent {
    /// Debug log
    log: Logger,
    /// Store the parent log - without "component = BootstrapAgent" - so
    /// other launched components can set their own value.
    parent_log: Logger,
    address: SocketAddrV6,

    /// Our share of the rack secret, if we have one.
    share: Mutex<Option<ShareDistribution>>,

    rss: Mutex<Option<RssHandle>>,
    sled_state: Mutex<SledAgentState>,
    sled_config: SledConfig,
    sp: Option<SpHandle>,
    ddmd_client: DdmAdminClient,
}

fn get_sled_agent_request_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH)
        .join("sled-agent-request.toml")
}

fn mac_to_bootstrap_ip(mac: MacAddr, interface_id: u64) -> Ipv6Addr {
    let mac_bytes = mac.into_array();
    assert_eq!(6, mac_bytes.len());

    Ipv6Addr::new(
        BOOTSTRAP_PREFIX,
        ((mac_bytes[0] as u16) << 8) | mac_bytes[1] as u16,
        ((mac_bytes[2] as u16) << 8) | mac_bytes[3] as u16,
        ((mac_bytes[4] as u16) << 8) | mac_bytes[5] as u16,
        (interface_id >> 48 & 0xffff).try_into().unwrap(),
        (interface_id >> 32 & 0xffff).try_into().unwrap(),
        (interface_id >> 16 & 0xffff).try_into().unwrap(),
        (interface_id & 0xfff).try_into().unwrap(),
    )
}

fn bootstrap_ip(
    link: PhysicalLink,
    interface_id: u64,
) -> Result<Ipv6Addr, dladm::GetMacError> {
    let mac = Dladm::get_mac(link)?;
    Ok(mac_to_bootstrap_ip(mac, interface_id))
}

// TODO(https://github.com/oxidecomputer/omicron/issues/945): This address
// could be randomly generated when it no longer needs to be durable.
fn bootstrap_address(
    link: PhysicalLink,
    interface_id: u64,
    port: u16,
) -> Result<SocketAddrV6, dladm::GetMacError> {
    let ip = bootstrap_ip(link, interface_id)?;
    Ok(SocketAddrV6::new(ip, port, 0, 0))
}

// Delete all VNICs that can be managed by the control plane.
//
// These are currently those that match the prefix `ox` or `vopte`.
pub async fn delete_omicron_vnics(log: &Logger) -> Result<(), BootstrapError> {
    let vnics = Dladm::get_vnics()?;
    stream::iter(vnics)
        .zip(stream::iter(std::iter::repeat(log.clone())))
        .map(Ok::<_, illumos_utils::dladm::DeleteVnicError>)
        .try_for_each_concurrent(None, |(vnic, log)| async {
            tokio::task::spawn_blocking(move || {
                warn!(
                  log,
                  "Deleting existing VNIC";
                    "vnic_name" => &vnic,
                    "vnic_kind" => ?LinkKind::from_name(&vnic).unwrap(),
                );
                Dladm::delete_vnic(&vnic)
            })
            .await
            .unwrap()
        })
        .await?;
    Ok(())
}

// Deletes all state which may be left-over from a previous execution of the
// Sled Agent.
//
// This may re-establish contact in the future, and re-construct a picture of
// the expected state of each service. However, at the moment, "starting from a
// known clean slate" is easier to work with.
async fn cleanup_all_old_global_state(
    log: &Logger,
) -> Result<(), BootstrapError> {
    // Identify all existing zones which should be managed by the Sled
    // Agent.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/725):
    // Currently, we're removing these zones. In the future, we should
    // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
    // to leave the running Zones intact).
    let zones = Zones::get().await?;
    stream::iter(zones)
        .zip(stream::iter(std::iter::repeat(log.clone())))
        .map(Ok::<_, illumos_utils::zone::AdmError>)
        .try_for_each_concurrent(None, |(zone, log)| async move {
            warn!(log, "Deleting existing zone"; "zone_name" => zone.name());
            Zones::halt_and_remove_logged(&log, zone.name()).await
        })
        .await?;

    // Identify all VNICs which should be managed by the Sled Agent.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/725)
    // Currently, we're removing these VNICs. In the future, we should
    // identify if they're being used by the aforementioned existing zones,
    // and track them once more.
    //
    // This should be accessible via:
    // $ dladm show-linkprop -c -p zone -o LINK,VALUE
    //
    // Note that we don't currently delete the VNICs in any particular
    // order. That should be OK, since we're definitely deleting the guest
    // VNICs before the xde devices, which is the main constraint.
    delete_omicron_vnics(&log).await?;

    // Also delete any extant xde devices. These should also eventually be
    // recovered / tracked, to avoid interruption of any guests that are
    // still running. That's currently irrelevant, since we're deleting the
    // zones anyway.
    //
    // This is also tracked by
    // https://github.com/oxidecomputer/omicron/issues/725.
    crate::opte::delete_all_xde_devices(&log)?;

    Ok(())
}

impl Agent {
    pub async fn new(
        log: Logger,
        sled_config: SledConfig,
        link: PhysicalLink,
        sp: Option<SpHandle>,
    ) -> Result<(Self, TrustQuorumMembership), BootstrapError> {
        let ba_log = log.new(o!(
            "component" => "BootstrapAgent",
        ));

        let address = bootstrap_address(link.clone(), 1, BOOTSTRAP_AGENT_PORT)?;

        // The only zone with a bootstrap ip address besides the global zone,
        // is the switch zone. We allocate this address here since we have
        // access to the physical link. This also allows us to keep bootstrap
        // address allocation in one place.
        //
        // If other zones end up needing bootstrap addresses, we'll have to
        // rethink this strategy, and plumb bootstrap adresses similar to
        // underlay addresses.
        let switch_zone_bootstrap_address = bootstrap_ip(link, 2)?;

        // We expect this directory to exist - ensure that it does, before any
        // subsequent operations which may write configs here.
        info!(
            log, "Ensuring config directory exists";
            "path" => omicron_common::OMICRON_CONFIG_PATH,
        );
        tokio::fs::create_dir_all(omicron_common::OMICRON_CONFIG_PATH)
            .await
            .map_err(|err| BootstrapError::Io {
                message: format!(
                    "Creating config directory {}",
                    omicron_common::OMICRON_CONFIG_PATH
                ),
                err,
            })?;

        let bootstrap_etherstub = Dladm::ensure_etherstub(
            illumos_utils::dladm::BOOTSTRAP_ETHERSTUB_NAME,
        )
        .map_err(|e| {
            BootstrapError::SledError(format!(
                "Can't access etherstub device: {}",
                e
            ))
        })?;

        let bootstrap_etherstub_vnic = Dladm::ensure_etherstub_vnic(
            &bootstrap_etherstub,
        )
        .map_err(|e| {
            BootstrapError::SledError(format!(
                "Can't access etherstub VNIC device: {}",
                e
            ))
        })?;

        Zones::ensure_has_global_zone_v6_address(
            bootstrap_etherstub_vnic.clone(),
            *address.ip(),
            "bootstrap6",
        )
        .map_err(|err| BootstrapError::BootstrapAddress { err })?;

        // Start trying to notify ddmd of our bootstrap address so it can
        // advertise it to other sleds.
        let ddmd_client = DdmAdminClient::new(log.clone())?;
        ddmd_client.advertise_prefix(Ipv6Subnet::new(*address.ip()));

        // Before we start creating zones, we need to ensure that the
        // necessary ZFS and Zone resources are ready.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/1934):
        // We should carefully consider which dataset this is using; it's
        // currently part of the ramdisk.
        Zfs::ensure_zoned_filesystem(
            ZONE_ZFS_DATASET,
            Mountpoint::Path(std::path::PathBuf::from(
                ZONE_ZFS_DATASET_MOUNTPOINT,
            )),
            // do_format=
            true,
        )?;

        // HardwareMonitor must be on the underlay network like all services
        let underlay_etherstub = Dladm::ensure_etherstub(
            illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME,
        )
        .map_err(|e| {
            BootstrapError::SledError(format!(
                "Can't access etherstub device: {}",
                e
            ))
        })?;

        let underlay_etherstub_vnic =
            Dladm::ensure_etherstub_vnic(&underlay_etherstub).map_err(|e| {
                BootstrapError::SledError(format!(
                    "Can't access etherstub VNIC device: {}",
                    e
                ))
            })?;

        // Before we start monitoring for hardware, ensure we're running from a
        // predictable state.
        //
        // This means all VNICs, zones, etc.
        cleanup_all_old_global_state(&log).await?;

        // Begin monitoring for hardware to handle tasks like initialization of
        // the switch zone.
        info!(log, "Bootstrap Agent monitoring for hardware");
        let hardware_monitor = HardwareMonitor::new(
            &ba_log,
            &sled_config,
            underlay_etherstub,
            underlay_etherstub_vnic,
            bootstrap_etherstub,
            switch_zone_bootstrap_address,
        )
        .await?;

        let agent = Agent {
            log: ba_log,
            parent_log: log,
            address,
            share: Mutex::new(None),
            rss: Mutex::new(None),
            sled_state: Mutex::new(SledAgentState::Before(Some(
                hardware_monitor,
            ))),
            sled_config,
            sp,
            ddmd_client,
        };

        let request_path = get_sled_agent_request_path();
        let trust_quorum = if request_path.exists() {
            info!(agent.log, "Sled already configured, loading sled agent");
            let sled_request: PersistentSledAgentRequest = toml::from_str(
                &tokio::fs::read_to_string(&request_path).await.map_err(
                    |err| BootstrapError::Io {
                        message: format!(
                            "Reading subnet path from {request_path:?}"
                        ),
                        err,
                    },
                )?,
            )
            .map_err(|err| BootstrapError::Toml { path: request_path, err })?;

            let trust_quorum_share =
                sled_request.trust_quorum_share.map(ShareDistribution::from);
            agent
                .request_agent(&sled_request.request, &trust_quorum_share)
                .await?;
            TrustQuorumMembership::Known(Arc::new(trust_quorum_share))
        } else {
            TrustQuorumMembership::Uninitialized
        };

        Ok((agent, trust_quorum))
    }

    /// Initializes the Sled Agent on behalf of the RSS.
    ///
    /// If the Sled Agent has already been initialized:
    /// - This method is idempotent for the same request
    /// - Thie method returns an error for different requests
    pub async fn request_agent(
        &self,
        request: &SledAgentRequest,
        trust_quorum_share: &Option<ShareDistribution>,
    ) -> Result<SledAgentResponse, BootstrapError> {
        info!(&self.log, "Loading Sled Agent: {:?}", request);

        let sled_address = request.sled_address();

        let mut state = self.sled_state.lock().await;

        match &mut *state {
            // We have not previously initialized a sled agent.
            SledAgentState::Before(hardware_monitor) => {
                if let Some(share) = trust_quorum_share.clone() {
                    self.establish_sled_quorum(share.clone()).await?;
                    *self.share.lock().await = Some(share);
                }

                // Stop the bootstrap agent from monitoring for hardware, and
                // pass control of service management to the sled agent.
                //
                // NOTE: If we fail at any point in the body of this function,
                // we should restart the hardware monitor, so we can react to
                // changes in the switch regardless of the success or failure of
                // this sled agent.
                let (hardware, services) = hardware_monitor
                    .take()
                    .expect("Hardware Monitor does not exist")
                    .stop()
                    .await
                    .expect("Failed to stop hardware monitor");

                // This acts like a "run-on-drop" closure, to restart the
                // hardware monitor in the bootstrap agent if we fail to
                // initialize the Sled Agent.
                //
                // In the "healthy" case, we can "cancel" and this is
                // effectively a no-op.
                struct RestartMonitor<'a> {
                    run: bool,
                    log: Logger,
                    hardware: Option<HardwareManager>,
                    services: Option<ServiceManager>,
                    monitor: &'a mut Option<HardwareMonitor>,
                }
                impl<'a> RestartMonitor<'a> {
                    fn cancel(mut self) {
                        self.run = false;
                    }
                }
                impl<'a> Drop for RestartMonitor<'a> {
                    fn drop(&mut self) {
                        if self.run {
                            *self.monitor = Some(HardwareMonitor::start(
                                &self.log,
                                self.hardware.take().unwrap(),
                                self.services.take().unwrap(),
                            ));
                        }
                    }
                }
                let restarter = RestartMonitor {
                    run: true,
                    log: self.log.clone(),
                    hardware: Some(hardware),
                    services: Some(services.clone()),
                    monitor: hardware_monitor,
                };

                // Server does not exist, initialize it.
                let server = SledServer::start(
                    &self.sled_config,
                    self.parent_log.clone(),
                    request.clone(),
                    services.clone(),
                )
                .await
                .map_err(|e| {
                    BootstrapError::SledError(format!(
                        "Could not start sled agent server: {e}"
                    ))
                })?;
                info!(&self.log, "Sled Agent loaded; recording configuration");

                // Record this request so the sled agent can be automatically
                // initialized on the next boot.
                //
                // danger handling: `serialized_request` contains our trust quorum
                // share; we do not log it and only write it to the designated path.
                let serialized_request = PersistentSledAgentRequest {
                    request: Cow::Borrowed(request),
                    trust_quorum_share: trust_quorum_share
                        .clone()
                        .map(Into::into),
                }
                .danger_serialize_as_toml()
                .expect("Cannot serialize request");

                let path = get_sled_agent_request_path();
                tokio::fs::write(&path, &serialized_request).await.map_err(
                    |err| BootstrapError::Io {
                        message: format!(
                            "Recording Sled Agent request to {path:?}"
                        ),
                        err,
                    },
                )?;

                // This is the point-of-no-return, where we're committed to the
                // sled agent starting.
                restarter.cancel();
                *state = SledAgentState::After(server);

                // Start trying to notify ddmd of our sled prefix so it can
                // advertise it to other sleds.
                //
                // TODO-security This ddmd_client is used to advertise both this
                // (underlay) address and our bootstrap address. Bootstrap addresses are
                // unauthenticated (connections made on them are auth'd via sprockets),
                // but underlay addresses should be exchanged via authenticated channels
                // between ddmd instances. It's TBD how that will work, but presumably
                // we'll need to do something different here for underlay vs bootstrap
                // addrs (either talk to a differently-configured ddmd, or include info
                // indicating which kind of address we're advertising).
                self.ddmd_client.advertise_prefix(request.subnet);

                Ok(SledAgentResponse { id: request.id })
            }
            // We have previously initialized a sled agent.
            SledAgentState::After(server) => {
                info!(&self.log, "Sled Agent already loaded");

                if server.id() != request.id {
                    let err_str = format!(
                        "Sled Agent already running with UUID {}, but {} was requested",
                        server.id(),
                        request.id,
                    );
                    return Err(BootstrapError::SledError(err_str));
                } else if &server.address().ip() != sled_address.ip() {
                    let err_str = format!(
                        "Sled Agent already running on address {}, but {} was requested",
                        server.address().ip(),
                        sled_address.ip(),
                    );
                    return Err(BootstrapError::SledError(err_str));
                }

                // Bail out if this request includes a trust quorum share that
                // doesn't match ours. TODO-correctness Do we need to handle a
                // partially-initialized rack where we may have a share from a
                // previously-started-but-not-completed init process? If rerunning
                // it produces different shares this check will fail.
                if *trust_quorum_share != *self.share.lock().await {
                    let err_str = concat!(
                        "Sled Agent already running with",
                        " a different trust quorum share"
                    )
                    .to_string();
                    return Err(BootstrapError::SledError(err_str));
                }

                return Ok(SledAgentResponse { id: server.id() });
            }
        }
    }

    /// Communicates with peers, sharing secrets, until the rack has been
    /// sufficiently unlocked.
    async fn establish_sled_quorum(
        &self,
        share: ShareDistribution,
    ) -> Result<RackSecret, BootstrapError> {
        let ddm_admin_client = DdmAdminClient::new(self.log.clone())?;
        let rack_secret = retry_notify(
            retry_policy_internal_service_aggressive(),
            || async {
                let other_agents = {
                    // Manually build up a `HashSet` instead of `.collect()`ing
                    // so we can log if we see any duplicates.
                    let mut addrs = HashSet::new();
                    for addr in ddm_admin_client
                        .peer_addrs()
                        .await
                        .map_err(BootstrapError::DdmError)
                        .map_err(|err| BackoffError::transient(err))?
                    {
                        // We should never see duplicates; that would mean
                        // maghemite thinks two different sleds have the same
                        // bootstrap address!
                        if !addrs.insert(addr) {
                            let msg = format!("Duplicate peer addresses received from ddmd: {addr}");
                            error!(&self.log, "{}", msg);
                            return Err(BackoffError::permanent(
                                BootstrapError::PeerAddresses(msg),
                            ));
                        }
                    }
                    addrs
                };
                info!(
                    &self.log,
                    "Bootstrap: Communicating with peers: {:?}", other_agents
                );

                // "-1" to account for ourselves.
                if other_agents.len() < share.threshold - 1 {
                    warn!(
                        &self.log,
                        "Not enough peers to start establishing quorum"
                    );
                    return Err(BackoffError::transient(
                        BootstrapError::TrustQuorum(
                            TrustQuorumError::NotEnoughPeers,
                        ),
                    ));
                }
                info!(
                    &self.log,
                    "Bootstrap: Enough peers to start share transfer"
                );

                // Retrieve verified rack_secret shares from a quorum of agents
                let other_agents: Vec<BootstrapAgentClient> = other_agents
                    .into_iter()
                    .map(|addr| {
                        let addr = SocketAddrV6::new(
                            addr,
                            BOOTSTRAP_AGENT_PORT,
                            0,
                            0,
                        );
                        BootstrapAgentClient::new(
                            addr,
                            &self.sp,
                            &share.member_device_id_certs,
                            self.log.new(o!(
                                "BootstrapAgentClient" => addr.to_string()),
                            ),
                        )
                    })
                    .collect();

                // TODO: Parallelize this and keep track of whose shares we've
                // already retrieved and don't resend. See
                // https://github.com/oxidecomputer/omicron/issues/514
                let mut shares = vec![share.share.clone()];
                for agent in &other_agents {
                    let share = agent.request_share().await
                        .map_err(|e| {
                            info!(&self.log, "Bootstrap: failed to retreive share from peer: {:?}", e);
                            BackoffError::transient(
                                BootstrapError::TrustQuorum(e.into()),
                            )
                        })?;
                    info!(
                        &self.log,
                        "Bootstrap: retreived share from peer: {}",
                        agent.addr()
                    );
                    shares.push(share);
                }
                let rack_secret = RackSecret::combine_shares(
                    share.threshold,
                    share.total_shares(),
                    &shares,
                )
                .map_err(|e| {
                    warn!(
                        &self.log,
                        "Bootstrap: failed to construct rack secret: {:?}", e
                    );
                    // TODO: We probably need to actually write an error
                    // handling routine that gives up in some cases based on
                    // the error returned from `RackSecret::combine_shares`.
                    // See https://github.com/oxidecomputer/omicron/issues/516
                    BackoffError::transient(
                        BootstrapError::TrustQuorum(
                            TrustQuorumError::RackSecretConstructionFailed(e),
                        ),
                    )
                })?;
                info!(self.log, "RackSecret computed from shares.");
                Ok(rack_secret)
            },
            |error, duration| {
                warn!(
                    self.log,
                    "Failed to unlock sleds (will retry after {:?}: {:#}",
                    duration,
                    error,
                )
            },
        )
        .await?;

        Ok(rack_secret)
    }

    /// Initializes the Rack Setup Service, if requested by `config`.
    pub async fn start_rss(
        &self,
        config: &Config,
    ) -> Result<(), BootstrapError> {
        if let Some(rss_config) = &config.rss_config {
            info!(&self.log, "bootstrap service initializing RSS");
            let rss = RssHandle::start_rss(
                &self.parent_log,
                rss_config.clone(),
                *self.address.ip(),
                self.sp.clone(),
                // TODO-cleanup: Remove this arg once RSS can discover the trust
                // quorum members over the management network.
                config
                    .sp_config
                    .as_ref()
                    .map(|sp_config| sp_config.trust_quorum_members.clone())
                    .unwrap_or_default(),
            );
            self.rss.lock().await.replace(rss);
        }
        Ok(())
    }

    /// Return the global zone address that the bootstrap agent binds to.
    pub fn address(&self) -> SocketAddrV6 {
        self.address
    }
}

// We intentionally DO NOT derive `Debug` or `Serialize`; both provide avenues
// by which we may accidentally log the contents of our trust quorum share.
#[derive(Deserialize, PartialEq)]
struct PersistentSledAgentRequest<'a> {
    request: Cow<'a, SledAgentRequest>,
    trust_quorum_share: Option<SerializableShareDistribution>,
}

impl PersistentSledAgentRequest<'_> {
    /// On success, the returned string will contain our raw
    /// `trust_quorum_share`. This method is named `danger_*` to remind the
    /// caller that they must not log this string.
    fn danger_serialize_as_toml(&self) -> Result<String, toml::ser::Error> {
        #[derive(Serialize)]
        #[serde(remote = "PersistentSledAgentRequest")]
        struct PersistentSledAgentRequestDef<'a> {
            request: Cow<'a, SledAgentRequest>,
            trust_quorum_share: Option<SerializableShareDistribution>,
        }

        let mut out = String::with_capacity(128);
        let serializer = toml::Serializer::new(&mut out);
        PersistentSledAgentRequestDef::serialize(self, serializer)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macaddr::MacAddr6;
    use uuid::Uuid;

    #[test]
    fn test_mac_to_bootstrap_ip() {
        let mac = MacAddr("a8:40:25:10:00:01".parse::<MacAddr6>().unwrap());

        assert_eq!(
            mac_to_bootstrap_ip(mac, 1),
            "fdb0:a840:2510:1::1".parse::<Ipv6Addr>().unwrap(),
        );
    }

    #[test]
    fn persistent_sled_agent_request_serialization_round_trips() {
        let secret = RackSecret::new();
        let (mut shares, verifier) = secret.split(2, 4).unwrap();

        let request = PersistentSledAgentRequest {
            request: Cow::Owned(SledAgentRequest {
                id: Uuid::new_v4(),
                rack_id: Uuid::new_v4(),
                gateway: crate::bootstrap::params::Gateway {
                    address: None,
                    mac: MacAddr6::nil(),
                },
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
            }),
            trust_quorum_share: Some(
                ShareDistribution {
                    threshold: 2,
                    verifier,
                    share: shares.pop().unwrap(),
                    member_device_id_certs: vec![],
                }
                .into(),
            ),
        };

        let serialized = request.danger_serialize_as_toml().unwrap();
        let deserialized: PersistentSledAgentRequest =
            toml::from_str(&serialized).unwrap();

        assert!(request == deserialized, "serialization round trip failed");
    }
}
