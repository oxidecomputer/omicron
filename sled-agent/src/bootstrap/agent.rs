// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::client::Client as BootstrapAgentClient;
use super::config::{
    Config, BOOTSTRAP_AGENT_HTTP_PORT, BOOTSTRAP_AGENT_SPROCKETS_PORT,
};
use super::ddm_admin_client::{DdmAdminClient, DdmError};
use super::hardware::HardwareMonitor;
use super::params::RackInitializeRequest;
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
use crate::updates::UpdateManager;
use futures::stream::{self, StreamExt, TryStreamExt};
use illumos_utils::dladm::{
    self, Dladm, Etherstub, EtherstubVnic, GetMacError, PhysicalLink,
};
use illumos_utils::zfs::{
    self, Mountpoint, Zfs, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT,
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

    #[error("Error cleaning up old state: {0}")]
    Cleanup(anyhow::Error),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Error monitoring hardware: {0}")]
    Hardware(#[from] crate::bootstrap::hardware::Error),

    #[error("Error managing sled agent: {0}")]
    SledError(String),

    #[error("Error deserializing toml from {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error(transparent)]
    TrustQuorum(#[from] TrustQuorumError),

    #[error("Error collecting peer addresses: {0}")]
    PeerAddresses(String),

    #[error("Failed to initialize bootstrap address: {err}")]
    BootstrapAddress { err: illumos_utils::zone::EnsureGzAddressError },

    #[error("Failed to initialize rack: {0}")]
    RackSetup(#[from] crate::rack_setup::service::SetupServiceError),

    #[error(transparent)]
    GetMacError(#[from] GetMacError),

    #[error("Failed to lookup VNICs on boot: {0}")]
    GetVnics(#[from] illumos_utils::dladm::GetVnicError),

    #[error("Failed to delete VNIC on boot: {0}")]
    DeleteVnic(#[from] illumos_utils::dladm::DeleteVnicError),

    #[error("Failed to get all datasets: {0}")]
    ZfsDatasetsList(anyhow::Error),

    #[error("Failed to destroy dataset: {0}")]
    ZfsDestroy(#[from] zfs::DestroyDatasetError),

    #[error("Failed to ensure ZFS filesystem: {0}")]
    ZfsEnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),

    #[error("Failed to perform Zone operation: {0}")]
    ZoneOperation(#[from] illumos_utils::zone::AdmError),

    #[error("Error managing guest networking: {0}")]
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Error accessing version information: {0}")]
    Version(#[from] crate::updates::Error),
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

fn underlay_etherstub() -> Result<Etherstub, BootstrapError> {
    Dladm::ensure_etherstub(illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME)
        .map_err(|e| {
            BootstrapError::SledError(format!(
                "Can't access etherstub device: {}",
                e
            ))
        })
}

fn underlay_etherstub_vnic(
    underlay_etherstub: &Etherstub,
) -> Result<EtherstubVnic, BootstrapError> {
    Dladm::ensure_etherstub_vnic(&underlay_etherstub).map_err(|e| {
        BootstrapError::SledError(format!(
            "Can't access etherstub VNIC device: {}",
            e
        ))
    })
}

fn bootstrap_etherstub() -> Result<Etherstub, BootstrapError> {
    Dladm::ensure_etherstub(illumos_utils::dladm::BOOTSTRAP_ETHERSTUB_NAME)
        .map_err(|e| {
            BootstrapError::SledError(format!(
                "Can't access etherstub device: {}",
                e
            ))
        })
}

/// The entity responsible for bootstrapping an Oxide rack.
pub struct Agent {
    /// Debug log
    log: Logger,
    /// Store the parent log - without "component = BootstrapAgent" - so
    /// other launched components can set their own value.
    parent_log: Logger,

    /// Bootstrap network address.
    ip: Ipv6Addr,

    /// Our share of the rack secret, if we have one.
    share: Mutex<Option<ShareDistribution>>,

    sled_state: Mutex<SledAgentState>,
    config: Config,
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

// TODO(https://github.com/oxidecomputer/omicron/issues/945): This address
// could be randomly generated when it no longer needs to be durable.
fn bootstrap_ip(
    link: PhysicalLink,
    interface_id: u64,
) -> Result<Ipv6Addr, dladm::GetMacError> {
    let mac = Dladm::get_mac(link)?;
    Ok(mac_to_bootstrap_ip(mac, interface_id))
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
    sled_hardware::cleanup::delete_omicron_vnics(&log)
        .await
        .map_err(|err| BootstrapError::Cleanup(err))?;

    // Also delete any extant xde devices. These should also eventually be
    // recovered / tracked, to avoid interruption of any guests that are
    // still running. That's currently irrelevant, since we're deleting the
    // zones anyway.
    //
    // This is also tracked by
    // https://github.com/oxidecomputer/omicron/issues/725.
    illumos_utils::opte::delete_all_xde_devices(&log)?;

    Ok(())
}

impl Agent {
    pub async fn new(
        log: Logger,
        config: Config,
        sled_config: SledConfig,
        sp: Option<SpHandle>,
    ) -> Result<(Self, TrustQuorumMembership), BootstrapError> {
        let ba_log = log.new(o!(
            "component" => "BootstrapAgent",
        ));
        let link = config.link.clone();
        let ip = bootstrap_ip(link.clone(), 1)?;

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

        let bootstrap_etherstub = bootstrap_etherstub()?;

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
            ip,
            "bootstrap6",
        )
        .map_err(|err| BootstrapError::BootstrapAddress { err })?;

        // Start trying to notify ddmd of our bootstrap address so it can
        // advertise it to other sleds.
        let ddmd_client = DdmAdminClient::new(log.clone())?;
        ddmd_client.advertise_prefix(Ipv6Subnet::new(ip));

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

        // Before we start monitoring for hardware, ensure we're running from a
        // predictable state.
        //
        // This means all VNICs, zones, etc.
        cleanup_all_old_global_state(&log).await?;

        // Begin monitoring for hardware to handle tasks like initialization of
        // the switch zone.
        info!(log, "Bootstrap Agent monitoring for hardware");

        let agent = Agent {
            log: ba_log,
            parent_log: log,
            ip,
            share: Mutex::new(None),
            sled_state: Mutex::new(SledAgentState::Before(None)),
            config: config.clone(),
            sled_config,
            sp,
            ddmd_client,
        };

        let hardware_monitor = agent.start_hardware_monitor().await?;
        *agent.sled_state.lock().await =
            SledAgentState::Before(Some(hardware_monitor));

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

    async fn start_hardware_monitor(
        &self,
    ) -> Result<HardwareMonitor, BootstrapError> {
        let underlay_etherstub = underlay_etherstub()?;
        let underlay_etherstub_vnic =
            underlay_etherstub_vnic(&underlay_etherstub)?;
        let bootstrap_etherstub = bootstrap_etherstub()?;
        let link = self.config.link.clone();
        let switch_zone_bootstrap_address = bootstrap_ip(link, 2)?;
        let hardware_monitor = HardwareMonitor::new(
            &self.log,
            &self.sled_config,
            underlay_etherstub,
            underlay_etherstub_vnic,
            bootstrap_etherstub,
            switch_zone_bootstrap_address,
        )
        .await?;
        Ok(hardware_monitor)
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
                let (hardware, services) = match hardware_monitor.take() {
                    // This is the normal case; transfer hardware monitoring responsibilities from
                    // the bootstrap agent to the sled agent.
                    Some(hardware_monitor) => hardware_monitor,
                    // This is a less likely case, but if we previously failed to start (or
                    // restart) the hardware monitor, for any reason, recreate it.
                    None => self.start_hardware_monitor().await?,
                }
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
                            BOOTSTRAP_AGENT_SPROCKETS_PORT,
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

    /// Runs the rack setup service to completion
    pub async fn rack_initialize(
        &self,
        request: RackInitializeRequest,
    ) -> Result<(), BootstrapError> {
        RssHandle::run_rss(
            &self.parent_log,
            request,
            self.ip,
            self.sp.clone(),
            // TODO-cleanup: Remove this arg once RSS can discover the trust
            // quorum members over the management network.
            self.config
                .sp_config
                .as_ref()
                .map(|sp_config| sp_config.trust_quorum_members.clone())
                .unwrap_or_default(),
        )
        .await?;
        Ok(())
    }

    /// Runs the rack setup service to completion
    pub async fn rack_reset(&self) -> Result<(), BootstrapError> {
        RssHandle::run_rss_reset(&self.parent_log, self.ip, None).await?;
        Ok(())
    }

    // The following "_locked" functions act on global state,
    // and take a MutexGuard as an argument, which may be unused.
    //
    // The input of the MutexGuard is intended to signify: this
    // method should only be called when the sled agent has been
    // dismantled, and is not concurrently executing!

    // Uninstall all oxide zones (except the switch zone)
    async fn uninstall_zones_locked(
        &self,
        _state: &tokio::sync::MutexGuard<'_, SledAgentState>,
    ) -> Result<(), BootstrapError> {
        const CONCURRENCY_CAP: usize = 32;
        futures::stream::iter(Zones::get().await?)
            .map(Ok::<_, anyhow::Error>)
            .try_for_each_concurrent(CONCURRENCY_CAP, |zone| async move {
                if zone.name() != "oxz_switch" {
                    Zones::halt_and_remove(zone.name()).await?;
                }
                Ok(())
            })
            .await
            .map_err(BootstrapError::Cleanup)?;
        Ok(())
    }

    async fn uninstall_sled_local_config_locked(
        &self,
        _state: &tokio::sync::MutexGuard<'_, SledAgentState>,
    ) -> Result<(), BootstrapError> {
        tokio::fs::remove_dir_all(omicron_common::OMICRON_CONFIG_PATH)
            .await
            .or_else(|err| match err.kind() {
                std::io::ErrorKind::NotFound => Ok(()),
                _ => Err(err),
            })
            .map_err(|err| BootstrapError::Io {
                message: format!(
                    "Deleting {}",
                    omicron_common::OMICRON_CONFIG_PATH
                ),
                err,
            })?;
        tokio::fs::create_dir_all(omicron_common::OMICRON_CONFIG_PATH)
            .await
            .map_err(|err| BootstrapError::Io {
                message: format!(
                    "Creating config directory {}",
                    omicron_common::OMICRON_CONFIG_PATH
                ),
                err,
            })?;
        Ok(())
    }

    async fn uninstall_networking_locked(
        &self,
        _state: &tokio::sync::MutexGuard<'_, SledAgentState>,
    ) -> Result<(), BootstrapError> {
        // NOTE: This is very similar to the invocations
        // in "sled_hardware::cleanup::cleanup_networking_resources",
        // with a few notable differences:
        //
        // - We can't remove bootstrap-related networking -- this operation
        // is performed via a request on the bootstrap network.
        // - We avoid deleting addresses using the chelsio link. Removing
        // these addresses would delete "cxgbe0/ll", and could render
        // the sled inaccessible via a local interface.

        sled_hardware::cleanup::delete_underlay_addresses(&self.log)
            .map_err(BootstrapError::Cleanup)?;
        sled_hardware::cleanup::delete_omicron_vnics(&self.log)
            .await
            .map_err(BootstrapError::Cleanup)?;
        illumos_utils::opte::delete_all_xde_devices(&self.log)?;
        Ok(())
    }

    async fn uninstall_storage_locked(
        &self,
        _state: &tokio::sync::MutexGuard<'_, SledAgentState>,
    ) -> Result<(), BootstrapError> {
        let datasets = zfs::get_all_omicron_datasets()
            .map_err(BootstrapError::ZfsDatasetsList)?;
        for dataset in &datasets {
            info!(self.log, "Removing dataset: {dataset}");
            zfs::Zfs::destroy_dataset(dataset)?;
        }

        Ok(())
    }

    /// Resets this sled, removing:
    ///
    /// - All control plane zones (except the switch zone)
    /// - All sled-local configuration
    /// - All underlay networking
    /// - All storage managed by the control plane
    ///
    /// This API is intended to put the sled into a state where it can
    /// subsequently be initialized via RSS.
    pub async fn sled_reset(&self) -> Result<(), BootstrapError> {
        let mut state = self.sled_state.lock().await;

        if let SledAgentState::After(_) = &mut *state {
            // We'd like to stop the old sled agent before starting a new
            // hardware monitor -- however, if we cannot start a new hardware
            // monitor, the bootstrap agent may be in a degraded state.
            let server = match std::mem::replace(
                &mut *state,
                SledAgentState::Before(None),
            ) {
                SledAgentState::After(server) => server,
                _ => panic!(
                    "Unexpected state (we should have just matched on it)"
                ),
            };
            server.close().await.map_err(BootstrapError::SledError)?;
        };

        // Try to reset the sled, but do not exit early on error.
        let result = async {
            self.uninstall_zones_locked(&state).await?;
            self.uninstall_sled_local_config_locked(&state).await?;
            self.uninstall_networking_locked(&state).await?;
            self.uninstall_storage_locked(&state).await?;
            Ok::<(), BootstrapError>(())
        }
        .await;

        // Try to restart the bootstrap agent hardware monitor before
        // returning any errors from reset.
        match &mut *state {
            SledAgentState::Before(None) => {
                let hardware_monitor = self.start_hardware_monitor()
                    .await
                    .map_err(|err| {
                        warn!(self.log, "Failed to restart bootstrap agent hardware monitor");
                        err
                    })?;
                *state = SledAgentState::Before(Some(hardware_monitor));
            }
            _ => {}
        };

        // Return any errors encountered resetting the sled.
        result?;

        Ok(())
    }

    pub async fn components_get(
        &self,
    ) -> Result<Vec<crate::updates::Component>, BootstrapError> {
        let updates = UpdateManager::new(self.sled_config.updates.clone());
        let components = updates.components_get().await?;
        Ok(components)
    }

    /// The GZ address used by the bootstrap agent for Sprockets.
    pub fn sprockets_address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip, BOOTSTRAP_AGENT_SPROCKETS_PORT, 0, 0)
    }

    /// The address used by the bootstrap agent to serve a dropshot interface.
    pub fn http_address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip, BOOTSTRAP_AGENT_HTTP_PORT, 0, 0)
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
                    mac: MacAddr6::nil().into(),
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
