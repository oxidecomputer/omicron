// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::config::{
    Config, BOOTSTORE_PORT, BOOTSTRAP_AGENT_HTTP_PORT,
    BOOTSTRAP_AGENT_RACK_INIT_PORT,
};
use super::hardware::HardwareMonitor;
use super::http_entrypoints::RackOperationStatus;
use super::params::RackInitializeRequest;
use super::params::StartSledAgentRequest;
use super::secret_retriever::LrtqOrHardcodedSecretRetriever;
use super::views::SledAgentResponse;
use crate::config::Config as SledConfig;
use crate::server::Server as SledServer;
use crate::services::ServiceManager;
use crate::storage_manager::{StorageManager, StorageResources};
use crate::updates::UpdateManager;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
use futures::stream::{self, StreamExt, TryStreamExt};
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm::{Dladm, Etherstub, EtherstubVnic, GetMacError};
use illumos_utils::zfs::{
    self, Mountpoint, Zfs, ZONE_ZFS_RAMDISK_DATASET,
    ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT,
};
use illumos_utils::zone::Zones;
use illumos_utils::{execute, PFEXEC};
use key_manager::{KeyManager, ReadinessSetter, StorageKeyRequester};
use omicron_common::address::Ipv6Subnet;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::ledger::{self, Ledger, Ledgerable};
use serde::{Deserialize, Serialize};
use sled_hardware::underlay::BootstrapInterface;
use sled_hardware::{Baseboard, HardwareManager};
use slog::Logger;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

mod rack_ops;

pub use rack_ops::RackInitId;
pub use rack_ops::RackResetId;
use rack_ops::RssAccess;
pub use rack_ops::RssAccessError;

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

    #[error("Failed to enable routing: {0}")]
    EnablingRouting(illumos_utils::ExecutionError),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Error monitoring hardware: {0}")]
    Hardware(#[from] crate::bootstrap::hardware::Error),

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] ledger::Error),

    #[error("Error managing sled agent: {0}")]
    SledError(String),

    #[error("Error collecting peer addresses: {0}")]
    PeerAddresses(String),

    #[error("Failed to initialize bootstrap address: {err}")]
    BootstrapAddress { err: illumos_utils::zone::EnsureGzAddressError },

    #[error("Failed to get bootstrap address: {err}")]
    GetBootstrapAddress { err: illumos_utils::zone::GetAddressError },

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

    #[error("Bootstore Error: {0}")]
    Bootstore(#[from] bootstore::NodeRequestError),

    #[error("Missing M.2 Paths for dataset: {0}")]
    MissingM2Paths(&'static str),
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

    /// Ensures that RSS (initialization or teardown) is not executed
    /// concurrently.
    rss_access: RssAccess,

    sled_state: Mutex<SledAgentState>,
    storage_resources: StorageResources,
    config: Config,
    sled_config: SledConfig,
    ddmd_client: DdmAdminClient,

    global_zone_bootstrap_link_local_address: Ipv6Addr,

    /// We maintain the handle just to show ownership, but don't use it, as the
    /// KeyManager task should run forever
    #[allow(unused)]
    key_manager_handle: JoinHandle<()>,

    /// Used to inform key requesters that the KeyManager is ready to be used.
    /// In particular this should be set when the rack is initialized
    key_manager_readiness: ReadinessSetter,

    /// We maintain a copy of the `StorageKeyRequester` so we can pass it
    /// through from the `HardwareManager` to the `StorageManager` when the
    /// `HardwareManager` gets recreated.
    storage_key_requester: StorageKeyRequester,

    /// Our sled's baseboard identity.
    baseboard: Baseboard,

    /// Handle for interacting with the local `bootstore::Node`
    bootstore: bootstore::NodeHandle,

    /// The join handle of the `bootstore::Node` task
    /// The bootstore main task runs forever. This handle field shows ownership.
    #[allow(unused)]
    bootstore_join_handle: JoinHandle<()>,

    /// The join handle of a task that polls DDMD and updates the bootstore with
    /// known peer addresses. This task runs forever. This handle field shows
    /// ownership.
    #[allow(unused)]
    bootstore_peer_update_handle: JoinHandle<()>,
}

const SLED_AGENT_REQUEST_FILE: &str = "sled-agent-request.toml";
const BOOTSTORE_FSM_STATE_FILE: &str = "bootstore-fsm-state.json";
const BOOTSTORE_NETWORK_CONFIG_FILE: &str = "bootstore-network-config.json";

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

async fn sled_config_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, BootstrapError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(SLED_AGENT_REQUEST_FILE))
        .collect();

    if paths.is_empty() {
        return Err(BootstrapError::MissingM2Paths(
            sled_hardware::disk::CONFIG_DATASET,
        ));
    }
    Ok(paths)
}

async fn bootstore_fsm_state_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, BootstrapError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_FSM_STATE_FILE))
        .collect();

    if paths.is_empty() {
        return Err(BootstrapError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

async fn bootstore_network_config_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, BootstrapError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_NETWORK_CONFIG_FILE))
        .collect();

    if paths.is_empty() {
        return Err(BootstrapError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

impl Agent {
    pub async fn new(
        log: Logger,
        config: Config,
        sled_config: SledConfig,
    ) -> Result<Self, BootstrapError> {
        let ba_log = log.new(o!(
            "component" => "BootstrapAgent",
        ));
        let link = config.link.clone();
        let ip = BootstrapInterface::GlobalZone.ip(&link)?;

        // We expect this directory to exist for Key Management
        // It's purposefully in the ramdisk and files only exist long enough
        // to create and mount encrypted datasets.
        info!(
            log, "Ensuring zfs key directory exists";
            "path" => sled_hardware::disk::KEYPATH_ROOT,
        );
        tokio::fs::create_dir_all(sled_hardware::disk::KEYPATH_ROOT)
            .await
            .map_err(|err| BootstrapError::Io {
                message: format!(
                    "Creating zfs key directory {}",
                    sled_hardware::disk::KEYPATH_ROOT
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

        let global_zone_bootstrap_link_local_address = Zones::get_address(
            None,
            // AddrObject::link_local() can only fail if the interface name is
            // malformed, but we just got it from `Dladm`, so we know it's
            // valid.
            &AddrObject::link_local(&bootstrap_etherstub_vnic.0).unwrap(),
        )
        .map_err(|err| BootstrapError::GetBootstrapAddress { err })?;

        // Convert the `IpNetwork` down to just the IP address.
        let global_zone_bootstrap_link_local_address =
            match global_zone_bootstrap_link_local_address.ip() {
                IpAddr::V4(_) => {
                    unreachable!("link local bootstrap address must be ipv6")
                }
                IpAddr::V6(addr) => addr,
            };

        // Start trying to notify ddmd of our bootstrap address so it can
        // advertise it to other sleds.
        let ddmd_client = DdmAdminClient::localhost(&log)?;
        ddmd_client.advertise_prefix(Ipv6Subnet::new(ip));

        // Before we create the switch zone, we need to ensure that the
        // necessary ZFS and Zone resources are ready. All other zones are
        // created on U.2 drives.
        let zoned = true;
        let do_format = true;
        let encryption_details = None;
        let quota = None;
        Zfs::ensure_filesystem(
            ZONE_ZFS_RAMDISK_DATASET,
            Mountpoint::Path(Utf8PathBuf::from(
                ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT,
            )),
            zoned,
            do_format,
            encryption_details,
            quota,
        )?;

        // Before we start monitoring for hardware, ensure we're running from a
        // predictable state.
        //
        // This means all VNICs, zones, etc.
        cleanup_all_old_global_state(&log).await?;

        // Ipv6 forwarding must be enabled to route traffic between zones,
        // including the switch zone which we may launch below if we find we're
        // actually running on a scrimlet.
        //
        // This should be a no-op if already enabled.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "/usr/sbin/routeadm",
            // Needed to access all zones, which are on the underlay.
            "-e",
            "ipv6-forwarding",
            "-u",
        ]);
        execute(cmd).map_err(|e| BootstrapError::EnablingRouting(e))?;

        // Spawn the `KeyManager` which is needed by the the StorageManager to
        // retrieve encryption keys.
        let (secret_retriever, key_manager_readiness) =
            LrtqOrHardcodedSecretRetriever::new();
        let (mut key_manager, storage_key_requester) =
            KeyManager::new(&log, secret_retriever);

        let handle = tokio::spawn(async move { key_manager.run().await });

        // Begin monitoring for hardware to handle tasks like initialization of
        // the switch zone.
        info!(log, "Bootstrap Agent monitoring for hardware");

        let hardware_monitor = Self::hardware_monitor(
            &ba_log,
            &config.link,
            &sled_config,
            global_zone_bootstrap_link_local_address,
            storage_key_requester.clone(),
        )
        .await?;

        let storage_resources = hardware_monitor.storage().clone();
        let baseboard = hardware_monitor.baseboard().clone();

        // Wait for at least the M.2 we booted from to show up.
        //
        // This gives the bootstrap agent a chance to read locally-stored
        // configs if any exist.
        loop {
            match storage_resources.boot_disk().await {
                Some(disk) => {
                    info!(ba_log, "Found boot disk M.2: {disk:?}");
                    break;
                }
                None => {
                    info!(ba_log, "Waiting for boot disk M.2...");
                    tokio::time::sleep(core::time::Duration::from_millis(250))
                        .await;
                }
            }
        }

        // Configure and start the bootstore
        let bootstore_config = bootstore::Config {
            id: baseboard.clone(),
            addr: SocketAddrV6::new(ip, BOOTSTORE_PORT, 0, 0),
            time_per_tick: std::time::Duration::from_millis(250),
            learn_timeout: std::time::Duration::from_secs(5),
            rack_init_timeout: std::time::Duration::from_secs(60),
            rack_secret_request_timeout: std::time::Duration::from_secs(30),
            fsm_state_ledger_paths: bootstore_fsm_state_paths(
                &storage_resources,
            )
            .await?,
            network_config_ledger_paths: bootstore_network_config_paths(
                &storage_resources,
            )
            .await?,
        };
        let (mut bootstore_node, bootstore_node_handle) =
            bootstore::Node::new(bootstore_config, &ba_log).await;

        let bootstore_join_handle =
            tokio::spawn(async move { bootstore_node.run().await });

        // Spawn a task for polling DDMD and updating bootstore
        let bootstore_peer_update_handle =
            Self::poll_ddmd_for_bootstore_peer_update(
                &ba_log,
                bootstore_node_handle.clone(),
            )
            .await;

        let paths = sled_config_paths(&storage_resources).await?;
        let maybe_ledger =
            Ledger::<PersistentSledAgentRequest>::new(&ba_log, paths).await;
        let make_bootstrap_agent = move |initialized| Agent {
            log: ba_log,
            parent_log: log,
            ip,
            rss_access: RssAccess::new(initialized),
            sled_state: Mutex::new(SledAgentState::Before(Some(
                hardware_monitor,
            ))),
            storage_resources,
            config: config.clone(),
            sled_config,
            ddmd_client,
            global_zone_bootstrap_link_local_address,
            key_manager_handle: handle,
            storage_key_requester,
            key_manager_readiness,
            baseboard,
            bootstore: bootstore_node_handle,
            bootstore_join_handle,
            bootstore_peer_update_handle,
        };
        let agent = if let Some(ledger) = maybe_ledger {
            let agent = make_bootstrap_agent(true);
            info!(agent.log, "Sled already configured, loading sled agent");
            let sled_request = ledger.data();
            agent.request_sled_agent(&sled_request.request).await?;
            agent
        } else {
            make_bootstrap_agent(false)
        };

        Ok(agent)
    }

    pub fn baseboard(&self) -> &Baseboard {
        &self.baseboard
    }

    async fn poll_ddmd_for_bootstore_peer_update(
        log: &Logger,
        bootstore_node_handle: bootstore::NodeHandle,
    ) -> JoinHandle<()> {
        let log = log.new(o!("component" => "bootstore_ddmd_poller"));
        tokio::spawn(async move {
            let mut current_peers: BTreeSet<SocketAddrV6> = BTreeSet::new();
            // We're talking to a service's admin interface on localhost and
            // we're only asking for its current state. We use a retry in a loop
            // instead of `backoff`.
            //
            // We also use this timeout in the case of spurious ddmd failures
            // that require a reconnection from the ddmd_client.
            const RETRY: tokio::time::Duration =
                tokio::time::Duration::from_secs(5);

            loop {
                let ddmd_client = match DdmAdminClient::localhost(&log) {
                    Ok(client) => {
                        info!(log, "Connected to ddmd");
                        client
                    }
                    Err(err) => {
                        warn!(log, "Failed to connect to ddmd:"; "err" => #%err);
                        tokio::time::sleep(RETRY).await;
                        continue;
                    }
                };

                loop {
                    match ddmd_client
                        .derive_bootstrap_addrs_from_prefixes(&[
                            BootstrapInterface::GlobalZone,
                        ])
                        .await
                    {
                        Ok(addrs) => {
                            let peers: BTreeSet<_> = addrs
                                .map(|ip| {
                                    SocketAddrV6::new(ip, BOOTSTORE_PORT, 0, 0)
                                })
                                .collect();
                            if peers != current_peers {
                                current_peers = peers;
                                if let Err(e) = bootstore_node_handle
                                    .load_peer_addresses(current_peers.clone())
                                    .await
                                {
                                    error!(
                                        log,
                                        concat!("Bootstore comms error: {}. ",
                                        "bootstore::Node task must have paniced",
                                    ),
                                        e
                                    );
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            warn!(
                                log, "Failed to get prefixes from ddmd";
                                "err" => #%err,
                            );
                            break;
                        }
                    }
                    tokio::time::sleep(RETRY).await;
                }
            }
        })
    }

    async fn start_hardware_monitor(
        &self,
    ) -> Result<HardwareMonitor, BootstrapError> {
        Self::hardware_monitor(
            &self.log,
            &self.config.link,
            &self.sled_config,
            self.global_zone_bootstrap_link_local_address,
            self.storage_key_requester.clone(),
        )
        .await
    }

    async fn hardware_monitor(
        log: &Logger,
        link: &illumos_utils::dladm::PhysicalLink,
        sled_config: &SledConfig,
        global_zone_bootstrap_link_local_address: Ipv6Addr,
        storage_key_requester: StorageKeyRequester,
    ) -> Result<HardwareMonitor, BootstrapError> {
        let underlay_etherstub = underlay_etherstub()?;
        let underlay_etherstub_vnic =
            underlay_etherstub_vnic(&underlay_etherstub)?;
        let bootstrap_etherstub = bootstrap_etherstub()?;
        let switch_zone_bootstrap_address =
            BootstrapInterface::SwitchZone.ip(&link)?;

        let hardware_monitor = HardwareMonitor::new(
            &log,
            &sled_config,
            global_zone_bootstrap_link_local_address,
            underlay_etherstub,
            underlay_etherstub_vnic,
            bootstrap_etherstub,
            switch_zone_bootstrap_address,
            storage_key_requester,
        )
        .await?;
        Ok(hardware_monitor)
    }

    /// Initializes the Sled Agent on behalf of the RSS.
    ///
    /// If the Sled Agent has already been initialized:
    /// - This method is idempotent for the same request
    /// - Thie method returns an error for different requests
    pub async fn request_sled_agent(
        &self,
        request: &StartSledAgentRequest,
    ) -> Result<SledAgentResponse, BootstrapError> {
        info!(&self.log, "Loading Sled Agent: {:?}", request);

        // Initialize the secret retriever used by the `KeyManager`
        if request.use_trust_quorum {
            info!(self.log, "KeyManager: using lrtq secret retriever");
            let salt = request.hash_rack_id();
            LrtqOrHardcodedSecretRetriever::init_lrtq(
                salt,
                self.bootstore.clone(),
            )
        } else {
            info!(self.log, "KeyManager: using hardcoded secret retriever");
            LrtqOrHardcodedSecretRetriever::init_hardcoded();
        }
        self.key_manager_readiness.set();

        let sled_address = request.sled_address();

        let mut state = self.sled_state.lock().await;

        match &mut *state {
            // We have not previously initialized a sled agent.
            SledAgentState::Before(hardware_monitor) => {
                // Stop the bootstrap agent from monitoring for hardware, and
                // pass control of service management to the sled agent.
                //
                // NOTE: If we fail at any point in the body of this function,
                // we should restart the hardware monitor, so we can react to
                // changes in the switch regardless of the success or failure of
                // this sled agent.
                let (hardware, services, storage) =
                    match hardware_monitor.take() {
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
                    storage: Option<StorageManager>,
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
                                self.storage.take().unwrap(),
                            ));
                        }
                    }
                }
                let restarter = RestartMonitor {
                    run: true,
                    log: self.log.clone(),
                    hardware: Some(hardware),
                    services: Some(services.clone()),
                    storage: Some(storage.clone()),
                    monitor: hardware_monitor,
                };

                // Server does not exist, initialize it.
                let server = SledServer::start(
                    &self.sled_config,
                    self.parent_log.clone(),
                    request.clone(),
                    services.clone(),
                    storage.clone(),
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
                let paths = sled_config_paths(&self.storage_resources).await?;
                let mut ledger = Ledger::new_with(
                    &self.log,
                    paths,
                    PersistentSledAgentRequest {
                        request: Cow::Borrowed(request),
                    },
                );
                ledger.commit().await?;

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

                return Ok(SledAgentResponse { id: server.id() });
            }
        }
    }

    /// Spawn a task to run the rack setup service.
    pub fn start_rack_initialize(
        self: &Arc<Self>,
        request: RackInitializeRequest,
    ) -> Result<RackInitId, RssAccessError> {
        self.rss_access.start_initializing(self, request)
    }

    /// Spawn a task to run rack reset.
    pub fn start_rack_reset(
        self: &Arc<Self>,
    ) -> Result<RackResetId, RssAccessError> {
        self.rss_access.start_reset(self)
    }

    /// Get the status of a spawned initialize or reset operation.
    pub fn initialization_reset_op_status(&self) -> RackOperationStatus {
        self.rss_access.operation_status()
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
        let config_dirs = self
            .storage_resources
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter();

        for dir in config_dirs {
            for entry in dir.read_dir_utf8().map_err(|err| {
                BootstrapError::Io { message: format!("Deleting {dir}"), err }
            })? {
                let entry = entry.map_err(|err| BootstrapError::Io {
                    message: format!("Deleting {dir}"),
                    err,
                })?;

                let path = entry.path();
                let file_type =
                    entry.file_type().map_err(|err| BootstrapError::Io {
                        message: format!("Deleting {path}"),
                        err,
                    })?;

                if file_type.is_dir() {
                    tokio::fs::remove_dir_all(path).await
                } else {
                    tokio::fs::remove_file(path).await
                }
                .map_err(|err| BootstrapError::Io {
                    message: format!("Deleting {path}"),
                    err,
                })?;
            }
        }
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
        let datasets = zfs::get_all_omicron_datasets_for_delete()
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
        result
    }

    pub fn get_bootstore_node_handle(&self) -> bootstore::NodeHandle {
        self.bootstore.clone()
    }

    pub async fn components_get(
        &self,
    ) -> Result<Vec<crate::updates::Component>, BootstrapError> {
        let updates = UpdateManager::new(self.sled_config.updates.clone());
        let components = updates.components_get().await?;
        Ok(components)
    }

    /// The GZ address used by the bootstrap agent for rack initialization by
    /// bootstrap agent running local to RSS on a scrimlet.
    pub fn rack_init_address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip, BOOTSTRAP_AGENT_RACK_INIT_PORT, 0, 0)
    }

    /// The address used by the bootstrap agent to serve a dropshot interface.
    pub fn http_address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip, BOOTSTRAP_AGENT_HTTP_PORT, 0, 0)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
struct PersistentSledAgentRequest<'a> {
    request: Cow<'a, StartSledAgentRequest>,
}

impl<'a> Ledgerable for PersistentSledAgentRequest<'a> {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;
    use uuid::Uuid;

    #[tokio::test]
    async fn persistent_sled_agent_request_serialization() {
        let logctx =
            test_setup_log("persistent_sled_agent_request_serialization");
        let log = &logctx.log;

        let request = PersistentSledAgentRequest {
            request: Cow::Owned(StartSledAgentRequest {
                id: Uuid::new_v4(),
                rack_id: Uuid::new_v4(),
                ntp_servers: vec![String::from("test.pool.example.com")],
                dns_servers: vec![String::from("1.1.1.1")],
                use_trust_quorum: false,
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
            }),
        };

        let tempdir = camino_tempfile::Utf8TempDir::new().unwrap();
        let paths = vec![tempdir.path().join("test-file")];

        let mut ledger = Ledger::new_with(log, paths.clone(), request.clone());
        ledger.commit().await.expect("Failed to write to ledger");

        let ledger = Ledger::<PersistentSledAgentRequest>::new(log, paths)
            .await
            .expect("Failed to read request");

        assert!(&request == ledger.data(), "serialization round trip failed");
        logctx.cleanup_successful();
    }
}
