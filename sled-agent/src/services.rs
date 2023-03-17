// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-local service management.
//!
//! For controlling zone-based storage services, refer to
//! [crate::storage_manager::StorageManager].
//!
//! For controlling virtual machine instances, refer to
//! [crate::instance_manager::InstanceManager].
//!
//! The [ServiceManager] provides separate mechanisms for services where the
//! "source-of-truth" is Nexus, compared with services where the
//! "source-of-truth" is the Sled Agent itself. Although we generally prefer to
//! delegate the decision of "which services run where" to Nexus, there are
//! situations where the Sled Agent must be capable of autonomously ensuring
//! that zones execute. For example, the "switch zone" contains services which
//! should automatically start when a Tofino device is detected, independently
//! of what other services Nexus wants to have executing on the sled.
//!
//! To accomplish this, the following interfaces are exposed:
//! - [ServiceManager::ensure_persistent] exposes an API to request a set of
//! services that should persist beyond reboot.
//! - [ServiceManager::activate_switch] exposes an API to specifically enable
//! or disable (via [ServiceManager::deactivate_switch]) the switch zone.

use crate::bootstrap::ddm_admin_client::{DdmAdminClient, DdmError};
use crate::params::{
    DendriteAsic, ServiceEnsureBody, ServiceType, ServiceZoneRequest, ZoneType,
};
use crate::smf_helper::SmfHelper;
use illumos_utils::dladm::{Dladm, Etherstub, EtherstubVnic, PhysicalLink};
use illumos_utils::link::{Link, VnicAllocator};
use illumos_utils::running_zone::{InstalledZone, RunningZone};
use illumos_utils::zfs::ZONE_ZFS_DATASET_MOUNTPOINT;
use illumos_utils::zone::AddressRequest;
use illumos_utils::zone::Zones;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::BOOTSTRAP_ARTIFACT_PORT;
use omicron_common::address::CRUCIBLE_PANTRY_PORT;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::OXIMETER_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::WICKETD_PORT;
use omicron_common::nexus_config::{
    self, DeploymentConfig as NexusDeploymentConfig,
};
use once_cell::sync::OnceCell;
use sled_hardware::underlay;
use sled_hardware::ScrimletMode;
use slog::Logger;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

// The filename of ServiceManager's internal storage.
const SERVICE_CONFIG_FILENAME: &str = "service.toml";
// The filename of a half-completed config, in need of parameters supplied at
// runtime.
const PARTIAL_CONFIG_FILENAME: &str = "config-partial.toml";
// The filename of a completed config, merging the partial config with
// additional appended parameters known at runtime.
const COMPLETE_CONFIG_FILENAME: &str = "config.toml";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot serialize TOML to file {path}: {err}")]
    TomlSerialize { path: PathBuf, err: toml::ser::Error },

    #[error("Cannot deserialize TOML from file {path}: {err}")]
    TomlDeserialize { path: PathBuf, err: toml::de::Error },

    #[error("I/O Error accessing {path}: {err}")]
    Io { path: PathBuf, err: std::io::Error },

    #[error("Failed to find device {device}")]
    MissingDevice { device: String },

    #[error("Sled Agent not initialized yet")]
    SledAgentNotReady,

    #[error("Switch zone error: {0}")]
    SwitchZone(anyhow::Error),

    #[error("Failed to issue SMF command: {0}")]
    SmfCommand(#[from] crate::smf_helper::Error),

    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: illumos_utils::running_zone::RunCommandError,
    },

    #[error("Failed to boot zone: {0}")]
    ZoneBoot(#[from] illumos_utils::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] illumos_utils::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] illumos_utils::running_zone::InstallZoneError),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Failed to access underlay device: {0}")]
    Underlay(#[from] underlay::Error),

    #[error("Failed to create Vnic for Nexus: {0}")]
    NexusVnicCreation(illumos_utils::dladm::CreateVnicError),

    #[error("Failed to create Vnic in switch zone: {0}")]
    SwitchVnicCreation(illumos_utils::dladm::CreateVnicError),

    #[error("Failed to add GZ addresses: {message}: {err}")]
    GzAddress {
        message: String,
        err: illumos_utils::zone::EnsureGzAddressError,
    },

    #[error("Could not initialize service {service} as requested: {message}")]
    BadServiceRequest { service: String, message: String },

    #[error("Services already configured for this Sled Agent")]
    ServicesAlreadyConfigured,
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        omicron_common::api::external::Error::InternalError {
            internal_message: err.to_string(),
        }
    }
}

/// The default path to service configuration, if one is not
/// explicitly provided.
pub fn default_services_config_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH).join(SERVICE_CONFIG_FILENAME)
}

// Converts a zone and service name into a full configuration directory path.
type ConfigDirGetter = Box<dyn Fn(&str, &str) -> PathBuf + Send + Sync>;

/// Configuration parameters which modify the [`ServiceManager`]'s behavior.
pub struct Config {
    /// Identifies the revision of the sidecar to be used.
    pub sidecar_revision: String,

    /// An optional internet gateway address for external services.
    pub gateway_address: Option<Ipv4Addr>,

    /// The path for the ServiceManager to store information about
    /// all running services.
    pub all_svcs_config_path: PathBuf,

    /// A function which returns the path the directory holding the
    /// service's configuration file.
    pub get_svc_config_dir: ConfigDirGetter,
}

impl Config {
    pub fn new(
        sidecar_revision: String,
        gateway_address: Option<Ipv4Addr>,
    ) -> Self {
        Self {
            sidecar_revision,
            gateway_address,
            all_svcs_config_path: default_services_config_path(),
            get_svc_config_dir: Box::new(|zone_name: &str, svc_name: &str| {
                PathBuf::from(ZONE_ZFS_DATASET_MOUNTPOINT)
                    .join(PathBuf::from(zone_name))
                    .join("root")
                    .join(format!("var/svc/manifest/site/{}", svc_name))
            }),
        }
    }
}

struct Task {
    // A signal for the initializer task to terminate
    exit_tx: oneshot::Sender<()>,
    // A task repeatedly trying to initialize the zone
    initializer: JoinHandle<()>,
}

impl Task {
    async fn stop(self) {
        // If this succeeds, we told the background task to exit
        // successfully. If it fails, the background task already
        // exited.
        let _ = self.exit_tx.send(());
        self.initializer.await.expect("Switch initializer task panicked");
    }
}

// Describes the Switch Zone state.
enum SwitchZone {
    // The switch zone is not currently running.
    Disabled,
    // The Zone is still initializing - it may be awaiting the initialization
    // of certain links.
    Initializing {
        // The request for the zone
        request: ServiceZoneRequest,
        // A background task which keeps looping until the zone is initialized
        worker: Option<Task>,
        // Filesystems for the switch zone to mount
        // Since Softnpu is currently managed via a UNIX socket, we need to
        // pass those files in to the SwitchZone so Dendrite can mmanage Softnpu
        filesystems: Vec<zone::Fs>,
    },
    // The Zone is currently running.
    Running {
        // The original request for the zone
        request: ServiceZoneRequest,
        // The currently running zone
        zone: RunningZone,
    },
}

/// Manages miscellaneous Sled-local services.
pub struct ServiceManagerInner {
    log: Logger,
    switch_zone: Mutex<SwitchZone>,
    scrimlet_override: Option<ScrimletMode>,
    sidecar_revision: String,
    zones: Mutex<Vec<RunningZone>>,
    underlay_vnic_allocator: VnicAllocator<Etherstub>,
    underlay_vnic: EtherstubVnic,
    bootstrap_vnic_allocator: VnicAllocator<Etherstub>,
    ddmd_client: DdmAdminClient,
    advertised_prefixes: Mutex<HashSet<Ipv6Subnet<SLED_PREFIX>>>,
    sled_info: OnceCell<SledAgentInfo>,
    switch_zone_bootstrap_address: Ipv6Addr,
}

// Late-binding information, only known once the sled agent is up and
// operational.
struct SledAgentInfo {
    config: Config,
    physical_link_vnic_allocator: VnicAllocator<PhysicalLink>,
    underlay_address: Ipv6Addr,
    rack_id: Uuid,
}

#[derive(Clone)]
pub struct ServiceManager {
    inner: Arc<ServiceManagerInner>,
}

impl ServiceManager {
    /// Creates a service manager.
    ///
    /// Args:
    /// - `log`: The logger
    /// - `etherstub`: An etherstub on which to allocate VNICs.
    /// - `underlay_vnic`: The underlay's VNIC in the Global Zone.
    /// - `stub_scrimlet`: Identifies how to launch the switch zone.
    pub async fn new(
        log: Logger,
        underlay_etherstub: Etherstub,
        underlay_vnic: EtherstubVnic,
        bootstrap_etherstub: Etherstub,
        scrimlet_override: Option<ScrimletMode>,
        sidecar_revision: String,
        switch_zone_bootstrap_address: Ipv6Addr,
    ) -> Result<Self, Error> {
        debug!(log, "Creating new ServiceManager");
        let log = log.new(o!("component" => "ServiceManager"));
        let mgr = Self {
            inner: Arc::new(ServiceManagerInner {
                log: log.clone(),
                // TODO(https://github.com/oxidecomputer/omicron/issues/725):
                // Load the switch zone if it already exists?
                switch_zone: Mutex::new(SwitchZone::Disabled),
                scrimlet_override,
                sidecar_revision,
                zones: Mutex::new(vec![]),
                underlay_vnic_allocator: VnicAllocator::new(
                    "Service",
                    underlay_etherstub,
                ),
                underlay_vnic,
                bootstrap_vnic_allocator: VnicAllocator::new(
                    "Bootstrap",
                    bootstrap_etherstub,
                ),
                ddmd_client: DdmAdminClient::new(log)?,
                advertised_prefixes: Mutex::new(HashSet::new()),
                sled_info: OnceCell::new(),
                switch_zone_bootstrap_address,
            }),
        };
        Ok(mgr)
    }

    /// Loads services from the services manager, and returns once all requested
    /// services have been started.
    pub async fn sled_agent_started(
        &self,
        config: Config,
        physical_link: PhysicalLink,
        underlay_address: Ipv6Addr,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        self.inner
            .sled_info
            .set(SledAgentInfo {
                config,
                physical_link_vnic_allocator: VnicAllocator::new(
                    "Public",
                    // NOTE: Right now, we only use a connection to one of the Chelsio
                    // links. Longer-term, when we we use OPTE, we'll be able to use both
                    // connections.
                    physical_link,
                ),
                underlay_address,
                rack_id,
            })
            .map_err(|_| "already set".to_string())
            .expect("Sled Agent should only start once");

        let config_path = self.services_config_path()?;
        if config_path.exists() {
            info!(
                &self.inner.log,
                "Sled services found at {}; loading",
                config_path.to_string_lossy()
            );
            let cfg: ServiceEnsureBody = toml::from_str(
                &tokio::fs::read_to_string(&config_path).await.map_err(
                    |err| Error::Io { path: config_path.clone(), err },
                )?,
            )
            .map_err(|err| Error::TomlDeserialize {
                path: config_path.clone(),
                err,
            })?;
            let mut existing_zones = self.inner.zones.lock().await;
            self.initialize_services_locked(&mut existing_zones, &cfg.services)
                .await?;
        } else {
            info!(
                &self.inner.log,
                "No sled services found at {}",
                config_path.to_string_lossy()
            );
        }

        Ok(())
    }

    // Returns either the path to the explicitly provided config path, or
    // chooses the default one.
    fn services_config_path(&self) -> Result<PathBuf, Error> {
        if let Some(info) = self.inner.sled_info.get() {
            Ok(info.config.all_svcs_config_path.clone())
        } else {
            Err(Error::SledAgentNotReady)
        }
    }

    // Advertise the /64 prefix of `address`, unless we already have.
    //
    // This method only blocks long enough to check our HashSet of
    // already-advertised prefixes; the actual request to ddmd to advertise the
    // prefix is spawned onto a background task.
    async fn advertise_prefix_of_address(&self, address: Ipv6Addr) {
        let subnet = Ipv6Subnet::new(address);
        if self.inner.advertised_prefixes.lock().await.insert(subnet) {
            self.inner.ddmd_client.advertise_prefix(subnet);
        }
    }

    // Check the services intended to run in the zone to determine whether any
    // physical devices need to be mapped into the zone when it is created.
    fn devices_needed(req: &ServiceZoneRequest) -> Result<Vec<String>, Error> {
        let mut devices = vec![];
        for svc in &req.services {
            match svc {
                ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic } => {
                    if let Ok(Some(n)) = tofino::get_tofino() {
                        if let Ok(device_path) = n.device_path() {
                            devices.push(device_path);
                            continue;
                        }
                    }
                    return Err(Error::MissingDevice {
                        device: "tofino".to_string(),
                    });
                }
                _ => (),
            }
        }

        for dev in &devices {
            if !Path::new(dev).exists() {
                return Err(Error::MissingDevice { device: dev.to_string() });
            }
        }
        Ok(devices)
    }

    // If we are running in the switch zone, we need a bootstrap vnic so we can
    // listen on a bootstrap address for the wicketd artifact server.
    //
    // No other zone besides the switch and global zones should have a
    // bootstrap address.
    fn bootstrap_vnic_needed(
        &self,
        req: &ServiceZoneRequest,
    ) -> Result<Option<Link>, Error> {
        if req.zone_type == ZoneType::Switch {
            match self.inner.bootstrap_vnic_allocator.new_bootstrap() {
                Ok(link) => Ok(Some(link)),
                Err(e) => Err(Error::SwitchVnicCreation(e)),
            }
        } else {
            Ok(None)
        }
    }

    // Check the services intended to run in the zone to determine whether any
    // physical links or vnics need to be mapped into the zone when it is created.
    //
    // NOTE: This function is implemented to return the first link found, under
    // the assumption that "at most one" would be necessary.
    fn link_needed(
        &self,
        req: &ServiceZoneRequest,
    ) -> Result<Option<Link>, Error> {
        for svc in &req.services {
            match svc {
                ServiceType::Nexus { .. } => {
                    // TODO: Remove once Nexus traffic is transmitted over OPTE.
                    match self
                        .inner
                        .sled_info
                        .get()
                        .ok_or_else(|| Error::SledAgentNotReady)?
                        .physical_link_vnic_allocator
                        .new_control(None)
                    {
                        Ok(n) => {
                            return Ok(Some(n));
                        }
                        Err(e) => {
                            return Err(Error::NexusVnicCreation(e));
                        }
                    }
                }
                ServiceType::Tfport { pkt_source } => {
                    // The tfport service requires a MAC device to/from which sidecar
                    // packets may be multiplexed.  If the link isn't present, don't
                    // bother trying to start the zone.
                    match Dladm::verify_link(pkt_source) {
                        Ok(link) => {
                            return Ok(Some(link));
                        }
                        Err(_) => {
                            return Err(Error::MissingDevice {
                                device: pkt_source.to_string(),
                            });
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(None)
    }

    // Check the services intended to run in the zone to determine whether any
    // additional privileges need to be enabled for the zone.
    fn privs_needed(req: &ServiceZoneRequest) -> Vec<String> {
        let mut needed = Vec::new();
        for svc in &req.services {
            if let ServiceType::Tfport { .. } = svc {
                needed.push("default".to_string());
                needed.push("sys_dl_config".to_string());
            }
        }
        needed
    }

    async fn initialize_zone(
        &self,
        request: &ServiceZoneRequest,
        filesystems: &Vec<zone::Fs>,
    ) -> Result<RunningZone, Error> {
        let device_names = Self::devices_needed(request)?;
        let bootstrap_vnic = self.bootstrap_vnic_needed(request)?;
        let link = self.link_needed(request)?;
        let limit_priv = Self::privs_needed(request);
        let needs_bootstrap_address = bootstrap_vnic.is_some();

        let devices: Vec<zone::Device> = device_names
            .iter()
            .map(|d| zone::Device { name: d.to_string() })
            .collect();

        let installed_zone = InstalledZone::install(
            &self.inner.log,
            &self.inner.underlay_vnic_allocator,
            &request.zone_type.to_string(),
            // unique_name=
            None,
            // dataset=
            &[],
            // filesystems=
            &filesystems,
            &devices,
            // opte_ports=
            vec![],
            bootstrap_vnic,
            link,
            limit_priv,
        )
        .await?;

        let running_zone = RunningZone::boot(installed_zone).await?;

        if needs_bootstrap_address {
            info!(
                self.inner.log,
                "Ensuring bootstrap address {} exists in switch zone",
                self.inner.switch_zone_bootstrap_address.to_string()
            );
            running_zone
                .ensure_bootstrap_address(
                    self.inner.switch_zone_bootstrap_address,
                )
                .await?;
        }

        for addr in &request.addresses {
            if *addr == Ipv6Addr::LOCALHOST {
                continue;
            }
            info!(
                self.inner.log,
                "Ensuring address {} exists",
                addr.to_string()
            );
            let addr_request =
                AddressRequest::new_static(IpAddr::V6(*addr), None);
            running_zone.ensure_address(addr_request).await?;
            info!(
                self.inner.log,
                "Ensuring address {} exists - OK",
                addr.to_string()
            );
        }

        info!(self.inner.log, "GZ addresses: {:#?}", request.gz_addresses);
        for &addr in &request.gz_addresses {
            info!(
                self.inner.log,
                "Ensuring GZ address {} exists",
                addr.to_string()
            );

            let addr_name =
                request.zone_type.to_string().replace(&['-', '_'][..], "");
            Zones::ensure_has_global_zone_v6_address(
                self.inner.underlay_vnic.clone(),
                addr,
                &addr_name,
            )
            .map_err(|err| Error::GzAddress {
                message: format!(
                    "adding address on behalf of service zone '{}'",
                    request.zone_type
                ),
                err,
            })?;

            // If this address is in a new ipv6 prefix, notify maghemite so
            // it can advertise it to other sleds.
            self.advertise_prefix_of_address(addr).await;
        }

        let maybe_gateway = if !request.gz_addresses.is_empty() {
            // If this service supplies its own GZ address, add a route.
            //
            // This is currently being used for the DNS service.
            //
            // TODO: consider limiting the number of GZ addresses which
            // can be supplied - now that we're actively using it, we
            // aren't really handling the "many GZ addresses" case, and it
            // doesn't seem necessary now.
            Some(request.gz_addresses[0])
        } else if let Some(info) = self.inner.sled_info.get() {
            // If the service has not supplied a GZ address, simply add
            // a route to the sled's underlay address.
            Some(info.underlay_address)
        } else {
            // If the underlay doesn't exist, no routing occurs.
            None
        };

        if let Some(gateway) = maybe_gateway {
            running_zone.add_default_route(gateway).await.map_err(|err| {
                Error::ZoneCommand { intent: "Adding Route".to_string(), err }
            })?;
        }

        for service in &request.services {
            // TODO: Related to
            // https://github.com/oxidecomputer/omicron/pull/1124 , should we
            // avoid importing this manifest?
            debug!(self.inner.log, "importing manifest");

            let smfh = SmfHelper::new(&running_zone, service);
            smfh.import_manifest()?;

            match &service {
                ServiceType::Nexus { internal_ip, external_ip } => {
                    info!(self.inner.log, "Setting up Nexus service");

                    let sled_info =
                        if let Some(info) = self.inner.sled_info.get() {
                            info
                        } else {
                            return Err(Error::SledAgentNotReady);
                        };

                    // The address of Nexus' external interface is a special
                    // case; it may be an IPv4 address.
                    let addr_request =
                        AddressRequest::new_static(*external_ip, None);
                    running_zone
                        .ensure_external_address_with_name(
                            addr_request,
                            "public",
                        )
                        .await?;

                    if let IpAddr::V4(_public_addr4) = *external_ip {
                        // If requested, create a default route back through
                        // the internet gateway.
                        if let Some(ref gateway) =
                            sled_info.config.gateway_address
                        {
                            running_zone
                                .add_default_route4(*gateway)
                                .await
                                .map_err(|err| Error::ZoneCommand {
                                    intent: "Adding Route".to_string(),
                                    err,
                                })?;
                        }
                    }

                    // Nexus takes a separate config file for parameters which
                    // cannot be known at packaging time.
                    let deployment_config = NexusDeploymentConfig {
                        id: request.id,
                        rack_id: sled_info.rack_id,

                        dropshot_external: dropshot::ConfigDropshot {
                            bind_address: SocketAddr::new(*external_ip, 80),
                            request_body_max_bytes: 1048576,
                            ..Default::default()
                        },
                        dropshot_internal: dropshot::ConfigDropshot {
                            bind_address: SocketAddr::new(
                                IpAddr::V6(*internal_ip),
                                NEXUS_INTERNAL_PORT,
                            ),
                            request_body_max_bytes: 1048576,
                            ..Default::default()
                        },
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            sled_info.underlay_address,
                        ),
                        database: nexus_config::Database::FromDns,
                    };

                    // Copy the partial config file to the expected location.
                    let config_dir = (sled_info.config.get_svc_config_dir)(
                        running_zone.name(),
                        &request.zone_type.to_string(),
                    );
                    let partial_config_path =
                        config_dir.join(PARTIAL_CONFIG_FILENAME);
                    let config_path = config_dir.join(COMPLETE_CONFIG_FILENAME);
                    tokio::fs::copy(partial_config_path, &config_path)
                        .await
                        .map_err(|err| Error::Io {
                            path: config_path.clone(),
                            err,
                        })?;

                    // Serialize the configuration and append it into the file.
                    let serialized_cfg =
                        toml::Value::try_from(&deployment_config)
                            .expect("Cannot serialize config");
                    let mut map = toml::map::Map::new();
                    map.insert("deployment".to_string(), serialized_cfg);
                    let config_str = toml::to_string(&map).map_err(|err| {
                        Error::TomlSerialize { path: config_path.clone(), err }
                    })?;
                    let mut file = tokio::fs::OpenOptions::new()
                        .append(true)
                        .open(&config_path)
                        .await
                        .map_err(|err| Error::Io {
                            path: config_path.clone(),
                            err,
                        })?;
                    file.write_all(config_str.as_bytes()).await.map_err(
                        |err| Error::Io { path: config_path.clone(), err },
                    )?;
                }
                ServiceType::InternalDns { server_address, dns_address } => {
                    info!(self.inner.log, "Setting up internal-dns service");
                    smfh.setprop(
                        "config/server_address",
                        format!(
                            "[{}]:{}",
                            server_address.ip(),
                            server_address.port(),
                        ),
                    )?;
                    smfh.setprop(
                        "config/dns_address",
                        &format!(
                            "[{}]:{}",
                            dns_address.ip(),
                            dns_address.port(),
                        ),
                    )?;

                    // Refresh the manifest with the new properties we set,
                    // so they become "effective" properties when the service is enabled.
                    smfh.refresh()?;
                }
                ServiceType::Oximeter => {
                    info!(self.inner.log, "Setting up oximeter service");

                    let address = request.addresses[0];
                    smfh.setprop("config/id", request.id)?;
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, OXIMETER_PORT),
                    )?;
                    smfh.refresh()?;
                }
                ServiceType::ManagementGatewayService => {
                    info!(self.inner.log, "Setting up MGS service");
                    smfh.setprop("config/id", request.id)?;

                    // Always tell MGS to listen on localhost so wicketd can
                    // contact it even before we have an underlay network.
                    smfh.addpropvalue(
                        "config/address",
                        &format!("[::1]:{MGS_PORT}"),
                    )?;

                    if let Some(address) = request.addresses.get(0) {
                        smfh.addpropvalue(
                            "config/address",
                            &format!("[{address}]:{MGS_PORT}"),
                        )?;
                    }

                    smfh.refresh()?;
                }
                ServiceType::Wicketd => {
                    info!(self.inner.log, "Setting up wicketd service");

                    smfh.setprop(
                        "config/address",
                        &format!("[::1]:{WICKETD_PORT}"),
                    )?;

                    // TODO: Use bootstrap address
                    smfh.setprop(
                        "config/artifact-address",
                        &format!("[::1]:{BOOTSTRAP_ARTIFACT_PORT}"),
                    )?;

                    smfh.setprop(
                        "config/mgs-address",
                        &format!("[::1]:{MGS_PORT}"),
                    )?;
                    smfh.refresh()?;
                }
                ServiceType::Dendrite { asic } => {
                    info!(self.inner.log, "Setting up dendrite service");

                    smfh.delpropvalue("config/address", "*")?;
                    for address in &request.addresses {
                        smfh.addpropvalue(
                            "config/address",
                            &format!("[{}]:{}", address, DENDRITE_PORT),
                        )?;
                    }
                    match asic {
                        DendriteAsic::TofinoAsic => {
                            // There should be exactly one device_name
                            // associated with this zone: the /dev path for
                            // the tofino ASIC.
                            let dev_cnt = device_names.len();
                            if dev_cnt == 1 {
                                smfh.setprop(
                                    "config/dev_path",
                                    device_names[0].clone(),
                                )?;
                            } else {
                                return Err(Error::SwitchZone(
                                    anyhow::anyhow!(
                                    "{dev_cnt} devices needed for tofino asic"
                                ),
                                ));
                            }
                            smfh.setprop(
                                "config/port_config",
                                "/opt/oxide/dendrite/misc/sidecar_config.toml",
                            )?;
                            smfh.setprop(
                                "config/board_rev",
                                &self.inner.sidecar_revision,
                            )?;
                        }
                        DendriteAsic::TofinoStub => smfh.setprop(
                            "config/port_config",
                            "/opt/oxide/dendrite/misc/model_config.toml",
                        )?,
                        DendriteAsic::Softnpu => {
                            smfh.setprop("config/mgmt", "uds")?;
                            smfh.setprop(
                                "config/uds_path",
                                "/opt/softnpu/stuff",
                            )?;
                        }
                    };
                    smfh.refresh()?;
                }
                ServiceType::Tfport { pkt_source } => {
                    info!(self.inner.log, "Setting up tfport service");

                    smfh.setprop("config/pkt_source", pkt_source)?;
                    smfh.setprop(
                        "config/host",
                        &format!("[{}]", Ipv6Addr::LOCALHOST),
                    )?;
                    smfh.setprop("config/port", &format!("{}", DENDRITE_PORT))?;
                    smfh.refresh()?;
                }
                ServiceType::CruciblePantry => {
                    info!(self.inner.log, "Setting up Crucible pantry service");

                    if let Some(address) = request.addresses.get(0) {
                        smfh.setprop(
                            "config/listen",
                            &format!("[{}]:{}", address, CRUCIBLE_PANTRY_PORT),
                        )?;
                    }
                    smfh.refresh()?;
                }
            }

            debug!(self.inner.log, "enabling service");
            smfh.enable()?;
        }
        Ok(running_zone)
    }

    // Populates `existing_zones` according to the requests in `services`.
    //
    // At the point this function is invoked, IP addresses have already been
    // allocated (by either RSS or Nexus). However, this function explicitly
    // assigns such addresses to interfaces within zones.
    async fn initialize_services_locked(
        &self,
        existing_zones: &mut Vec<RunningZone>,
        requests: &Vec<ServiceZoneRequest>,
    ) -> Result<(), Error> {
        // TODO(https://github.com/oxidecomputer/omicron/issues/726):
        // As long as we ensure the requests don't overlap, we could
        // parallelize this request.
        for req in requests {
            info!(
                self.inner.log,
                "Ensuring service zone is initialized: {:?}", req.zone_type
            );
            // Before we bother allocating anything for this request, check if
            // this service has already been created.
            let expected_zone_name =
                InstalledZone::get_zone_name(&req.zone_type.to_string(), None);
            if existing_zones.iter().any(|z| z.name() == expected_zone_name) {
                info!(
                    self.inner.log,
                    "Service zone {} already exists", req.zone_type
                );
                continue;
            } else {
                info!(
                    self.inner.log,
                    "Service zone {} does not yet exist", req.zone_type
                );
            }

            let running_zone = self
                .initialize_zone(
                    req,
                    // filesystems=
                    &vec![],
                )
                .await?;
            existing_zones.push(running_zone);
        }
        Ok(())
    }

    /// Ensures that particular services should be initialized.
    ///
    /// These services will be instantiated by this function, will be recorded
    /// to a local file to ensure they start automatically on next boot.
    pub async fn ensure_persistent(
        &self,
        request: ServiceEnsureBody,
    ) -> Result<(), Error> {
        let mut existing_zones = self.inner.zones.lock().await;
        let config_path = self.services_config_path()?;

        let services_to_initialize = {
            if config_path.exists() {
                let cfg: ServiceEnsureBody = toml::from_str(
                    &tokio::fs::read_to_string(&config_path).await.map_err(
                        |err| Error::Io { path: config_path.clone(), err },
                    )?,
                )
                .map_err(|err| Error::TomlDeserialize {
                    path: config_path.clone(),
                    err,
                })?;
                let known_services = cfg.services;

                let known_set: HashSet<&ServiceZoneRequest> =
                    HashSet::from_iter(known_services.iter());
                let requested_set = HashSet::from_iter(request.services.iter());

                if !requested_set.is_superset(&known_set) {
                    // The caller may only request services additively.
                    //
                    // We may want to use a different mechanism for zone removal, in
                    // the case of changing configurations, rather than just doing
                    // that removal implicitly.
                    warn!(
                        self.inner.log,
                        "Cannot request services on this sled, differing configurations: {:#?}",
                        known_set.symmetric_difference(&requested_set)
                    );
                    return Err(Error::ServicesAlreadyConfigured);
                }
                requested_set
                    .difference(&known_set)
                    .map(|s| (*s).clone())
                    .collect::<Vec<ServiceZoneRequest>>()
            } else {
                request.services.clone()
            }
        };

        self.initialize_services_locked(
            &mut existing_zones,
            &services_to_initialize,
        )
        .await?;

        let serialized_services = toml::Value::try_from(&request)
            .expect("Cannot serialize service list");
        let services_str =
            toml::to_string(&serialized_services).map_err(|err| {
                Error::TomlSerialize { path: config_path.clone(), err }
            })?;
        tokio::fs::write(&config_path, services_str)
            .await
            .map_err(|err| Error::Io { path: config_path.clone(), err })?;

        Ok(())
    }

    /// Ensures that a switch zone exists with the provided IP adddress.
    pub async fn activate_switch(
        &self,
        switch_zone_ip: Option<Ipv6Addr>,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ensuring scrimlet services (enabling services)");
        let mut filesystems: Vec<zone::Fs> = vec![];

        let services = match self.inner.scrimlet_override {
            Some(mode) => match mode {
                ScrimletMode::Stub => {
                    vec![
                        ServiceType::Dendrite {
                            asic: DendriteAsic::TofinoStub,
                        },
                        ServiceType::ManagementGatewayService,
                        ServiceType::Wicketd,
                    ]
                }
                ScrimletMode::Softnpu => {
                    let softnpu_filesystem = zone::Fs {
                        ty: "lofs".to_string(),
                        dir: "/opt/softnpu/stuff".to_string(),
                        special: "/opt/oxide/softnpu/stuff".to_string(),
                        ..Default::default()
                    };
                    filesystems.push(softnpu_filesystem);
                    vec![
                        ServiceType::Dendrite { asic: DendriteAsic::Softnpu },
                        ServiceType::ManagementGatewayService,
                        ServiceType::Wicketd,
                    ]
                }
                _ => {
                    vec![]
                }
            },
            None => {
                vec![
                    ServiceType::ManagementGatewayService,
                    ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic },
                    ServiceType::Tfport { pkt_source: "tfpkt0".to_string() },
                    ServiceType::Wicketd,
                ]
            }
        };

        let mut addresses =
            if let Some(ip) = switch_zone_ip { vec![ip] } else { vec![] };
        addresses.push(Ipv6Addr::LOCALHOST);

        let request = ServiceZoneRequest {
            id: Uuid::new_v4(),
            zone_type: ZoneType::Switch,
            addresses,
            gz_addresses: vec![],
            services,
        };

        self.ensure_switch_zone(
            // request=
            Some(request),
            // filesystems=
            filesystems,
        )
        .await
    }

    /// Ensures that no switch zone is active.
    pub async fn deactivate_switch(&self) -> Result<(), Error> {
        self.ensure_switch_zone(
            // request=
            None,
            // filesystems=
            vec![],
        )
        .await
    }

    // Forcefully initialize a switch zone.
    //
    // This is a helper function for "ensure_switch_zone".
    fn start_switch_zone(
        self,
        switch_zone: &mut SwitchZone,
        request: ServiceZoneRequest,
        filesystems: Vec<zone::Fs>,
    ) {
        let (exit_tx, exit_rx) = oneshot::channel();
        *switch_zone = SwitchZone::Initializing {
            request,
            filesystems,
            worker: Some(Task {
                exit_tx,
                initializer: tokio::task::spawn(async move {
                    self.initialize_zone_loop(exit_rx).await
                }),
            }),
        };
    }

    // Moves the current state to align with the "request".
    async fn ensure_switch_zone(
        &self,
        request: Option<ServiceZoneRequest>,
        filesystems: Vec<zone::Fs>,
    ) -> Result<(), Error> {
        let log = &self.inner.log;
        let mut switch_zone = self.inner.switch_zone.lock().await;

        match (&mut *switch_zone, request) {
            (SwitchZone::Disabled, Some(request)) => {
                info!(log, "Enabling switch zone (new)");
                self.clone().start_switch_zone(
                    &mut switch_zone,
                    request,
                    filesystems,
                );
            }
            (SwitchZone::Initializing { request, .. }, Some(new_request)) => {
                info!(log, "Enabling switch zone (already underway)");
                // The switch zone has not started yet -- we can simply replace
                // the next request with our new request.
                *request = new_request;
            }
            (SwitchZone::Running { request, zone }, Some(new_request))
                if request.addresses != new_request.addresses =>
            {
                info!(log, "Re-enabling running switch zone (new address)");
                *request = new_request;

                let address = request
                    .addresses
                    .get(0)
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|| "".to_string());

                for service in &request.services {
                    let smfh = SmfHelper::new(&zone, service);

                    match service {
                        ServiceType::ManagementGatewayService => {
                            // Remove any existing `config/address` values
                            // without deleting the property itself.
                            smfh.delpropvalue("config/address", "*")?;

                            // Restore the localhost address that we always add
                            // when setting up MGS.
                            smfh.addpropvalue(
                                "config/address",
                                &format!("[::1]:{MGS_PORT}"),
                            )?;

                            // Add the underlay address.
                            smfh.setprop(
                                "config/address",
                                &format!("[{address}]:{MGS_PORT}"),
                            )?;

                            smfh.refresh()?;
                        }
                        ServiceType::Dendrite { .. } => {
                            smfh.delpropvalue("config/address", "*")?;
                            for address in &request.addresses {
                                smfh.addpropvalue(
                                    "config/address",
                                    &format!("[{}]:{}", address, DENDRITE_PORT),
                                )?;
                            }
                            smfh.refresh()?;
                        }
                        ServiceType::Tfport { .. } => {
                            // Since tfport and dpd communicate using localhost,
                            // the tfport service shouldn't need to be restarted.
                        }
                        _ => (),
                    }
                }
            }
            (SwitchZone::Running { .. }, Some(_)) => {
                info!(log, "Enabling switch zone (already complete)");
            }
            (SwitchZone::Disabled, None) => {
                info!(log, "Disabling switch zone (already complete)");
            }
            (SwitchZone::Initializing { worker, .. }, None) => {
                info!(log, "Disabling switch zone (was initializing)");
                worker.take().unwrap().stop().await;
                *switch_zone = SwitchZone::Disabled;
            }
            (SwitchZone::Running { zone, .. }, None) => {
                info!(log, "Disabling switch zone (was running)");
                let _ = zone.stop().await;
                *switch_zone = SwitchZone::Disabled;
            }
        }
        Ok(())
    }

    // Body of a tokio task responsible for running until the switch zone is
    // inititalized, or it has been told to stop.
    async fn initialize_zone_loop(&self, mut exit_rx: oneshot::Receiver<()>) {
        loop {
            {
                let mut switch_zone = self.inner.switch_zone.lock().await;
                match &*switch_zone {
                    SwitchZone::Initializing {
                        request, filesystems, ..
                    } => {
                        match self.initialize_zone(&request, filesystems).await
                        {
                            Ok(zone) => {
                                *switch_zone = SwitchZone::Running {
                                    request: request.clone(),
                                    zone,
                                };
                                return;
                            }
                            Err(err) => {
                                warn!(
                                    self.inner.log,
                                    "Failed to initialize switch zone: {err}"
                                );
                            }
                        }
                    }
                    _ => return,
                }
            }

            tokio::select! {
                // If we've been told to stop trying, bail.
                _ = &mut exit_rx => return,

                // Poll for the device every second - this timeout is somewhat
                // arbitrary, but we probably don't want to use backoff here.
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => (),
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::params::ZoneType;
    use illumos_utils::{
        dladm::{
            Etherstub, MockDladm, BOOTSTRAP_ETHERSTUB_NAME,
            UNDERLAY_ETHERSTUB_NAME, UNDERLAY_ETHERSTUB_VNIC_NAME,
        },
        svc,
        zone::MockZones,
    };
    use std::net::Ipv6Addr;
    use std::os::unix::process::ExitStatusExt;
    use uuid::Uuid;

    // Just a placeholder. Not used.
    const SWITCH_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;

    const EXPECTED_ZONE_NAME: &str = "oxz_oximeter";

    // Returns the expectations for a new service to be created.
    fn expect_new_service() -> Vec<Box<dyn std::any::Any>> {
        // Create a VNIC
        let create_vnic_ctx = MockDladm::create_vnic_context();
        create_vnic_ctx.expect().return_once(
            |physical_link: &Etherstub, _, _, _| {
                assert_eq!(&physical_link.0, &UNDERLAY_ETHERSTUB_NAME);
                Ok(())
            },
        );
        // Install the Omicron Zone
        let install_ctx = MockZones::install_omicron_zone_context();
        install_ctx.expect().return_once(|_, name, _, _, _, _, _, _| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        // Boot the zone
        let boot_ctx = MockZones::boot_context();
        boot_ctx.expect().return_once(|name| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });

        // Ensure the address exists
        let ensure_address_ctx = MockZones::ensure_address_context();
        ensure_address_ctx.expect().return_once(|_, _, _| {
            Ok(ipnetwork::IpNetwork::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 64)
                .unwrap())
        });

        // Wait for the networking service.
        let wait_ctx = svc::wait_for_service_context();
        wait_ctx.expect().return_once(|_, _| Ok(()));
        // Import the manifest, enable the service
        let execute_ctx = illumos_utils::execute_context();
        execute_ctx.expect().times(..).returning(|_| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: vec![],
                stderr: vec![],
            })
        });

        vec![
            Box::new(create_vnic_ctx),
            Box::new(install_ctx),
            Box::new(boot_ctx),
            Box::new(ensure_address_ctx),
            Box::new(wait_ctx),
            Box::new(execute_ctx),
        ]
    }

    // Prepare to call "ensure" for a new service, then actually call "ensure".
    async fn ensure_new_service(mgr: &ServiceManager, id: Uuid) {
        let _expectations = expect_new_service();

        mgr.ensure_persistent(ServiceEnsureBody {
            services: vec![ServiceZoneRequest {
                id,
                zone_type: ZoneType::Oximeter,
                addresses: vec![Ipv6Addr::LOCALHOST],
                gz_addresses: vec![],
                services: vec![ServiceType::Oximeter],
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to call "ensure" for a service which already exists. We should
    // return the service without actually installing a new zone.
    async fn ensure_existing_service(mgr: &ServiceManager, id: Uuid) {
        mgr.ensure_persistent(ServiceEnsureBody {
            services: vec![ServiceZoneRequest {
                id,
                zone_type: ZoneType::Oximeter,
                addresses: vec![Ipv6Addr::LOCALHOST],
                gz_addresses: vec![],
                services: vec![ServiceType::Oximeter],
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to drop the service manager.
    //
    // This will shut down all allocated zones, and delete their
    // associated VNICs.
    fn drop_service_manager(mgr: ServiceManager) {
        let halt_ctx = MockZones::halt_and_remove_logged_context();
        halt_ctx.expect().returning(|_, name| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        let delete_vnic_ctx = MockDladm::delete_vnic_context();
        delete_vnic_ctx.expect().returning(|_| Ok(()));

        // Explicitly drop the servie manager
        drop(mgr);
    }

    struct TestConfig {
        config_dir: tempfile::TempDir,
    }

    impl TestConfig {
        async fn new() -> Self {
            let config_dir = tempfile::TempDir::new().unwrap();
            tokio::fs::File::create(
                config_dir.path().join(PARTIAL_CONFIG_FILENAME),
            )
            .await
            .unwrap();
            Self { config_dir }
        }

        fn make_config(&self) -> Config {
            let all_svcs_config_path =
                self.config_dir.path().join(SERVICE_CONFIG_FILENAME);
            let svc_config_dir = self.config_dir.path().to_path_buf();
            Config {
                sidecar_revision: "rev_whatever_its_a_test".to_string(),
                gateway_address: None,
                all_svcs_config_path,
                get_svc_config_dir: Box::new(
                    move |_zone_name: &str, _svc_name: &str| {
                        svc_config_dir.clone()
                    },
                ),
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_ensure_service");
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        let mgr = ServiceManager::new(
            log,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            None,
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
        )
        .await
        .unwrap();

        mgr.sled_agent_started(
            test_config.make_config(),
            PhysicalLink("link".to_string()),
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service_which_already_exists() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_ensure_service_which_already_exists",
        );
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        let mgr = ServiceManager::new(
            log,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            None,
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
        )
        .await
        .unwrap();

        mgr.sled_agent_started(
            test_config.make_config(),
            PhysicalLink("link".to_string()),
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        ensure_existing_service(&mgr, id).await;
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_services_are_recreated_on_reboot() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_are_recreated_on_reboot",
        );
        let test_config = TestConfig::new().await;

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            None,
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
        )
        .await
        .unwrap();

        mgr.sled_agent_started(
            test_config.make_config(),
            PhysicalLink("link".to_string()),
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        drop_service_manager(mgr);

        // Before we re-create the service manager - notably, using the same
        // config file! - expect that a service gets initialized.
        let _expectations = expect_new_service();
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            None,
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
        )
        .await
        .unwrap();

        mgr.sled_agent_started(
            test_config.make_config(),
            PhysicalLink("link".to_string()),
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_services_do_not_persist_without_config() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_do_not_persist_without_config",
        );
        let test_config = TestConfig::new().await;

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            None,
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
        )
        .await
        .unwrap();
        mgr.sled_agent_started(
            test_config.make_config(),
            PhysicalLink("link".to_string()),
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        drop_service_manager(mgr);

        // Next, delete the config. This means the service we just created will
        // not be remembered on the next initialization.
        let config = test_config.make_config();
        std::fs::remove_file(&config.all_svcs_config_path).unwrap();

        // Observe that the old service is not re-initialized.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            None,
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
        )
        .await
        .unwrap();
        mgr.sled_agent_started(
            test_config.make_config(),
            PhysicalLink("link".to_string()),
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }
}
