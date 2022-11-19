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
//! - [ServiceManager::ensure_switch] exposes an API to specifically enable
//! or disable the switch zone.

use crate::bootstrap::ddm_admin_client::{DdmAdminClient, DdmError};
use crate::common::underlay;
use crate::illumos::dladm::{Dladm, Etherstub, EtherstubVnic, PhysicalLink};
use crate::illumos::link::{Link, VnicAllocator};
use crate::illumos::running_zone::{InstalledZone, RunningZone};
use crate::illumos::zfs::ZONE_ZFS_DATASET_MOUNTPOINT;
use crate::illumos::zone::AddressRequest;
use crate::params::{
    DendriteAsic, ServiceEnsureBody, ServiceType, ServiceZoneRequest,
};
use crate::smf_helper::SmfHelper;
use crate::zone::Zones;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::OXIMETER_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::nexus_config::{
    self, DeploymentConfig as NexusDeploymentConfig,
};
use omicron_common::postgres_config::PostgresConfigWithUrl;
use slog::Logger;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
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

    #[error("Failed to issue SMF command: {0}")]
    SmfCommand(#[from] crate::smf_helper::Error),

    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: crate::illumos::running_zone::RunCommandError,
    },

    #[error("Failed to boot zone: {0}")]
    ZoneBoot(#[from] crate::illumos::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] crate::illumos::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] crate::illumos::running_zone::InstallZoneError),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Failed to access underlay device: {0}")]
    Underlay(#[from] underlay::Error),

    #[error("Failed to create Vnic for Nexus: {0}")]
    NexusVnicCreation(crate::illumos::dladm::CreateVnicError),

    #[error("Failed to add GZ addresses: {message}: {err}")]
    GzAddress {
        message: String,
        err: crate::illumos::zone::EnsureGzAddressError,
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
    /// An optional internet gateway address for external services.
    pub gateway_address: Option<Ipv4Addr>,

    /// The path for the ServiceManager to store information about
    /// all running services.
    pub all_svcs_config_path: PathBuf,

    /// A function which returns the path the directory holding the
    /// service's configuration file.
    pub get_svc_config_dir: ConfigDirGetter,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gateway_address: None,
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

/// Manages miscellaneous Sled-local services.
pub struct ServiceManagerInner {
    log: Logger,
    config: Config,
    zones: Mutex<Vec<RunningZone>>,
    vnic_allocator: VnicAllocator<Etherstub>,
    physical_link_vnic_allocator: VnicAllocator<PhysicalLink>,
    underlay_vnic: EtherstubVnic,
    underlay_address: Ipv6Addr,
    rack_id: Uuid,
    ddmd_client: DdmAdminClient,
    advertised_prefixes: Mutex<HashSet<Ipv6Subnet<SLED_PREFIX>>>,
}

#[derive(Clone)]
pub struct ServiceManager {
    inner: Arc<ServiceManagerInner>,
}

impl ServiceManager {
    /// Creates a service manager, which returns once all requested services
    /// have been started.
    ///
    /// Args:
    /// - `log`: The logger
    /// - `etherstub`: An etherstub on which to allocate VNICs.
    /// - `underlay_vnic`: The underlay's VNIC in the Global Zone.
    /// - `config_path`: An optional path to a configuration file to store
    /// the record of services. By default, [`default_services_config_path`]
    /// is used.
    pub async fn new(
        log: Logger,
        etherstub: Etherstub,
        underlay_vnic: EtherstubVnic,
        underlay_address: Ipv6Addr,
        config: Config,
        physical_link: PhysicalLink,
        rack_id: Uuid,
    ) -> Result<Self, Error> {
        debug!(log, "Creating new ServiceManager");
        let log = log.new(o!("component" => "ServiceManager"));
        let mgr = Self {
            inner: Arc::new(ServiceManagerInner {
                log: log.clone(),
                config,
                zones: Mutex::new(vec![]),
                vnic_allocator: VnicAllocator::new("Service", etherstub),
                physical_link_vnic_allocator: VnicAllocator::new(
                    "Public",
                    // NOTE: Right now, we only use a connection to one of the Chelsio
                    // links. Longer-term, when we we use OPTE, we'll be able to use both
                    // connections.
                    physical_link,
                ),
                underlay_vnic,
                underlay_address,
                rack_id,
                ddmd_client: DdmAdminClient::new(log)?,
                advertised_prefixes: Mutex::new(HashSet::new()),
            }),
        };

        let config_path = mgr.services_config_path();
        if config_path.exists() {
            info!(
                &mgr.inner.log,
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
            let mut existing_zones = mgr.inner.zones.lock().await;
            mgr.initialize_services_locked(&mut existing_zones, &cfg.services)
                .await?;
        } else {
            info!(
                &mgr.inner.log,
                "No sled services found at {}",
                config_path.to_string_lossy()
            );
        }

        Ok(mgr)
    }

    // Returns either the path to the explicitly provided config path, or
    // chooses the default one.
    fn services_config_path(&self) -> PathBuf {
        self.inner.config.all_svcs_config_path.clone()
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
    fn devices_needed(
        req: &ServiceZoneRequest,
    ) -> Result<Vec<String>, Error> {
        let mut devices = vec![];
        for svc in &req.services {
            match svc {
                ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic } => {
                    // When running on a real sidecar, we need the /dev/tofino
                    // device to talk to the tofino ASIC.
                    devices.push("/dev/tofino".to_string());
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
                ServiceType::Nexus { .. } => {
                    // TODO: Remove once Nexus traffic is transmitted over OPTE.
                    match self
                        .inner
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
    ) -> Result<RunningZone, Error> {
        let device_names = Self::devices_needed(request)?;
        let link = self.link_needed(request)?;
        let limit_priv = Self::privs_needed(request);

        let devices: Vec<zone::Device> = device_names
            .iter()
            .map(|d| zone::Device { name: d.to_string() })
            .collect();

        let installed_zone = InstalledZone::install(
            &self.inner.log,
            &self.inner.vnic_allocator,
            &request.zone_type.to_string(),
            // unique_name=
            None,
            // dataset=
            &[],
            &devices,
            // opte_ports=
            vec![],
            link,
            limit_priv,
        )
        .await?;

        let running_zone = RunningZone::boot(installed_zone).await?;

        for addr in &request.addresses {
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

        let gateway = if !request.gz_addresses.is_empty() {
            // If this service supplies its own GZ address, add a route.
            //
            // This is currently being used for the DNS service.
            //
            // TODO: consider limiting the number of GZ addresses which
            // can be supplied - now that we're actively using it, we
            // aren't really handling the "many GZ addresses" case, and it
            // doesn't seem necessary now.
            request.gz_addresses[0]
        } else {
            self.inner.underlay_address
        };

        running_zone.add_default_route(gateway).await.map_err(|err| {
            Error::ZoneCommand { intent: "Adding Route".to_string(), err }
        })?;

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
                            self.inner.config.gateway_address
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

                    let cert_file = PathBuf::from("/var/nexus/certs/cert.pem");
                    let key_file = PathBuf::from("/var/nexus/certs/key.pem");

                    // Nexus takes a separate config file for parameters which
                    // cannot be known at packaging time.
                    let deployment_config = NexusDeploymentConfig {
                        id: request.id,
                        rack_id: self.inner.rack_id,

                        // Request two dropshot servers: One for HTTP (port 80),
                        // one for HTTPS (port 443).
                        dropshot_external: vec![
                            dropshot::ConfigDropshot {
                                bind_address: SocketAddr::new(*external_ip, 443),
                                request_body_max_bytes: 1048576,
                                tls: Some(dropshot::ConfigTls { cert_file, key_file }),
                            },
                            dropshot::ConfigDropshot {
                                bind_address: SocketAddr::new(*external_ip, 80),
                                request_body_max_bytes: 1048576,
                                ..Default::default()
                            },
                        ],
                        dropshot_internal: dropshot::ConfigDropshot {
                            bind_address: SocketAddr::new(IpAddr::V6(*internal_ip), NEXUS_INTERNAL_PORT),
                            request_body_max_bytes: 1048576,
                            ..Default::default()
                        },
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            self.inner.underlay_address,
                        ),
                        // TODO: Switch to inferring this URL by DNS.
                        database: nexus_config::Database::FromUrl {
                            url: PostgresConfigWithUrl::from_str(
                                "postgresql://root@[fd00:1122:3344:0101::3]:32221/omicron?sslmode=disable"
                            ).unwrap(),
                        }
                    };

                    // Copy the partial config file to the expected location.
                    let config_dir = (self.inner.config.get_svc_config_dir)(
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

                    let address = request.addresses[0];
                    smfh.setprop("config/id", request.id)?;
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, MGS_PORT),
                    )?;
                    smfh.refresh()?;
                }
                ServiceType::Dendrite { asic } => {
                    info!(self.inner.log, "Setting up dendrite service");

                    let address = request.addresses[0];
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, DENDRITE_PORT,),
                    )?;
                    match *asic {
                        DendriteAsic::TofinoAsic => smfh.setprop(
                            "config/port_config",
                            "/opt/oxide/dendrite/misc/sidecar_config.toml",
                        )?,
                        DendriteAsic::TofinoStub => smfh.setprop(
                            "config/port_config",
                            "/opt/oxide/dendrite/misc/model_config.toml",
                        )?,
                        DendriteAsic::Softnpu => {}
                    };
                    smfh.refresh()?;
                }
                ServiceType::Tfport { pkt_source } => {
                    info!(self.inner.log, "Setting up tfport service");

                    smfh.setprop("config/pkt_source", pkt_source)?;
                    let address = request.addresses[0];
                    smfh.setprop("config/host", &format!("[{}]", address))?;
                    smfh.setprop("config/port", &format!("{}", DENDRITE_PORT))?;
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

            let running_zone = self.initialize_zone(req).await?;
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
        let config_path = self.services_config_path();

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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::illumos::{
        dladm::{Etherstub, MockDladm, ETHERSTUB_NAME, ETHERSTUB_VNIC_NAME},
        svc,
        zone::MockZones,
    };
    use crate::params::ZoneType;
    use std::net::Ipv6Addr;
    use std::os::unix::process::ExitStatusExt;
    use uuid::Uuid;

    const EXPECTED_ZONE_NAME: &str = "oxz_oximeter";

    // Returns the expectations for a new service to be created.
    fn expect_new_service() -> Vec<Box<dyn std::any::Any>> {
        // Create a VNIC
        let create_vnic_ctx = MockDladm::create_vnic_context();
        create_vnic_ctx.expect().return_once(
            |physical_link: &Etherstub, _, _, _| {
                assert_eq!(&physical_link.0, &ETHERSTUB_NAME);
                Ok(())
            },
        );
        // Install the Omicron Zone
        let install_ctx = MockZones::install_omicron_zone_context();
        install_ctx.expect().return_once(|_, name, _, _, _, _, _| {
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
        let execute_ctx = crate::illumos::execute_context();
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
                all_svcs_config_path,
                get_svc_config_dir: Box::new(
                    move |_zone_name: &str, _svc_name: &str| {
                        svc_config_dir.clone()
                    },
                ),
                ..Default::default()
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
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            test_config.make_config(),
            PhysicalLink("link".to_string()),
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
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            test_config.make_config(),
            PhysicalLink("link".to_string()),
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
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            test_config.make_config(),
            PhysicalLink("link".to_string()),
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
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            test_config.make_config(),
            PhysicalLink("link".to_string()),
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
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            test_config.make_config(),
            PhysicalLink("link".to_string()),
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
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            config,
            PhysicalLink("link".to_string()),
            Uuid::new_v4(),
        )
        .await
        .unwrap();
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }
}
