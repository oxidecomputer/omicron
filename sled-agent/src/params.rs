// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::internal::nexus::{
    DiskRuntimeState, InstanceRuntimeState,
};
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use thiserror::Error;
use uuid::Uuid;

pub use illumos_utils::opte::params::VpcFirewallRule;
pub use illumos_utils::opte::params::VpcFirewallRulesEnsureBody;
pub use sled_hardware::DendriteAsic;

/// Used to request a Disk state change
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase", tag = "state", content = "instance")]
pub enum DiskStateRequested {
    Detached,
    Attached(Uuid),
    Destroyed,
    Faulted,
}

impl DiskStateRequested {
    /// Returns whether the requested state is attached to an Instance or not.
    pub fn is_attached(&self) -> bool {
        match self {
            DiskStateRequested::Detached => false,
            DiskStateRequested::Destroyed => false,
            DiskStateRequested::Faulted => false,

            DiskStateRequested::Attached(_) => true,
        }
    }
}

/// Sent from to a sled agent to establish the runtime state of a Disk
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DiskEnsureBody {
    /// Last runtime state of the Disk known to Nexus (used if the agent has
    /// never seen this Disk before).
    pub initial_runtime: DiskRuntimeState,
    /// requested runtime state of the Disk
    pub target: DiskStateRequested,
}

/// Describes the instance hardware.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceHardware {
    pub runtime: InstanceRuntimeState,
    pub nics: Vec<NetworkInterface>,
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub external_ips: Vec<IpAddr>,
    pub firewall_rules: Vec<VpcFirewallRule>,
    // TODO: replace `propolis_client::handmade::*` with locally-modeled request type
    pub disks: Vec<propolis_client::handmade::api::DiskRequest>,
    pub cloud_init_bytes: Option<String>,
}

/// The body of a request to ensure that an instance is known to a sled agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// A description of the instance's virtual hardware and the initial runtime
    /// state this sled agent should store for this incarnation of the instance.
    pub initial: InstanceHardware,
}

/// The body of a request to move a previously-ensured instance into a specific
/// runtime state.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstancePutStateBody {
    /// The state into which the instance should be driven.
    pub state: InstanceStateRequested,
}

/// The response sent from a request to move an instance into a specific runtime
/// state.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstancePutStateResponse {
    /// The current runtime state of the instance after handling the request to
    /// change its state. If the instance's state did not change, this field is
    /// `None`.
    pub updated_runtime: Option<InstanceRuntimeState>,
}

/// The response sent from a request to unregister an instance.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceUnregisterResponse {
    /// The current state of the instance after handling the request to
    /// unregister it. If the instance's state did not change, this field is
    /// `None`.
    pub updated_runtime: Option<InstanceRuntimeState>,
}

/// Parameters used when directing Propolis to initialize itself via live
/// migration.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrationTargetParams {
    /// The Propolis ID of the migration source.
    pub src_propolis_id: Uuid,

    /// The address of the Propolis server that will serve as the migration
    /// source.
    pub src_propolis_addr: SocketAddr,
}

/// Requestable running state of an Instance.
///
/// A subset of [`omicron_common::api::external::InstanceState`].
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum InstanceStateRequested {
    /// Run this instance by migrating in from a previous running incarnation of
    /// the instance.
    MigrationTarget(InstanceMigrationTargetParams),
    /// Start the instance if it is not already running.
    Running,
    /// Stop the instance.
    Stopped,
    /// Immediately reset the instance, as though it had stopped and immediately
    /// began to run again.
    Reboot,
}

impl Display for InstanceStateRequested {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

impl InstanceStateRequested {
    fn label(&self) -> &str {
        match self {
            InstanceStateRequested::MigrationTarget(_) => "migrating in",
            InstanceStateRequested::Running => "running",
            InstanceStateRequested::Stopped => "stopped",
            InstanceStateRequested::Reboot => "reboot",
        }
    }

    /// Returns true if the state represents a stopped Instance.
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceStateRequested::MigrationTarget(_) => false,
            InstanceStateRequested::Running => false,
            InstanceStateRequested::Stopped => true,
            InstanceStateRequested::Reboot => false,
        }
    }
}

/// Instance runtime state to update for a migration.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrationSourceParams {
    pub migration_id: Uuid,
    pub dst_propolis_id: Uuid,
}

/// The body of a request to set or clear the migration identifiers from a
/// sled agent's instance state records.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstancePutMigrationIdsBody {
    /// The last runtime state known to this requestor. This request will
    /// succeed if either (a) the Propolis generation in the sled agent's
    /// runtime state matches the generation in this record, or (b) the sled
    /// agent's runtime state matches what would result from applying this
    /// request to the caller's runtime state. This latter condition provides
    /// idempotency.
    pub old_runtime: InstanceRuntimeState,

    /// The migration identifiers to set. If `None`, this operation clears the
    /// migration IDs.
    pub migration_params: Option<InstanceMigrationSourceParams>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum DiskType {
    U2,
    M2,
}

impl From<sled_hardware::DiskVariant> for DiskType {
    fn from(v: sled_hardware::DiskVariant) -> Self {
        use sled_hardware::DiskVariant::*;
        match v {
            U2 => Self::U2,
            M2 => Self::M2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Zpool {
    pub id: Uuid,
    pub disk_type: DiskType,
}

/// The type of a dataset, and an auxiliary information necessary
/// to successfully launch a zone managing the associated data.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetKind {
    CockroachDb,
    Crucible,
    Clickhouse,
}

impl DatasetKind {
    /// Returns the type of the zone which manages this dataset.
    pub fn zone_type(&self) -> ZoneType {
        match *self {
            DatasetKind::CockroachDb => ZoneType::CockroachDb,
            DatasetKind::Crucible => ZoneType::Crucible,
            DatasetKind::Clickhouse => ZoneType::Clickhouse,
        }
    }

    /// Returns the service type which runs in the zone managing this dataset.
    ///
    /// NOTE: This interface is only viable because datasets run a single
    /// service in their zone. If that precondition is no longer true, this
    /// interface should be re-visited.
    pub fn service_type(&self) -> ServiceType {
        match *self {
            DatasetKind::CockroachDb => ServiceType::CockroachDb,
            DatasetKind::Crucible => ServiceType::Crucible,
            DatasetKind::Clickhouse => ServiceType::Clickhouse,
        }
    }
}

impl From<DatasetKind> for sled_agent_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb => Self::CockroachDb,
            Crucible => Self::Crucible,
            Clickhouse => Self::Clickhouse,
        }
    }
}

impl From<DatasetKind> for nexus_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb { .. } => Self::Cockroach,
            Crucible => Self::Crucible,
            Clickhouse => Self::Clickhouse,
        }
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DatasetKind::*;
        let s = match self {
            Crucible => "crucible",
            CockroachDb { .. } => "cockroachdb",
            Clickhouse => "clickhouse",
        };
        write!(f, "{}", s)
    }
}

/// Used to request a new dataset kind exists within a zpool.
///
/// Many dataset types are associated with services that will be
/// instantiated when the dataset is detected.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DatasetEnsureBody {
    // The UUID of the dataset, as well as the service using it directly.
    pub id: Uuid,
    // The name (and UUID) of the Zpool which we are inserting into.
    pub zpool_id: Uuid,
    // The type of the filesystem.
    pub dataset_kind: DatasetKind,
    // The address on which the zone will listen for requests.
    pub address: SocketAddrV6,
}

impl From<DatasetEnsureBody> for sled_agent_client::types::DatasetEnsureBody {
    fn from(p: DatasetEnsureBody) -> Self {
        Self {
            zpool_id: p.zpool_id,
            dataset_kind: p.dataset_kind.into(),
            address: p.address.to_string(),
            id: p.id,
        }
    }
}

/// Describes service-specific parameters.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServiceType {
    Nexus {
        /// The address at which the internal nexus server is reachable.
        internal_ip: Ipv6Addr,
        /// The address at which the external nexus server is reachable.
        external_ip: IpAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
    },
    ExternalDns {
        /// The address at which the external DNS server API is reachable.
        http_address: SocketAddrV6,
        /// The address at which the external DNS server is reachable.
        dns_address: SocketAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
    },
    InternalDns {
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
    },
    Oximeter,
    // We should never receive external requests to start wicketd, MGS,
    // dendrite, tfport, or maghemite: these are all services running in the
    // global zone or switch zone that we start autonomously. We tag them with
    // `serde(skip)` both to omit them from our OpenAPI definition and to avoid
    // needing their contained types to implement `JsonSchema + Deserialize +
    // Serialize`.
    #[serde(skip)]
    ManagementGatewayService,
    #[serde(skip)]
    Wicketd {
        baseboard: Baseboard,
    },
    #[serde(skip)]
    Dendrite {
        asic: DendriteAsic,
    },
    #[serde(skip)]
    Tfport {
        pkt_source: String,
    },
    #[serde(skip)]
    Maghemite {
        mode: String,
    },
    CruciblePantry,
    BoundaryNtp {
        ntp_servers: Vec<String>,
        dns_servers: Vec<String>,
        domain: Option<String>,
        /// The service vNIC providing outbound connectivity using OPTE.
        nic: NetworkInterface,
        /// The SNAT configuration for outbound connections.
        snat_cfg: SourceNatConfig,
    },
    InternalNtp {
        ntp_servers: Vec<String>,
        dns_servers: Vec<String>,
        domain: Option<String>,
    },
    Clickhouse,
    CockroachDb,
    Crucible,
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match self {
            ServiceType::Nexus { .. } => write!(f, "nexus"),
            ServiceType::ExternalDns { .. } => write!(f, "external_dns"),
            ServiceType::InternalDns { .. } => write!(f, "internal_dns"),
            ServiceType::Oximeter => write!(f, "oximeter"),
            ServiceType::ManagementGatewayService => write!(f, "mgs"),
            ServiceType::Wicketd { .. } => write!(f, "wicketd"),
            ServiceType::Dendrite { .. } => write!(f, "dendrite"),
            ServiceType::Tfport { .. } => write!(f, "tfport"),
            ServiceType::CruciblePantry => write!(f, "crucible/pantry"),
            ServiceType::BoundaryNtp { .. }
            | ServiceType::InternalNtp { .. } => write!(f, "ntp"),
            ServiceType::Maghemite { .. } => write!(f, "mg-ddm"),
            ServiceType::Clickhouse => write!(f, "clickhouse"),
            ServiceType::CockroachDb => write!(f, "cockroachdb"),
            ServiceType::Crucible => write!(f, "crucible"),
        }
    }
}

impl crate::smf_helper::Service for ServiceType {
    fn service_name(&self) -> String {
        self.to_string()
    }
    fn smf_name(&self) -> String {
        match self {
            // NOTE: This style of service-naming is deprecated
            ServiceType::Dendrite { .. }
            | ServiceType::Tfport { .. }
            | ServiceType::Maghemite { .. } => {
                format!("svc:/system/illumos/{}", self.service_name())
            }
            _ => format!("svc:/oxide/{}", self.service_name()),
        }
    }
    fn should_import(&self) -> bool {
        true
    }
}

/// Error returned by attempting to convert an internal service (i.e., a service
/// started autonomously by sled-agent) into a
/// `sled_agent_client::types::ServiceType` to be sent to a remote sled-agent.
#[derive(Debug, Clone, Copy, Error)]
#[error("This service may only be started autonomously by sled-agent")]
pub struct AutonomousServiceOnlyError;

impl TryFrom<ServiceType> for sled_agent_client::types::ServiceType {
    type Error = AutonomousServiceOnlyError;

    fn try_from(s: ServiceType) -> Result<Self, Self::Error> {
        use sled_agent_client::types::ServiceType as AutoSt;
        use ServiceType as St;

        match s {
            St::Nexus { internal_ip, external_ip, nic } => {
                Ok(AutoSt::Nexus { internal_ip, external_ip, nic: nic.into() })
            }
            St::ExternalDns { http_address, dns_address, nic } => {
                Ok(AutoSt::ExternalDns {
                    http_address: http_address.to_string(),
                    dns_address: dns_address.to_string(),
                    nic: nic.into(),
                })
            }
            St::InternalDns { http_address, dns_address } => {
                Ok(AutoSt::InternalDns {
                    http_address: http_address.to_string(),
                    dns_address: dns_address.to_string(),
                })
            }
            St::Oximeter => Ok(AutoSt::Oximeter),
            St::CruciblePantry => Ok(AutoSt::CruciblePantry),
            St::BoundaryNtp {
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Ok(AutoSt::BoundaryNtp {
                ntp_servers,
                dns_servers,
                domain,
                nic: nic.into(),
                snat_cfg: snat_cfg.into(),
            }),
            St::InternalNtp { ntp_servers, dns_servers, domain } => {
                Ok(AutoSt::InternalNtp { ntp_servers, dns_servers, domain })
            }
            St::Clickhouse => Ok(AutoSt::Clickhouse),
            St::CockroachDb => Ok(AutoSt::CockroachDb),
            St::Crucible => Ok(AutoSt::Crucible),
            St::ManagementGatewayService
            | St::Wicketd { .. }
            | St::Dendrite { .. }
            | St::Tfport { .. }
            | St::Maghemite { .. } => Err(AutonomousServiceOnlyError),
        }
    }
}

/// The type of zone which may be requested from Sled Agent
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum ZoneType {
    Clickhouse,
    CockroachDb,
    CruciblePantry,
    Crucible,
    ExternalDns,
    InternalDns,
    Nexus,
    Ntp,
    Oximeter,
    Switch,
}

impl From<ZoneType> for sled_agent_client::types::ZoneType {
    fn from(zt: ZoneType) -> Self {
        match zt {
            ZoneType::Clickhouse => Self::Clickhouse,
            ZoneType::CockroachDb => Self::CockroachDb,
            ZoneType::Crucible => Self::Crucible,
            ZoneType::CruciblePantry => Self::CruciblePantry,
            ZoneType::InternalDns => Self::InternalDns,
            ZoneType::ExternalDns => Self::ExternalDns,
            ZoneType::Nexus => Self::Nexus,
            ZoneType::Ntp => Self::Ntp,
            ZoneType::Oximeter => Self::Oximeter,
            ZoneType::Switch => Self::Switch,
        }
    }
}

impl std::fmt::Display for ZoneType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ZoneType::*;
        let name = match self {
            Clickhouse => "clickhouse",
            CockroachDb => "cockroachdb",
            Crucible => "crucible",
            CruciblePantry => "crucible_pantry",
            ExternalDns => "external_dns",
            InternalDns => "internal_dns",
            Nexus => "nexus",
            Ntp => "ntp",
            Oximeter => "oximeter",
            Switch => "switch",
        };
        write!(f, "{name}")
    }
}

/// Describes a request to create a zone running one or more services.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceZoneRequest {
    // The UUID of the zone to be initialized.
    pub id: Uuid,
    // The type of the zone to be created.
    pub zone_type: ZoneType,
    // The addresses on which the service should listen for requests.
    pub addresses: Vec<Ipv6Addr>,
    // Datasets which should be managed by this service.
    #[serde(default)]
    pub dataset: Option<crate::storage::dataset::DatasetName>,
    // The addresses in the global zone which should be created, if necessary
    // to route to the service.
    //
    // For addresses allocated within the Sled's Subnet, no extra address should
    // be necessary. However, for other services - such the DNS service, which
    // exists outside the sleds's typical subnet - adding an address in the GZ
    // is necessary to allow inter-zone traffic routing.
    #[serde(default)]
    pub gz_addresses: Vec<Ipv6Addr>,
    // Services that should be run in the zone
    pub services: Vec<ServiceZoneService>,
}

impl ServiceZoneRequest {
    // The full name of the zone, if it was to be created as a zone.
    pub fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            &self.zone_type.to_string(),
            self.zone_name_unique_identifier().as_deref(),
        )
    }

    // The name of a unique identifier for the zone, if one is necessary.
    pub fn zone_name_unique_identifier(&self) -> Option<String> {
        self.dataset.as_ref().map(|d| d.pool().to_string())
    }
}

impl TryFrom<ServiceZoneRequest>
    for sled_agent_client::types::ServiceZoneRequest
{
    type Error = AutonomousServiceOnlyError;

    fn try_from(s: ServiceZoneRequest) -> Result<Self, Self::Error> {
        let mut services = Vec::with_capacity(s.services.len());
        for service in s.services {
            services.push(service.try_into()?);
        }

        Ok(Self {
            id: s.id,
            zone_type: s.zone_type.into(),
            addresses: s.addresses,
            dataset: s.dataset.map(|d| d.into()),
            gz_addresses: s.gz_addresses,
            services,
        })
    }
}

/// Used to request that the Sled initialize a single service.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceZoneService {
    pub id: Uuid,
    pub details: ServiceType,
}

impl TryFrom<ServiceZoneService>
    for sled_agent_client::types::ServiceZoneService
{
    type Error = AutonomousServiceOnlyError;

    fn try_from(s: ServiceZoneService) -> Result<Self, Self::Error> {
        let details = s.details.try_into()?;
        Ok(Self { id: s.id, details })
    }
}

/// Used to request that the Sled initialize multiple services.
///
/// This may be used to record that certain sleds are responsible for
/// launching services which may not be associated with a dataset, such
/// as Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ServiceEnsureBody {
    pub services: Vec<ServiceZoneRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct TimeSync {
    /// The synchronization state of the sled, true when the system clock
    /// and the NTP clock are in sync (to within a small window).
    pub sync: bool,
    // These could both be f32, but there is a problem with progenitor/typify
    // where, although the f32 correctly becomes "float" (and not "double") in
    // the API spec, that "float" gets converted back to f64 when generating
    // the client.
    /// The estimated error bound on the frequency.
    pub skew: f64,
    /// The current offset between the NTP clock and system clock.
    pub correction: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SledRole {
    /// The sled is a general compute sled.
    Gimlet,
    /// The sled is attached to the network switch, and has additional
    /// responsibilities.
    Scrimlet,
}
