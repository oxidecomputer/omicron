// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::internal::nexus::{
    DiskRuntimeState, InstanceRuntimeState,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use uuid::Uuid;

pub use illumos_utils::opte::params::NetworkInterface;
pub use illumos_utils::opte::params::SourceNatConfig;
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

/// Sent to a sled agent to establish the runtime state of an Instance
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// Last runtime state of the Instance known to Nexus (used if the agent
    /// has never seen this Instance before).
    pub initial: InstanceHardware,
    /// requested runtime state of the Instance
    pub target: InstanceStateRequested,
    /// If we're migrating this instance, the details needed to drive the migration
    pub migrate: Option<InstanceMigrationTargetParams>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrationTargetParams {
    pub src_propolis_id: Uuid,
    pub src_propolis_addr: SocketAddr,
}

/// Requestable running state of an Instance.
///
/// A subset of [`omicron_common::api::external::InstanceState`].
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum InstanceStateRequested {
    /// Start the instance if it is not already running.
    Running,
    /// Stop the instance.
    Stopped,
    /// Issue a reset command to the instance, such that it should
    /// stop and then immediately become running.
    Reboot,
    /// Stop the instance and delete it.
    Destroyed,
}

impl Display for InstanceStateRequested {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

impl InstanceStateRequested {
    fn label(&self) -> &str {
        match self {
            InstanceStateRequested::Running => "running",
            InstanceStateRequested::Stopped => "stopped",
            InstanceStateRequested::Reboot => "reboot",
            InstanceStateRequested::Destroyed => "destroyed",
        }
    }

    /// Returns true if the state represents a stopped Instance.
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceStateRequested::Running => false,
            InstanceStateRequested::Stopped => true,
            InstanceStateRequested::Reboot => false,
            InstanceStateRequested::Destroyed => true,
        }
    }
}

/// Instance runtime state to update for a migration.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrationSourceParams {
    pub migration_id: Uuid,
    pub dst_propolis_id: Uuid,
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
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetKind {
    CockroachDb {
        /// The addresses of all nodes within the cluster.
        all_addresses: Vec<SocketAddrV6>,
    },
    Crucible,
    Clickhouse,
}

impl From<DatasetKind> for sled_agent_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb { all_addresses } => Self::CockroachDb(
                all_addresses.iter().map(|a| a.to_string()).collect(),
            ),
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
        internal_ip: Ipv6Addr,
        external_ip: IpAddr,
    },
    InternalDns {
        server_address: SocketAddrV6,
        dns_address: SocketAddrV6,
    },
    Oximeter,
    ManagementGatewayService,
    Wicketd,
    Dendrite {
        asic: DendriteAsic,
    },
    Tfport {
        pkt_source: String,
    },
    CruciblePantry,
    Ntp {
        ntp_servers: Vec<String>,
        boundary: bool,
        dns_servers: Vec<String>,
        domain: Option<String>,
    },
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match self {
            ServiceType::Nexus { .. } => write!(f, "nexus"),
            ServiceType::InternalDns { .. } => write!(f, "internal_dns"),
            ServiceType::Oximeter => write!(f, "oximeter"),
            ServiceType::ManagementGatewayService => write!(f, "mgs"),
            ServiceType::Wicketd => write!(f, "wicketd"),
            ServiceType::Dendrite { .. } => write!(f, "dendrite"),
            ServiceType::Tfport { .. } => write!(f, "tfport"),
            ServiceType::CruciblePantry => write!(f, "crucible_pantry"),
            ServiceType::Ntp { .. } => write!(f, "ntp"),
        }
    }
}

impl crate::smf_helper::Service for ServiceType {
    fn service_name(&self) -> String {
        self.to_string()
    }
    fn smf_name(&self) -> String {
        format!("svc:/system/illumos/{}", self.service_name())
    }
    fn should_import(&self) -> bool {
        true
    }
}

impl From<ServiceType> for sled_agent_client::types::ServiceType {
    fn from(s: ServiceType) -> Self {
        use sled_agent_client::types::ServiceType as AutoSt;
        use ServiceType as St;

        match s {
            St::Nexus { internal_ip, external_ip } => {
                AutoSt::Nexus { internal_ip, external_ip }
            }
            St::InternalDns { server_address, dns_address } => {
                AutoSt::InternalDns {
                    server_address: server_address.to_string(),
                    dns_address: dns_address.to_string(),
                }
            }
            St::Oximeter => AutoSt::Oximeter,
            St::ManagementGatewayService => AutoSt::ManagementGatewayService,
            St::Wicketd => AutoSt::Wicketd,
            St::Dendrite { asic } => {
                use sled_agent_client::types::DendriteAsic as AutoAsic;
                let asic = match asic {
                    DendriteAsic::TofinoAsic => AutoAsic::TofinoAsic,
                    DendriteAsic::TofinoStub => AutoAsic::TofinoStub,
                    DendriteAsic::SoftNpu => AutoAsic::SoftNpu,
                };
                AutoSt::Dendrite { asic }
            }
            St::Tfport { pkt_source } => AutoSt::Tfport { pkt_source },
            St::CruciblePantry => AutoSt::CruciblePantry,
            St::Ntp { ntp_servers, boundary, dns_servers, domain } => {
                AutoSt::Ntp { ntp_servers, boundary, dns_servers, domain }
            }
        }
    }
}

/// The type of zone which may be requested from Sled Agent
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub enum ZoneType {
    #[serde(rename = "internal_dns")]
    InternalDNS,
    #[serde(rename = "nexus")]
    Nexus,
    #[serde(rename = "oximeter")]
    Oximeter,
    #[serde(rename = "switch")]
    Switch,
    #[serde(rename = "crucible_pantry")]
    CruciblePantry,
    #[serde(rename = "ntp")]
    NTP,
}

impl From<ZoneType> for sled_agent_client::types::ZoneType {
    fn from(zt: ZoneType) -> Self {
        match zt {
            ZoneType::InternalDNS => Self::InternalDns,
            ZoneType::Nexus => Self::Nexus,
            ZoneType::Oximeter => Self::Oximeter,
            ZoneType::Switch => Self::Switch,
            ZoneType::CruciblePantry => Self::CruciblePantry,
            ZoneType::NTP => Self::Ntp,
        }
    }
}

impl std::fmt::Display for ZoneType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ZoneType::*;
        let name = match self {
            InternalDNS => "internal_dns",
            Nexus => "nexus",
            Oximeter => "oximeter",
            Switch => "switch",
            CruciblePantry => "crucible_pantry",
            NTP => "ntp",
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
    pub services: Vec<ServiceType>,
}

impl From<ServiceZoneRequest> for sled_agent_client::types::ServiceZoneRequest {
    fn from(s: ServiceZoneRequest) -> Self {
        let mut services = Vec::new();
        for service in s.services {
            services.push(service.into())
        }

        Self {
            id: s.id,
            zone_type: s.zone_type.into(),
            addresses: s.addresses,
            gz_addresses: s.gz_addresses,
            services,
        }
    }
}

/// Used to request that the Sled initialize certain services on initialization.
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
    pub sync: bool,
    // These could both be f32, but there is a problem with progenitor/typify
    // where, although the f32 correctly becomes "float" (and not "double") in
    // the API spec, that "float" gets converted back to f64 when generating
    // the client.
    pub skew: f64,
    pub correction: f64,
}
