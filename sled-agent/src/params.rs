// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use internal_dns_client::names::{BackendName, ServiceName, AAAA, SRV};
use omicron_common::address::{
    DENDRITE_PORT, NEXUS_INTERNAL_PORT, OXIMETER_PORT,
};
use omicron_common::api::external;
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, InstanceRuntimeState,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use uuid::Uuid;

/// Information required to construct a virtual network interface for a guest
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterface {
    pub name: external::Name,
    pub ip: IpAddr,
    pub mac: external::MacAddr,
    pub subnet: external::IpNet,
    pub vni: external::Vni,
    pub primary: bool,
    pub slot: u8,
}

/// An IP address and port range used for instance source NAT, i.e., making
/// outbound network connections from guests.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
pub struct SourceNatConfig {
    /// The external address provided to the instance
    pub ip: IpAddr,
    /// The first port used for instance NAT, inclusive.
    pub first_port: u16,
    /// The last port used for instance NAT, also inclusive.
    pub last_port: u16,
}

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub rules: Vec<VpcFirewallRule>,
}

/// VPC firewall rule after object name resolution has been performed by Nexus
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRule {
    pub status: external::VpcFirewallRuleStatus,
    pub direction: external::VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    pub filter_hosts: Option<Vec<external::IpNet>>,
    pub filter_ports: Option<Vec<external::L4PortRange>>,
    pub filter_protocols: Option<Vec<external::VpcFirewallRuleProtocol>>,
    pub action: external::VpcFirewallRuleAction,
    pub priority: external::VpcFirewallRulePriority,
}

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
    pub disks: Vec<propolis_client::api::DiskRequest>,
    pub cloud_init_bytes: Option<String>,
}

/// Sent to a sled agent to establish the runtime state of an Instance
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// Last runtime state of the Instance known to Nexus (used if the agent
    /// has never seen this Instance before).
    pub initial: InstanceHardware,
    /// requested runtime state of the Instance
    pub target: InstanceRuntimeStateRequested,
    /// If we're migrating this instance, the details needed to drive the migration
    pub migrate: Option<InstanceMigrateParams>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrateParams {
    pub src_propolis_id: Uuid,
    pub src_propolis_addr: SocketAddr,
}

/// Requestable running state of an Instance.
///
/// A subset of [`external::InstanceState`].
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
    /// Migrate the instance to another node.
    Migrating,
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
            InstanceStateRequested::Migrating => "migrating",
            InstanceStateRequested::Destroyed => "destroyed",
        }
    }

    /// Returns true if the state represents a stopped Instance.
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceStateRequested::Running => false,
            InstanceStateRequested::Stopped => true,
            InstanceStateRequested::Reboot => false,
            InstanceStateRequested::Migrating => false,
            InstanceStateRequested::Destroyed => true,
        }
    }
}

/// Instance runtime state to update for a migration.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeStateMigrateParams {
    pub migration_id: Uuid,
    pub dst_propolis_id: Uuid,
}

/// Used to request an Instance state change from a sled agent
///
/// Right now, it's only the run state and migration id that can
/// be changed, though we might want to support changing properties
/// like "ncpus" here.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeStateRequested {
    pub run_state: InstanceStateRequested,
    pub migration_params: Option<InstanceRuntimeStateMigrateParams>,
}

/// Request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleRequest {
    /// Character index in the serial buffer from which to read, counting the bytes output since
    /// instance start. If this is not provided, `most_recent` must be provided, and if this *is*
    /// provided, `most_recent` must *not* be provided.
    pub from_start: Option<u64>,
    /// Character index in the serial buffer from which to read, counting *backward* from the most
    /// recently buffered data retrieved from the instance. (See note on `from_start` about mutual
    /// exclusivity)
    pub most_recent: Option<u64>,
    /// Maximum number of bytes of buffered serial console contents to return. If the requested
    /// range runs to the end of the available buffer, the data returned will be shorter than
    /// `max_bytes`.
    pub max_bytes: Option<u64>,
}

/// Contents of an Instance's serial console buffer.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceSerialConsoleData {
    /// The bytes starting from the requested offset up to either the end of the buffer or the
    /// request's `max_bytes`. Provided as a u8 array rather than a string, as it may not be UTF-8.
    pub data: Vec<u8>,
    /// The absolute offset since boot (suitable for use as `byte_offset` in a subsequent request)
    /// of the last byte returned in `data`.
    pub last_byte_offset: u64,
}

// The type of networking 'ASIC' the Dendrite service is expected to manage
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Copy, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum DendriteAsic {
    TofinoAsic,
    TofinoStub,
    Softnpu,
}

impl std::fmt::Display for DendriteAsic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DendriteAsic::TofinoAsic => "tofino_asic",
                DendriteAsic::TofinoStub => "tofino_stub",
                DendriteAsic::Softnpu => "softnpu",
            }
        )
    }
}

impl From<DendriteAsic> for sled_agent_client::types::DendriteAsic {
    fn from(a: DendriteAsic) -> Self {
        match a {
            DendriteAsic::TofinoAsic => Self::TofinoAsic,
            DendriteAsic::TofinoStub => Self::TofinoStub,
            DendriteAsic::Softnpu => Self::Softnpu,
        }
    }
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

impl DatasetEnsureBody {
    pub fn aaaa(&self) -> AAAA {
        AAAA::Zone(self.id)
    }

    pub fn srv(&self) -> SRV {
        match self.dataset_kind {
            DatasetKind::Crucible => {
                SRV::Backend(BackendName::Crucible, self.id)
            }
            DatasetKind::Clickhouse => SRV::Service(ServiceName::Clickhouse),
            DatasetKind::CockroachDb { .. } => {
                SRV::Service(ServiceName::Cockroach)
            }
        }
    }

    pub fn address(&self) -> SocketAddrV6 {
        self.address
    }
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
    Nexus { internal_ip: Ipv6Addr, external_ip: IpAddr },
    InternalDns { server_address: SocketAddrV6, dns_address: SocketAddrV6 },
    Oximeter,
    Dendrite { asic: DendriteAsic },
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
            St::Dendrite { asic } => AutoSt::Dendrite { asic: asic.into() },
        }
    }
}

/// Describes a request to create a service. This information
/// should be sufficient for a Sled Agent to start a zone
/// containing the requested service.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceRequest {
    // The UUID of the service to be initialized.
    pub id: Uuid,
    // The name of the service to be created.
    pub name: String,
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
    // Any other service-specific parameters.
    pub service_type: ServiceType,
}

impl ServiceRequest {
    pub fn aaaa(&self) -> AAAA {
        AAAA::Zone(self.id)
    }

    pub fn srv(&self) -> SRV {
        match self.service_type {
            ServiceType::InternalDns { .. } => {
                SRV::Service(ServiceName::InternalDNS)
            }
            ServiceType::Nexus { .. } => SRV::Service(ServiceName::Nexus),
            ServiceType::Oximeter => SRV::Service(ServiceName::Oximeter),
            ServiceType::Dendrite { .. } => SRV::Service(ServiceName::Dendrite),
        }
    }

    pub fn address(&self) -> SocketAddrV6 {
        match self.service_type {
            ServiceType::InternalDns { server_address, .. } => server_address,
            ServiceType::Nexus { internal_ip, .. } => {
                SocketAddrV6::new(internal_ip, NEXUS_INTERNAL_PORT, 0, 0)
            }
            ServiceType::Oximeter => {
                SocketAddrV6::new(self.addresses[0], OXIMETER_PORT, 0, 0)
            }
            ServiceType::Dendrite { .. } => {
                SocketAddrV6::new(self.addresses[0], DENDRITE_PORT, 0, 0)
            }
        }
    }
}

impl From<ServiceRequest> for sled_agent_client::types::ServiceRequest {
    fn from(s: ServiceRequest) -> Self {
        Self {
            id: s.id,
            name: s.name,
            addresses: s.addresses,
            gz_addresses: s.gz_addresses,
            service_type: s.service_type.into(),
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
    pub services: Vec<ServiceRequest>,
}
