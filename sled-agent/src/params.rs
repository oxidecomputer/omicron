// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::zone_bundle::PriorityOrder;
pub use crate::zone_bundle::ZoneBundleCause;
pub use crate::zone_bundle::ZoneBundleId;
pub use crate::zone_bundle::ZoneBundleMetadata;
pub use illumos_utils::opte::params::DhcpConfig;
pub use illumos_utils::opte::params::VpcFirewallRule;
pub use illumos_utils::opte::params::VpcFirewallRulesEnsureBody;
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, InstanceProperties, InstanceRuntimeState,
    SledInstanceState, VmmRuntimeState,
};
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use omicron_common::disk::DiskVariant;
use omicron_common_extended::inventory::{OmicronZoneConfig, OmicronZoneType};
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use sled_hardware::DendriteAsic;
use sled_hardware_types::Baseboard;
use sled_storage::dataset::DatasetName;
use sled_storage::dataset::DatasetType;
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use std::time::Duration;
use uuid::Uuid;

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
    pub properties: InstanceProperties,
    pub nics: Vec<NetworkInterface>,
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: Vec<IpAddr>,
    pub firewall_rules: Vec<VpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    // TODO: replace `propolis_client::*` with locally-modeled request type
    pub disks: Vec<propolis_client::types::DiskRequest>,
    pub cloud_init_bytes: Option<String>,
}

/// Metadata used to track statistics about an instance.
///
// NOTE: The instance ID is not here, since it's already provided in other
// pieces of the instance-related requests. It is pulled from there when
// publishing metrics for the instance.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceMetadata {
    pub silo_id: Uuid,
    pub project_id: Uuid,
}

impl From<InstanceMetadata> for propolis_client::types::InstanceMetadata {
    fn from(md: InstanceMetadata) -> Self {
        Self { silo_id: md.silo_id, project_id: md.project_id }
    }
}

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// A description of the instance's virtual hardware and the initial runtime
    /// state this sled agent should store for this incarnation of the instance.
    pub hardware: InstanceHardware,

    /// The instance runtime state for the instance being registered.
    pub instance_runtime: InstanceRuntimeState,

    /// The initial VMM runtime state for the VMM being registered.
    pub vmm_runtime: VmmRuntimeState,

    /// The ID of the VMM being registered. This may not be the active VMM ID in
    /// the instance runtime state (e.g. if the new VMM is going to be a
    /// migration target).
    pub propolis_id: PropolisUuid,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,

    /// Metadata used to track instance statistics.
    pub metadata: InstanceMetadata,
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
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstancePutStateResponse {
    /// The current runtime state of the instance after handling the request to
    /// change its state. If the instance's state did not change, this field is
    /// `None`.
    pub updated_runtime: Option<SledInstanceState>,
}

/// The response sent from a request to unregister an instance.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceUnregisterResponse {
    /// The current state of the instance after handling the request to
    /// unregister it. If the instance's state did not change, this field is
    /// `None`.
    pub updated_runtime: Option<SledInstanceState>,
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
    pub dst_propolis_id: PropolisUuid,
}

/// The body of a request to set or clear the migration identifiers from a
/// sled agent's instance state records.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstancePutMigrationIdsBody {
    /// The last instance runtime state known to this requestor. This request
    /// will succeed if either (a) the state generation in the sled agent's
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

impl From<DiskVariant> for DiskType {
    fn from(v: DiskVariant) -> Self {
        match v {
            DiskVariant::U2 => Self::U2,
            DiskVariant::M2 => Self::M2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Zpool {
    pub id: ZpoolUuid,
    pub disk_type: DiskType,
}

/// Extension trait for `OmicronZoneConfig`.
///
/// This lives here because it is pretty specific to sled-agent, and also
/// requires extra dependencies that omicron-common-extended doesn't have
pub(crate) trait OmicronZoneConfigExt {
    fn zone_name(&self) -> String;
}

impl OmicronZoneConfigExt for OmicronZoneConfig {
    fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            &self.zone_type.kind().service_str(),
            Some(self.id),
        )
    }
}

/// Extension trait for `OmicronZoneType` and `OmicronZoneConfig`.
///
/// This lives here because it requires extra dependencies that
/// omicron-common-extended doesn't have.
pub(crate) trait OmicronZoneTypeExt {
    fn as_omicron_zone_type(&self) -> &OmicronZoneType;

    /// If this kind of zone has an associated dataset, return the dataset's name.
    /// Otherwise, return `None`.
    fn dataset_name(&self) -> Option<DatasetName> {
        self.dataset_name_and_address().map(|(name, _)| name)
    }

    /// If this kind of zone has an associated dataset, return the dataset's name
    /// and the associated "service address". Otherwise, return `None`.
    fn dataset_name_and_address(&self) -> Option<(DatasetName, SocketAddrV6)> {
        let (dataset, dataset_kind, address) = match self.as_omicron_zone_type()
        {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. }
            | OmicronZoneType::CruciblePantry { .. } => None,
            OmicronZoneType::Clickhouse { dataset, address, .. } => {
                Some((dataset, DatasetType::Clickhouse, address))
            }
            OmicronZoneType::ClickhouseKeeper { dataset, address, .. } => {
                Some((dataset, DatasetType::ClickhouseKeeper, address))
            }
            OmicronZoneType::CockroachDb { dataset, address, .. } => {
                Some((dataset, DatasetType::CockroachDb, address))
            }
            OmicronZoneType::Crucible { dataset, address, .. } => {
                Some((dataset, DatasetType::Crucible, address))
            }
            OmicronZoneType::ExternalDns { dataset, http_address, .. } => {
                Some((dataset, DatasetType::ExternalDns, http_address))
            }
            OmicronZoneType::InternalDns { dataset, http_address, .. } => {
                Some((dataset, DatasetType::InternalDns, http_address))
            }
        }?;

        Some((
            DatasetName::new(dataset.pool_name.clone(), dataset_kind),
            *address,
        ))
    }
}

impl OmicronZoneTypeExt for OmicronZoneType {
    fn as_omicron_zone_type(&self) -> &OmicronZoneType {
        self
    }
}

impl OmicronZoneTypeExt for OmicronZoneConfig {
    fn as_omicron_zone_type(&self) -> &OmicronZoneType {
        &self.zone_type
    }
}

/// The type of zone that Sled Agent may run
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum ZoneType {
    Clickhouse,
    ClickhouseKeeper,
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

impl std::fmt::Display for ZoneType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ZoneType::*;
        let name = match self {
            Clickhouse => "clickhouse",
            ClickhouseKeeper => "clickhouse_keeper",
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

impl crate::smf_helper::Service for OmicronZoneType {
    fn service_name(&self) -> String {
        // For historical reasons, crucible-pantry is the only zone type whose
        // SMF service does not match the canonical name that we use for the
        // zone.
        match self {
            OmicronZoneType::CruciblePantry { .. } => {
                "crucible/pantry".to_owned()
            }
            _ => self.kind().service_str().to_owned(),
        }
    }
    fn smf_name(&self) -> String {
        format!("svc:/oxide/{}", self.service_name())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct TimeSync {
    /// The synchronization state of the sled, true when the system clock
    /// and the NTP clock are in sync (to within a small window).
    pub sync: bool,
    /// The NTP reference ID.
    pub ref_id: u32,
    /// The NTP reference IP address.
    pub ip_addr: IpAddr,
    /// The NTP stratum (our upstream's stratum plus one).
    pub stratum: u8,
    /// The NTP reference time (i.e. what chrony thinks the current time is, not
    /// necessarily the current system time).
    pub ref_time: f64,
    // This could be f32, but there is a problem with progenitor/typify
    // where, although the f32 correctly becomes "float" (and not "double") in
    // the API spec, that "float" gets converted back to f64 when generating
    // the client.
    /// The current offset between the NTP clock and system clock.
    pub correction: f64,
}

/// Parameters used to update the zone bundle cleanup context.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CleanupContextUpdate {
    /// The new period on which automatic cleanups are run.
    pub period: Option<Duration>,
    /// The priority ordering for preserving old zone bundles.
    pub priority: Option<PriorityOrder>,
    /// The new limit on the underlying dataset quota allowed for bundles.
    pub storage_limit: Option<u8>,
}

/// Used to dynamically update external IPs attached to an instance.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, JsonSchema, Serialize,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum InstanceExternalIpBody {
    Ephemeral(IpAddr),
    Floating(IpAddr),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct EstablishedConnection {
    baseboard: Baseboard,
    addr: SocketAddrV6,
}

impl From<(Baseboard, SocketAddrV6)> for EstablishedConnection {
    fn from(value: (Baseboard, SocketAddrV6)) -> Self {
        EstablishedConnection { baseboard: value.0, addr: value.1 }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootstoreStatus {
    pub fsm_ledger_generation: u64,
    pub network_config_ledger_generation: Option<u64>,
    pub fsm_state: String,
    pub peers: BTreeSet<SocketAddrV6>,
    pub established_connections: Vec<EstablishedConnection>,
    pub accepted_connections: BTreeSet<SocketAddrV6>,
    pub negotiating_connections: BTreeSet<SocketAddrV6>,
}

impl From<bootstore::schemes::v0::Status> for BootstoreStatus {
    fn from(value: bootstore::schemes::v0::Status) -> Self {
        BootstoreStatus {
            fsm_ledger_generation: value.fsm_ledger_generation,
            network_config_ledger_generation: value
                .network_config_ledger_generation,
            fsm_state: value.fsm_state.to_string(),
            peers: value.peers,
            established_connections: value
                .connections
                .into_iter()
                .map(EstablishedConnection::from)
                .collect(),
            accepted_connections: value.accepted_connections,
            negotiating_connections: value.negotiating_connections,
        }
    }
}
