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
use illumos_utils::zpool::ZpoolName;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, InstanceProperties, InstanceRuntimeState,
    SledInstanceState, VmmRuntimeState,
};
use omicron_common::api::internal::shared::{
    NetworkInterface, SourceNatConfig,
};
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use sled_hardware::DendriteAsic;
use sled_hardware_types::Baseboard;
use sled_storage::dataset::DatasetKind;
use sled_storage::dataset::DatasetName;
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::str::FromStr;
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
    pub id: ZpoolUuid,
    pub disk_type: DiskType,
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

pub type OmicronPhysicalDiskConfig =
    sled_storage::disk::OmicronPhysicalDiskConfig;
pub type OmicronPhysicalDisksConfig =
    sled_storage::disk::OmicronPhysicalDisksConfig;
pub type DatasetConfig = sled_storage::disk::DatasetConfig;
pub type DatasetsConfig = sled_storage::disk::DatasetsConfig;

/// Describes the set of Omicron-managed zones running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZonesConfig {
    /// generation number of this configuration
    ///
    /// This generation number is owned by the control plane (i.e., RSS or
    /// Nexus, depending on whether RSS-to-Nexus handoff has happened).  It
    /// should not be bumped within Sled Agent.
    ///
    /// Sled Agent rejects attempts to set the configuration to a generation
    /// older than the one it's currently running.
    pub generation: Generation,

    /// list of running zones
    pub zones: Vec<OmicronZoneConfig>,
}

impl OmicronZonesConfig {
    /// Generation 1 of `OmicronZonesConfig` is always the set of no zones.
    pub const INITIAL_GENERATION: Generation = Generation::from_u32(1);
}

impl From<OmicronZonesConfig> for sled_agent_client::types::OmicronZonesConfig {
    fn from(local: OmicronZonesConfig) -> Self {
        Self {
            generation: local.generation,
            zones: local.zones.into_iter().map(|s| s.into()).collect(),
        }
    }
}

/// Describes one Omicron-managed zone running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneConfig {
    pub id: Uuid,
    pub underlay_address: Ipv6Addr,

    /// The pool on which we'll place this zone's filesystem.
    ///
    /// Note that this is transient -- the sled agent is permitted to
    /// destroy the zone's dataset on this pool each time the zone is
    /// initialized.
    pub filesystem_pool: Option<ZpoolName>,
    pub zone_type: OmicronZoneType,
}

impl From<OmicronZoneConfig> for sled_agent_client::types::OmicronZoneConfig {
    fn from(local: OmicronZoneConfig) -> Self {
        Self {
            id: local.id,
            underlay_address: local.underlay_address,
            filesystem_pool: local.filesystem_pool,
            zone_type: local.zone_type.into(),
        }
    }
}

impl OmicronZoneConfig {
    /// If this kind of zone has an associated dataset, returns the dataset's
    /// name.  Othrwise, returns `None`.
    pub fn dataset_name(&self) -> Option<DatasetName> {
        self.zone_type.dataset_name()
    }

    /// If this kind of zone has an associated dataset, return the dataset's
    /// name and the associated "service address".  Otherwise, returns `None`.
    pub fn dataset_name_and_address(
        &self,
    ) -> Option<(DatasetName, SocketAddrV6)> {
        self.zone_type.dataset_name_and_address()
    }

    /// Returns the name that is (or will be) used for the illumos zone
    /// associated with this zone
    pub fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            &self.zone_type.zone_type_str(),
            Some(self.id),
        )
    }
}

/// Describes a persistent ZFS dataset associated with an Omicron zone
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneDataset {
    pub pool_name: ZpoolName,
}

impl From<OmicronZoneDataset> for sled_agent_client::types::OmicronZoneDataset {
    fn from(local: OmicronZoneDataset) -> Self {
        Self {
            pool_name: omicron_common::zpool_name::ZpoolName::from_str(
                &local.pool_name.to_string(),
            )
            .unwrap(),
        }
    }
}

/// Describes what kind of zone this is (i.e., what component is running in it)
/// as well as any type-specific configuration
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OmicronZoneType {
    BoundaryNtp {
        address: SocketAddrV6,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
        /// The service vNIC providing outbound connectivity using OPTE.
        nic: NetworkInterface,
        /// The SNAT configuration for outbound connections.
        snat_cfg: SourceNatConfig,
    },

    Clickhouse {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    ClickhouseKeeper {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },
    CockroachDb {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    Crucible {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },
    CruciblePantry {
        address: SocketAddrV6,
    },
    ExternalDns {
        dataset: OmicronZoneDataset,
        /// The address at which the external DNS server API is reachable.
        http_address: SocketAddrV6,
        /// The address at which the external DNS server is reachable.
        dns_address: SocketAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
    },
    InternalDns {
        dataset: OmicronZoneDataset,
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
        /// The addresses in the global zone which should be created
        ///
        /// For the DNS service, which exists outside the sleds's typical subnet
        /// - adding an address in the GZ is necessary to allow inter-zone
        /// traffic routing.
        gz_address: Ipv6Addr,

        /// The address is also identified with an auxiliary bit of information
        /// to ensure that the created global zone address can have a unique
        /// name.
        gz_address_index: u32,
    },
    InternalNtp {
        address: SocketAddrV6,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
    },
    Nexus {
        /// The address at which the internal nexus server is reachable.
        internal_address: SocketAddrV6,
        /// The address at which the external nexus server is reachable.
        external_ip: IpAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
        /// Whether Nexus's external endpoint should use TLS
        external_tls: bool,
        /// External DNS servers Nexus can use to resolve external hosts.
        external_dns_servers: Vec<IpAddr>,
    },
    Oximeter {
        address: SocketAddrV6,
    },
}

impl OmicronZoneType {
    /// Returns a canonical string identifying the type of zone this is
    ///
    /// This is used to construct zone names, SMF service names, etc.
    pub fn zone_type_str(&self) -> String {
        match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. } => ZoneType::Ntp,

            OmicronZoneType::Clickhouse { .. } => ZoneType::Clickhouse,
            OmicronZoneType::ClickhouseKeeper { .. } => {
                ZoneType::ClickhouseKeeper
            }
            OmicronZoneType::CockroachDb { .. } => ZoneType::CockroachDb,
            OmicronZoneType::Crucible { .. } => ZoneType::Crucible,
            OmicronZoneType::CruciblePantry { .. } => ZoneType::CruciblePantry,
            OmicronZoneType::ExternalDns { .. } => ZoneType::ExternalDns,
            OmicronZoneType::InternalDns { .. } => ZoneType::InternalDns,
            OmicronZoneType::Nexus { .. } => ZoneType::Nexus,
            OmicronZoneType::Oximeter { .. } => ZoneType::Oximeter,
        }
        .to_string()
    }

    /// If this kind of zone has an associated dataset, returns the dataset's
    /// name.  Othrwise, returns `None`.
    pub fn dataset_name(&self) -> Option<DatasetName> {
        self.dataset_name_and_address().map(|d| d.0)
    }

    /// If this kind of zone has an associated dataset, return the dataset's
    /// name and the associated "service address".  Otherwise, returns `None`.
    pub fn dataset_name_and_address(
        &self,
    ) -> Option<(DatasetName, SocketAddrV6)> {
        let (dataset, dataset_kind, address) = match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. }
            | OmicronZoneType::CruciblePantry { .. } => None,
            OmicronZoneType::Clickhouse { dataset, address, .. } => {
                Some((dataset, DatasetKind::Clickhouse, address))
            }
            OmicronZoneType::ClickhouseKeeper { dataset, address, .. } => {
                Some((dataset, DatasetKind::ClickhouseKeeper, address))
            }
            OmicronZoneType::CockroachDb { dataset, address, .. } => {
                Some((dataset, DatasetKind::CockroachDb, address))
            }
            OmicronZoneType::Crucible { dataset, address, .. } => {
                Some((dataset, DatasetKind::Crucible, address))
            }
            OmicronZoneType::ExternalDns { dataset, http_address, .. } => {
                Some((dataset, DatasetKind::ExternalDns, http_address))
            }
            OmicronZoneType::InternalDns { dataset, http_address, .. } => {
                Some((dataset, DatasetKind::InternalDns, http_address))
            }
        }?;

        Some((
            DatasetName::new(dataset.pool_name.clone(), dataset_kind),
            *address,
        ))
    }

    /// Does this zone require time synchronization before it is initialized?"
    ///
    /// This function is somewhat conservative - the set of services
    /// that can be launched before timesync has completed is intentionally kept
    /// small, since it would be easy to add a service that expects time to be
    /// reasonably synchronized.
    pub fn requires_timesync(&self) -> bool {
        match self {
            // These zones can be initialized and started before time has been
            // synchronized. For the NTP zones, this should be self-evident --
            // we need the NTP zone to actually perform time synchronization!
            //
            // The DNS zone is a bit of an exception here, since the NTP zone
            // itself may rely on DNS lookups as a dependency.
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::InternalDns { .. } => false,
            _ => true,
        }
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
            _ => self.zone_type_str(),
        }
    }
    fn smf_name(&self) -> String {
        format!("svc:/oxide/{}", self.service_name())
    }
}

impl From<OmicronZoneType> for sled_agent_client::types::OmicronZoneType {
    fn from(local: OmicronZoneType) -> Self {
        use sled_agent_client::types::OmicronZoneType as Other;
        match local {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Other::BoundaryNtp {
                address: address.to_string(),
                dns_servers,
                domain,
                ntp_servers,
                snat_cfg,
                nic,
            },
            OmicronZoneType::Clickhouse { address, dataset } => {
                Other::Clickhouse {
                    address: address.to_string(),
                    dataset: dataset.into(),
                }
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                Other::ClickhouseKeeper {
                    address: address.to_string(),
                    dataset: dataset.into(),
                }
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                Other::CockroachDb {
                    address: address.to_string(),
                    dataset: dataset.into(),
                }
            }
            OmicronZoneType::Crucible { address, dataset } => Other::Crucible {
                address: address.to_string(),
                dataset: dataset.into(),
            },
            OmicronZoneType::CruciblePantry { address } => {
                Other::CruciblePantry { address: address.to_string() }
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => Other::ExternalDns {
                dataset: dataset.into(),
                http_address: http_address.to_string(),
                dns_address: dns_address.to_string(),
                nic,
            },
            OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => Other::InternalDns {
                dataset: dataset.into(),
                http_address: http_address.to_string(),
                dns_address: dns_address.to_string(),
                gz_address,
                gz_address_index,
            },
            OmicronZoneType::InternalNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
            } => Other::InternalNtp {
                address: address.to_string(),
                ntp_servers,
                dns_servers,
                domain,
            },
            OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Other::Nexus {
                external_dns_servers,
                external_ip,
                external_tls,
                internal_address: internal_address.to_string(),
                nic,
            },
            OmicronZoneType::Oximeter { address } => {
                Other::Oximeter { address: address.to_string() }
            }
        }
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

// Our SledRole and Baseboard types do not have to be identical to the Nexus
// ones, but they generally should be, and this avoids duplication.  If it
// becomes easier to maintain a separate copy, we should do that.
pub type SledRole = nexus_client::types::SledRole;

/// Identifies information about disks which may be attached to Sleds.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDisk {
    pub identity: omicron_common::disk::DiskIdentity,
    pub variant: sled_hardware::DiskVariant,
    pub slot: i64,
}

/// Identifies information about zpools managed by the control plane
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryZpool {
    pub id: ZpoolUuid,
    pub total_size: ByteCount,
}

/// Identifies information about datasets within Oxide-managed zpools
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDataset {
    /// Although datasets mandated by the control plane will have UUIDs,
    /// datasets can be created (and have been created) without UUIDs.
    pub id: Option<DatasetUuid>,

    /// This name is the full path of the dataset.
    // This is akin to [sled_storage::dataset::DatasetName::full_name],
    // and it's also what you'd see when running "zfs list".
    pub name: String,

    /// The amount of remaining space usable by the dataset (and children)
    /// assuming there is no other activity within the pool.
    pub available: ByteCount,

    /// The amount of space consumed by this dataset and descendents.
    pub used: ByteCount,

    /// The maximum amount of space usable by a dataset and all descendents.
    pub quota: Option<ByteCount>,

    /// The minimum amount of space guaranteed to a dataset and descendents.
    pub reservation: Option<ByteCount>,

    /// The compression algorithm used for this dataset, if any.
    pub compression: String,
}

impl From<illumos_utils::zfs::DatasetProperties> for InventoryDataset {
    fn from(props: illumos_utils::zfs::DatasetProperties) -> Self {
        Self {
            id: props.id,
            name: props.name,
            available: props.avail,
            used: props.used,
            quota: props.quota,
            reservation: props.reservation,
            compression: props.compression,
        }
    }
}

/// Identity and basic status information about this sled agent
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Inventory {
    pub sled_id: Uuid,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub baseboard: Baseboard,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,
    pub disks: Vec<InventoryDisk>,
    pub zpools: Vec<InventoryZpool>,
    pub datasets: Vec<InventoryDataset>,
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
