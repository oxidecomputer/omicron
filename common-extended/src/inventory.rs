// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory types shared between Nexus and sled-agent.

use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};

use omicron_common::{
    api::{
        external::{ByteCount, Generation},
        internal::shared::{NetworkInterface, SourceNatConfig},
    },
    disk::DiskVariant,
    zpool_name::ZpoolName,
};
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
// Export this type for convenience -- this way, dependents don't have to
// depend on sled-hardware-types.
pub use sled_hardware_types::Baseboard;
use uuid::Uuid;

/// Identifies information about disks which may be attached to Sleds.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDisk {
    pub identity: omicron_common::disk::DiskIdentity,
    pub variant: DiskVariant,
    pub slot: i64,
}

/// Identifies information about zpools managed by the control plane
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryZpool {
    pub id: ZpoolUuid,
    pub total_size: ByteCount,
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
}

/// Describes the role of the sled within the rack.
///
/// Note that this may change if the sled is physically moved
/// within the rack.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SledRole {
    /// The sled is a general compute sled.
    Gimlet,
    /// The sled is attached to the network switch, and has additional
    /// responsibilities.
    Scrimlet,
}

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

/// Describes a persistent ZFS dataset associated with an Omicron zone
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneDataset {
    pub pool_name: ZpoolName,
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
    /// Returns the [`ZoneKind`] corresponding to this variant.
    pub fn kind(&self) -> ZoneKind {
        match self {
            OmicronZoneType::BoundaryNtp { .. } => ZoneKind::BoundaryNtp,
            OmicronZoneType::Clickhouse { .. } => ZoneKind::Clickhouse,
            OmicronZoneType::ClickhouseKeeper { .. } => {
                ZoneKind::ClickhouseKeeper
            }
            OmicronZoneType::CockroachDb { .. } => ZoneKind::CockroachDb,
            OmicronZoneType::Crucible { .. } => ZoneKind::Crucible,
            OmicronZoneType::CruciblePantry { .. } => ZoneKind::CruciblePantry,
            OmicronZoneType::ExternalDns { .. } => ZoneKind::ExternalDns,
            OmicronZoneType::InternalDns { .. } => ZoneKind::InternalDns,
            OmicronZoneType::InternalNtp { .. } => ZoneKind::InternalNtp,
            OmicronZoneType::Nexus { .. } => ZoneKind::Nexus,
            OmicronZoneType::Oximeter { .. } => ZoneKind::Oximeter,
        }
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

    /// Identifies whether this is an NTP zone
    pub fn is_ntp(&self) -> bool {
        match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. } => true,

            OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::ExternalDns { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// Identifies whether this is a Nexus zone
    pub fn is_nexus(&self) -> bool {
        match self {
            OmicronZoneType::Nexus { .. } => true,

            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::ExternalDns { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// Identifies whether this a Crucible (not Crucible pantry) zone
    pub fn is_crucible(&self) -> bool {
        match self {
            OmicronZoneType::Crucible { .. } => true,

            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::ExternalDns { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// This zone's external IP
    pub fn external_ip(&self) -> Option<IpAddr> {
        match self {
            OmicronZoneType::Nexus { external_ip, .. } => Some(*external_ip),
            OmicronZoneType::ExternalDns { dns_address, .. } => {
                Some(dns_address.ip())
            }
            OmicronZoneType::BoundaryNtp { snat_cfg, .. } => Some(snat_cfg.ip),

            OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Oximeter { .. } => None,
        }
    }

    /// The service vNIC providing external connectivity to this zone
    pub fn service_vnic(&self) -> Option<&NetworkInterface> {
        match self {
            OmicronZoneType::Nexus { nic, .. }
            | OmicronZoneType::ExternalDns { nic, .. }
            | OmicronZoneType::BoundaryNtp { nic, .. } => Some(nic),

            OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Oximeter { .. } => None,
        }
    }
}

/// Like [`OmicronZoneType`], but without any associated data.
///
/// This enum is meant to correspond exactly 1:1 with `OmicronZoneType`.
///
/// # String representations of this type
///
/// There are no fewer than four string representations for this type, all
/// slightly different from each other.
///
/// 1. [`Self::zone_prefix`]: Used to construct zone names.
/// 2. [`Self::service_prefix`]: Used to construct SMF service names.
/// 3. [`Self::name_prefix`]: Used to construct `Name` instances.
/// 4. [`Self::report_str`]: Used for reporting and testing.
///
/// There is no `Display` impl to ensure that users explicitly choose the
/// representation they want. (Please play close attention to this decision!
/// The functions are all similar but different, and we don't currently have
/// great type safety around the choice.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ZoneKind {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    CockroachDb,
    Crucible,
    CruciblePantry,
    ExternalDns,
    InternalDns,
    InternalNtp,
    Nexus,
    Oximeter,
}

impl ZoneKind {
    /// The NTP prefix used for both BoundaryNtp and InternalNtp zones and
    /// services.
    pub const NTP_PREFIX: &'static str = "ntp";

    /// Return a string that is used to construct **zone names**. This string
    /// is guaranteed to be stable over time.
    pub fn zone_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string that is used to construct **SMF service names**. This
    /// string is guaranteed to be stable over time.
    pub fn service_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            // Note "crucible/pantry" for historical reasons.
            ZoneKind::CruciblePantry => "crucible/pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string suitable for use **in `Name` instances**. This string
    /// is guaranteed to be stable over time.
    ///
    /// This string uses dashes rather than underscores, as required by `Name`.
    pub fn name_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp" here.
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse-keeper",
            // Note "cockroach" for historical reasons.
            ZoneKind::CockroachDb => "cockroach",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible-pantry",
            ZoneKind::ExternalDns => "external-dns",
            ZoneKind::InternalDns => "internal-dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string that is used for reporting and error messages. This is
    /// **not guaranteed** to be stable.
    ///
    /// If you're displaying a user-friendly message, prefer this method.
    pub fn report_str(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp => "boundary_ntp",
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::CockroachDb => "cockroach_db",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::InternalNtp => "internal_ntp",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }
}
