// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};

use iddqd::IdOrdItem;
use iddqd::id_upcast;
use omicron_common::api::external;
use omicron_common::zpool_name::ZpoolName;
use omicron_generation_kinds::Generation;
use omicron_generation_kinds::SledConfigGeneration;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result::{self, SnakeCaseResult};
use omicron_ledger::Ledgerable;
use omicron_uuid_kinds::{
    DatasetUuid, MupdateOverrideUuid, PhysicalDiskUuid, SledUuid,
};
use sled_hardware_types::{BaseboardId, SledCpuFamily};

use crate::v1::disk::{DatasetConfig, OmicronPhysicalDiskConfig};
use crate::v1::inventory::{
    BootPartitionContents, ConfigReconcilerInventoryResult,
    HostPhase2DesiredSlots, InventoryDataset, InventoryDisk,
    OmicronZoneDataset, OmicronZoneImageSource, OrphanedDataset,
    RemoveMupdateOverrideInventory, SledRole,
};
use crate::v10::inventory::NetworkInterface;
use crate::v11;
use crate::v11::inventory::{
    SourceNatConfigGeneric, SourceNatConfigV4, SourceNatConfigV6,
};
use crate::v14::inventory::{
    OmicronFileSourceResolverInventory, OmicronSingleMeasurement,
};
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v40::inventory::{FmdInventory, FmdInventoryError};
use crate::v46::inventory::SvcsEnabledNotOnlineResult;
use crate::v49::inventory::OmicronSledUpdateDisposition;
use crate::v50;

/// The maximum number of external IPs a single service zone may have.
//
// NOTE: This is a pretty arbitrary number, just something to prevent huge
// requests. It also should be enforced by the database or Neuxs, when we allow
// operators control over which IPs DNS listens on, or which IP Pools Nexus
// draws from. That's part of #10574.
const MAX_ZONE_EXTERNAL_IPS: usize = 16;

// Helper to check the length of an array of IPs / socket addrs.
fn check_length(count: usize) -> Result<(), ZoneExternalAddrsError> {
    if count == 0 {
        return Err(ZoneExternalAddrsError::Empty);
    }
    if count > MAX_ZONE_EXTERNAL_IPS {
        return Err(ZoneExternalAddrsError::TooMany { count });
    }
    Ok(())
}

/// A non-empty, bounded set of external IPs for a Nexus zone.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(try_from = "BTreeSet<IpAddr>", into = "BTreeSet<IpAddr>")]
pub struct NexusExternalIps(
    #[schemars(length(min = 1, max = "MAX_ZONE_EXTERNAL_IPS"))]
    pub(crate)  BTreeSet<IpAddr>,
);

impl NexusExternalIps {
    /// Construct from a list of IPs.
    pub fn new(ips: BTreeSet<IpAddr>) -> Result<Self, ZoneExternalAddrsError> {
        check_length(ips.len())?;
        Ok(Self(ips))
    }
}

impl TryFrom<BTreeSet<IpAddr>> for NexusExternalIps {
    type Error = ZoneExternalAddrsError;

    fn try_from(value: BTreeSet<IpAddr>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<NexusExternalIps> for BTreeSet<IpAddr> {
    fn from(ips: NexusExternalIps) -> Self {
        ips.0
    }
}

/// A non-empty, bounded set of external socket addrs for an External DNS zone.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(try_from = "Vec<SocketAddr>", into = "Vec<SocketAddr>")]
pub struct ExternalDnsAddrs(
    #[schemars(length(min = 1, max = "MAX_ZONE_EXTERNAL_IPS"))]
    pub(crate)  BTreeMap<IpAddr, u16>,
);

impl ExternalDnsAddrs {
    /// Construct from a list of addresses.
    pub fn new(addrs: Vec<SocketAddr>) -> Result<Self, ZoneExternalAddrsError> {
        check_length(addrs.len())?;
        let mut inner = BTreeMap::new();
        for (ip, port) in addrs.iter().map(|addr| (addr.ip(), addr.port())) {
            if inner.insert(ip, port).is_some() {
                return Err(ZoneExternalAddrsError::DuplicateIp { ip });
            }
        }
        Ok(Self(inner))
    }
}

impl TryFrom<Vec<SocketAddr>> for ExternalDnsAddrs {
    type Error = ZoneExternalAddrsError;

    fn try_from(value: Vec<SocketAddr>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ExternalDnsAddrs> for Vec<SocketAddr> {
    fn from(addrs: ExternalDnsAddrs) -> Self {
        addrs.iter().collect()
    }
}

/// Errors constructing a list of zone external IPs.
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum ZoneExternalAddrsError {
    #[error("must provide at least one external address")]
    Empty,
    #[error(
        "too many external addresses: {count} (maximum is {MAX_ZONE_EXTERNAL_IPS})"
    )]
    TooMany { count: usize },
    #[error("external IP addresses must all be unique, but {ip} is duplicated")]
    DuplicateIp { ip: IpAddr },
}

/// Source NAT configuration for a boundary NTP zone.
///
/// Boundary NTP reaches upstream servers via source NAT and needs a source
/// address per IP version it wants to reach them on: at most one per family,
/// and at least one overall.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ZoneSnatConfig {
    Ipv4Only(SourceNatConfigV4),
    Ipv6Only(SourceNatConfigV6),
    DualStack { ipv4: SourceNatConfigV4, ipv6: SourceNatConfigV6 },
}

impl From<SourceNatConfigGeneric> for ZoneSnatConfig {
    fn from(snat: SourceNatConfigGeneric) -> Self {
        match snat.ip {
            IpAddr::V4(ip) => ZoneSnatConfig::Ipv4Only(SourceNatConfigV4 {
                ip,
                first_port: snat.first_port,
                last_port: snat.last_port,
            }),
            IpAddr::V6(ip) => ZoneSnatConfig::Ipv6Only(SourceNatConfigV6 {
                ip,
                first_port: snat.first_port,
                last_port: snat.last_port,
            }),
        }
    }
}

impl TryFrom<ZoneSnatConfig> for SourceNatConfigGeneric {
    type Error = external::Error;

    fn try_from(snat: ZoneSnatConfig) -> Result<Self, Self::Error> {
        match snat {
            ZoneSnatConfig::Ipv4Only(c) => Ok(SourceNatConfigGeneric {
                ip: IpAddr::V4(c.ip),
                first_port: c.first_port,
                last_port: c.last_port,
            }),
            ZoneSnatConfig::Ipv6Only(c) => Ok(SourceNatConfigGeneric {
                ip: IpAddr::V6(c.ip),
                first_port: c.first_port,
                last_port: c.last_port,
            }),
            ZoneSnatConfig::DualStack { .. } => {
                Err(external::Error::invalid_request(
                    "cannot represent a dual-stack boundary NTP SNAT \
                     configuration in this API version",
                ))
            }
        }
    }
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

/// Describes one Omicron-managed zone running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneConfig {
    pub id: OmicronZoneUuid,

    /// The pool on which we'll place this zone's root filesystem.
    ///
    /// Note that the root filesystem is transient -- the sled agent is
    /// permitted to destroy this dataset each time the zone is initialized.
    pub filesystem_pool: Option<ZpoolName>,
    pub zone_type: OmicronZoneType,
    // Use `InstallDataset` if this field is not present in a deserialized
    // blueprint or ledger.
    #[serde(default = "OmicronZoneImageSource::deserialize_default")]
    pub image_source: OmicronZoneImageSource,
}

impl IdOrdItem for OmicronZoneConfig {
    type Key<'a> = OmicronZoneUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
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
        snat: ZoneSnatConfig,
    },

    /// Type of clickhouse zone used for a single node clickhouse deployment
    Clickhouse {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    /// A zone used to run a Clickhouse Keeper node
    ///
    /// Keepers are only used in replicated clickhouse setups
    ClickhouseKeeper {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    /// A zone used to run a Clickhouse Server in a replicated deployment
    ClickhouseServer {
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
        /// The addresses at which the external DNS server is reachable.
        dns_addresses: ExternalDnsAddrs,
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
    },
    Nexus {
        /// The address at which the internal nexus server is reachable.
        internal_address: SocketAddrV6,
        /// The port at which the internal lockstep server is reachable. This
        /// shares the same IP address with `internal_address`.
        #[serde(default = "default_nexus_lockstep_port")]
        lockstep_port: u16,
        /// The addresses at which the external nexus server is reachable.
        external_ips: NexusExternalIps,
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

fn default_nexus_lockstep_port() -> u16 {
    omicron_common::address::NEXUS_LOCKSTEP_PORT
}

impl From<v11::inventory::OmicronZoneType> for OmicronZoneType {
    fn from(v11: v11::inventory::OmicronZoneType) -> Self {
        use v11::inventory::OmicronZoneType as Prev;
        match v11 {
            Prev::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Self::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat: snat_cfg.into(),
            },
            Prev::Clickhouse { address, dataset } => {
                Self::Clickhouse { address, dataset }
            }
            Prev::ClickhouseKeeper { address, dataset } => {
                Self::ClickhouseKeeper { address, dataset }
            }
            Prev::ClickhouseServer { address, dataset } => {
                Self::ClickhouseServer { address, dataset }
            }
            Prev::CockroachDb { address, dataset } => {
                Self::CockroachDb { address, dataset }
            }
            Prev::Crucible { address, dataset } => {
                Self::Crucible { address, dataset }
            }
            Prev::CruciblePantry { address } => {
                Self::CruciblePantry { address }
            }
            Prev::ExternalDns { dataset, http_address, dns_address, nic } => {
                Self::ExternalDns {
                    dataset,
                    http_address,
                    dns_addresses: ExternalDnsAddrs::from_single(dns_address),
                    nic,
                }
            }
            Prev::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => Self::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            },
            Prev::InternalNtp { address } => Self::InternalNtp { address },
            Prev::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Self::Nexus {
                internal_address,
                lockstep_port,
                external_ips: NexusExternalIps::from_single(external_ip),
                nic,
                external_tls,
                external_dns_servers,
            },
            Prev::Oximeter { address } => Self::Oximeter { address },
        }
    }
}

impl TryFrom<OmicronZoneType> for v11::inventory::OmicronZoneType {
    type Error = external::Error;

    fn try_from(new: OmicronZoneType) -> Result<Self, Self::Error> {
        match new {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat,
            } => Ok(Self::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg: snat.try_into()?,
            }),
            OmicronZoneType::Clickhouse { address, dataset } => {
                Ok(Self::Clickhouse { address, dataset })
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                Ok(Self::ClickhouseKeeper { address, dataset })
            }
            OmicronZoneType::ClickhouseServer { address, dataset } => {
                Ok(Self::ClickhouseServer { address, dataset })
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                Ok(Self::CockroachDb { address, dataset })
            }
            OmicronZoneType::Crucible { address, dataset } => {
                Ok(Self::Crucible { address, dataset })
            }
            OmicronZoneType::CruciblePantry { address } => {
                Ok(Self::CruciblePantry { address })
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_addresses,
                nic,
            } => {
                let dns_address =
                    dns_addresses.into_single().ok_or_else(|| {
                        external::Error::invalid_request(
                            "cannot represent multiple external DNS addresses \
                             in this API version",
                        )
                    })?;
                Ok(Self::ExternalDns {
                    dataset,
                    http_address,
                    dns_address,
                    nic,
                })
            }
            OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => Ok(Self::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            }),
            OmicronZoneType::InternalNtp { address } => {
                Ok(Self::InternalNtp { address })
            }
            OmicronZoneType::Nexus {
                internal_address,
                lockstep_port,
                external_ips,
                nic,
                external_tls,
                external_dns_servers,
            } => {
                let external_ip =
                    external_ips.into_single().ok_or_else(|| {
                        external::Error::invalid_request(
                            "cannot represent multiple Nexus external IPs in \
                             this API version",
                        )
                    })?;
                Ok(Self::Nexus {
                    internal_address,
                    lockstep_port,
                    external_ip,
                    nic,
                    external_tls,
                    external_dns_servers,
                })
            }
            OmicronZoneType::Oximeter { address } => {
                Ok(Self::Oximeter { address })
            }
        }
    }
}

impl From<v11::inventory::OmicronZoneConfig> for OmicronZoneConfig {
    fn from(v11: v11::inventory::OmicronZoneConfig) -> Self {
        Self {
            id: v11.id,
            filesystem_pool: v11.filesystem_pool,
            zone_type: v11.zone_type.into(),
            image_source: v11.image_source,
        }
    }
}

impl TryFrom<OmicronZoneConfig> for v11::inventory::OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(new: OmicronZoneConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            id: new.id,
            filesystem_pool: new.filesystem_pool,
            zone_type: new.zone_type.try_into()?,
            image_source: new.image_source,
        })
    }
}

impl From<v11::inventory::OmicronZonesConfig> for OmicronZonesConfig {
    fn from(v11: v11::inventory::OmicronZonesConfig) -> Self {
        Self {
            generation: v11.generation,
            zones: v11.zones.into_iter().map(Into::into).collect(),
        }
    }
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: SledConfigGeneration,
    pub disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    pub datasets: IdOrdMap<DatasetConfig>,
    pub zones: IdOrdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    #[serde(default = "HostPhase2DesiredSlots::current_contents")]
    pub host_phase_2: HostPhase2DesiredSlots,
    // We purposely skip a serde default here to work around some ledger
    // versioning quirks
    pub measurements: BTreeSet<OmicronSingleMeasurement>,
    pub update_disposition: OmicronSledUpdateDisposition,
}

// NOTE: Most trait impls live in the `impls` module of this crate and are only
// implemented for the `latest` version of each type. However,
// `OmicronSledConfig` is special: it's not only used in the sled-agent API
// (which would only require trait impls on `latest`); it's also ledgered to
// disk to support cold boot of the rack. In the ledgering case, we have to be
// able to handle reading older versions, which means all the old versions we
// support also need to implement `Ledgerable`. Therefore, we implement this
// trait for this specific version (and do so for every other version of
// `OmicronSledConfig` too).
impl Ledgerable for OmicronSledConfig {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    fn generation_bump(&mut self) {
        // DO NOTHING!
        //
        // Generation bumps must only ever come from nexus and will be encoded
        // in the struct itself
    }
}

impl From<v50::inventory::OmicronSledConfig> for OmicronSledConfig {
    fn from(old: v50::inventory::OmicronSledConfig) -> Self {
        Self {
            generation: old.generation,
            disks: old.disks,
            datasets: old.datasets,
            zones: old.zones.into_iter().map(Into::into).collect(),
            remove_mupdate_override: old.remove_mupdate_override,
            host_phase_2: old.host_phase_2,
            measurements: old.measurements,
            update_disposition: old.update_disposition,
        }
    }
}

impl TryFrom<OmicronSledConfig> for v50::inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(new: OmicronSledConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: new.generation,
            disks: new.disks,
            datasets: new.datasets,
            zones: new
                .zones
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            remove_mupdate_override: new.remove_mupdate_override,
            host_phase_2: new.host_phase_2,
            measurements: new.measurements,
            update_disposition: new.update_disposition,
        })
    }
}

/// Status of the sled-agent-config-reconciler task.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ConfigReconcilerInventoryStatus {
    /// The reconciler task has not yet run for the first time since sled-agent
    /// started.
    NotYetRun,
    /// The reconciler task is actively running.
    Running {
        config: Box<OmicronSledConfig>,
        started_at: DateTime<Utc>,
        running_for: Duration,
    },
    /// The reconciler task is currently idle, but previously did complete a
    /// reconciliation attempt.
    ///
    /// This variant does not include the `OmicronSledConfig` used in the last
    /// attempt, because that's always available via
    /// [`ConfigReconcilerInventory::last_reconciled_config`].
    Idle { completed_at: DateTime<Utc>, ran_for: Duration },
}

impl TryFrom<ConfigReconcilerInventoryStatus>
    for v50::inventory::ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        new: ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        match new {
            ConfigReconcilerInventoryStatus::NotYetRun => Ok(Self::NotYetRun),
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Ok(Self::Running {
                config: Box::new((*config).try_into()?),
                started_at,
                running_for,
            }),
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                Ok(Self::Idle { completed_at, ran_for })
            }
        }
    }
}

/// Describes the last attempt made by the sled-agent-config-reconciler to
/// reconcile the current sled config against the actual state of the sled.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigReconcilerInventory {
    pub last_reconciled_config: OmicronSledConfig,
    pub external_disks:
        BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult>,
    pub datasets: BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult>,
    pub orphaned_datasets: IdOrdMap<OrphanedDataset>,
    pub zones: BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult>,
    pub boot_partitions: BootPartitionContents,
    /// The result of removing the mupdate override file on disk.
    ///
    /// `None` if `remove_mupdate_override` was not provided in the sled config.
    pub remove_mupdate_override: Option<RemoveMupdateOverrideInventory>,
}

impl TryFrom<ConfigReconcilerInventory>
    for v50::inventory::ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(new: ConfigReconcilerInventory) -> Result<Self, Self::Error> {
        Ok(Self {
            last_reconciled_config: new.last_reconciled_config.try_into()?,
            external_disks: new.external_disks,
            datasets: new.datasets,
            orphaned_datasets: new.orphaned_datasets,
            zones: new.zones,
            boot_partitions: new.boot_partitions,
            remove_mupdate_override: new.remove_mupdate_override,
        })
    }
}

/// Identity and basic status information about this sled agent
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Inventory {
    pub sled_id: SledUuid,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub baseboard_id: BaseboardId,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,
    pub cpu_family: SledCpuFamily,
    pub reservoir_size: ByteCount,
    pub disks: Vec<InventoryDisk>,
    pub zpools: Vec<InventoryZpool>,
    pub datasets: Vec<InventoryDataset>,
    pub ledgered_sled_config: Option<OmicronSledConfig>,
    pub reconciler_status: ConfigReconcilerInventoryStatus,
    pub last_reconciliation: Option<ConfigReconcilerInventory>,
    pub file_source_resolver: OmicronFileSourceResolverInventory,
    pub smf_services_enabled_not_online: SvcsEnabledNotOnlineResult,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<FmdInventory, FmdInventoryError>::json_schema"
    )]
    pub fmd: Result<FmdInventory, FmdInventoryError>,
}

impl TryFrom<Inventory> for v50::inventory::Inventory {
    type Error = external::Error;

    fn try_from(new: Inventory) -> Result<Self, Self::Error> {
        Ok(Self {
            sled_id: new.sled_id,
            sled_agent_address: new.sled_agent_address,
            sled_role: new.sled_role,
            baseboard_id: new.baseboard_id,
            usable_hardware_threads: new.usable_hardware_threads,
            usable_physical_ram: new.usable_physical_ram,
            cpu_family: new.cpu_family,
            reservoir_size: new.reservoir_size,
            disks: new.disks,
            zpools: new.zpools,
            datasets: new.datasets,
            ledgered_sled_config: new
                .ledgered_sled_config
                .map(TryInto::try_into)
                .transpose()?,
            reconciler_status: new.reconciler_status.try_into()?,
            last_reconciliation: new
                .last_reconciliation
                .map(TryInto::try_into)
                .transpose()?,
            file_source_resolver: new.file_source_resolver,
            smf_services_enabled_not_online: new
                .smf_services_enabled_not_online,
            reference_measurements: new.reference_measurements,
            fmd: new.fmd,
        })
    }
}
