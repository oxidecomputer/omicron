// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;

use chrono::{DateTime, Utc};
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::{
    address::{Ip, NUM_SOURCE_NAT_PORTS},
    api::external::{ByteCount, Generation},
    disk::{DatasetConfig, OmicronPhysicalDiskConfig},
    zpool_name::ZpoolName,
};
use omicron_ledger::Ledgerable;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::{DatasetUuid, OmicronZoneUuid};
use omicron_uuid_kinds::{MupdateOverrideUuid, PhysicalDiskUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::impls::inventory::SourceNatConfigError;
use crate::v1::inventory::{
    BootPartitionContents, ConfigReconcilerInventoryResult,
    HostPhase2DesiredSlots, InventoryDataset, InventoryDisk, InventoryZpool,
    OmicronZoneDataset, OmicronZoneImageSource, OrphanedDataset,
    RemoveMupdateOverrideInventory, SledRole, ZoneImageResolverInventory,
};
use crate::v10;
use crate::v10::inventory::NetworkInterface;
use sled_hardware_types::{Baseboard, SledCpuFamily};

/// Identity and basic status information about this sled agent
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Inventory {
    pub sled_id: SledUuid,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub baseboard: Baseboard,
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
    pub zone_image_resolver: ZoneImageResolverInventory,
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

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: Generation,
    // Serialize and deserialize disks, datasets, and zones as maps for
    // backwards compatibility. Newer IdOrdMaps should not use IdOrdMapAsMap.
    #[serde(
        with = "iddqd::id_ord_map::IdOrdMapAsMap::<OmicronPhysicalDiskConfig>"
    )]
    pub disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    #[serde(with = "iddqd::id_ord_map::IdOrdMapAsMap::<DatasetConfig>")]
    pub datasets: IdOrdMap<DatasetConfig>,
    #[serde(with = "iddqd::id_ord_map::IdOrdMapAsMap::<OmicronZoneConfig>")]
    pub zones: IdOrdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    #[serde(default = "HostPhase2DesiredSlots::current_contents")]
    pub host_phase_2: HostPhase2DesiredSlots,
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
        snat_cfg: SourceNatConfigGeneric,
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
    },
    Nexus {
        /// The address at which the internal nexus server is reachable.
        internal_address: SocketAddrV6,
        /// The port at which the internal lockstep server is reachable. This
        /// shares the same IP address with `internal_address`.
        #[serde(default = "default_nexus_lockstep_port")]
        lockstep_port: u16,
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

fn default_nexus_lockstep_port() -> u16 {
    omicron_common::address::NEXUS_LOCKSTEP_PORT
}

use omicron_common::api::external;

impl TryFrom<v10::inventory::Inventory> for Inventory {
    type Error = external::Error;

    fn try_from(v10: v10::inventory::Inventory) -> Result<Self, Self::Error> {
        Ok(Self {
            sled_id: v10.sled_id,
            sled_agent_address: v10.sled_agent_address,
            sled_role: v10.sled_role,
            baseboard: v10.baseboard,
            usable_hardware_threads: v10.usable_hardware_threads,
            usable_physical_ram: v10.usable_physical_ram,
            cpu_family: v10.cpu_family,
            reservoir_size: v10.reservoir_size,
            disks: v10.disks,
            zpools: v10.zpools,
            datasets: v10.datasets,
            ledgered_sled_config: v10
                .ledgered_sled_config
                .map(TryInto::try_into)
                .transpose()?,
            reconciler_status: v10.reconciler_status.try_into()?,
            last_reconciliation: v10
                .last_reconciliation
                .map(TryInto::try_into)
                .transpose()?,
            zone_image_resolver: v10.zone_image_resolver,
        })
    }
}

impl TryFrom<v10::inventory::OmicronSledConfig> for OmicronSledConfig {
    type Error = external::Error;

    fn try_from(
        v10: v10::inventory::OmicronSledConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: v10.generation,
            disks: v10.disks,
            datasets: v10.datasets,
            zones: v10
                .zones
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            remove_mupdate_override: v10.remove_mupdate_override,
            host_phase_2: v10.host_phase_2,
        })
    }
}

impl TryFrom<v10::inventory::OmicronZoneConfig> for OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(
        v10: v10::inventory::OmicronZoneConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v10.id,
            filesystem_pool: v10.filesystem_pool,
            zone_type: v10.zone_type.try_into()?,
            image_source: v10.image_source,
        })
    }
}

impl TryFrom<v10::inventory::OmicronZoneType> for OmicronZoneType {
    type Error = external::Error;

    fn try_from(
        v10: v10::inventory::OmicronZoneType,
    ) -> Result<Self, Self::Error> {
        match v10 {
            v10::inventory::OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => {
                let snat_cfg = SourceNatConfigGeneric::new(
                    snat_cfg.ip,
                    snat_cfg.first_port,
                    snat_cfg.last_port,
                )
                .map_err(|e| external::Error::invalid_request(e.to_string()))?;
                Ok(Self::BoundaryNtp {
                    address,
                    ntp_servers,
                    dns_servers,
                    domain,
                    nic,
                    snat_cfg,
                })
            }
            v10::inventory::OmicronZoneType::Clickhouse {
                address,
                dataset,
            } => Ok(OmicronZoneType::Clickhouse { address, dataset }),
            v10::inventory::OmicronZoneType::ClickhouseKeeper {
                address,
                dataset,
            } => Ok(OmicronZoneType::ClickhouseKeeper { address, dataset }),
            v10::inventory::OmicronZoneType::ClickhouseServer {
                address,
                dataset,
            } => Ok(OmicronZoneType::ClickhouseServer { address, dataset }),
            v10::inventory::OmicronZoneType::CockroachDb {
                address,
                dataset,
            } => Ok(OmicronZoneType::CockroachDb { address, dataset }),
            v10::inventory::OmicronZoneType::Crucible { address, dataset } => {
                Ok(OmicronZoneType::Crucible { address, dataset })
            }
            v10::inventory::OmicronZoneType::CruciblePantry { address } => {
                Ok(OmicronZoneType::CruciblePantry { address })
            }
            v10::inventory::OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => Ok(OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            }),
            v10::inventory::OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => Ok(OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            }),
            v10::inventory::OmicronZoneType::InternalNtp { address } => {
                Ok(OmicronZoneType::InternalNtp { address })
            }
            v10::inventory::OmicronZoneType::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(OmicronZoneType::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            }),
            v10::inventory::OmicronZoneType::Oximeter { address } => {
                Ok(OmicronZoneType::Oximeter { address })
            }
        }
    }
}

impl TryFrom<v10::inventory::ConfigReconcilerInventory>
    for ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(
        v10: v10::inventory::ConfigReconcilerInventory,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            last_reconciled_config: v10.last_reconciled_config.try_into()?,
            external_disks: v10.external_disks,
            datasets: v10.datasets,
            orphaned_datasets: v10.orphaned_datasets,
            zones: v10.zones,
            boot_partitions: v10.boot_partitions,
            remove_mupdate_override: v10.remove_mupdate_override,
        })
    }
}

impl TryFrom<v10::inventory::ConfigReconcilerInventoryStatus>
    for ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        v10: v10::inventory::ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        match v10 {
            v10::inventory::ConfigReconcilerInventoryStatus::NotYetRun => {
                Ok(ConfigReconcilerInventoryStatus::NotYetRun)
            }
            v10::inventory::ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Ok(ConfigReconcilerInventoryStatus::Running {
                config: Box::new((*config).try_into()?),
                started_at,
                running_for,
            }),
            v10::inventory::ConfigReconcilerInventoryStatus::Idle {
                completed_at,
                ran_for,
            } => Ok(ConfigReconcilerInventoryStatus::Idle {
                completed_at,
                ran_for,
            }),
        }
    }
}

impl TryFrom<v10::inventory::OmicronZonesConfig> for OmicronZonesConfig {
    type Error = external::Error;

    fn try_from(
        v10: v10::inventory::OmicronZonesConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: v10.generation,
            zones: v10
                .zones
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

// Conversions from v11 to v10 for response types
impl TryFrom<Inventory> for v10::inventory::Inventory {
    type Error = external::Error;

    fn try_from(v11: Inventory) -> Result<Self, Self::Error> {
        Ok(Self {
            sled_id: v11.sled_id,
            sled_agent_address: v11.sled_agent_address,
            sled_role: v11.sled_role,
            baseboard: v11.baseboard,
            usable_hardware_threads: v11.usable_hardware_threads,
            usable_physical_ram: v11.usable_physical_ram,
            cpu_family: v11.cpu_family,
            reservoir_size: v11.reservoir_size,
            disks: v11.disks,
            zpools: v11.zpools,
            datasets: v11.datasets,
            ledgered_sled_config: v11
                .ledgered_sled_config
                .map(TryInto::try_into)
                .transpose()?,
            reconciler_status: v11.reconciler_status.try_into()?,
            last_reconciliation: v11
                .last_reconciliation
                .map(TryInto::try_into)
                .transpose()?,
            zone_image_resolver: v11.zone_image_resolver,
        })
    }
}

impl TryFrom<OmicronSledConfig> for v10::inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(v11: OmicronSledConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: v11.generation,
            disks: v11.disks,
            datasets: v11.datasets,
            zones: v11
                .zones
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            remove_mupdate_override: v11.remove_mupdate_override,
            host_phase_2: v11.host_phase_2,
        })
    }
}

impl TryFrom<OmicronZoneConfig> for v10::inventory::OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(v11: OmicronZoneConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v11.id,
            filesystem_pool: v11.filesystem_pool,
            zone_type: v11.zone_type.try_into()?,
            image_source: v11.image_source,
        })
    }
}

impl TryFrom<OmicronZoneType> for v10::inventory::OmicronZoneType {
    type Error = external::Error;

    fn try_from(v11: OmicronZoneType) -> Result<Self, Self::Error> {
        use crate::v1::inventory::SourceNatConfig;

        match v11 {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Ok(v10::inventory::OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg: SourceNatConfig::new(
                    snat_cfg.ip,
                    snat_cfg.first_port,
                    snat_cfg.last_port,
                )
                .map_err(|e| {
                    external::Error::invalid_request(format!(
                        "invalid SNAT config: {e}"
                    ))
                })?,
            }),
            OmicronZoneType::Clickhouse { address, dataset } => {
                Ok(v10::inventory::OmicronZoneType::Clickhouse {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                Ok(v10::inventory::OmicronZoneType::ClickhouseKeeper {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::ClickhouseServer { address, dataset } => {
                Ok(v10::inventory::OmicronZoneType::ClickhouseServer {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                Ok(v10::inventory::OmicronZoneType::CockroachDb {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::Crucible { address, dataset } => {
                Ok(v10::inventory::OmicronZoneType::Crucible {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::CruciblePantry { address } => {
                Ok(v10::inventory::OmicronZoneType::CruciblePantry { address })
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => Ok(v10::inventory::OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            }),
            OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => Ok(v10::inventory::OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            }),
            OmicronZoneType::InternalNtp { address } => {
                Ok(v10::inventory::OmicronZoneType::InternalNtp { address })
            }
            OmicronZoneType::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(v10::inventory::OmicronZoneType::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            }),
            OmicronZoneType::Oximeter { address } => {
                Ok(v10::inventory::OmicronZoneType::Oximeter { address })
            }
        }
    }
}

impl TryFrom<ConfigReconcilerInventory>
    for v10::inventory::ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(v11: ConfigReconcilerInventory) -> Result<Self, Self::Error> {
        Ok(Self {
            last_reconciled_config: v11.last_reconciled_config.try_into()?,
            external_disks: v11.external_disks,
            datasets: v11.datasets,
            orphaned_datasets: v11.orphaned_datasets,
            zones: v11.zones,
            boot_partitions: v11.boot_partitions,
            remove_mupdate_override: v11.remove_mupdate_override,
        })
    }
}

impl TryFrom<ConfigReconcilerInventoryStatus>
    for v10::inventory::ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        v11: ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        match v11 {
            ConfigReconcilerInventoryStatus::NotYetRun => {
                Ok(v10::inventory::ConfigReconcilerInventoryStatus::NotYetRun)
            }
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Ok(v10::inventory::ConfigReconcilerInventoryStatus::Running {
                config: Box::new((*config).try_into()?),
                started_at,
                running_for,
            }),
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                Ok(v10::inventory::ConfigReconcilerInventoryStatus::Idle {
                    completed_at,
                    ran_for,
                })
            }
        }
    }
}

impl TryFrom<OmicronZonesConfig> for v10::inventory::OmicronZonesConfig {
    type Error = external::Error;

    fn try_from(v11: OmicronZonesConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: v11.generation,
            zones: v11
                .zones
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Helper trait specifying the name of the JSON Schema for a `SourceNatConfig`.
///
/// This exists so we can use a generic type and have the names of the concrete
/// type aliases be the same as the name of schema object.
pub trait SnatSchema {
    fn json_schema_name() -> String;
}

impl SnatSchema for Ipv4Addr {
    fn json_schema_name() -> String {
        String::from("SourceNatConfigV4")
    }
}

impl SnatSchema for Ipv6Addr {
    fn json_schema_name() -> String {
        String::from("SourceNatConfigV6")
    }
}

impl SnatSchema for IpAddr {
    fn json_schema_name() -> String {
        String::from("SourceNatConfigGeneric")
    }
}

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Note that `Deserialize` is manually implemented; if you make any changes to
// the fields of this structure, you must make them to that implementation too.
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    daft::Diffable,
)]
pub struct SourceNatConfig<T: Ip> {
    /// The external address provided to the instance or service.
    pub ip: T,
    /// The first port used for source NAT, inclusive.
    pub(crate) first_port: u16,
    /// The last port used for source NAT, also inclusive.
    pub(crate) last_port: u16,
}

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Private type only used for deriving the actual JSON schema object itself,
// and for checked deserialization.
//
// The fields of `SourceNatConfigShadow` should exactly match the fields
// of `SourceNatConfig`. We're not really using serde's remote derive,
// but by adding the attribute we get compile-time checking that all the
// field names and types match. (It doesn't check the _order_, but that
// should be fine as long as we're using JSON or similar formats.)
#[derive(Deserialize, JsonSchema)]
#[serde(remote = "SourceNatConfig")]
struct SourceNatConfigShadow<T: Ip + SnatSchema> {
    /// The external address provided to the instance or service.
    ip: T,
    /// The first port used for source NAT, inclusive.
    first_port: u16,
    /// The last port used for source NAT, also inclusive.
    last_port: u16,
}

pub type SourceNatConfigV4 = SourceNatConfig<Ipv4Addr>;
pub type SourceNatConfigV6 = SourceNatConfig<Ipv6Addr>;
pub type SourceNatConfigGeneric = SourceNatConfig<IpAddr>;

impl<T> JsonSchema for SourceNatConfig<T>
where
    T: Ip + SnatSchema,
{
    fn schema_name() -> String {
        <T as SnatSchema>::json_schema_name()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        SourceNatConfigShadow::<T>::json_schema(generator)
    }
}

// We implement `Deserialize` manually to add validity checking on the port
// range.
impl<'de, T> Deserialize<'de> for SourceNatConfig<T>
where
    T: Ip + SnatSchema + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let shadow = SourceNatConfigShadow::deserialize(deserializer)?;
        SourceNatConfig::new(shadow.ip, shadow.first_port, shadow.last_port)
            .map_err(D::Error::custom)
    }
}

// This impl is here instead of crate::impls because we need it in our
// Deserialize implementation.
impl<T: Ip> SourceNatConfig<T> {
    /// Construct a `SourceNatConfig` with the given port range, both inclusive.
    ///
    /// # Errors
    ///
    /// Fails if `(first_port, last_port)` is not aligned to
    /// [`NUM_SOURCE_NAT_PORTS`].
    pub fn new(
        ip: T,
        first_port: u16,
        last_port: u16,
    ) -> Result<Self, SourceNatConfigError> {
        if first_port.is_multiple_of(NUM_SOURCE_NAT_PORTS)
            && last_port
                .checked_sub(first_port)
                .and_then(|diff| diff.checked_add(1))
                == Some(NUM_SOURCE_NAT_PORTS)
        {
            Ok(Self { ip, first_port, last_port })
        } else {
            Err(SourceNatConfigError::UnalignedPortPair {
                first_port,
                last_port,
            })
        }
    }
}
