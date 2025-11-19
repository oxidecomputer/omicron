// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_sled_agent_shared::inventory::{
    BootPartitionContents, ConfigReconcilerInventoryResult,
    HostPhase2DesiredSlots, InventoryDataset, InventoryDisk, InventoryZpool,
    OmicronZoneDataset, OmicronZoneImageSource, OrphanedDataset,
    RemoveMupdateOverrideInventory, SledRole, ZoneImageResolverInventory,
};
use omicron_common::address::NEXUS_LOCKSTEP_PORT;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::v8;
use sled_hardware_types::Baseboard;
use sled_hardware_types::SledCpuFamily;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::time::Duration;

/// Identity and basic status information about this sled agent
#[derive(Deserialize, Serialize, JsonSchema)]
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

impl From<Inventory> for v8::Inventory {
    fn from(value: Inventory) -> Self {
        let Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
        } = value;
        Self {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config: ledgered_sled_config.map(Into::into),
            reconciler_status: reconciler_status.into(),
            last_reconciliation: last_reconciliation.map(Into::into),
            zone_image_resolver,
        }
    }
}

impl From<v8::Inventory> for Inventory {
    fn from(value: v8::Inventory) -> Self {
        let v8::Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
        } = value;
        Self {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config: ledgered_sled_config.map(Into::into),
            reconciler_status: reconciler_status.into(),
            last_reconciliation: last_reconciliation.map(Into::into),
            zone_image_resolver,
        }
    }
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct OmicronSledConfig {
    pub generation: Generation,
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

impl From<OmicronSledConfig> for v8::OmicronSledConfig {
    fn from(value: OmicronSledConfig) -> Self {
        Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones.into_iter().map(Into::into).collect(),
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        }
    }
}

impl From<v8::OmicronSledConfig> for OmicronSledConfig {
    fn from(value: v8::OmicronSledConfig) -> Self {
        Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones.into_iter().map(Into::into).collect(),
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        }
    }
}

/// Describes one Omicron-managed zone running on a sled
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
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

impl From<OmicronZoneConfig> for v8::OmicronZoneConfig {
    fn from(value: OmicronZoneConfig) -> Self {
        Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type.into(),
            image_source: value.image_source,
        }
    }
}

impl From<v8::OmicronZoneConfig> for OmicronZoneConfig {
    fn from(value: v8::OmicronZoneConfig) -> Self {
        Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type.into(),
            image_source: value.image_source,
        }
    }
}

/// Describes what kind of zone this is (i.e., what component is running in it)
/// as well as any type-specific configuration
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
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

impl From<OmicronZoneType> for v8::OmicronZoneType {
    fn from(value: OmicronZoneType) -> Self {
        match value {
            OmicronZoneType::BoundaryNtp {
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
                snat_cfg,
            },
            OmicronZoneType::Clickhouse { address, dataset } => {
                Self::Clickhouse { address, dataset }
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                Self::ClickhouseKeeper { address, dataset }
            }
            OmicronZoneType::ClickhouseServer { address, dataset } => {
                Self::ClickhouseServer { address, dataset }
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                Self::CockroachDb { address, dataset }
            }
            OmicronZoneType::Crucible { address, dataset } => {
                Self::Crucible { address, dataset }
            }
            OmicronZoneType::CruciblePantry { address } => {
                Self::CruciblePantry { address }
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => Self::ExternalDns { dataset, http_address, dns_address, nic },
            OmicronZoneType::InternalDns {
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
            OmicronZoneType::InternalNtp { address } => {
                Self::InternalNtp { address }
            }
            OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Self::Nexus {
                internal_address,
                lockstep_port: NEXUS_LOCKSTEP_PORT,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            },
            OmicronZoneType::Oximeter { address } => Self::Oximeter { address },
        }
    }
}

impl From<v8::OmicronZoneType> for OmicronZoneType {
    fn from(value: v8::OmicronZoneType) -> Self {
        match value {
            v8::OmicronZoneType::BoundaryNtp {
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
                snat_cfg,
            },
            v8::OmicronZoneType::Clickhouse { address, dataset } => {
                Self::Clickhouse { address, dataset }
            }
            v8::OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                Self::ClickhouseKeeper { address, dataset }
            }
            v8::OmicronZoneType::ClickhouseServer { address, dataset } => {
                Self::ClickhouseServer { address, dataset }
            }
            v8::OmicronZoneType::CockroachDb { address, dataset } => {
                Self::CockroachDb { address, dataset }
            }
            v8::OmicronZoneType::Crucible { address, dataset } => {
                Self::Crucible { address, dataset }
            }
            v8::OmicronZoneType::CruciblePantry { address } => {
                Self::CruciblePantry { address }
            }
            v8::OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => Self::ExternalDns { dataset, http_address, dns_address, nic },
            v8::OmicronZoneType::InternalDns {
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
            v8::OmicronZoneType::InternalNtp { address } => {
                Self::InternalNtp { address }
            }
            v8::OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
                lockstep_port: _,
            } => Self::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            },
            v8::OmicronZoneType::Oximeter { address } => {
                Self::Oximeter { address }
            }
        }
    }
}

/// Describes the last attempt made by the sled-agent-config-reconciler to
/// reconcile the current sled config against the actual state of the sled.
#[derive(Deserialize, Serialize, JsonSchema)]
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

impl From<ConfigReconcilerInventory> for v8::ConfigReconcilerInventory {
    fn from(value: ConfigReconcilerInventory) -> Self {
        Self {
            last_reconciled_config: value.last_reconciled_config.into(),
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions,
            remove_mupdate_override: value.remove_mupdate_override,
        }
    }
}

impl From<v8::ConfigReconcilerInventory> for ConfigReconcilerInventory {
    fn from(value: v8::ConfigReconcilerInventory) -> Self {
        Self {
            last_reconciled_config: value.last_reconciled_config.into(),
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions,
            remove_mupdate_override: value.remove_mupdate_override,
        }
    }
}

/// Status of the sled-agent-config-reconciler task.
#[derive(Deserialize, Serialize, JsonSchema)]
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

impl From<ConfigReconcilerInventoryStatus>
    for v8::ConfigReconcilerInventoryStatus
{
    fn from(value: ConfigReconcilerInventoryStatus) -> Self {
        match value {
            ConfigReconcilerInventoryStatus::NotYetRun => Self::NotYetRun,
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Self::Running {
                config: Box::new((*config).into()),
                started_at,
                running_for,
            },
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                Self::Idle { completed_at, ran_for }
            }
        }
    }
}

impl From<v8::ConfigReconcilerInventoryStatus>
    for ConfigReconcilerInventoryStatus
{
    fn from(value: v8::ConfigReconcilerInventoryStatus) -> Self {
        match value {
            v8::ConfigReconcilerInventoryStatus::NotYetRun => Self::NotYetRun,
            v8::ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Self::Running {
                config: Box::new((*config).into()),
                started_at,
                running_for,
            },
            v8::ConfigReconcilerInventoryStatus::Idle {
                completed_at,
                ran_for,
            } => Self::Idle { completed_at, ran_for },
        }
    }
}
