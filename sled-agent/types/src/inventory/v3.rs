// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_sled_agent_shared::inventory;
use nexus_sled_agent_shared::inventory::BootPartitionContents;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use nexus_sled_agent_shared::inventory::RemoveMupdateOverrideInventory;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use omicron_common::address::NEXUS_LOCKSTEP_PORT;
use omicron_common::api::external;
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

impl TryFrom<inventory::Inventory> for Inventory {
    type Error = external::Error;

    fn try_from(value: inventory::Inventory) -> Result<Self, Self::Error> {
        let ledgered_sled_config = value
            .ledgered_sled_config
            .map(OmicronSledConfig::try_from)
            .transpose()?;
        let reconciler_status = value.reconciler_status.try_into()?;
        let last_reconciliation = value
            .last_reconciliation
            .map(ConfigReconcilerInventory::try_from)
            .transpose()?;
        Ok(Self {
            sled_id: value.sled_id,
            sled_agent_address: value.sled_agent_address,
            sled_role: value.sled_role,
            baseboard: value.baseboard,
            usable_hardware_threads: value.usable_hardware_threads,
            usable_physical_ram: value.usable_physical_ram,
            cpu_family: value.cpu_family,
            reservoir_size: value.reservoir_size,
            disks: value.disks,
            zpools: value.zpools,
            datasets: value.datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver: value.zone_image_resolver,
        })
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

impl TryFrom<OmicronSledConfig> for inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: OmicronSledConfig) -> Result<Self, Self::Error> {
        let zones = value
            .zones
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        })
    }
}

impl TryFrom<inventory::OmicronSledConfig> for OmicronSledConfig {
    type Error = external::Error;

    fn try_from(
        value: inventory::OmicronSledConfig,
    ) -> Result<Self, Self::Error> {
        let zones = value
            .zones
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        })
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

impl TryFrom<OmicronZoneConfig> for inventory::OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(value: OmicronZoneConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type.try_into()?,
            image_source: value.image_source,
        })
    }
}

impl TryFrom<inventory::OmicronZoneConfig> for OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(
        value: inventory::OmicronZoneConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type.try_into()?,
            image_source: value.image_source,
        })
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

impl TryFrom<OmicronZoneType> for inventory::OmicronZoneType {
    type Error = external::Error;

    fn try_from(value: OmicronZoneType) -> Result<Self, Self::Error> {
        match value {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Ok(Self::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic: nic.try_into()?,
                snat_cfg,
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
                dns_address,
                nic,
            } => Ok(Self::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic: nic.try_into()?,
            }),
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
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(Self::Nexus {
                internal_address,
                lockstep_port: NEXUS_LOCKSTEP_PORT,
                external_ip,
                nic: nic.try_into()?,
                external_tls,
                external_dns_servers,
            }),
            OmicronZoneType::Oximeter { address } => {
                Ok(Self::Oximeter { address })
            }
        }
    }
}

impl TryFrom<inventory::OmicronZoneType> for OmicronZoneType {
    type Error = external::Error;

    fn try_from(
        value: inventory::OmicronZoneType,
    ) -> Result<Self, Self::Error> {
        match value {
            inventory::OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Ok(Self::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic: nic.try_into()?,
                snat_cfg,
            }),
            inventory::OmicronZoneType::Clickhouse { address, dataset } => {
                Ok(Self::Clickhouse { address, dataset })
            }
            inventory::OmicronZoneType::ClickhouseKeeper {
                address,
                dataset,
            } => Ok(Self::ClickhouseKeeper { address, dataset }),
            inventory::OmicronZoneType::ClickhouseServer {
                address,
                dataset,
            } => Ok(Self::ClickhouseServer { address, dataset }),
            inventory::OmicronZoneType::CockroachDb { address, dataset } => {
                Ok(Self::CockroachDb { address, dataset })
            }
            inventory::OmicronZoneType::Crucible { address, dataset } => {
                Ok(Self::Crucible { address, dataset })
            }
            inventory::OmicronZoneType::CruciblePantry { address } => {
                Ok(Self::CruciblePantry { address })
            }
            inventory::OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => Ok(Self::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic: nic.try_into()?,
            }),
            inventory::OmicronZoneType::InternalDns {
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
            inventory::OmicronZoneType::InternalNtp { address } => {
                Ok(Self::InternalNtp { address })
            }
            inventory::OmicronZoneType::Nexus {
                internal_address,
                lockstep_port: _,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(Self::Nexus {
                internal_address,
                external_ip,
                nic: nic.try_into()?,
                external_tls,
                external_dns_servers,
            }),
            inventory::OmicronZoneType::Oximeter { address } => {
                Ok(Self::Oximeter { address })
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

impl TryFrom<inventory::ConfigReconcilerInventory>
    for ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(
        value: inventory::ConfigReconcilerInventory,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            last_reconciled_config: value.last_reconciled_config.try_into()?,
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions,
            remove_mupdate_override: value.remove_mupdate_override,
        })
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

impl TryFrom<inventory::ConfigReconcilerInventoryStatus>
    for ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        value: inventory::ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        match value {
            inventory::ConfigReconcilerInventoryStatus::NotYetRun => {
                Ok(Self::NotYetRun)
            }
            inventory::ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Ok(Self::Running {
                config: Box::new((*config).try_into()?),
                started_at,
                running_for,
            }),
            inventory::ConfigReconcilerInventoryStatus::Idle {
                completed_at,
                ran_for,
            } => Ok(Self::Idle { completed_at, ran_for }),
        }
    }
}
