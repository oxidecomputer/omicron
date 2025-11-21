// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 10 of the sled-agent API types.

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
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::ExternalIpConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use omicron_common::api::internal::shared::SourceNatConfigGeneric;
use omicron_common::api::internal::shared::external_ip::v1::SourceNatConfig;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::instance::InstanceMetadata;
use sled_agent_types::instance::InstanceMulticastMembership;
use sled_agent_types::instance::VmmSpec;
use sled_agent_types::inventory::v9;
use sled_hardware_types::Baseboard;
use sled_hardware_types::SledCpuFamily;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::time::Duration;
use uuid::Uuid;

// Need conversions between v9 to live here, since this isn't public in
// sled-agent-types
//
// Need conversions between v10 and current, in `nexus_sled_agent_shared` and
// friends.

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// The virtual hardware configuration this virtual machine should have when
    /// it is started.
    pub vmm_spec: VmmSpec,

    /// Information about the sled-local configuration that needs to be
    /// established to make the VM's virtual hardware fully functional.
    pub local_config: InstanceSledLocalConfig,

    /// The initial VMM runtime state for the VMM being registered.
    pub vmm_runtime: VmmRuntimeState,

    /// The ID of the instance for which this VMM is being created.
    pub instance_id: InstanceUuid,

    /// The ID of the migration in to this VMM, if this VMM is being
    /// ensured is part of a migration in. If this is `None`, the VMM is not
    /// being created due to a migration.
    pub migration_id: Option<Uuid>,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,

    /// Metadata used to track instance statistics.
    pub metadata: InstanceMetadata,
}

impl TryFrom<v9::InstanceEnsureBody> for InstanceEnsureBody {
    type Error = external::Error;

    fn try_from(value: v9::InstanceEnsureBody) -> Result<Self, Self::Error> {
        let v9::InstanceEnsureBody {
            vmm_spec,
            local_config,
            vmm_runtime,
            instance_id,
            migration_id,
            propolis_addr,
            metadata,
        } = value;
        let local_config = local_config.try_into()?;
        Ok(Self {
            vmm_spec,
            local_config,
            vmm_runtime,
            instance_id,
            migration_id,
            propolis_addr,
            metadata,
        })
    }
}

impl TryFrom<InstanceEnsureBody>
    for sled_agent_types::instance::InstanceEnsureBody
{
    type Error = external::Error;

    fn try_from(value: InstanceEnsureBody) -> Result<Self, Self::Error> {
        let InstanceEnsureBody {
            vmm_spec,
            local_config,
            vmm_runtime,
            instance_id,
            migration_id,
            propolis_addr,
            metadata,
        } = value;
        let local_config = local_config.try_into()?;
        Ok(Self {
            vmm_spec,
            local_config,
            vmm_runtime,
            instance_id,
            migration_id,
            propolis_addr,
            metadata,
        })
    }
}

/// Describes sled-local configuration that a sled-agent must establish to make
/// the instance's virtual hardware fully functional.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceSledLocalConfig {
    pub hostname: Hostname,
    pub nics: Vec<NetworkInterface>,
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: Vec<IpAddr>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl TryFrom<v9::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    type Error = external::Error;

    fn try_from(
        value: v9::InstanceSledLocalConfig,
    ) -> Result<Self, Self::Error> {
        let v9::InstanceSledLocalConfig {
            hostname,
            nics,
            source_nat,
            ephemeral_ip,
            floating_ips,
            multicast_groups,
            firewall_rules,
            dhcp_config,
            delegated_zvols,
        } = value;
        let nics = nics
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        let firewall_rules = firewall_rules
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            hostname,
            nics,
            source_nat,
            ephemeral_ip,
            floating_ips,
            multicast_groups,
            firewall_rules,
            dhcp_config,
            delegated_zvols,
        })
    }
}

impl TryFrom<InstanceSledLocalConfig>
    for sled_agent_types::instance::InstanceSledLocalConfig
{
    type Error = external::Error;

    fn try_from(value: InstanceSledLocalConfig) -> Result<Self, Self::Error> {
        let InstanceSledLocalConfig {
            hostname,
            nics,
            source_nat,
            ephemeral_ip,
            floating_ips,
            multicast_groups,
            firewall_rules,
            dhcp_config,
            delegated_zvols,
        } = value;
        let external_ips = ExternalIpConfig::try_from_generic(
            Some(source_nat),
            ephemeral_ip,
            floating_ips,
        )
        .map_err(|e| external::Error::invalid_request(e.to_string()))?;
        Ok(Self {
            hostname,
            nics,
            external_ips,
            multicast_groups,
            firewall_rules,
            dhcp_config,
            delegated_zvols,
        })
    }
}

/// Describes what kind of zone this is (i.e., what component is running in it)
/// as well as any type-specific configuration
#[derive(
    Clone, Debug, Deserialize, Eq, Serialize, JsonSchema, PartialEq, Hash,
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

const fn default_nexus_lockstep_port() -> u16 {
    omicron_common::address::NEXUS_LOCKSTEP_PORT
}

impl TryFrom<v9::OmicronZoneType> for OmicronZoneType {
    type Error = external::Error;

    fn try_from(value: v9::OmicronZoneType) -> Result<Self, Self::Error> {
        match value {
            v9::OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => {
                let nic = nic.try_into()?;
                Ok(Self::BoundaryNtp {
                    address,
                    ntp_servers,
                    dns_servers,
                    domain,
                    nic,
                    snat_cfg,
                })
            }
            v9::OmicronZoneType::Clickhouse { address, dataset } => {
                Ok(Self::Clickhouse { address, dataset })
            }
            v9::OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                Ok(Self::ClickhouseKeeper { address, dataset })
            }
            v9::OmicronZoneType::ClickhouseServer { address, dataset } => {
                Ok(Self::ClickhouseServer { address, dataset })
            }
            v9::OmicronZoneType::CockroachDb { address, dataset } => {
                Ok(Self::CockroachDb { address, dataset })
            }
            v9::OmicronZoneType::Crucible { address, dataset } => {
                Ok(Self::Crucible { address, dataset })
            }
            v9::OmicronZoneType::CruciblePantry { address } => {
                Ok(Self::CruciblePantry { address })
            }
            v9::OmicronZoneType::ExternalDns {
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
            v9::OmicronZoneType::InternalDns {
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
            v9::OmicronZoneType::InternalNtp { address } => {
                Ok(Self::InternalNtp { address })
            }
            v9::OmicronZoneType::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(Self::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic: nic.try_into()?,
                external_tls,
                external_dns_servers,
            }),
            v9::OmicronZoneType::Oximeter { address } => {
                Ok(Self::Oximeter { address })
            }
        }
    }
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
            } => {
                let (first_port, last_port) = snat_cfg.port_range_raw();
                let snat_cfg = SourceNatConfigGeneric::new(
                    snat_cfg.ip,
                    first_port,
                    last_port,
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
                nic,
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
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(Self::Nexus {
                internal_address,
                lockstep_port,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            }),
            OmicronZoneType::Oximeter { address } => {
                Ok(Self::Oximeter { address })
            }
        }
    }
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Eq, Serialize, JsonSchema, PartialEq)]
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

impl TryFrom<v9::OmicronSledConfig> for OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: v9::OmicronSledConfig) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<OmicronSledConfig> for inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: OmicronSledConfig) -> Result<Self, Self::Error> {
        todo!()
    }
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

impl TryFrom<v9::OmicronZoneConfig> for OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(value: v9::OmicronZoneConfig) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<OmicronZoneConfig> for inventory::OmicronZoneConfig {
    type Error = external::Error;

    fn try_from(value: OmicronZoneConfig) -> Result<Self, Self::Error> {
        todo!()
    }
}

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

impl TryFrom<Inventory> for v9::Inventory {
    type Error = external::Error;

    fn try_from(value: Inventory) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<Inventory> for inventory::Inventory {
    type Error = external::Error;

    fn try_from(value: Inventory) -> Result<Self, Self::Error> {
        todo!();
    }
}

impl TryFrom<inventory::Inventory> for Inventory {
    type Error = external::Error;

    fn try_from(value: inventory::Inventory) -> Result<Self, Self::Error> {
        todo!();
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

impl TryFrom<v9::ConfigReconcilerInventory> for ConfigReconcilerInventory {
    type Error = external::Error;

    fn try_from(
        value: v9::ConfigReconcilerInventory,
    ) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<ConfigReconcilerInventory>
    for inventory::ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(value: ConfigReconcilerInventory) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<inventory::ConfigReconcilerInventory>
    for ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(
        value: inventory::ConfigReconcilerInventory,
    ) -> Result<Self, Self::Error> {
        todo!()
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

impl TryFrom<v9::ConfigReconcilerInventoryStatus>
    for ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        value: v9::ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        todo!();
    }
}

impl TryFrom<ConfigReconcilerInventoryStatus>
    for inventory::ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        value: ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<inventory::ConfigReconcilerInventoryStatus>
    for ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        value: inventory::ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        todo!()
    }
}
