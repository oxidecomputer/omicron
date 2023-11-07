// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::storage::dataset::DatasetName;
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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
pub use sled_hardware::DendriteAsic;
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;
use illumos_utils::zpool::ZpoolName;

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
    pub external_ips: Vec<IpAddr>,
    pub firewall_rules: Vec<VpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    // TODO: replace `propolis_client::handmade::*` with locally-modeled request type
    pub disks: Vec<propolis_client::handmade::api::DiskRequest>,
    pub cloud_init_bytes: Option<String>,
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
    pub propolis_id: Uuid,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,
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
#[derive(Serialize, Deserialize, JsonSchema)]
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
    pub dst_propolis_id: Uuid,
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
    pub id: Uuid,
    pub disk_type: DiskType,
}

/// The type of a dataset, and an auxiliary information necessary
/// to successfully launch a zone managing the associated data.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetKind {
    CockroachDb,
    Crucible,
    Clickhouse,
    ClickhouseKeeper,
    ExternalDns,
    InternalDns,
}

impl From<DatasetKind> for sled_agent_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb => Self::CockroachDb,
            Crucible => Self::Crucible,
            Clickhouse => Self::Clickhouse,
            ClickhouseKeeper => Self::ClickhouseKeeper,
            ExternalDns => Self::ExternalDns,
            InternalDns => Self::InternalDns,
        }
    }
}

impl From<DatasetKind> for nexus_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb => Self::Cockroach,
            Crucible => Self::Crucible,
            Clickhouse => Self::Clickhouse,
            ClickhouseKeeper => Self::ClickhouseKeeper,
            ExternalDns => Self::ExternalDns,
            InternalDns => Self::InternalDns,
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
            ClickhouseKeeper => "clickhouse_keeper",
            ExternalDns { .. } => "external_dns",
            InternalDns { .. } => "internal_dns",
        };
        write!(f, "{}", s)
    }
}

/// Describes service-specific parameters.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServiceType {
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
    ExternalDns {
        /// The address at which the external DNS server API is reachable.
        http_address: SocketAddrV6,
        /// The address at which the external DNS server is reachable.
        dns_address: SocketAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
    },
    InternalDns {
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
        /// The addresses in the global zone which should be created
        ///
        /// For the DNS service, which exists outside the sleds's typical subnet - adding an
        /// address in the GZ is necessary to allow inter-zone traffic routing.
        gz_address: Ipv6Addr,

        /// The address is also identified with an auxiliary bit of information
        /// to ensure that the created global zone address can have a unique name.
        gz_address_index: u32,
    },
    Oximeter {
        address: SocketAddrV6,
    },
    // We should never receive external requests to start wicketd, MGS, sp-sim
    // dendrite, tfport, or maghemite: these are all services running in the
    // global zone or switch zone that we start autonomously. We tag them with
    // `serde(skip)` both to omit them from our OpenAPI definition and to avoid
    // needing their contained types to implement `JsonSchema + Deserialize +
    // Serialize`.
    #[serde(skip)]
    ManagementGatewayService,
    #[serde(skip)]
    Wicketd {
        baseboard: Baseboard,
    },
    #[serde(skip)]
    Dendrite {
        asic: DendriteAsic,
    },
    #[serde(skip)]
    Tfport {
        pkt_source: String,
        asic: DendriteAsic,
    },
    #[serde(skip)]
    Uplink,
    #[serde(skip)]
    MgDdm {
        mode: String,
    },
    #[serde(skip)]
    Mgd,
    #[serde(skip)]
    SpSim,
    CruciblePantry {
        address: SocketAddrV6,
    },
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
    InternalNtp {
        address: SocketAddrV6,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
    },
    Clickhouse {
        address: SocketAddrV6,
    },
    ClickhouseKeeper {
        address: SocketAddrV6,
    },
    CockroachDb {
        address: SocketAddrV6,
    },
    Crucible {
        address: SocketAddrV6,
    },
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match self {
            ServiceType::Nexus { .. } => write!(f, "nexus"),
            ServiceType::ExternalDns { .. } => write!(f, "external_dns"),
            ServiceType::InternalDns { .. } => write!(f, "internal_dns"),
            ServiceType::Oximeter { .. } => write!(f, "oximeter"),
            ServiceType::ManagementGatewayService => write!(f, "mgs"),
            ServiceType::Wicketd { .. } => write!(f, "wicketd"),
            ServiceType::Dendrite { .. } => write!(f, "dendrite"),
            ServiceType::Tfport { .. } => write!(f, "tfport"),
            ServiceType::Uplink { .. } => write!(f, "uplink"),
            ServiceType::CruciblePantry { .. } => write!(f, "crucible/pantry"),
            ServiceType::BoundaryNtp { .. }
            | ServiceType::InternalNtp { .. } => write!(f, "ntp"),
            ServiceType::MgDdm { .. } => write!(f, "mg-ddm"),
            ServiceType::Mgd => write!(f, "mgd"),
            ServiceType::SpSim => write!(f, "sp-sim"),
            ServiceType::Clickhouse { .. } => write!(f, "clickhouse"),
            ServiceType::ClickhouseKeeper { .. } => {
                write!(f, "clickhouse_keeper")
            }
            ServiceType::CockroachDb { .. } => write!(f, "cockroachdb"),
            ServiceType::Crucible { .. } => write!(f, "crucible"),
        }
    }
}

impl crate::smf_helper::Service for ServiceType {
    fn service_name(&self) -> String {
        self.to_string()
    }
    fn smf_name(&self) -> String {
        format!("svc:/oxide/{}", self.service_name())
    }
    fn should_import(&self) -> bool {
        true
    }
}

/// Error returned by attempting to convert an internal service (i.e., a service
/// started autonomously by sled-agent) into a
/// `sled_agent_client::types::ServiceType` to be sent to a remote sled-agent.
#[derive(Debug, Clone, Copy, Error)]
#[error("This service may only be started autonomously by sled-agent")]
pub struct AutonomousServiceOnlyError;

impl TryFrom<ServiceType> for sled_agent_client::types::ServiceType {
    type Error = AutonomousServiceOnlyError;

    fn try_from(s: ServiceType) -> Result<Self, Self::Error> {
        use sled_agent_client::types::ServiceType as AutoSt;
        use ServiceType as St;

        match s {
            St::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => Ok(AutoSt::Nexus {
                internal_address: internal_address.to_string(),
                external_ip,
                nic: nic.into(),
                external_tls,
                external_dns_servers,
            }),
            St::ExternalDns { http_address, dns_address, nic } => {
                Ok(AutoSt::ExternalDns {
                    http_address: http_address.to_string(),
                    dns_address: dns_address.to_string(),
                    nic: nic.into(),
                })
            }
            St::InternalDns {
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => Ok(AutoSt::InternalDns {
                http_address: http_address.to_string(),
                dns_address: dns_address.to_string(),
                gz_address,
                gz_address_index,
            }),
            St::Oximeter { address } => {
                Ok(AutoSt::Oximeter { address: address.to_string() })
            }
            St::CruciblePantry { address } => {
                Ok(AutoSt::CruciblePantry { address: address.to_string() })
            }
            St::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => Ok(AutoSt::BoundaryNtp {
                address: address.to_string(),
                ntp_servers,
                dns_servers,
                domain,
                nic: nic.into(),
                snat_cfg: snat_cfg.into(),
            }),
            St::InternalNtp { address, ntp_servers, dns_servers, domain } => {
                Ok(AutoSt::InternalNtp {
                    address: address.to_string(),
                    ntp_servers,
                    dns_servers,
                    domain,
                })
            }
            St::Clickhouse { address } => {
                Ok(AutoSt::Clickhouse { address: address.to_string() })
            }
            St::ClickhouseKeeper { address } => {
                Ok(AutoSt::ClickhouseKeeper { address: address.to_string() })
            }
            St::CockroachDb { address } => {
                Ok(AutoSt::CockroachDb { address: address.to_string() })
            }
            St::Crucible { address } => {
                Ok(AutoSt::Crucible { address: address.to_string() })
            }
            St::ManagementGatewayService
            | St::SpSim
            | St::Wicketd { .. }
            | St::Dendrite { .. }
            | St::Tfport { .. }
            | St::Uplink
            | St::Mgd
            | St::MgDdm { .. } => Err(AutonomousServiceOnlyError),
        }
    }
}

/// The type of zone which may be requested from Sled Agent
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

impl From<ZoneType> for sled_agent_client::types::ZoneType {
    fn from(zt: ZoneType) -> Self {
        match zt {
            ZoneType::Clickhouse => Self::Clickhouse,
            ZoneType::ClickhouseKeeper => Self::ClickhouseKeeper,
            ZoneType::CockroachDb => Self::CockroachDb,
            ZoneType::Crucible => Self::Crucible,
            ZoneType::CruciblePantry => Self::CruciblePantry,
            ZoneType::InternalDns => Self::InternalDns,
            ZoneType::ExternalDns => Self::ExternalDns,
            ZoneType::Nexus => Self::Nexus,
            ZoneType::Ntp => Self::Ntp,
            ZoneType::Oximeter => Self::Oximeter,
            ZoneType::Switch => Self::Switch,
        }
    }
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

/// Describes a request to provision a specific dataset
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct DatasetRequest {
    pub id: Uuid,
    pub name: crate::storage::dataset::DatasetName,
    pub service_address: SocketAddrV6,
}

impl From<DatasetRequest> for sled_agent_client::types::DatasetRequest {
    fn from(d: DatasetRequest) -> Self {
        Self {
            id: d.id,
            name: d.name.into(),
            service_address: d.service_address.to_string(),
        }
    }
}

/// Describes a request to create a zone running one or more services.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceZoneRequest {
    // The UUID of the zone to be initialized.
    // TODO: Should this be removed? If we have UUIDs on the services, what's
    // the point of this?
    pub id: Uuid,
    // The type of the zone to be created.
    pub zone_type: ZoneType,
    // The addresses on which the service should listen for requests.
    pub addresses: Vec<Ipv6Addr>,
    // Datasets which should be managed by this service.
    #[serde(default)]
    pub dataset: Option<DatasetRequest>,
    // Services that should be run in the zone
    pub services: Vec<ServiceZoneService>,
}

impl ServiceZoneRequest {
    // The full name of the zone, if it was to be created as a zone.
    pub fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            &self.zone_type.to_string(),
            self.zone_name_unique_identifier(),
        )
    }

    // The name of a unique identifier for the zone, if one is necessary.
    pub fn zone_name_unique_identifier(&self) -> Option<Uuid> {
        match &self.zone_type {
            // The switch zone is necessarily a singleton.
            ZoneType::Switch => None,
            // All other zones should be identified by their zone UUID.
            ZoneType::Clickhouse
            | ZoneType::ClickhouseKeeper
            | ZoneType::CockroachDb
            | ZoneType::Crucible
            | ZoneType::ExternalDns
            | ZoneType::InternalDns
            | ZoneType::Nexus
            | ZoneType::CruciblePantry
            | ZoneType::Ntp
            | ZoneType::Oximeter => Some(self.id),
        }
    }
}

impl TryFrom<ServiceZoneRequest>
    for sled_agent_client::types::ServiceZoneRequest
{
    type Error = AutonomousServiceOnlyError;

    fn try_from(s: ServiceZoneRequest) -> Result<Self, Self::Error> {
        let mut services = Vec::with_capacity(s.services.len());
        for service in s.services {
            services.push(service.try_into()?);
        }

        Ok(Self {
            id: s.id,
            zone_type: s.zone_type.into(),
            addresses: s.addresses,
            dataset: s.dataset.map(|d| d.into()),
            services,
        })
    }
}

impl ServiceZoneRequest {
    pub fn into_nexus_service_req(
        &self,
        sled_id: Uuid,
    ) -> Result<
        Vec<nexus_client::types::ServicePutRequest>,
        AutonomousServiceOnlyError,
    > {
        use nexus_client::types as NexusTypes;

        let mut services = vec![];
        for svc in &self.services {
            let service_id = svc.id;
            let zone_id = Some(self.id);
            match &svc.details {
                ServiceType::Nexus {
                    external_ip,
                    internal_address,
                    nic,
                    ..
                } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: internal_address.to_string(),
                        kind: NexusTypes::ServiceKind::Nexus {
                            external_address: *external_ip,
                            nic: NexusTypes::ServiceNic {
                                id: nic.id,
                                name: nic.name.clone(),
                                ip: nic.ip,
                                mac: nic.mac,
                            },
                        },
                    });
                }
                ServiceType::ExternalDns { http_address, dns_address, nic } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: http_address.to_string(),
                        kind: NexusTypes::ServiceKind::ExternalDns {
                            external_address: dns_address.ip(),
                            nic: NexusTypes::ServiceNic {
                                id: nic.id,
                                name: nic.name.clone(),
                                ip: nic.ip,
                                mac: nic.mac,
                            },
                        },
                    });
                }
                ServiceType::InternalDns { http_address, .. } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: http_address.to_string(),
                        kind: NexusTypes::ServiceKind::InternalDns,
                    });
                }
                ServiceType::Oximeter { address } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::Oximeter,
                    });
                }
                ServiceType::CruciblePantry { address } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::CruciblePantry,
                    });
                }
                ServiceType::BoundaryNtp { address, snat_cfg, nic, .. } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::BoundaryNtp {
                            snat: snat_cfg.into(),
                            nic: NexusTypes::ServiceNic {
                                id: nic.id,
                                name: nic.name.clone(),
                                ip: nic.ip,
                                mac: nic.mac,
                            },
                        },
                    });
                }
                ServiceType::InternalNtp { address, .. } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::InternalNtp,
                    });
                }
                ServiceType::Clickhouse { address } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::Clickhouse,
                    });
                }
                ServiceType::ClickhouseKeeper { address } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::ClickhouseKeeper,
                    });
                }
                ServiceType::Crucible { address } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::Crucible,
                    });
                }
                ServiceType::CockroachDb { address } => {
                    services.push(NexusTypes::ServicePutRequest {
                        service_id,
                        zone_id,
                        sled_id,
                        address: address.to_string(),
                        kind: NexusTypes::ServiceKind::Cockroach,
                    });
                }
                ServiceType::ManagementGatewayService
                | ServiceType::SpSim
                | ServiceType::Wicketd { .. }
                | ServiceType::Dendrite { .. }
                | ServiceType::MgDdm { .. }
                | ServiceType::Mgd
                | ServiceType::Tfport { .. }
                | ServiceType::Uplink => {
                    return Err(AutonomousServiceOnlyError);
                }
            }
        }

        Ok(services)
    }
}

/// Used to request that the Sled initialize a single service.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceZoneService {
    pub id: Uuid,
    pub details: ServiceType,
}

impl TryFrom<ServiceZoneService>
    for sled_agent_client::types::ServiceZoneService
{
    type Error = AutonomousServiceOnlyError;

    fn try_from(s: ServiceZoneService) -> Result<Self, Self::Error> {
        let details = s.details.try_into()?;
        Ok(Self { id: s.id, details })
    }
}

/// Used to request that the Sled initialize multiple services.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ServiceEnsureBody {
    pub services: Vec<ServiceZoneRequest>,
}

/// Describes the set of Omicron zones running on a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct OmicronZonesConfig {
    // XXX-dap generation number
    pub zones: Vec<OmicronZoneConfig>,
}

/// Describes one Omicron zone running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneConfig {
    pub id: Uuid,
    pub underlay_address: Ipv6Addr,
    pub zone_type: OmicronZoneType,
}

impl OmicronZoneConfig {
    // XXX-dap should the caller (RSS) should specify this directly?  That
    // eliminates one reason why Sled Agent needs to know about what kind of
    // dataset it's looking at.
    pub fn dataset_name(&self) -> Option<DatasetName> {
        let (dataset, dataset_kind) = match &self.zone_type {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. }
            | OmicronZoneType::CruciblePantry { .. } => None,
            OmicronZoneType::Clickhouse { dataset, .. } => {
                Some((dataset, DatasetKind::Clickhouse))
            }
            OmicronZoneType::ClickhouseKeeper { dataset, .. } => {
                Some((dataset, DatasetKind::ClickhouseKeeper))
            }
            OmicronZoneType::CockroachDb { dataset, .. } => {
                Some((dataset, DatasetKind::CockroachDb))
            }
            OmicronZoneType::Crucible { dataset, .. } => {
                Some((dataset, DatasetKind::Crucible))
            }
            OmicronZoneType::ExternalDns { dataset, .. } => {
                Some((dataset, DatasetKind::ExternalDns))
            }
            OmicronZoneType::InternalDns { dataset, .. } => {
                Some((dataset, DatasetKind::InternalDns))
            }
        }?;

        DatasetName::new(dataset.pool_name, dataset_kind)
    }
}

/// Describes a persistent ZFS dataset associated with an Omicron zone
pub struct OmicronZoneDataset {
    pool_name: ZpoolName,
}

/// Describes what component is running in this zone and its associated
/// type-specific configuration
///
/// XXX-dap ideally this would not be necessary at all!  Sled Agent shouldn't
/// have to know about the things running on it, I think?
/// XXX-dap commonize with ServiceType (well, probably remove ServiceType)
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

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SledRole {
    /// The sled is a general compute sled.
    Gimlet,
    /// The sled is attached to the network switch, and has additional
    /// responsibilities.
    Scrimlet,
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
