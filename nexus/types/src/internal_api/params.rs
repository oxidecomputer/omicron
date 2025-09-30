// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.

use crate::deployment::Blueprint;
use crate::external_api::params::PhysicalDiskKind;
use crate::external_api::shared::Baseboard;
use crate::external_api::shared::IpRange;
use nexus_sled_agent_shared::inventory::{SledCpuFamily, SledRole};
use nexus_sled_agent_shared::recovery_silo::RecoverySiloConfig;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Name;
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::api::internal::shared::AllowedSourceIps;
use omicron_common::api::internal::shared::ExternalPortDiscovery;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use uuid::Uuid;

/// Sent by a sled agent to Nexus to inform about resources
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct SledAgentInfo {
    /// The address of the sled agent's API endpoint
    pub sa_address: SocketAddrV6,

    /// The port of the Repo Depot API endpoint, on the same IP as `sa_address`
    pub repo_depot_port: u16,

    /// Describes the responsibilities of the sled
    pub role: SledRole,

    /// Describes the sled's identity
    pub baseboard: Baseboard,

    /// The number of hardware threads which can execute on this sled
    pub usable_hardware_threads: u32,

    /// Amount of RAM which may be used by the Sled's OS
    pub usable_physical_ram: ByteCount,

    /// Amount of RAM dedicated to the VMM reservoir
    ///
    /// Must be smaller than "usable_physical_ram"
    pub reservoir_size: ByteCount,

    /// The family of the sled's CPU.
    pub cpu_family: SledCpuFamily,

    /// The generation number of this request from sled-agent
    pub generation: Generation,

    /// Whether the sled-agent has been decommissioned by nexus
    ///
    /// This flag is only set to true by nexus. Setting it on an upsert from
    /// sled-agent has no effect.
    pub decommissioned: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SwitchPutRequest {
    pub baseboard: Baseboard,
    pub rack_id: Uuid,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SwitchPutResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PhysicalDiskPutRequest {
    pub id: PhysicalDiskUuid,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub variant: PhysicalDiskKind,
    #[schemars(with = "Uuid")]
    pub sled_id: SledUuid,
}

/// Identifies information about a Zpool that should be part of the control
/// plane.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ZpoolPutRequest {
    #[schemars(with = "Uuid")]
    pub id: ZpoolUuid,

    #[schemars(with = "Uuid")]
    pub sled_id: SledUuid,

    pub physical_disk_id: PhysicalDiskUuid,
}

/// Describes the RSS allocated values for a service vnic
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct ServiceNic {
    pub id: Uuid,
    pub name: Name,
    pub ip: IpAddr,
    pub mac: MacAddr,
    pub slot: u8,
}

/// Describes the purpose of the service.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type", content = "content")]
pub enum ServiceKind {
    Clickhouse,
    ClickhouseKeeper,
    Cockroach,
    Crucible,
    CruciblePantry,
    ExternalDns { external_address: IpAddr, nic: ServiceNic },
    InternalDns,
    Nexus { external_address: IpAddr, nic: ServiceNic },
    Oximeter,
    Dendrite,
    Tfport,
    BoundaryNtp { snat: SourceNatConfig, nic: ServiceNic },
    InternalNtp,
    Mgd,
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ServiceKind::*;
        let s = match self {
            Clickhouse => "clickhouse",
            ClickhouseKeeper => "clickhouse_keeper",
            Cockroach => "cockroach",
            Crucible => "crucible",
            ExternalDns { .. } => "external_dns",
            InternalDns => "internal_dns",
            Nexus { .. } => "nexus",
            Oximeter => "oximeter",
            Dendrite => "dendrite",
            Tfport => "tfport",
            CruciblePantry => "crucible_pantry",
            BoundaryNtp { .. } | InternalNtp => "ntp",
            Mgd => "mgd",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CrucibleDatasetCreateRequest {
    pub zpool_id: ZpoolUuid,
    pub dataset_id: DatasetUuid,
    pub address: SocketAddrV6,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct RackInitializationRequest {
    /// Blueprint describing services initialized by RSS.
    pub blueprint: Blueprint,

    /// "Managed" physical disks owned by the control plane
    pub physical_disks: Vec<PhysicalDiskPutRequest>,

    /// Zpools created within the physical disks created by the control plane.
    pub zpools: Vec<ZpoolPutRequest>,

    /// Crucible datasets on the rack which have been provisioned by RSS.
    // TODO-cleanup This field should be removed; its contents are completely
    // described by `blueprint`. However, removing it requires significant work
    // in `nexus-test-utils` and friends
    // (<https://github.com/oxidecomputer/omicron/issues/7081>).
    pub crucible_datasets: Vec<CrucibleDatasetCreateRequest>,
    /// Ranges of the service IP pool which may be used for internal services,
    /// such as Nexus.
    pub internal_services_ip_pool_ranges: Vec<IpRange>,
    /// x.509 Certificates used to encrypt communication with the external API.
    pub certs: Vec<Certificate>,
    /// initial internal DNS config
    pub internal_dns_zone_config: internal_dns_types::config::DnsConfigParams,
    /// delegated DNS name for external DNS
    pub external_dns_zone_name: String,
    /// configuration for the initial (recovery) Silo
    pub recovery_silo: RecoverySiloConfig,
    /// The external qsfp ports per sidecar
    pub external_port_count: ExternalPortDiscovery,
    /// Initial rack network configuration
    pub rack_network_config: RackNetworkConfig,
    /// IPs or subnets allowed to make requests to user-facing services
    pub allowed_source_ips: AllowedSourceIps,
}

pub type DnsConfigParams = internal_dns_types::config::DnsConfigParams;
pub type DnsConfigZone = internal_dns_types::config::DnsConfigZone;
pub type DnsRecord = internal_dns_types::config::DnsRecord;
pub type Srv = internal_dns_types::config::Srv;

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}

/// Parameters used when migrating an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrateRequest {
    /// The ID of the sled to which to migrate the target instance.
    #[schemars(with = "Uuid")]
    pub dst_sled_id: SledUuid,
}
