// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.

use crate::external_api::params::UserId;
use crate::external_api::shared::IpRange;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::str::FromStr;
use uuid::Uuid;

/// Describes the role of the sled within the rack.
///
/// Note that this may change if the sled is physically moved
/// within the rack.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SledRole {
    /// The sled is a general compute sled.
    Gimlet,
    /// The sled is attached to the network switch, and has additional
    /// responsibilities.
    Scrimlet,
}

/// Describes properties that should uniquely identify a Gimlet.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Baseboard {
    pub identifier: String,
    pub model: String,
    pub revision: i64,
}

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentStartupInfo {
    /// The address of the sled agent's API endpoint
    pub sa_address: SocketAddrV6,

    /// Describes the responsibilities of the sled
    pub role: SledRole,

    /// Describes the sled's identity
    pub baseboard: Baseboard,

    /// The number of hardware threads which can execute on this sled
    pub usable_hardware_threads: u32,

    /// Amount of RAM which may be used by the Sled's OS
    pub usable_physical_ram: ByteCount,
}

/// Describes the type of physical disk.
#[derive(
    Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "content")]
pub enum PhysicalDiskKind {
    M2,
    U2,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PhysicalDiskPutRequest {
    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub variant: PhysicalDiskKind,
    pub sled_id: Uuid,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct PhysicalDiskPutResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PhysicalDiskDeleteRequest {
    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub sled_id: Uuid,
}

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ZpoolPutRequest {
    /// Total size of the pool.
    pub size: ByteCount,

    // Information to identify the disk to which this zpool belongs
    pub disk_vendor: String,
    pub disk_serial: String,
    pub disk_model: String,
    // TODO: We could include any other data from `ZpoolInfo` we want,
    // such as "allocated/free" space and pool health?
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ZpoolPutResponse {}

/// Describes the purpose of the dataset.
#[derive(
    Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    Crucible,
    Cockroach,
    Clickhouse,
}

impl fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DatasetKind::*;
        let s = match self {
            Crucible => "crucible",
            Cockroach => "cockroach",
            Clickhouse => "clickhouse",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for DatasetKind {
    type Err = omicron_common::api::external::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DatasetKind::*;
        match s {
            "crucible" => Ok(Crucible),
            "cockroach" => Ok(Cockroach),
            "clickhouse" => Ok(Clickhouse),
            _ => Err(Self::Err::InternalError {
                internal_message: format!("Unknown dataset kind: {}", s),
            }),
        }
    }
}

/// Describes a dataset within a pool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatasetPutRequest {
    /// Address on which a service is responding to requests for the
    /// dataset.
    pub address: SocketAddrV6,

    /// Type of dataset being inserted.
    pub kind: DatasetKind,
}

/// Describes the purpose of the service.
#[derive(
    Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "content")]
pub enum ServiceKind {
    ExternalDns,
    ExternalDnsConfig,
    InternalDns,
    InternalDnsConfig,
    Nexus { external_address: IpAddr },
    Oximeter,
    Dendrite,
    Tfport,
    CruciblePantry,
    Ntp,
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ServiceKind::*;
        let s = match self {
            ExternalDnsConfig => "external_dns_config",
            ExternalDns => "external_dns",
            InternalDnsConfig => "internal_dns_config",
            InternalDns => "internal_dns",
            Nexus { .. } => "nexus",
            Oximeter => "oximeter",
            Dendrite => "dendrite",
            Tfport => "tfport",
            CruciblePantry => "crucible_pantry",
            Ntp => "ntp",
        };
        write!(f, "{}", s)
    }
}

/// Describes a service on a sled
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ServicePutRequest {
    pub service_id: Uuid,
    pub sled_id: Uuid,
    pub zone_id: Option<Uuid>,

    /// Address on which a service is responding to requests.
    pub address: SocketAddrV6,

    /// Type of service being inserted.
    pub kind: ServiceKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatasetCreateRequest {
    pub zpool_id: Uuid,
    pub dataset_id: Uuid,
    pub request: DatasetPutRequest,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct Certificate {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certificate")
            .field("cert", &self.cert)
            .field("key", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct RackInitializationRequest {
    /// Services on the rack which have been created by RSS.
    pub services: Vec<ServicePutRequest>,
    /// Datasets on the rack which have been provisioned by RSS.
    pub datasets: Vec<DatasetCreateRequest>,
    /// Ranges of the service IP pool which may be used for internal services,
    /// such as Nexus.
    pub internal_services_ip_pool_ranges: Vec<IpRange>,
    /// x.509 Certificates used to encrypt communication with the external API.
    pub certs: Vec<Certificate>,
    /// initial internal DNS config
    pub internal_dns_zone_config: dns_service_client::types::DnsConfigParams,
    /// delegated DNS name for external DNS
    pub external_dns_zone_name: String,
    /// configuration for the initial (recovery) Silo
    pub recovery_silo: RecoverySiloConfig,
}

pub type DnsConfigParams = dns_service_client::types::DnsConfigParams;
pub type DnsConfigZone = dns_service_client::types::DnsConfigZone;
pub type DnsRecord = dns_service_client::types::DnsRecord;
pub type Srv = dns_service_client::types::Srv;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct RecoverySiloConfig {
    pub silo_name: Name,
    pub user_name: UserId,
    pub user_password_hash: PasswordHash,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "String")]
pub struct PasswordHash(omicron_passwords::PasswordHashString);

impl From<omicron_passwords::PasswordHashString> for PasswordHash {
    fn from(value: omicron_passwords::PasswordHashString) -> Self {
        PasswordHash(value)
    }
}

impl From<PasswordHash> for omicron_passwords::PasswordHashString {
    fn from(value: PasswordHash) -> Self {
        value.0
    }
}

impl JsonSchema for PasswordHash {
    fn schema_name() -> String {
        "PasswordHash".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A password hash in PHC string format".to_string()),
                description: Some(
                    "Password hashes must be in PHC (Password Hashing \
                    Competition) string format.  Passwords must be hashed \
                    with Argon2id.  Password hashes may be rejected if the \
                    parameters appear not to be secure enough."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            ..Default::default()
        }
        .into()
    }
}

impl TryFrom<String> for PasswordHash {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(PasswordHash(omicron_passwords::parse_phc_hash(&value)?))
    }
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}
