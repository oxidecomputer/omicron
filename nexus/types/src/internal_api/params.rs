// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.
use omicron_common::api::external::ByteCount;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv6Addr;
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

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentStartupInfo {
    /// The address of the sled agent's API endpoint
    pub sa_address: SocketAddrV6,

    /// Describes the responsibilities of the sled
    pub role: SledRole,
}

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ZpoolPutRequest {
    /// Total size of the pool.
    pub size: ByteCount,
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
    InternalDNS,
    Nexus {
        // TODO(https://github.com/oxidecomputer/omicron/issues/1530):
        // While it's true that Nexus will only run with a single address,
        // we want to convey information about the available pool of addresses
        // when handing off from RSS -> Nexus.
        external_address: IpAddr,
    },
    Oximeter,
    Dendrite,
    Tfport,
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ServiceKind::*;
        let s = match self {
            InternalDNS => "internal_dns",
            Nexus { .. } => "nexus",
            Oximeter => "oximeter",
            Dendrite => "dendrite",
            Tfport => "tfport",
        };
        write!(f, "{}", s)
    }
}

/// Describes a service on a sled
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ServicePutRequest {
    pub service_id: Uuid,
    pub sled_id: Uuid,

    /// Address on which a service is responding to requests.
    pub address: Ipv6Addr,

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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RackInitializationRequest {
    pub services: Vec<ServicePutRequest>,
    pub datasets: Vec<DatasetCreateRequest>,
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530):
    // While it's true that Nexus will only run with a single address,
    // we want to convey information about the available pool of addresses
    // when handing off from RSS -> Nexus.

    // TODO(https://github.com/oxidecomputer/omicron/issues/1528):
    // Support passing x509 cert info.
    pub certs: Vec<Certificate>,
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}
