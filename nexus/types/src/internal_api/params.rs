// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.
use omicron_common::api::external::ByteCount;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
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
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq)]
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

/// Describes which ZFS properties should be set for a particular allocated
/// dataset.
// TODO: This could be useful for indicating quotas, or
// for Nexus instructing the Sled Agent "what to format, and where".
//
// For now, the Sled Agent is a bit more proactive about allocation
// decisions - see the "storage manager" section of the Sled Agent for
// more details. Nexus, in response, merely advises minimums/maximums
// for dataset sizes.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DatasetPutResponse {
    /// A minimum reservation size for a filesystem.
    /// Refer to ZFS native properties for more detail.
    pub reservation: Option<ByteCount>,
    /// A maximum quota on filesystem usage.
    /// Refer to ZFS native properties for more detail.
    pub quota: Option<ByteCount>,
}

/// Describes the purpose of the service.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    InternalDNS,
    Nexus,
    Oximeter,
    Dendrite,
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ServiceKind::*;
        let s = match self {
            InternalDNS => "internal_dns",
            Nexus => "nexus",
            Oximeter => "oximeter",
            Dendrite => "dendrite",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ServiceKind {
    type Err = omicron_common::api::external::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ServiceKind::*;
        match s {
            "nexus" => Ok(Nexus),
            "oximeter" => Ok(Oximeter),
            "internal_dns" => Ok(InternalDNS),
            "dendrite" => Ok(Dendrite),
            _ => Err(Self::Err::InternalError {
                internal_message: format!("Unknown service kind: {}", s),
            }),
        }
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

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}
