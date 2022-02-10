// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Params define the request bodies of API endpoints for creating or updating resources.
 */
use omicron_common::api::external::ByteCount;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentStartupInfo {
    /// the address of the sled agent's API endpoint
    pub sa_address: SocketAddr,
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

/// Describes a dataset within a pool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatasetPutRequest {
    /// Address on which a service is responding to requests for the
    /// dataset.
    pub address: SocketAddr,

    /// Type of dataset being inserted.
    pub kind: omicron_common::api::internal::nexus::DatasetKind,
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

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}
