// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-related types for the Sled Agent API.

use omicron_common::address::{Ipv6Subnet, SLED_PREFIX_LENGTH};
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use uuid::Uuid;

/// A request to Add a given sled after rack initialization has occurred
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct AddSledRequest {
    pub sled_id: BaseboardId,
    pub start_request: StartSledAgentRequest,
}

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequest {
    /// The current generation number of data as stored in CRDB.
    ///
    /// The initial generation is set during RSS time and then only mutated
    /// by Nexus. For now, we don't actually anticipate mutating this data,
    /// but we leave open the possiblity.
    pub generation: u64,

    // Which version of the data structure do we have. This is to help with
    // deserialization and conversion in future updates.
    pub schema_version: u32,

    // The actual configuration details
    pub body: StartSledAgentRequestBody,
}

/// This is the actual app level data of `StartSledAgentRequest`
///
/// We nest it below the "header" of `generation` and `schema_version` so that
/// we can perform partial deserialization of `EarlyNetworkConfig` to only read
/// the header and defer deserialization of the body once we know the schema
/// version. This is possible via the use of [`serde_json::value::RawValue`] in
/// future (post-v1) deserialization paths.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequestBody {
    /// Uuid of the Sled Agent to be created.
    pub id: SledUuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// Use trust quorum for key generation
    pub use_trust_quorum: bool,

    /// Is this node an LRTQ learner node?
    ///
    /// We only put the node into learner mode if `use_trust_quorum` is also
    /// true.
    pub is_lrtq_learner: bool,

    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX_LENGTH>,
}
