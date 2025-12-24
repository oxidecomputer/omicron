// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-related types for the Sled Agent API.

use async_trait::async_trait;
use omicron_common::address::{Ipv6Subnet, SLED_PREFIX};
use omicron_common::ledger::Ledgerable;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Re-export BaseboardId and UnknownBaseboardError from sled-hardware-types
pub use sled_hardware_types::{BaseboardId, UnknownBaseboardError};

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
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

#[async_trait]
impl Ledgerable for StartSledAgentRequest {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    fn generation_bump(&mut self) {
        // DO NOTHING!
        //
        // Generation bumps must only ever come from nexus and will be encoded
        // in the struct itself
    }

    // Attempt to deserialize the v1 or v0 version and return
    // the v1 version.
    fn deserialize(
        s: &str,
    ) -> Result<StartSledAgentRequest, serde_json::Error> {
        // Try to deserialize the latest version of the data structure (v1). If
        // that succeeds we are done.
        if let Ok(val) = serde_json::from_str::<StartSledAgentRequest>(s) {
            return Ok(val);
        }

        // We don't have the latest version. Try to deserialize v0 and then
        // convert it to the latest version.
        let v0 = serde_json::from_str::<PersistentSledAgentRequest>(s)?.request;
        Ok(v0.into())
    }
}

/// The version of `StartSledAgentRequest` we originally shipped with.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequestV0 {
    /// Uuid of the Sled Agent to be created.
    pub id: SledUuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// The external NTP servers to use
    pub ntp_servers: Vec<String>,

    /// The external DNS servers to use
    pub dns_servers: Vec<std::net::IpAddr>,

    /// Use trust quorum for key generation
    pub use_trust_quorum: bool,

    // Note: The order of these fields is load bearing, because we serialize
    // `SledAgentRequest`s as toml. `subnet` serializes as a TOML table, so it
    // must come after non-table fields.
    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl From<StartSledAgentRequestV0> for StartSledAgentRequest {
    fn from(v0: StartSledAgentRequestV0) -> Self {
        StartSledAgentRequest {
            generation: 0,
            schema_version: 1,
            body: StartSledAgentRequestBody {
                id: v0.id,
                rack_id: v0.rack_id,
                use_trust_quorum: v0.use_trust_quorum,
                is_lrtq_learner: false,
                subnet: v0.subnet,
            },
        }
    }
}

// A wrapper around StartSledAgentRequestV0 that was used
// for the ledger format.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
struct PersistentSledAgentRequest {
    request: StartSledAgentRequestV0,
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;

    #[test]
    fn serialize_start_sled_agent_v0_deserialize_v1() {
        let v0 = PersistentSledAgentRequest {
            request: StartSledAgentRequestV0 {
                id: SledUuid::new_v4(),
                rack_id: Uuid::new_v4(),
                ntp_servers: vec![String::from("test.pool.example.com")],
                dns_servers: vec!["1.1.1.1".parse().unwrap()],
                use_trust_quorum: false,
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
            },
        };
        let serialized = serde_json::to_string(&v0).unwrap();
        let expected = StartSledAgentRequest {
            generation: 0,
            schema_version: 1,
            body: StartSledAgentRequestBody {
                id: v0.request.id,
                rack_id: v0.request.rack_id,
                use_trust_quorum: v0.request.use_trust_quorum,
                is_lrtq_learner: false,
                subnet: v0.request.subnet,
            },
        };

        let actual: StartSledAgentRequest =
            Ledgerable::deserialize(&serialized).unwrap();
        assert_eq!(expected, actual);
    }
}
