// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use serde::{Deserialize, Serialize};
use sled_agent_types::sled::StartSledAgentRequest;
use std::borrow::Cow;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Request<'a> {
    /// Send configuration information for launching a Sled Agent.
    StartSledAgentRequest(Cow<'a, StartSledAgentRequest>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestEnvelope<'a> {
    pub version: u32,
    pub request: Request<'a>,
}

pub(super) mod version {
    pub(crate) const V1: u32 = 1;
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use omicron_common::address::Ipv6Subnet;
    use sled_agent_types::sled::StartSledAgentRequestBody;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn json_serialization_round_trips() {
        let envelope = RequestEnvelope {
            version: 1,
            request: Request::StartSledAgentRequest(Cow::Owned(
                StartSledAgentRequest {
                    generation: 0,
                    schema_version: 1,
                    body: StartSledAgentRequestBody {
                        id: Uuid::new_v4(),
                        rack_id: Uuid::new_v4(),
                        use_trust_quorum: false,
                        is_lrtq_learner: false,
                        subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                    },
                },
            )),
        };

        let serialized = serde_json::to_vec(&envelope).unwrap();
        let deserialized: RequestEnvelope =
            serde_json::from_slice(serialized.as_slice()).unwrap();

        assert!(envelope == deserialized, "serialization round trip failed");
    }
}
