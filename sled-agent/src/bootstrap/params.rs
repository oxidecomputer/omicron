// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use std::borrow::Cow;

use omicron_common::address::{Ipv6Subnet, SLED_PREFIX};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identity signed by local RoT and Oxide certificate chain.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledAgentRequest {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Request<'a> {
    /// Send configuration information for launching a Sled Agent.
    SledAgentRequest(Cow<'a, SledAgentRequest>),

    /// Request the sled's share of the rack secret.
    ShareRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RequestEnvelope<'a> {
    pub version: u32,
    pub request: Request<'a>,
}

pub(super) mod version {
    pub(crate) const V1: u32 = 1;
}
