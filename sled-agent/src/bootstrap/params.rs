// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use super::trust_quorum::ShareDistribution;
use omicron_common::address::{Ipv6Subnet, SLED_PREFIX};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SledAgentRequest {
    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,

    /// Share of the rack secret for this Sled Agent.
    // TODO-cleanup This is currently optional because we don't do trust quorum
    // shares for single-node deployments (i.e., most dev/test environments),
    // but eventually this should be required.
    pub trust_quorum_share: Option<ShareDistribution>,
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
