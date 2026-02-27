// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * `EarlyNetworkConfig` is gone, replaced by the following two types.
//! * `WriteNetworkConfigRequest` includes the `generation` from
//!   `EarlyNetworkConfig` and a versioned `EarlyNetworkConfigBody`. This is the
//!   type used in the sled-agent API handler to write new network configs, but
//!   it is not itself serialized in the bootstore. (It's converted into a
//!   `NetworkConfig` containing an `EarlyNetworkConfigEnvelope`.) This type
//!   will need a new version any time `EarlyNetworkConfigBody` changes, but the
//!   new type should be trivial.
//! * `EarlyNetworkConfigEnvelope` includes the `schema_version` from
//!   `EarlyNetworkConfig` and an opaque JSON blob `body`. This allows it to be
//!   deserialized independently from the versioning of the
//!   `EarlyNetworkConfigBody` it contains. This type does not need a new
//!   version when `EarlyNetworkConfigBody` changes, but the deserialization
//!   code it contains will need to be updated to account for the new possible
//!   schema version. This type does _not_ derive `JsonSchema`, because it is
//!   not expected to be used in any APIs.

use crate::v20::early_networking::EarlyNetworkConfigBody;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Structure for requests from Nexus to sled-agent to write a new
/// `EarlyNetworkConfigBody` into the replicated bootstore.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WriteNetworkConfigRequest {
    pub generation: u64,
    pub body: EarlyNetworkConfigBody,
}

impl From<crate::v20::early_networking::EarlyNetworkConfig>
    for WriteNetworkConfigRequest
{
    fn from(value: crate::v20::early_networking::EarlyNetworkConfig) -> Self {
        Self { generation: value.generation, body: value.body }
    }
}

/// Envelope containing a versioned JSON blob (an `EarlyNetworkConfigBody`).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EarlyNetworkConfigEnvelope {
    // Which version of `EarlyNetworkConfigBody` is serialized into `body`.
    pub(crate) schema_version: u32,

    // The actual early network configuration details.
    //
    // These are a serialized `EarlyNetworkConfigBody` of some version. We must
    // inspect `schema_version` to know how to interpret this value.
    pub(crate) body: serde_json::Value,
}
