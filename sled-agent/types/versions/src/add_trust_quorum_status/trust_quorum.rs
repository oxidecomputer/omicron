// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum types for the Sled Agent API.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Network configuration used to bring up the control plane.
///
/// This type mirrors `bootstore::schemes::v0::NetworkConfig` but adds
/// `JsonSchema` for API compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumNetworkConfig {
    pub generation: u64,
    /// A serialized blob of configuration data (base64 encoded).
    #[schemars(with = "String")]
    #[serde(with = "serde_human_bytes::base64_vec")]
    pub blob: Vec<u8>,
}

impl From<bootstore::schemes::v0::NetworkConfig> for TrustQuorumNetworkConfig {
    fn from(config: bootstore::schemes::v0::NetworkConfig) -> Self {
        TrustQuorumNetworkConfig {
            generation: config.generation,
            blob: config.blob,
        }
    }
}

impl From<TrustQuorumNetworkConfig> for bootstore::schemes::v0::NetworkConfig {
    fn from(config: TrustQuorumNetworkConfig) -> Self {
        bootstore::schemes::v0::NetworkConfig {
            generation: config.generation,
            blob: config.blob,
        }
    }
}
