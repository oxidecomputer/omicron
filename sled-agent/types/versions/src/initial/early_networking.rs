// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

use bootstore::schemes::v0 as bootstore;
use omicron_common::api::internal::shared::rack_init::v1::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from
/// `RackInitializeRequest` necessary for use beyond RSS.
/// This is just for the initial rack configuration and cold boot purposes.
/// Updates come from Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfig {
    // The current generation number of data as stored in CRDB.
    // The initial generation is set during RSS time and then only mutated
    // by Nexus.
    pub generation: u64,

    // Which version of the data structure do we have. This is to help with
    // deserialization and conversion in future updates.
    pub schema_version: u32,

    // The actual configuration details
    pub body: EarlyNetworkConfigBody,
}

/// This is the actual configuration of EarlyNetworking.
///
/// We nest it below the "header" of `generation` and `schema_version` so that
/// we can perform partial deserialization of `EarlyNetworkConfig` to only read
/// the header and defer deserialization of the body once we know the schema
/// version. This is possible via the use of [`serde_json::value::RawValue`] in
/// future (post-v1) deserialization paths.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfigBody {
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    // Rack network configuration as delivered from RSS or Nexus
    pub rack_network_config: Option<RackNetworkConfig>,
}

impl From<EarlyNetworkConfig> for bootstore::NetworkConfig {
    fn from(value: EarlyNetworkConfig) -> Self {
        // Can this ever actually fail?
        // We literally just deserialized the same data in RSS
        let blob = serde_json::to_vec(&value).unwrap();

        // Yes this is duplicated, but that seems fine.
        let generation = value.generation;

        bootstore::NetworkConfig { generation, blob }
    }
}
