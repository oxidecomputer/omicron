// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for system-level networking.
//!
//! Changes in this version:
//!
//! * [`SystemNetworkingConfig`]'s [`RackNetworkConfig`] now contains
//!   [`super::early_networking::BgpPeerConfig`]s with the new `src_addr` field.

use super::early_networking::RackNetworkConfig;
use crate::v39::system_networking::BlueprintExternalNetworkingConfig;
use crate::v42;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// All configuration needed to set up system-level networking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemNetworkingConfig {
    pub rack_network_config: RackNetworkConfig,

    /// External networking configuration specified by blueprints.
    pub blueprint_external_networking_config:
        Option<BlueprintExternalNetworkingConfig>,
}

impl SystemNetworkingConfig {
    pub const SCHEMA_VERSION: u32 = 8;
}

// See ../early_networking.rs for why it is okay for this to be fallible.
impl TryFrom<v42::system_networking::SystemNetworkingConfig>
    for SystemNetworkingConfig
{
    type Error = anyhow::Error;

    fn try_from(
        value: v42::system_networking::SystemNetworkingConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            rack_network_config: value.rack_network_config.try_into()?,
            blueprint_external_networking_config: value
                .blueprint_external_networking_config,
        })
    }
}

impl From<SystemNetworkingConfig>
    for v42::system_networking::SystemNetworkingConfig
{
    fn from(value: SystemNetworkingConfig) -> Self {
        Self {
            rack_network_config: value.rack_network_config.into(),
            blueprint_external_networking_config: value
                .blueprint_external_networking_config,
        }
    }
}

/// Structure for requests from Nexus to sled-agent to write a new
/// [`SystemNetworkingConfig`] into the replicated bootstore.
///
/// [`WriteNetworkConfigRequest`] INTENTIONALLY does not have a `From`
/// implementation from prior API versions. It is critically important that
/// sled-agent not attempt to rewrite old [`SystemNetworkingConfig`] types to
/// the latest version. For more about this, see the comments on the relevant
/// endpoint in `sled-agent-api`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WriteNetworkConfigRequest {
    pub generation: u64,
    pub body: SystemNetworkingConfig,
}
