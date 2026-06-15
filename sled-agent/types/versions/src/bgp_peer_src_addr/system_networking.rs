// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for system-level networking.
//!
//! Changes in this version:
//!
//! * [`SystemNetworkingConfig::rack_network_config`] is now a
//!   [`crate::v42::early_networking::RackNetworkConfig`] (previously
//!   `crate::v30::early_networking::RackNetworkConfig`), which carries
//!   the updated `RouterPeerType::Numbered` with the new `src_addr` field.

use crate::v33;
use crate::v39;
use crate::v42::early_networking::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use v33::system_networking::ServiceZoneNatEntries;
pub use v33::system_networking::ServiceZoneNatEntriesError;
pub use v33::system_networking::ServiceZoneNatEntry;
pub use v33::system_networking::ServiceZoneNatKind;
pub use v39::system_networking::BlueprintExternalNetworkingConfig;

/// All configuration needed to set up system-level networking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemNetworkingConfig {
    pub rack_network_config: RackNetworkConfig,

    /// External networking configuration specified by blueprints.
    pub blueprint_external_networking_config:
        Option<BlueprintExternalNetworkingConfig>,
}

impl SystemNetworkingConfig {
    pub const SCHEMA_VERSION: u32 = 7;
}

impl TryFrom<v39::system_networking::SystemNetworkingConfig>
    for SystemNetworkingConfig
{
    type Error = anyhow::Error;

    fn try_from(
        value: v39::system_networking::SystemNetworkingConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            // Upgrade: v30 RackNetworkConfig → v42 RackNetworkConfig.
            // The new src_addr field defaults to None for all peers.
            rack_network_config: value.rack_network_config.into(),
            blueprint_external_networking_config: value
                .blueprint_external_networking_config,
        })
    }
}

impl From<SystemNetworkingConfig>
    for v39::system_networking::SystemNetworkingConfig
{
    fn from(value: SystemNetworkingConfig) -> Self {
        Self {
            // Downgrade: v42 RackNetworkConfig → v30 RackNetworkConfig.
            // The src_addr field is dropped.
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
