// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for system-level networking.
//!
//! Changes in this version:
//!
//! * [`SystemNetworkingConfig`] picks up the updated [`RackNetworkConfig`]
//!   from this version, which transitively includes the new `src_addr` field
//!   on [`BgpPeerConfig`].

use super::early_networking::RackNetworkConfig;
use crate::v33;
use crate::v39;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// External networking configuration controlled by Reconfigurator via
/// blueprints.
pub type BlueprintExternalNetworkingConfig =
    v39::system_networking::BlueprintExternalNetworkingConfig;

/// All configuration needed to set up system-level networking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SystemNetworkingConfig {
    pub rack_network_config: RackNetworkConfig,

    /// External networking configuration specified by blueprints.
    //
    // This field is optional for two reasons:
    //
    // 1. RSS has to initially populate a `SystemNetworkingConfig` with no
    //    blueprint-based networking config to start all the sled-agents. Once
    //    they all start, it computes a service plan, at which point it can fill
    //    this field in.
    // 2. Backwards compatibility: prior versions of this type did not store
    //    this information at all. If the bootstore contains an earlier
    //    `SystemNetworkingConfig` that we need to convert to the latest
    //    version, `blueprint_external_networking_config` will be `None`.
    //
    // In the future, if we can find a way to relax RSS, we can eventually make
    // this field non-optional (once we're confident all deployed systems are
    // past the release we start populating this field).
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
        old: v39::system_networking::SystemNetworkingConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            rack_network_config: old.rack_network_config.into(),
            blueprint_external_networking_config: old
                .blueprint_external_networking_config,
        })
    }
}

impl From<SystemNetworkingConfig>
    for v39::system_networking::SystemNetworkingConfig
{
    fn from(new: SystemNetworkingConfig) -> Self {
        Self {
            rack_network_config: new.rack_network_config.into(),
            blueprint_external_networking_config: new
                .blueprint_external_networking_config,
        }
    }
}

impl From<SystemNetworkingConfig>
    for v33::system_networking::SystemNetworkingConfig
{
    fn from(new: SystemNetworkingConfig) -> Self {
        let v39_config: v39::system_networking::SystemNetworkingConfig =
            new.into();
        v39_config.into()
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
