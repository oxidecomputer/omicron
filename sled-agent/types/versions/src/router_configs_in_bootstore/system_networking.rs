// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for system-level networking.
//!
//! [`SystemNetworkingConfig`] now carries the router-configuration specs for
//! each switch's mgd plus the router list for control-plane service ports
//! (RFD 662).

use crate::v39::system_networking::BlueprintExternalNetworkingConfig;
use crate::v48;
use crate::v48::early_networking::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::router_config::{
    RouterConfigListEntry, SwitchRouterConfigs, default_router_list,
};

/// All configuration needed to set up system-level networking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

    /// Router-configuration specs per switch, rendered by Nexus. The scrimlet
    /// mgd reconciler combines them with `rack_network_config` and applies
    /// the result to its switch's mgd.
    pub switch_router_configs: SwitchRouterConfigs,

    /// The prioritized tunnel-router list for control-plane service ports
    /// (created by sled-agent before Nexus exists, so it rides the
    /// bootstore).
    pub control_plane_router_list: Vec<RouterConfigListEntry>,
}

impl SystemNetworkingConfig {
    pub const SCHEMA_VERSION: u32 = 10;
}

// See ../early_networking.rs for why it is okay for this to be fallible.
impl TryFrom<v48::system_networking::SystemNetworkingConfig>
    for SystemNetworkingConfig
{
    type Error = anyhow::Error;

    fn try_from(
        value: v48::system_networking::SystemNetworkingConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            rack_network_config: value.rack_network_config,
            blueprint_external_networking_config: value
                .blueprint_external_networking_config,
            switch_router_configs: SwitchRouterConfigs::new(),
            control_plane_router_list: default_router_list(),
        })
    }
}

impl From<SystemNetworkingConfig>
    for v48::system_networking::SystemNetworkingConfig
{
    fn from(value: SystemNetworkingConfig) -> Self {
        Self {
            rack_network_config: value.rack_network_config,
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
