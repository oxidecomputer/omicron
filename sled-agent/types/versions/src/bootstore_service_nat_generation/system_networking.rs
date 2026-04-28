// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for system-level networking.
//!
//! Changes in this version:
//!
//! * New types added:
//!   * [`BlueprintExternalNetworkingConfig`], which wraps a
//!     [`ServiceZoneNatEntries`] and a generation number from the blueprint.
//! * Old types redefined:
//!   * [`SystemNetworkingConfig`] loses the `service_zone_nat_entries` field it
//!     had in favor of the new
//!     [`SystemNetworkingConfig::blueprint_external_networking_config`] field.

use crate::v30::early_networking::RackNetworkConfig;
use crate::v33;
use crate::v33::system_networking::ServiceZoneNatEntries;
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// External networking configuration controlled by Reconfigurator via
/// blueprints.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BlueprintExternalNetworkingConfig {
    /// The current generation number of the blueprint's external networking
    /// config.
    ///
    /// This generation number is only bumped when a new blueprint is produced
    /// that changes the external networking configuration in some way.
    pub blueprint_external_networking_generation: Generation,

    /// Set of all Omicron service zone NAT entries.
    pub service_zone_nat_entries: ServiceZoneNatEntries,
}

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
    //    this information at all, and we must be able to cleanly handle that at
    //    runtime.
    //
    // In the future, if we can find a way to relax RSS, we can eventually make
    // this field non-optional (once we're confident all deployed systems are
    // past the release we start populating this field).
    pub blueprint_external_networking_config:
        Option<BlueprintExternalNetworkingConfig>,
}

impl SystemNetworkingConfig {
    pub const SCHEMA_VERSION: u32 = 6;
}

impl From<SystemNetworkingConfig>
    for v33::system_networking::SystemNetworkingConfig
{
    fn from(value: SystemNetworkingConfig) -> Self {
        Self {
            rack_network_config: value.rack_network_config,
            service_zone_nat_entries: value
                .blueprint_external_networking_config
                .map(|config| config.service_zone_nat_entries),
        }
    }
}

impl From<v33::system_networking::SystemNetworkingConfig>
    for SystemNetworkingConfig
{
    fn from(value: v33::system_networking::SystemNetworkingConfig) -> Self {
        Self {
            rack_network_config: value.rack_network_config,
            blueprint_external_networking_config: value
                .service_zone_nat_entries
                .map(|service_zone_nat_entries| {
                    // Any config older than this version is from before when
                    // the blueprint _had_ an `external_networking_generation`.
                    // The initial migration set the value to
                    // `Generation::new()` (i.e., `1`), so we do the same here.
                    BlueprintExternalNetworkingConfig {
                        blueprint_external_networking_generation:
                            Generation::new(),
                        service_zone_nat_entries,
                    }
                }),
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
