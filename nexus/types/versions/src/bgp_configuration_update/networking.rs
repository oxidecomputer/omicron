// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `BGP_CONFIGURATION_UPDATE` version.
//!
//! Changes in this version:
//!
//! * New [`BgpConfigUpdate`] type to allow updating a BGP configuration's
//!   `name`, `description`, `max_paths` and `bgp_announce_set_id` fields
//!   without deleting and recreating the object.

use omicron_common::api::external::{Name, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v20::early_networking::MaxPathConfig;

/// Parameters for updating a BGP configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpConfigUpdate {
    /// Update the name of this BGP configuration.
    pub name: Name,

    /// Update the description of this BGP configuration.
    pub description: String,

    /// The autonomous system number of this BGP configuration. Cannot be updated.
    pub asn: u32,

    /// Update the BGP announce set associated with this configuration.
    pub bgp_announce_set_id: NameOrId,

    /// Update the maximum number of equal-cost paths.
    pub max_paths: MaxPathConfig,
}
