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

use omicron_common::api::external::{IdentityMetadataUpdateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v20::early_networking::MaxPathConfig;

/// Parameters for updating a BGP configuration
///
/// If a value is not specified, it will remain unchanged.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpConfigUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,

    /// Update the BGP announce set associated with this configuration.
    pub bgp_announce_set_id: Option<NameOrId>,

    /// Update the maximum number of equal-cost paths.
    pub max_paths: Option<MaxPathConfig>,
}
