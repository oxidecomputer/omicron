// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project types for version UPDATE_VALUE_SEMANTICS.

use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a `Project`
///
/// A `PUT` replaces the resource, so every field is required: `name` and
/// `description` must both be present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectUpdate {
    pub name: Name,
    pub description: String,
}
