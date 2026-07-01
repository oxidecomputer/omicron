// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Identity metadata types for version STRICT_PUT_BODIES.

use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Update-time identity-related parameters for strict PUT bodies.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IdentityMetadataUpdateParamsStrict {
    pub name: Name,
    pub description: String,
}
