// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for version STRICT_PUT_BODIES.

use omicron_common::api::external::Nullable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleUpdate {
    /// User comment for the support bundle
    pub user_comment: Nullable<String>,
}
