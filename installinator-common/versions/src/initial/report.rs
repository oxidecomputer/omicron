// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report-related types for the Installinator API.

use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReportQuery {
    /// A unique identifier for the update.
    pub update_id: MupdateUuid,
}
