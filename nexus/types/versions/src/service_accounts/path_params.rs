// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Path parameter types for version SERVICE_ACCOUNTS.

use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ServiceAccountPath {
    /// Name or ID of the service account
    pub service_account: NameOrId,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ServiceAccountKeyPath {
    /// Name or ID of the service account
    pub service_account: NameOrId,
    /// Name or ID of the service account key
    pub key: NameOrId,
}
