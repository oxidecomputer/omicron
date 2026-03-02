// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for the Nexus external API.

use chrono::DateTime;
use chrono::Utc;
use omicron_uuid_kinds::SupportBundleUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SupportBundlePath {
    #[doc = "ID of the "]
    #[doc = "support bundle"]
    pub bundle_id: Uuid,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleFilePath {
    #[serde(flatten)]
    pub bundle: SupportBundlePath,

    /// The file within the bundle to download
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleCreate {
    /// User comment for the support bundle
    pub user_comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleUpdate {
    /// User comment for the support bundle
    pub user_comment: Option<String>,
}

#[derive(
    Debug, Clone, Copy, JsonSchema, Serialize, Deserialize, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum SupportBundleState {
    /// Support Bundle still actively being collected.
    ///
    /// This is the initial state for a Support Bundle, and it will
    /// automatically transition to either "Failing" or "Active".
    ///
    /// If a user no longer wants to access a Support Bundle, they can
    /// request cancellation, which will transition to the "Destroying" state.
    Collecting,

    /// Support Bundle is being destroyed.
    ///
    /// Once backing storage has been freed, this bundle is destroyed.
    Destroying,

    /// Support Bundle was not created successfully, or was created and has lost
    /// backing storage.
    ///
    /// The record of the bundle still exists for readability, but the only
    /// valid operation on these bundles is to destroy them.
    Failed,

    /// Support Bundle has been processed, and is ready for usage.
    Active,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct SupportBundleInfo {
    #[schemars(with = "Uuid")]
    pub id: SupportBundleUuid,
    pub time_created: DateTime<Utc>,
    pub reason_for_creation: String,
    pub reason_for_failure: Option<String>,
    pub user_comment: Option<String>,
    pub state: SupportBundleState,
}
