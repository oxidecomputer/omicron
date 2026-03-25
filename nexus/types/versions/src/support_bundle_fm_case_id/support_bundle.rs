// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for version SUPPORT_BUNDLE_FM_CASE_ID.
//!
//! Adds `fm_case_id` to `SupportBundleInfo`.

use chrono::DateTime;
use chrono::Utc;
use omicron_uuid_kinds::SupportBundleUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::v2025_11_20_00::support_bundle::SupportBundleState;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct SupportBundleInfo {
    #[schemars(with = "Uuid")]
    pub id: SupportBundleUuid,
    pub time_created: DateTime<Utc>,
    pub reason_for_creation: String,
    pub reason_for_failure: Option<String>,
    pub user_comment: Option<String>,
    pub state: SupportBundleState,
    pub fm_case_id: Option<Uuid>,
}

impl From<SupportBundleInfo>
    for crate::v2025_11_20_00::support_bundle::SupportBundleInfo
{
    fn from(
        new: SupportBundleInfo,
    ) -> crate::v2025_11_20_00::support_bundle::SupportBundleInfo {
        crate::v2025_11_20_00::support_bundle::SupportBundleInfo {
            id: new.id,
            time_created: new.time_created,
            reason_for_creation: new.reason_for_creation,
            reason_for_failure: new.reason_for_failure,
            user_comment: new.user_comment,
            state: new.state,
        }
    }
}
