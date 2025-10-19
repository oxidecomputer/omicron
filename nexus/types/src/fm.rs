// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management types.

use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{CollectionUuid, OmicronZoneUuid, SitrepUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct SitrepMetadata {
    pub id: SitrepUuid,
    pub parent_sitrep_id: Option<SitrepUuid>,
    pub inv_collection_id: CollectionUuid,
    pub creator_id: OmicronZoneUuid,
    pub comment: String,
    pub time_created: DateTime<Utc>,
}
