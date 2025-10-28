// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management types.

use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{CollectionUuid, OmicronZoneUuid, SitrepUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct Sitrep {
    pub metadata: SitrepMetadata,
    // TODO(eliza): draw the rest of the sitrep
}

impl Sitrep {
    pub fn id(&self) -> SitrepUuid {
        self.metadata.id
    }

    pub fn parent_id(&self) -> Option<SitrepUuid> {
        self.metadata.parent_sitrep_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct SitrepVersion {
    pub id: SitrepUuid,
    pub version: u32,
    pub time_made_current: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct SitrepMetadata {
    pub id: SitrepUuid,
    pub parent_sitrep_id: Option<SitrepUuid>,
    pub inv_collection_id: CollectionUuid,
    pub creator_id: OmicronZoneUuid,
    pub comment: String,
    pub time_created: DateTime<Utc>,
}
