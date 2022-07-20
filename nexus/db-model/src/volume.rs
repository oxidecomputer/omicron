// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Region};
use crate::collection::DatastoreCollection;
use crate::schema::{region, volume};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Asset,
    Queryable,
    Insertable,
    Debug,
    Selectable,
    Serialize,
    Deserialize,
    Clone,
)]
#[diesel(table_name = volume)]
pub struct Volume {
    #[diesel(embed)]
    identity: VolumeIdentity,
    time_deleted: Option<DateTime<Utc>>,

    rcgen: Generation,

    data: String,
}

impl Volume {
    pub fn new(id: Uuid, data: String) -> Self {
        Self {
            identity: VolumeIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            data,
        }
    }

    pub fn data(&self) -> &str {
        &self.data
    }
}

// Volumes contain regions
impl DatastoreCollection<Region> for Volume {
    type CollectionId = Uuid;
    type GenerationNumberColumn = volume::dsl::rcgen;
    type CollectionTimeDeletedColumn = volume::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::volume_id;
}
