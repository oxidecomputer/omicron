// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing fault management sitreps in the database.

use crate::SqlU32;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{CollectionKind, OmicronZoneKind, SitrepKind};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sitrep)]
pub struct SitrepMetadata {
    pub id: DbTypedUuid<SitrepKind>,
    pub parent_sitrep_id: Option<DbTypedUuid<SitrepKind>>,
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub creator_id: DbTypedUuid<OmicronZoneKind>,
    pub comment: String,
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = fm_current_sitrep)]
pub struct CurrentSitrep {
    pub version: SqlU32,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub response_authorized: bool,
    pub time_made_current: DateTime<Utc>,
}
