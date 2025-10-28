// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing fault management sitreps in the database.

use crate::SqlU32;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{fm_sitrep, fm_sitrep_history};
use omicron_uuid_kinds::{CollectionKind, OmicronZoneKind, SitrepKind};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sitrep)]
pub struct SitrepMetadata {
    pub id: DbTypedUuid<SitrepKind>,
    pub parent_sitrep_id: Option<DbTypedUuid<SitrepKind>>,
    pub inv_collection_id: DbTypedUuid<CollectionKind>,
    pub creator_id: DbTypedUuid<OmicronZoneKind>,
    pub comment: String,
    pub time_created: DateTime<Utc>,
}

impl From<SitrepMetadata> for nexus_types::fm::SitrepMetadata {
    fn from(db_meta: SitrepMetadata) -> Self {
        let SitrepMetadata {
            id,
            parent_sitrep_id,
            inv_collection_id,
            creator_id,
            comment,
            time_created,
        } = db_meta;
        Self {
            id: id.into(),
            parent_sitrep_id: parent_sitrep_id.map(Into::into),
            inv_collection_id: inv_collection_id.into(),
            creator_id: creator_id.into(),
            comment,
            time_created,
        }
    }
}

impl From<nexus_types::fm::SitrepMetadata> for SitrepMetadata {
    fn from(db_meta: nexus_types::fm::SitrepMetadata) -> Self {
        let nexus_types::fm::SitrepMetadata {
            id,
            parent_sitrep_id,
            inv_collection_id,
            creator_id,
            comment,
            time_created,
        } = db_meta;
        Self {
            id: id.into(),
            parent_sitrep_id: parent_sitrep_id.map(Into::into),
            inv_collection_id: inv_collection_id.into(),
            creator_id: creator_id.into(),
            comment,
            time_created,
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = fm_sitrep_history)]
pub struct SitrepVersion {
    pub version: SqlU32,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub time_made_current: DateTime<Utc>,
}

impl From<SitrepVersion> for nexus_types::fm::SitrepVersion {
    fn from(db_version: SitrepVersion) -> Self {
        let SitrepVersion { sitrep_id, version, time_made_current } =
            db_version;
        Self {
            id: sitrep_id.into(),
            version: version.into(),
            time_made_current,
        }
    }
}
