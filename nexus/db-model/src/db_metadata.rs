// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SemverVersion;
use crate::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::db_metadata;
use nexus_db_schema::schema::db_metadata_nexus;
use omicron_uuid_kinds::{
    BlueprintKind, BlueprintUuid, OmicronZoneKind, OmicronZoneUuid,
};
use serde::{Deserialize, Serialize};

/// Internal database metadata
#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = db_metadata)]
pub struct DbMetadata {
    singleton: bool,
    time_created: DateTime<Utc>,
    time_modified: DateTime<Utc>,
    version: SemverVersion,
    target_version: Option<SemverVersion>,
}

impl DbMetadata {
    pub fn time_created(&self) -> &DateTime<Utc> {
        &self.time_created
    }

    pub fn time_modified(&self) -> &DateTime<Utc> {
        &self.time_modified
    }

    pub fn version(&self) -> &SemverVersion {
        &self.version
    }
}

impl_enum_type!(
    DbMetadataNexusStateEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Serialize, Deserialize)]
    pub enum DbMetadataNexusState;

    // Enum values
    Active => b"active"
    NotYet => b"not_yet"
    Quiesced => b"quiesced"
);

#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = db_metadata_nexus)]
pub struct DbMetadataNexus {
    nexus_id: DbTypedUuid<OmicronZoneKind>,
    last_drained_blueprint_id: Option<DbTypedUuid<BlueprintKind>>,
    state: DbMetadataNexusState,
}

impl DbMetadataNexus {
    pub fn new(nexus_id: OmicronZoneUuid, state: DbMetadataNexusState) -> Self {
        Self {
            nexus_id: nexus_id.into(),
            last_drained_blueprint_id: None,
            state,
        }
    }

    pub fn state(&self) -> DbMetadataNexusState {
        self.state
    }

    pub fn nexus_id(&self) -> OmicronZoneUuid {
        self.nexus_id.into()
    }

    pub fn last_drained_blueprint_id(&self) -> Option<BlueprintUuid> {
        self.last_drained_blueprint_id.map(|id| id.into())
    }
}
