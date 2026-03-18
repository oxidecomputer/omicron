// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::EreporterType;
use crate::SpMgsSlot;
use crate::SpType;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use nexus_db_schema::schema::ereporter_restart;
use omicron_uuid_kinds::EreporterRestartKind;

/// A restart generation for an ereport reporter at a particular physical
/// location.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = ereporter_restart)]
pub struct EreporterRestart {
    /// The restart generation's ID.
    pub id: DbTypedUuid<EreporterRestartKind>,
    /// The generation of this start relative to `(reporter_type, slot_type,
    /// slot)`.
    pub generation: i64,
    pub reporter_type: EreporterType,
    pub slot_type: SpType,
    pub slot: SpMgsSlot,
    pub time_first_seen: DateTime<Utc>,
}
