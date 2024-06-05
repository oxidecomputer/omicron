// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::migration;
use crate::MigrationState;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// The state of a migration as understood by Nexus.
#[derive(
    Clone, Debug, Queryable, Insertable, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = migration)]
pub struct Migration {
    /// The migration's UUID.
    ///
    /// This is the primary key of the migration table and is referenced by the
    /// `instance` table's `migration_id` field.
    pub id: Uuid,

    /// The state of the migration source VMM.
    pub source_state: MigrationState,

    /// The ID of the migration source VMM.
    pub source_propolis_id: Uuid,

    /// The state of the migration target VMM.
    pub target_state: MigrationState,

    /// The ID of the migration target VMM.
    pub target_propolis_id: Uuid,
}
