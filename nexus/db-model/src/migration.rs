// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Generation;
use crate::schema::migration;
use crate::MigrationState;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::internal::nexus;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// The state of a migration as understood by Nexus.
#[derive(
    Clone,
    Debug,
    Queryable,
    Insertable,
    Selectable,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
)]
#[diesel(table_name = migration)]
pub struct Migration {
    /// The migration's UUID.
    ///
    /// This is the primary key of the migration table and is referenced by the
    /// `instance` table's `migration_id` field.
    pub id: Uuid,

    /// The instance that was migrated.
    pub instance_id: Uuid,

    /// The time at which this migration record was created.
    pub time_created: DateTime<Utc>,

    /// The time at which this migration record was deleted,
    pub time_deleted: Option<DateTime<Utc>>,

    /// The state of the migration source VMM.
    pub source_state: MigrationState,

    /// The ID of the migration source VMM.
    pub source_propolis_id: Uuid,

    /// The generation number for the source state.
    pub source_gen: Generation,

    /// The time the source VMM state was most recently updated.
    pub time_source_updated: Option<DateTime<Utc>>,

    /// The state of the migration target VMM.
    pub target_state: MigrationState,

    /// The ID of the migration target VMM.
    pub target_propolis_id: Uuid,

    /// The generation number for the target state.
    pub target_gen: Generation,

    /// The time the target VMM state was most recently updated.
    pub time_target_updated: Option<DateTime<Utc>>,
}

impl Migration {
    pub fn new(
        migration_id: Uuid,
        instance_id: InstanceUuid,
        source_propolis_id: Uuid,
        target_propolis_id: Uuid,
    ) -> Self {
        Self {
            id: migration_id,
            instance_id: instance_id.into_untyped_uuid(),
            time_created: Utc::now(),
            time_deleted: None,
            source_state: nexus::MigrationState::Pending.into(),
            source_propolis_id,
            source_gen: Generation::new(),
            time_source_updated: None,
            target_state: nexus::MigrationState::Pending.into(),
            target_propolis_id,
            target_gen: Generation::new(),
            time_target_updated: None,
        }
    }

    /// Returns `true` if either side reports that the migration is in a
    /// terminal state.
    pub fn is_terminal(&self) -> bool {
        self.source_state.is_terminal() || self.target_state.is_terminal()
    }

    /// Returns `true` if either side of the migration has failed.
    pub fn either_side_failed(&self) -> bool {
        self.source_state == MigrationState::FAILED
            || self.target_state == MigrationState::FAILED
    }

    /// Returns `true` if either side of the migration has completed.
    pub fn either_side_completed(&self) -> bool {
        self.source_state == MigrationState::COMPLETED
            || self.target_state == MigrationState::COMPLETED
    }
}
