// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The fault management sitrep's representation of Active Problems.
//!
//! This is distinct from [`crate::ActiveProblem`], the externally-visible
//! Active Problem record that FM rendezvous creates for each
//! [`FmActiveProblem`] in the sitrep being executed.

use crate::DbTypedGeneration;
use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_active_problem;
use nexus_db_schema::schema::fm_active_problem_revision;
use omicron_generation_kinds::ActiveProblemRevKind;
use omicron_uuid_kinds::{ActiveProblemKind, SitrepKind};

/// An Active Problem in a fault management sitrep.
///
/// This corresponds to a record in the `fm_active_problem` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_active_problem)]
pub struct FmActiveProblem {
    pub id: DbTypedUuid<ActiveProblemKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the Active Problem was
    /// first requested; see [`Self::requested_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which the Active Problem was first requested.
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,
}

/// A revision of an Active Problem in a fault management sitrep.
///
/// This corresponds to a record in the `fm_active_problem_revision` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_active_problem_revision)]
pub struct FmActiveProblemRevision {
    pub problem_id: DbTypedUuid<ActiveProblemKind>,
    pub revision: DbTypedGeneration<ActiveProblemRevKind>,

    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which this revision was first
    /// created; see [`Self::created_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this revision of the Active Problem was first
    /// created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,

    /// The time at which the Active Problem was resolved, if it is resolved
    /// as of this revision.
    ///
    /// If this is `None`, the Active Problem will remain active when the
    /// revision is created.
    pub time_resolved: Option<DateTime<Utc>>,
    /// The Active Problem's problem code as of this revision.
    pub code: SqlU32,
    pub summary: String,
    pub description: String,
    pub action_required: String,

    /// An internal human-readable comment explaining why the diagnosis engine
    /// requested this revision.
    ///
    /// This is used for debugging the sitrep, and is not part of the
    /// externally visible Active Problem revision.
    pub comment: String,
}
