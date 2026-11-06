// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The fault management sitrep's representation of problems.
//!
//! This is distinct from [`crate::Problem`], the externally-visible problem
//! record that FM rendezvous creates for each [`FmProblem`] in the sitrep
//! being executed.

use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_problem;
use omicron_uuid_kinds::{ProblemKind, SitrepKind};

/// A problem in a fault management sitrep.
///
/// This corresponds to a record in the `fm_problem` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_problem)]
pub struct FmProblem {
    pub id: DbTypedUuid<ProblemKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the problem was first
    /// requested; see [`Self::requested_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which the problem was first requested.
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,
}
