// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The fault management sitrep's representation of Active Problems.
//!
//! This is distinct from [`crate::ActiveProblem`], the externally-visible
//! Active Problem record that FM rendezvous creates for each
//! [`FmActiveProblem`] in the sitrep being executed.

use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_active_problem;
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
