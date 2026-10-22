// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The fault management sitrep's representation of problems.
//!
//! This is distinct from [`crate::Problem`], the externally-visible problem
//! record that FM rendezvous creates for each [`FmProblem`] in the sitrep
//! being executed.

use crate::DbTypedGeneration;
use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_problem;
use nexus_db_schema::schema::fm_problem_alert;
use nexus_db_schema::schema::fm_problem_revision;
use nexus_db_schema::schema::fm_problem_support_bundle;
use nexus_types::fm::problem::{
    AlertLink, ProblemCode, SitrepProblem, SitrepRevision, SupportBundleLink,
};
use omicron_common::api::external::Error;
use omicron_generation_kinds::ProblemRevKind;
use omicron_uuid_kinds::{
    AlertKind, ProblemKind, ProblemUuid, SitrepKind, SupportBundleKind,
};

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

/// A revision of a problem in a fault management sitrep.
///
/// This corresponds to a record in the `fm_problem_revision` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_problem_revision)]
pub struct FmProblemRevision {
    pub problem_id: DbTypedUuid<ProblemKind>,
    pub revision: DbTypedGeneration<ProblemRevKind>,

    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which this revision was first
    /// created; see [`Self::created_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this revision of the problem was first created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,

    /// The time at which the problem was resolved, if it is resolved as of
    /// this revision.
    ///
    /// If this is `None`, the problem will remain active when the revision is
    /// created.
    pub time_resolved: Option<DateTime<Utc>>,
    /// The problem's problem code as of this revision.
    pub code: SqlU32,
    pub summary: String,
    pub description: String,
    pub action_required: String,

    /// An internal human-readable comment explaining why the diagnosis engine
    /// requested this revision.
    ///
    /// This is used for debugging the sitrep, and is not part of the
    /// externally visible problem revision.
    pub comment: String,
}

/// A link between a problem in a fault management sitrep, and an alert.
/// The referenced alert need not be one requested in the same sitrep, and a
/// link will be created when the sitrep is executed as long as the referenced
/// alert still exists.
///
/// This corresponds to a record in the `fm_problem_alert` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_problem_alert)]
pub struct FmProblemAlert {
    pub problem_id: DbTypedUuid<ProblemKind>,
    pub alert_id: DbTypedUuid<AlertKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the link was first created;
    /// see [`Self::created_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this link was first created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    /// The problem revision with which the alert is associated.
    pub revision: DbTypedGeneration<ProblemRevKind>,
    pub comment: String,
}

/// A link between a problem in a fault management sitrep and a support
/// bundle. The support bundle need not have been requested in the same sitrep,
/// and a link will be created when the sitrep is executed as long as the
/// referenced support bundle still exists.
///
/// This corresponds to a record in the `fm_problem_support_bundle` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_problem_support_bundle)]
pub struct FmProblemSupportBundle {
    pub problem_id: DbTypedUuid<ProblemKind>,
    pub support_bundle_id: DbTypedUuid<SupportBundleKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the link was first created;
    /// see [`Self::created_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this link was first created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    /// The problem revision with which the support bundle is associated.
    pub revision: DbTypedGeneration<ProblemRevKind>,
    pub comment: String,
}

impl FmProblem {
    /// Returns the `fm_problem` record for `problem` in the sitrep
    /// `sitrep_id`.
    ///
    /// The problem's revisions, and records representing links to alerts and
    /// support bundles, are stored in separate tables. See
    /// [`FmProblemRevision::from_sitrep`], [`FmProblemAlert::from_sitrep`], and
    /// [`FmProblemSupportBundle::from_sitrep`].
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem: &SitrepProblem,
    ) -> Self {
        let SitrepProblem {
            id,
            requested_sitrep_id,
            revisions: _,
            alerts: _,
            support_bundles: _,
        } = problem;
        Self {
            id: (*id).into(),
            sitrep_id: sitrep_id.into(),
            requested_sitrep_id: (*requested_sitrep_id).into(),
        }
    }
}

impl FmProblemRevision {
    /// Constructs an `fm_problem_revision` record for `revision` of the
    /// problem `problem_id` in the sitrep `sitrep_id`.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem_id: ProblemUuid,
        revision: SitrepRevision,
    ) -> Self {
        let SitrepRevision {
            number,
            requested_sitrep_id,
            time_resolved,
            code,
            summary,
            description,
            action_required,
            comment,
        } = revision;
        Self {
            problem_id: problem_id.into(),
            revision: number.into(),
            sitrep_id: sitrep_id.into(),
            created_sitrep_id: requested_sitrep_id.into(),
            time_resolved,
            code: SqlU32::new(code.into()),
            summary,
            description,
            action_required,
            comment,
        }
    }
}

impl TryFrom<FmProblemRevision> for SitrepRevision {
    type Error = Error;

    /// # Errors
    ///
    /// Returns an internal error if the revision's problem code is not known
    /// to this version of the system software.
    fn try_from(revision: FmProblemRevision) -> Result<Self, Error> {
        let FmProblemRevision {
            problem_id: _,
            revision: number,
            sitrep_id: _,
            created_sitrep_id,
            time_resolved,
            code,
            summary,
            description,
            action_required,
            comment,
        } = revision;
        Ok(Self {
            number: number.into(),
            requested_sitrep_id: created_sitrep_id.into(),
            time_resolved,
            code: ProblemCode::try_from(u32::from(code))?,
            summary,
            description,
            action_required,
            comment,
        })
    }
}

impl FmProblemAlert {
    /// Constructs an `fm_problem_alert` record for `link` from the
    /// problem `problem_id` in the sitrep `sitrep_id`.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem_id: ProblemUuid,
        link: AlertLink,
    ) -> Self {
        let AlertLink { alert_id, revision, created_sitrep_id, comment } = link;
        Self {
            problem_id: problem_id.into(),
            alert_id: alert_id.into(),
            sitrep_id: sitrep_id.into(),
            created_sitrep_id: created_sitrep_id.into(),
            revision: revision.into(),
            comment,
        }
    }
}

impl From<FmProblemAlert> for AlertLink {
    fn from(link: FmProblemAlert) -> Self {
        let FmProblemAlert {
            problem_id: _,
            alert_id,
            sitrep_id: _,
            created_sitrep_id,
            revision,
            comment,
        } = link;
        Self {
            alert_id: alert_id.into(),
            revision: revision.into(),
            created_sitrep_id: created_sitrep_id.into(),
            comment,
        }
    }
}

impl FmProblemSupportBundle {
    /// Constructs an `fm_problem_support_bundle` record for `link` from
    /// the problem `problem_id` in the sitrep `sitrep_id`.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem_id: ProblemUuid,
        link: SupportBundleLink,
    ) -> Self {
        let SupportBundleLink {
            support_bundle_id,
            revision,
            created_sitrep_id,
            comment,
        } = link;
        Self {
            problem_id: problem_id.into(),
            support_bundle_id: support_bundle_id.into(),
            sitrep_id: sitrep_id.into(),
            created_sitrep_id: created_sitrep_id.into(),
            revision: revision.into(),
            comment,
        }
    }
}

impl From<FmProblemSupportBundle> for SupportBundleLink {
    fn from(link: FmProblemSupportBundle) -> Self {
        let FmProblemSupportBundle {
            problem_id: _,
            support_bundle_id,
            sitrep_id: _,
            created_sitrep_id,
            revision,
            comment,
        } = link;
        Self {
            support_bundle_id: support_bundle_id.into(),
            revision: revision.into(),
            created_sitrep_id: created_sitrep_id.into(),
            comment,
        }
    }
}
