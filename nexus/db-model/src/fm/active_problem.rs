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
use nexus_db_schema::schema::fm_active_problem_alert;
use nexus_db_schema::schema::fm_active_problem_revision;
use nexus_db_schema::schema::fm_active_problem_support_bundle;
use nexus_types::fm::active_problem::{
    AlertLink, ProblemCode, SitrepActiveProblem, SitrepRevision,
    SupportBundleLink,
};
use omicron_common::api::external::Error;
use omicron_generation_kinds::ActiveProblemRevKind;
use omicron_uuid_kinds::{
    ActiveProblemKind, ActiveProblemUuid, AlertKind, SitrepKind,
    SupportBundleKind,
};

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

/// A link between an Active Problem in a fault management sitrep, and an alert.
/// The referenced alert need not be one requested in the same sitrep, and a
/// link will be created when the sitrep is executed as long as the referenced
/// alert still exists.
///
/// This corresponds to a record in the `fm_active_problem_alert` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_active_problem_alert)]
pub struct FmActiveProblemAlert {
    pub problem_id: DbTypedUuid<ActiveProblemKind>,
    pub alert_id: DbTypedUuid<AlertKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the link was first created;
    /// see [`Self::created_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this link was first created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    /// The Active Problem revision with which the alert is associated.
    pub revision: DbTypedGeneration<ActiveProblemRevKind>,
    pub comment: String,
}

/// A link between an Active Problem fault management sitrep and a support
/// bundle. The support bundle need not have been requested in the same sitrep,
/// and a link will be created when the sitrep is executed as long as the
/// referenced support bundle still exists.
///
/// This corresponds to a record in the `fm_active_problem_support_bundle`
/// table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_active_problem_support_bundle)]
pub struct FmActiveProblemSupportBundle {
    pub problem_id: DbTypedUuid<ActiveProblemKind>,
    pub support_bundle_id: DbTypedUuid<SupportBundleKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the link was first created;
    /// see [`Self::created_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this link was first created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    /// The Active Problem revision with which the support bundle is
    /// associated.
    pub revision: DbTypedGeneration<ActiveProblemRevKind>,
    pub comment: String,
}

impl FmActiveProblem {
    /// Returns the `fm_active_problem` record for `problem` in the sitrep
    /// `sitrep_id`.
    ///
    /// The problem's revisions, and records representing links to alerts and
    /// support bundles, are stored in separate tables. See
    /// [`FmActiveProblemRevision::from_sitrep`],
    /// [`FmActiveProblemAlert::from_sitrep`], and
    /// [`FmActiveProblemSupportBundle::from_sitrep`].
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem: &SitrepActiveProblem,
    ) -> Self {
        let SitrepActiveProblem {
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

impl FmActiveProblemRevision {
    /// Constructs an `fm_active_problem_revision` record for `revision` of the
    /// Active Problem `problem_id` in the sitrep `sitrep_id`.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem_id: ActiveProblemUuid,
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

impl TryFrom<FmActiveProblemRevision> for SitrepRevision {
    type Error = Error;

    /// # Errors
    ///
    /// Returns an internal error if the revision's problem code is not known
    /// to this version of the system software.
    fn try_from(revision: FmActiveProblemRevision) -> Result<Self, Error> {
        let FmActiveProblemRevision {
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

impl FmActiveProblemAlert {
    /// Constructs an `fm_active_problem_alert` record for `link` from the
    /// Active Problem `problem_id` in the sitrep `sitrep_id`.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem_id: ActiveProblemUuid,
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

impl From<FmActiveProblemAlert> for AlertLink {
    fn from(link: FmActiveProblemAlert) -> Self {
        let FmActiveProblemAlert {
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

impl FmActiveProblemSupportBundle {
    /// Constructs an `fm_active_problem_support_bundle` record for `link` from
    /// the Active Problem `problem_id` in the sitrep `sitrep_id`.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem_id: ActiveProblemUuid,
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

impl From<FmActiveProblemSupportBundle> for SupportBundleLink {
    fn from(link: FmActiveProblemSupportBundle) -> Self {
        let FmActiveProblemSupportBundle {
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
