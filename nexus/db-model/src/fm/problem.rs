// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The fault management sitrep's representation of problems.
//!
//! This is distinct from [`crate::Problem`], the externally-visible problem
//! record that FM rendezvous creates for each [`FmProblemRequest`] in the sitrep
//! being executed.

use crate::DbTypedGeneration;
use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_problem_alert;
use nexus_db_schema::schema::fm_problem_request;
use nexus_db_schema::schema::fm_problem_revision_request;
use nexus_db_schema::schema::fm_problem_support_bundle;
use nexus_types::fm::problem::{
    AlertLink, ProblemCode, SitrepProblem, SitrepRevision, SupportBundleLink,
};
use omicron_common::api::external::Error;
use omicron_generation_kinds::ProblemRevKind;
use omicron_uuid_kinds::{
    AlertKind, ProblemKind, ProblemUuid, SitrepKind, SupportBundleKind,
};

/// An active problem requested by a fault management diagnosis engine.
///
/// This is the sitrep's internal representation of a problem. In contrast, the
/// [`crate::Problem`] type represents an external-API-visible representation of
/// that problem. An entry in this table indicates that the sitrep wishes for a
/// corresponding entry in the `omicron.public.problem` table to exist for this
/// problem. Sitrep rendezvous creates the corresponding [`crate::Problem`]
/// records for all problems requested in the current sitrep.
///
/// This corresponds to a record in the `fm_problem_request` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_problem_request)]
pub struct FmProblemRequest {
    pub id: DbTypedUuid<ProblemKind>,
    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which the problem was first
    /// requested; see [`Self::requested_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which the problem was first requested.
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,

    /// An internal human-readable comment explaining why the diagnosis engine
    /// created this problem.
    ///
    /// This is used for debugging the sitrep, and is not part of the
    /// externally visible problem.
    pub comment: String,
}

/// A request from a fault management diagnosis engine to create a revision of
/// an active problem.
///
/// This is the sitrep's internal representation of a revision. Rendezvous
/// creates the externally visible [`crate::ProblemRevision`] records for every
/// revision requested in the sitrep being executed. Revision requests are
/// carried forward into subsequent sitreps until the problem has been resolved
/// *and* the externally visible revision record has been created.
///
/// Diagnosis engines may not request a revision that un-resolves a resolved
/// problem. Once the `time_resolved` field has been set in the most recent
/// revision, the sitrep builder will reject requests to create additional
/// revisions of the problem.
///
/// This corresponds to a record in the `fm_problem_revision_request` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_problem_revision_request)]
pub struct FmProblemRevisionRequest {
    pub problem_id: DbTypedUuid<ProblemKind>,
    /// The revision number.
    ///
    /// This must be greater than zero, which is enforced by a `CHECK`
    /// constraint in the database. The problem record, on the other hand, may
    /// have a revision of zero, but this is reserved for problems whose initial
    /// revision have not yet been created.
    pub revision: DbTypedGeneration<ProblemRevKind>,

    /// The sitrep that this record is part of.
    ///
    /// Note that this is *not* the sitrep in which this revision was first
    /// requested; see [`Self::requested_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this revision of the problem was first requested.
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,

    /// The time at which the problem was resolved, if it is resolved as of
    /// this revision.
    ///
    /// If this is `None`, the problem is active as of this revision. If it is
    /// `Some`, the problem is resolved.
    pub time_resolved: Option<DateTime<Utc>>,
    /// The problem's problem code as of this revision.
    ///
    /// This determines the problem's class, as well as its documentation.
    pub code: SqlU32,
    /// A one-sentence, human-readable summary of the problem as of this
    /// revision.
    pub summary: String,
    /// A human-readable description (one or more paragraphs) of the problem as
    /// of this revision.
    pub description: String,
    /// A human-readable description (one or more paragraphs) of any operator
    /// actions required to address the problem.
    pub action_required: String,

    /// An internal human-readable comment explaining why the diagnosis engine
    /// requested this revision.
    ///
    /// This is used for debugging the sitrep, and is not part of the
    /// externally visible problem revision.
    pub comment: String,
}

/// A request in a fault management sitrep that an alert be linked to a problem.
///
/// This is a sitrep-internal representation of a *request* that an alert be
/// linked to the problem. The existence of this record does not create an
/// external-API-visible link between the alert and the problem. Instead, this
/// record will be created by rendezvous when executing the sitrep that contains
/// this record.
///
/// The referenced alert need not be one requested in the same sitrep, and a
/// link will be created when the sitrep is executed as long as the referenced
/// alert still exists. Rendezvous must not create the externally visible link
/// until both the problem and the alert exist. Otherwise, a caller listing a
/// problem's alerts may see an alert that does not exist yet.
///
/// Once the problem is no longer part of the sitrep *and* rendezvous has
/// created the link, this record need not be carried forward into subsequent
/// sitreps.
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
    /// see [`Self::linked_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this link was first created.
    ///
    /// This is *not* the sitrep that requested the alert.
    pub linked_sitrep_id: DbTypedUuid<SitrepKind>,
    /// The problem revision with which the alert is associated.
    ///
    /// This is used to order a problem's alerts by revision, and must be
    /// greater than zero.
    pub revision: DbTypedGeneration<ProblemRevKind>,
    /// A human-readable comment explaining why the alert is linked to the
    /// problem, for debugging purposes.
    pub comment: String,
}

/// A link between a problem in a fault management sitrep and a support
/// bundle.
///
/// This is a sitrep-internal representation of a *request* that a support
/// bundle be linked to the problem. The existence of this record does not
/// create an external-API-visible link between the support bundle and the
/// problem. Instead, this record will be created by rendezvous when executing
/// the sitrep that contains this record.
///
/// The referenced support bundle need not be one requested in the same sitrep,
/// and a link will be created when the sitrep is executed as long as the
/// referenced support bundle still exists. Rendezvous must not create the externally
/// visible link until both the problem and the support bundle exist. Otherwise,
/// a caller listing a problem's support bundle may see one that does not exist
/// yet.
///
/// Once the problem is no longer part of the sitrep *and* rendezvous has
/// created the link, this record need not be carried forward into subsequent
/// sitreps.
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
    /// see [`Self::linked_sitrep_id`].
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The sitrep in which this link was first created.
    ///
    /// This is *not* the sitrep that requested the support bundle.
    pub linked_sitrep_id: DbTypedUuid<SitrepKind>,
    /// The problem revision with which the support bundle is associated.
    ///
    /// This is used to order a problem's support bundles by revision, and must
    /// be greater than zero.
    pub revision: DbTypedGeneration<ProblemRevKind>,
    /// A human-readable comment explaining why the support bundle is linked
    /// to the problem, for debugging purposes.
    pub comment: String,
}

impl FmProblemRequest {
    /// Returns the `fm_problem_request` record for `problem` in the sitrep
    /// `sitrep_id`.
    ///
    /// The problem's revisions, and records representing links to alerts and
    /// support bundles, are stored in separate tables. See
    /// [`FmProblemRevisionRequest::from_sitrep`], [`FmProblemAlert::from_sitrep`], and
    /// [`FmProblemSupportBundle::from_sitrep`].
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        problem: &SitrepProblem,
    ) -> Self {
        let SitrepProblem {
            id,
            requested_sitrep_id,
            comment,
            revisions: _,
            alerts: _,
            support_bundles: _,
        } = problem;
        Self {
            id: (*id).into(),
            sitrep_id: sitrep_id.into(),
            requested_sitrep_id: (*requested_sitrep_id).into(),
            comment: comment.clone(),
        }
    }
}

impl FmProblemRevisionRequest {
    /// Constructs an `fm_problem_revision_request` record for `revision` of the
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
            requested_sitrep_id: requested_sitrep_id.into(),
            time_resolved,
            code: SqlU32::new(code.into()),
            summary,
            description,
            action_required,
            comment,
        }
    }
}

impl TryFrom<FmProblemRevisionRequest> for SitrepRevision {
    type Error = Error;

    /// # Errors
    ///
    /// Returns an internal error if the revision's problem code is not known
    /// to this version of the system software.
    fn try_from(revision: FmProblemRevisionRequest) -> Result<Self, Error> {
        let FmProblemRevisionRequest {
            problem_id: _,
            revision: number,
            sitrep_id: _,
            requested_sitrep_id,
            time_resolved,
            code,
            summary,
            description,
            action_required,
            comment,
        } = revision;
        Ok(Self {
            number: number.into(),
            requested_sitrep_id: requested_sitrep_id.into(),
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
        let AlertLink { alert_id, revision, linked_sitrep_id, comment } = link;
        Self {
            problem_id: problem_id.into(),
            alert_id: alert_id.into(),
            sitrep_id: sitrep_id.into(),
            linked_sitrep_id: linked_sitrep_id.into(),
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
            linked_sitrep_id,
            revision,
            comment,
        } = link;
        Self {
            alert_id: alert_id.into(),
            revision: revision.into(),
            linked_sitrep_id: linked_sitrep_id.into(),
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
            linked_sitrep_id,
            comment,
        } = link;
        Self {
            problem_id: problem_id.into(),
            support_bundle_id: support_bundle_id.into(),
            sitrep_id: sitrep_id.into(),
            linked_sitrep_id: linked_sitrep_id.into(),
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
            linked_sitrep_id,
            revision,
            comment,
        } = link;
        Self {
            support_bundle_id: support_bundle_id.into(),
            revision: revision.into(),
            linked_sitrep_id: linked_sitrep_id.into(),
            comment,
        }
    }
}
