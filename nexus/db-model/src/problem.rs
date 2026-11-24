// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representations of problems (see [RFD 699]).
//!
//! A problem is stored as a [`Problem`] record, which holds only the problem's
//! identity and its current revision number, and a series of
//! [`ProblemRevision`] records, one per revision. Everything that may change
//! over the life of the problem lives in the revision records, so that the
//! full revision history of the problem is retained. The current
//! state of a problem is the revision whose `revision` number equals the
//! problem's `current_revision`.
//!
//! ## Alerts and Support Bundles
//!
//! Alerts and support bundles attached to a problem are represented by the
//! [`ProblemAlert`] and [`ProblemSupportBundle`] association tables. Rows in
//! these tables record the ID of the attached alert or support bundle, the
//! problem ID, and the revision of the problem at which the alert or support
//! bundle was linked to the problem. Because alerts and support bundles may be
//! attached to multiple problems, we must use a join table (rather than a
//! foreign key on `alert` or `support_bundle`) to express this relationship.
//!
//! Records in these tables are logically "owned" by the problem, and are
//! removed only when the problem itself is deleted. Because these records are
//! not removed while the problem still exists, the alerts and support bundles
//! attached to a problem at a given revision number are any records where the
//! attachment's revision number is less than or equal to that revision number.
//! Queries that list these resources for a given problem should always include
//! a `WHERE` clause that selects only resources attached at the problem's
//! current revision or earlier. This ensures that external API callers do not
//! encounter revisions that have not yet been fully created.
//!
//! These association records may exist before the alert or support bundle is
//! created, and may outlive it if it is deleted. Listing alerts or support
//! bundles selects records from the alert or support bundle tables, so it will
//! not return records that do not yet exist or have been deleted.
//!
//! [RFD 699]: https://rfd.shared.oxide.computer/rfd/0699

use crate::DbTypedGeneration;
use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::{
    problem, problem_alert, problem_revision, problem_support_bundle,
};
use nexus_types::external_api::problem as api;
use nexus_types::fm::problem::AlertLink;
use nexus_types::fm::problem::ProblemCode;
use nexus_types::fm::problem::SupportBundleLink;
use nexus_types::identity::Asset;
use omicron_common::api::external::Error;
use omicron_generation_kinds::ProblemRevKind;
use omicron_generation_kinds::ProblemRevNum;
use omicron_uuid_kinds::AlertKind;
use omicron_uuid_kinds::ProblemKind;
use omicron_uuid_kinds::ProblemUuid;
use omicron_uuid_kinds::SupportBundleKind;

/// A problem.
///
/// This corresponds to a record in the `problem` table.
#[derive(Clone, Debug, PartialEq, Queryable, Insertable, Selectable, Asset)]
#[diesel(table_name = problem)]
#[asset(uuid_kind = ProblemKind)]
pub struct Problem {
    #[diesel(embed)]
    pub identity: ProblemIdentity,
    pub time_deleted: Option<DateTime<Utc>>,
    /// The revision number of this problem's current revision.
    ///
    /// This selects the current revision from the `problem_revision`
    /// table.
    ///
    /// A problem may have a current revision of zero if and only if it is in
    /// the process of being created, before its first revision has been made
    /// current. Such problems are not returned when listing problems.
    pub current_revision: DbTypedGeneration<ProblemRevKind>,
}

impl Problem {
    pub fn current_revision(&self) -> ProblemRevNum {
        self.current_revision.into()
    }
}

/// A revision of a problem.
///
/// This corresponds to a record in the `problem_revision` table.
#[derive(Clone, Debug, PartialEq, Queryable, Insertable, Selectable)]
#[diesel(table_name = problem_revision)]
pub struct ProblemRevision {
    pub problem_id: DbTypedUuid<ProblemKind>,
    pub revision: DbTypedGeneration<ProblemRevKind>,
    /// The time at which this revision was created.
    ///
    /// This is *not* the time at which the problem itself was created.
    pub time_created: DateTime<Utc>,
    /// The time at which the problem was resolved, if it is resolved
    /// as of this revision.
    ///
    /// If this is `None`, the problem was active as of this revision.
    pub time_resolved: Option<DateTime<Utc>>,
    /// The problem's problem code, as of this revision.
    pub code: SqlU32,
    pub summary: String,
    pub description: String,
    pub action_required: String,
}

/// An alert attached to a problem.
///
/// This corresponds to a record in the `problem_alert` table.
///
/// Records in this table are logically part of the problem itself, and are
/// removed when it is deleted. They are created by FM rendezvous based on
/// requests in the sitrep [`crate::fm::FmProblemAlert`] table.
#[derive(Clone, Debug, PartialEq, Queryable, Insertable, Selectable)]
#[diesel(table_name = problem_alert)]
pub struct ProblemAlert {
    pub problem_id: DbTypedUuid<ProblemKind>,
    pub alert_id: DbTypedUuid<AlertKind>,
    /// The revision of the problem in which the alert was attached.
    ///
    /// The alert remains attached to all subsequent revisions of the problem.
    /// This must be greater than zero, which is enforced by a `CHECK`
    /// constraint in the database.
    pub revision: DbTypedGeneration<ProblemRevKind>,
}

impl ProblemAlert {
    /// Constructs the `problem_alert` record that should be created for a
    /// sitrep [`AlertLink`] requested by the problem with the specified
    /// `problem_id`.
    pub fn from_sitrep(problem_id: ProblemUuid, link: &AlertLink) -> Self {
        let AlertLink {
            alert_id,
            revision,
            // The sitrep in which the link was created and the comment
            // explaining it are debugging information that's internal to the
            // sitrep. These are not part of the externally-visible attachment
            // record.
            linked_sitrep_id: _,
            comment: _,
        } = link;
        Self {
            problem_id: problem_id.into(),
            alert_id: (*alert_id).into(),
            revision: (*revision).into(),
        }
    }
}

/// A support bundle attached to a problem.
///
/// This corresponds to a record in the `problem_support_bundle` table, and is
/// used to list support bundles attached to the problem as of a particular
/// revision.
///
/// Records in this table are logically owned by the problem, and are removed
/// when the problem is deleted. Records in this table are created by FM
/// rendezvous based on requests in the sitrep
/// [`crate::fm::FmProblemSupportBundle`] table.
#[derive(Clone, Debug, PartialEq, Queryable, Insertable, Selectable)]
#[diesel(table_name = problem_support_bundle)]
pub struct ProblemSupportBundle {
    pub problem_id: DbTypedUuid<ProblemKind>,
    pub support_bundle_id: DbTypedUuid<SupportBundleKind>,
    /// The revision of the problem in which the support bundle was attached.
    ///
    /// The support bundle remains attached to all subsequent revisions of the
    /// problem. This must be greater than zero, which is enforced by a `CHECK`
    /// constraint in the database.
    pub revision: DbTypedGeneration<ProblemRevKind>,
}

impl ProblemSupportBundle {
    /// Constructs the `problem_support_bundle` record that should be created
    /// for a sitrep [`SupportBundleLink`] requested by the problem with the
    /// specified `problem_id`.
    pub fn from_sitrep(
        problem_id: ProblemUuid,
        link: &SupportBundleLink,
    ) -> Self {
        let SupportBundleLink {
            support_bundle_id,
            revision,
            // The sitrep in which the link was created and the comment
            // explaining it are debugging information that's internal to the
            // sitrep. These are not part of the externally-visible attachment
            // record.
            linked_sitrep_id: _,
            comment: _,
        } = link;
        Self {
            problem_id: problem_id.into(),
            support_bundle_id: (*support_bundle_id).into(),
            revision: (*revision).into(),
        }
    }
}

/// A view of a problem at its current revision.
#[derive(Debug, Clone)]
pub struct ProblemView {
    pub identity: ProblemIdentity,
    pub time_deleted: Option<DateTime<Utc>>,
    pub time_resolved: Option<DateTime<Utc>>,
    pub revision: ProblemRevNum,
    pub code: ProblemCode,
    pub summary: String,
    pub description: String,
    pub action_required: String,
}

impl TryFrom<(Problem, ProblemRevision)> for ProblemView {
    type Error = Error;

    fn try_from(
        (metadata, rev): (Problem, ProblemRevision),
    ) -> Result<Self, Self::Error> {
        let Problem { identity, current_revision, time_deleted } = metadata;
        let ProblemRevision {
            problem_id,
            revision,
            time_created: _, // we don't care when the revision was created here.
            time_resolved,
            code,
            summary,
            description,
            action_required,
        } = rev;
        let id = identity.id;

        let code = ProblemCode::try_from(u32::from(code))?;

        // If the caller is trying to glue a revision of a totally unrelated
        // problem onto this problem, well, that's a bug!
        if identity.id != problem_id {
            return Err(Error::InternalError {
                internal_message: format!(
                    "expected a revision of problem {id}, but found a \
                    revision of problem {problem_id}",
                ),
            });
        }

        // Is this the revision we were supposed to have queried?
        if revision != current_revision {
            return Err(Error::InternalError {
                internal_message: format!(
                    "revision {} is not the current revision of problem \
                     {id} (expected revision {})",
                    revision.0, current_revision.0,
                ),
            });
        }

        Ok(Self {
            identity,
            revision: revision.into(),
            time_resolved,
            time_deleted,
            code,
            summary,
            description,
            action_required,
        })
    }
}

impl Asset for ProblemView {
    type IdType = ProblemUuid;

    fn id(&self) -> Self::IdType {
        self.identity.id.into()
    }

    fn time_created(&self) -> DateTime<Utc> {
        self.identity.time_created
    }

    fn time_modified(&self) -> DateTime<Utc> {
        self.identity.time_modified
    }
}

impl From<ProblemView> for api::Problem {
    fn from(view: ProblemView) -> Self {
        let identity = view.identity();
        let ProblemView {
            identity: _identity,
            revision,
            time_resolved,
            // We shouldn't be attempting to convert a deleted problem
            // into an API response, since any queries should exclude deleted
            // records...,
            time_deleted: _,
            code,
            summary,
            description,
            action_required,
        } = view;
        let (state, time_state_changed) = match time_resolved {
            Some(time_resolved) => (api::ProblemState::Resolved, time_resolved),
            // A problem becomes active when it is created, so the time at which
            // an active problem entered its state is the time it was created.
            None => (api::ProblemState::Active, identity.time_created),
        };
        Self {
            identity,
            revision: revision.as_u64(),
            state,
            time_state_changed,
            code: code.into(),
            class: code.description().class.to_string(),
            summary,
            description,
            action_required,
        }
    }
}

/// A filter for listing resources associated with a problem as of a particular
/// revision.
///
/// Resources attached at any revision less than or equal to `rev` match this
/// filter. Resources attached in later revisions do not, so that resources
/// attached in a revision that rendezvous has not yet made current are not
/// visible.
#[derive(Debug, Copy, Clone)]
pub struct ProblemRevisionFilter {
    pub id: DbTypedUuid<ProblemKind>,
    pub rev: DbTypedGeneration<ProblemRevKind>,
}

impl From<&'_ Problem> for ProblemRevisionFilter {
    fn from(problem: &Problem) -> Self {
        Self { id: problem.identity.id, rev: problem.current_revision }
    }
}

impl From<&'_ ProblemView> for ProblemRevisionFilter {
    fn from(problem: &ProblemView) -> Self {
        Self { id: problem.identity.id, rev: problem.revision.into() }
    }
}
