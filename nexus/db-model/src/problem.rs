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
//! [RFD 699]: https://rfd.shared.oxide.computer/rfd/0699

use crate::DbTypedGeneration;
use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::{problem, problem_revision};
use nexus_types::external_api::problem as api;
use nexus_types::fm::problem::ProblemCode;
use nexus_types::identity::Asset;
use omicron_common::api::external::Error;
use omicron_generation_kinds::ProblemGeneration;
use omicron_generation_kinds::ProblemGenerationKind;
use omicron_uuid_kinds::ProblemKind;
use omicron_uuid_kinds::ProblemUuid;

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
    pub current_revision: DbTypedGeneration<ProblemGenerationKind>,
}

impl Problem {
    pub fn current_revision(&self) -> ProblemGeneration {
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
    pub revision: DbTypedGeneration<ProblemGenerationKind>,
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

/// A view of a problem at its current revision.
#[derive(Debug, Clone)]
pub struct ProblemView {
    pub identity: ProblemIdentity,
    pub time_deleted: Option<DateTime<Utc>>,
    pub time_resolved: Option<DateTime<Utc>>,
    pub revision: ProblemGeneration,
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
        let state = match time_resolved {
            Some(_) => api::ProblemState::Resolved,
            None => api::ProblemState::Active,
        };
        Self {
            identity,
            revision: revision.as_u64(),
            time_resolved,
            state,
            code: code.into(),
            class: code.description().class.to_string(),
            summary,
            description,
            action_required,
        }
    }
}
