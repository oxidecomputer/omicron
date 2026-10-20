// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representations of Active Problems (see [RFD 699]).
//!
//! An Active Problem is stored as an [`ActiveProblem`] record, which holds
//! only the problem's identity and its current revision number, and a series
//! of [`ActiveProblemRevision`] records, one per revision. Everything that
//! may change over the life of the problem lives in the revision records, so
//! that the full revision history of the problem is retained. The current
//! state of a problem is the revision whose `revision` number equals the
//! problem's `current_revision`.
//!
//! [RFD 699]: https://rfd.shared.oxide.computer/rfd/0699

use crate::DbTypedGeneration;
use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::{active_problem, active_problem_revision};
use nexus_types::external_api::active_problem as api;
use nexus_types::fm::active_problem::ProblemCode;
use nexus_types::identity::Asset;
use omicron_common::api::external::Error;
use omicron_generation_kinds::ActiveProblemRevGenerationKind;
use omicron_generation_kinds::ActiveProblemRevNum;
use omicron_uuid_kinds::ActiveProblemKind;
use omicron_uuid_kinds::ActiveProblemUuid;

/// An Active Problem.
///
/// This corresponds to a record in the `active_problem` table.
#[derive(Clone, Debug, PartialEq, Queryable, Insertable, Selectable, Asset)]
#[diesel(table_name = active_problem)]
#[asset(uuid_kind = ActiveProblemKind)]
pub struct ActiveProblem {
    #[diesel(embed)]
    pub identity: ActiveProblemIdentity,
    pub time_deleted: Option<DateTime<Utc>>,
    /// The revision number of this Active Problem's current revision.
    ///
    /// This selects the current revision from the `active_problem_revision`
    /// table.
    pub current_revision: DbTypedGeneration<ActiveProblemRevGenerationKind>,
}

impl ActiveProblem {
    pub fn current_revision(&self) -> ActiveProblemRevNum {
        self.current_revision.into()
    }
}

/// A revision of an Active Problem.
///
/// This corresponds to a record in the `active_problem_revision` table.
#[derive(Clone, Debug, PartialEq, Queryable, Insertable, Selectable)]
#[diesel(table_name = active_problem_revision)]
pub struct ActiveProblemRevision {
    pub problem_id: DbTypedUuid<ActiveProblemKind>,
    pub revision: DbTypedGeneration<ActiveProblemRevGenerationKind>,
    /// The time at which this revision was created.
    ///
    /// This is *not* the time at which the Active Problem itself was created.
    pub time_created: DateTime<Utc>,
    /// The time at which the Active Problem was resolved, if it is resolved
    /// as of this revision.
    ///
    /// If this is `None`, the Active Problem was active as of this revision.
    pub time_resolved: Option<DateTime<Utc>>,
    /// The Active Problem's problem code, as of this revision.
    pub code: SqlU32,
    pub summary: String,
    pub description: String,
    pub action_required: String,
}

/// A view of an Active Problem at its current revision.
#[derive(Debug, Clone)]
pub struct ActiveProblemView {
    pub identity: ActiveProblemIdentity,
    pub time_deleted: Option<DateTime<Utc>>,
    pub time_resolved: Option<DateTime<Utc>>,
    pub revision: ActiveProblemRevNum,
    pub code: ProblemCode,
    pub summary: String,
    pub description: String,
    pub action_required: String,
}

impl TryFrom<(ActiveProblem, ActiveProblemRevision)> for ActiveProblemView {
    type Error = Error;

    fn try_from(
        (metadata, rev): (ActiveProblem, ActiveProblemRevision),
    ) -> Result<Self, Self::Error> {
        let ActiveProblem { identity, current_revision, time_deleted } =
            metadata;
        let ActiveProblemRevision {
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
                    "expected a revision of Active Problem {id}, but found a \
                    revision of Active Problem {problem_id}",
                ),
            });
        }

        // Is this the revision we were supposed to have queried?
        if revision != current_revision {
            return Err(Error::InternalError {
                internal_message: format!(
                    "revision {} is not the current revision of Active Problem \
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

impl Asset for ActiveProblemView {
    type IdType = ActiveProblemUuid;

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

impl From<ActiveProblemView> for api::ActiveProblem {
    fn from(view: ActiveProblemView) -> Self {
        let identity = view.identity();
        let ActiveProblemView {
            identity: _identity,
            revision,
            time_resolved,
            // We shouldn't be attempting to convert a deleted active problem
            // into an API response, since any queries should exclude deleted
            // records...,
            time_deleted: _,
            code,
            summary,
            description,
            action_required,
        } = view;
        let state = match time_resolved {
            Some(_) => api::ActiveProblemState::Resolved,
            None => api::ActiveProblemState::Active,
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
