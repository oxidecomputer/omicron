// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Active Problems.
//!
//! See [RFD 699](https://rfd.shared.oxide.computer/rfd/0699) for details on
//! Active Problems.

use super::DataStore;
use super::RunnableQuery;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::ActiveProblem;
use crate::db::model::ActiveProblemRevision;
use crate::db::model::ActiveProblemView;
use crate::db::pagination::paginated_multicolumn;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::active_problem::dsl as problem_dsl;
use nexus_db_schema::schema::active_problem_revision::dsl as rev_dsl;
use nexus_types::external_api::active_problem as api;
use nexus_types::identity::Asset;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl DataStore {
    /// Fetches the current revision of an already-fetched Active Problem
    /// record, returning a view of the problem at that revision.
    ///
    /// Since revisions are immutable once written, the returned
    /// [`ActiveProblemView`] is consistent as of the time `problem` was
    /// fetched, even if a newer revision has been created since then.
    ///
    /// The `problem` and `authz_problem` arguments must represent the same
    /// Active Problem (i.e. because they were both returned by a call to
    /// `lookup::ActiveProblem::fetch_for`). If they do not, this method returns
    /// an internal error.
    pub async fn active_problem_fetch_current_revision(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::ActiveProblem,
        problem: ActiveProblem,
    ) -> LookupResult<ActiveProblemView> {
        // Make sure the authz problem and the problem model refer to the same
        // Active Problem ID. We don't *strictly* need to check this before
        // hitting the DB, since the returned revision would not match the
        // problem, and `ActiveProblemView::try_from` will fail, but this saves
        // us a database query and lets us return a slightly more descriptive
        // error message.
        if problem.id() != authz_problem.id() {
            return Err(Error::InternalError {
                internal_message: format!(
                    "attempted to fetch the current revision of Active \
                     Problem {} using an `authz::ActiveProblem` for \
                     Active Problem {}. this is a bug!",
                    problem.id(),
                    authz_problem.id(),
                ),
            });
        }

        opctx.authorize(authz::Action::Read, authz_problem).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let revision = rev_dsl::active_problem_revision
            .filter(
                rev_dsl::problem_id.eq(authz_problem.id().into_untyped_uuid()),
            )
            .filter(rev_dsl::revision.eq(problem.current_revision))
            .select(ActiveProblemRevision::as_select())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_problem),
                )
            })?;
        ActiveProblemView::try_from((problem, revision))
    }

    /// List Active Problems matching the provided set of `filters`, paginated
    /// by `time_created` and `ID`.
    pub async fn active_problem_list(
        &self,
        opctx: &OpContext,
        filter: &api::ActiveProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<ActiveProblemView> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let problems = Self::active_problem_list_query(filter, pagparams)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        problems
            .into_iter()
            .map(ActiveProblemView::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    fn active_problem_list_query(
        filter: &api::ActiveProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> impl RunnableQuery<(ActiveProblem, ActiveProblemRevision)> + use<>
    {
        let mut query = paginated_multicolumn(
            problem_dsl::active_problem,
            (problem_dsl::time_created, problem_dsl::id),
            pagparams,
        )
        .filter(problem_dsl::time_deleted.is_null())
        .inner_join(
            rev_dsl::active_problem_revision.on(rev_dsl::problem_id
                .eq(problem_dsl::id)
                .and(rev_dsl::revision.eq(problem_dsl::current_revision))),
        )
        .select((
            ActiveProblem::as_select(),
            ActiveProblemRevision::as_select(),
        ));
        match filter.state {
            Some(api::ActiveProblemState::Active) => {
                query = query.filter(rev_dsl::time_resolved.is_null())
            }
            Some(api::ActiveProblemState::Resolved) => {
                query = query.filter(rev_dsl::time_resolved.is_not_null())
            }
            None => {}
        }
        query
    }
}
