// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Active Problems.
//!
//! See [RFD 699](https://rfd.shared.oxide.computer/rfd/0699) for details on
//! Active Problems.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::ActiveProblem;
use crate::db::model::ActiveProblemRevision;
use crate::db::model::ActiveProblemView;
use async_bb8_diesel::AsyncRunQueryDsl;

use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::active_problem::dsl as problem_dsl;
use nexus_db_schema::schema::active_problem_revision::dsl as rev_dsl;
use omicron_common::api::external::LookupResult;

use omicron_uuid_kinds::GenericUuid;

impl DataStore {
    pub async fn active_problem_fetch(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::ActiveProblem,
    ) -> LookupResult<ActiveProblemView> {
        opctx.authorize(authz::Action::Read, authz_problem).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let (problem, revision) = problem_dsl::active_problem
            .filter(problem_dsl::id.eq(authz_problem.id().into_untyped_uuid()))
            .filter(problem_dsl::time_deleted.is_null())
            .inner_join(
                rev_dsl::active_problem_revision.on(rev_dsl::problem_id
                    .eq(problem_dsl::id)
                    .and(rev_dsl::revision.eq(problem_dsl::current_revision))),
            )
            .select((
                ActiveProblem::as_select(),
                ActiveProblemRevision::as_select(),
            ))
            .get_result_async::<(ActiveProblem, ActiveProblemRevision)>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_problem),
                )
            })?;
        ActiveProblemView::try_from((problem, revision))
    }
}
