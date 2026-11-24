// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Problems (see [RFD 699]).
//!
//! [RFD 699]: https://rfd.shared.oxide.computer/rfd/0699

use super::Nexus;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::AlertFilters;
use nexus_db_queries::db::model::ProblemView;
use nexus_types::external_api::alert;
use nexus_types::external_api::problem as api;
use nexus_types::external_api::problem::ProblemSelector;
use nexus_types::external_api::support_bundle::SupportBundleInfo;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_generation_kinds::ProblemRevNum;
use uuid::Uuid;

impl Nexus {
    pub fn problem_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        ProblemSelector { problem_id }: ProblemSelector,
    ) -> LookupResult<lookup::Problem<'a>> {
        let problem =
            LookupPath::new(opctx, &self.db_datastore).problem_id(problem_id);
        Ok(problem)
    }

    /// Lists the current revisions of problems, optionally filtered by state,
    /// paginated by the time each problem was created and its ID.
    pub async fn problem_list(
        &self,
        opctx: &OpContext,
        params: &api::ProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<ProblemView> {
        self.db_datastore.problem_list(opctx, params, pagparams).await
    }

    /// Fetches the current revision of a problem.
    pub async fn problem_view(
        &self,
        opctx: &OpContext,
        problem_lookup: &lookup::Problem<'_>,
    ) -> LookupResult<ProblemView> {
        let (authz_problem, problem) =
            fetch_problem(problem_lookup, authz::Action::Read).await?;
        self.db_datastore
            .problem_fetch_current_revision(opctx, &authz_problem, problem)
            .await
    }

    /// Lists the problems which cause the requested problem.
    pub async fn problem_causes_list(
        &self,
        problem_lookup: &lookup::Problem<'_>,
        _selector: &api::ProblemListParams,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<api::RelatedProblem> {
        let (_authz_problem, _problem) =
            fetch_problem(problem_lookup, authz::Action::ListChildren).await?;
        // TODO(eliza): finish this part
        Ok(Vec::new())
    }

    /// Lists the problems caused by the requested problem.
    pub async fn problem_consequences_list(
        &self,
        problem_lookup: &lookup::Problem<'_>,
        _selector: &api::ProblemListParams,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<api::RelatedProblem> {
        let (_authz_problem, _problem) =
            fetch_problem(problem_lookup, authz::Action::ListChildren).await?;
        // TODO(eliza): finish this part
        Ok(Vec::new())
    }

    /// Lists the resources impacted by the requested problem.
    pub async fn problem_impact_list(
        &self,
        problem_lookup: &lookup::Problem<'_>,
        _selector: &api::ImpactListParams,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<api::ProblemImpact> {
        let (_authz_problem, _problem) =
            fetch_problem(problem_lookup, authz::Action::ListChildren).await?;
        // TODO(eliza): actually implement impact lists
        Ok(Vec::new())
    }

    /// Lists the support bundles attached to the requested problem.
    pub async fn problem_support_bundles_list(
        &self,
        opctx: &OpContext,
        problem_lookup: &lookup::Problem<'_>,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<SupportBundleInfo> {
        let (_authz_problem, problem) =
            fetch_problem(problem_lookup, authz::Action::ListChildren).await?;

        let bundles = self
            .db_datastore
            .problem_list_support_bundles(opctx, &problem, pagparams)
            .await?
            .into_iter()
            .map(SupportBundleInfo::from)
            .collect();

        Ok(bundles)
    }

    /// Lists alerts associated with the requested problem.
    pub async fn problem_alert_list(
        &self,
        opctx: &OpContext,
        problem_lookup: &lookup::Problem<'_>,
        params: &alert::AlertListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<alert::Alert> {
        // First, return an error on invalid query params (before looking up the
        // problem)
        let filters = AlertFilters::try_from(params.clone())?;
        let (_authz_problem, problem) =
            fetch_problem(problem_lookup, authz::Action::ListChildren).await?;
        // Next, fetch the problem's current revision, in order to use it to
        // construct the alert filters.
        let filters = filters.for_problem(&problem);

        let alerts = self
            .db_datastore
            .alert_list_matching(opctx, &filters, &pagparams)
            .await?
            .into_iter()
            .map(|(alert, _)| alert.into())
            .collect();

        Ok(alerts)
    }
}

async fn fetch_problem(
    lookup: &lookup::Problem<'_>,
    action: authz::Action,
) -> LookupResult<(authz::Problem, db::model::Problem)> {
    let (authz_problem, problem) = lookup.fetch_for(action).await?;
    // A problem at revision zero is still being created by rendezvous, and
    // is not visible in the external API until its first revision is
    // created and set as the current revision. `problem_view` returns a 404
    // for problems at revision zero, so for consistency, this function
    // should as well.
    if problem.current_revision() == ProblemRevNum::ZERO {
        return Err(authz_problem.not_found());
    }

    Ok((authz_problem, problem))
}
