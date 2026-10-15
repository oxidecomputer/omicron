// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Active Problems (see [RFD 699]).
//!
//! [RFD 699]: https://rfd.shared.oxide.computer/rfd/0699

use super::Nexus;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::model::ActiveProblemView;
use nexus_types::external_api::active_problem as api;
use nexus_types::external_api::active_problem::ActiveProblemSelector;
use nexus_types::external_api::alert;
use nexus_types::external_api::support_bundle::SupportBundleInfo;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;

impl Nexus {
    pub fn active_problem_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        ActiveProblemSelector { problem_id }: ActiveProblemSelector,
    ) -> LookupResult<lookup::ActiveProblem<'a>> {
        let problem = LookupPath::new(opctx, &self.db_datastore)
            .active_problem_id(problem_id);
        Ok(problem)
    }

    /// Fetches the current revision of an Active Problem.
    pub async fn active_problem_view(
        &self,
        opctx: &OpContext,
        problem_lookup: &lookup::ActiveProblem<'_>,
    ) -> LookupResult<ActiveProblemView> {
        let (.., authz_problem) =
            problem_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore.active_problem_fetch(opctx, &authz_problem).await
    }

    /// Lists the Active Problems which cause the requested Active Problem.
    pub async fn active_problem_causes_list(
        &self,
        problem_lookup: &lookup::ActiveProblem<'_>,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<api::RelatedActiveProblem> {
        let (.., _authz_problem) =
            problem_lookup.lookup_for(authz::Action::ListChildren).await?;
        // TODO(eliza): finish this part
        Ok(Vec::new())
    }

    /// Lists the Active Problems caused by the requested Active Problem.
    pub async fn active_problem_consequences_list(
        &self,
        problem_lookup: &lookup::ActiveProblem<'_>,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<api::RelatedActiveProblem> {
        let (.., _authz_problem) =
            problem_lookup.lookup_for(authz::Action::ListChildren).await?;
        // TODO(eliza): finish this part
        Ok(Vec::new())
    }

    /// Lists the resources impacted by the requested Active Problem.
    pub async fn active_problem_impact_list(
        &self,
        problem_lookup: &lookup::ActiveProblem<'_>,
        _pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<api::ActiveProblemImpact> {
        let (.., _authz_problem) =
            problem_lookup.lookup_for(authz::Action::ListChildren).await?;
        // TODO(eliza): actually implement impact lists
        Ok(Vec::new())
    }

    /// Lists the support bundles attached to the requested Active Problem.
    pub async fn active_problem_support_bundles_list(
        &self,
        problem_lookup: &lookup::ActiveProblem<'_>,
        _pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<SupportBundleInfo> {
        let (.., _authz_problem) =
            problem_lookup.lookup_for(authz::Action::ListChildren).await?;
        // TODO(eliza): actually add a way to associate active problems and
        // support bundles...
        Ok(Vec::new())
    }

    /// Lists alerts associated with the requested Active Problem.
    pub async fn active_problem_alert_list(
        &self,
        problem_lookup: &lookup::ActiveProblem<'_>,
        _params: &alert::AlertListParams,
        _pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<alert::Alert> {
        let (.., _authz_problem) =
            problem_lookup.lookup_for(authz::Action::ListChildren).await?;
        // TODO(eliza): actually add a way to associate active problems and
        // alerts...
        Ok(Vec::new())
    }
}
