// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries related to clickhouse policy

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::expression::SelectableHelper;
use diesel::QueryDsl;
use nexus_db_model::ClickhousePolicy as DbClickhousePolicy;
use nexus_db_model::SqlU32;
use nexus_types::deployment::ClickhousePolicy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    pub async fn clickhouse_policy_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> ListResultVec<Option<ClickhousePolicy>> {
        use db::schema::clickhouse_policy;

        opctx
            .authorize(authz::Action::ListChildren, &authz::BLUEPRINT_CONFIG)
            .await?;

        let policies = paginated(
            clickhouse_policy::table,
            clickhouse_policy::version,
            pagparams,
        )
        .select(DbClickhousePolicy::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(policies.into_iter().map(|p| p.into_clickhouse_policy).collect())
    }
}
