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
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    pub async fn clickhouse_policy_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> ListResultVec<ClickhousePolicy> {
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

        Ok(policies.into_iter().map(ClickhousePolicy::from).collect())
    }

    pub async fn clickhouse_policy_insert_latest_version(
        &self,
        opctx: &OpContext,
        policy: ClickhousePolicy,
    ) -> Result<(), Error> {
        use db::schema::clickhouse_policy::dsl;

        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        if policy.version == 1 {
            return self
                .clickhouse_policy_insert_first_policy(opctx, policy)
                .await;
        }

        // Example query:
        // INSERT INTO clickhouse_policy SELECT 2, 'both', 3, 5, now() \
        // from clickhouse_policy \
        //   where version in  \
        //     (select version from clickhouse_policy WHERE version in \
        //        \\ (select version from clickhouse_policy \
        //              ORDER BY version DESC LIMIT 1) AND version = 1);

        todo!()
    }

    /// Insert the first clickhouse policy in the database at version 1.
    ///
    /// Only insert this policy if no other policy exists yet.
    async fn clickhouse_policy_insert_first_policy(
        &self,
        opctx: &OpContext,
        policy: ClickhousePolicy,
    ) -> Result<(), Error> {
        use db::schema::clickhouse_policy::dsl;

        // An example query:
        //
        // INSERT INTO clickhouse_policy \
        //    (version, clickhouse_mode, clickhouse_cluster_target_servers,  \
        //       clickhouse_cluster_target_keepers, time_created) \
        //    SELECT 1, 'single_node_only', 0, 0, now() \
        //    WHERE NOT EXISTS (SELECT * FROM clickhouse_policy)

        todo!()
    }
}
