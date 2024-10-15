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
use diesel::dsl::sql_query;
use diesel::expression::SelectableHelper;
use diesel::sql_types;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use nexus_db_model::ClickhouseModeEnum;
use nexus_db_model::ClickhousePolicy as DbClickhousePolicy;
use nexus_db_model::DbClickhouseMode;
use nexus_db_model::SqlU32;
use nexus_db_model::SqlU8;
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

    /// Return the clickhouse policy with the highest version
    pub async fn clickhouse_policy_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<ClickhousePolicy>, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use db::schema::clickhouse_policy::dsl;

        let latest_policy = dsl::clickhouse_policy
            .order_by(dsl::version.desc())
            .first_async::<DbClickhousePolicy>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest_policy.map(Into::into))
    }

    /// Insert the current version of the policy in the database
    ///
    /// Only succeeds if the prior version is the latest version currently
    /// in the `clickhouse_policy` table. If there are no versions currently
    /// in the table, then the current policy must be at version 1.
    pub async fn clickhouse_policy_insert_latest_version(
        &self,
        opctx: &OpContext,
        policy: &ClickhousePolicy,
    ) -> Result<(), Error> {
        if policy.version < 1 {
            return Err(Error::invalid_request(
                "policy version must be greater than 0",
            ));
        }
        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        let num_inserted = if policy.version == 1 {
            self.clickhouse_policy_insert_first_policy(opctx, &policy).await?
        } else {
            let prev_version = policy.version - 1;

            sql_query(
                r"INSERT INTO clickhouse_policy 
                 (version, clickhouse_mode, clickhouse_cluster_target_servers, 
                  clickhouse_cluster_target_keepers, time_created) 
                 SELECT $1, $2, $3, $4, $5 
                  FROM clickhouse_policy where version IN 
                    (SELECT version FROM clickhouse_policy WHERE version IN 
                      (SELECT version FROM clikchouse_policy 
                        ORDER BY version DESC LIMIT 1) 
                     AND version = $6)",
            )
            .bind::<sql_types::BigInt, SqlU32>(policy.version.into())
            .bind::<ClickhouseModeEnum, DbClickhouseMode>((&policy.mode).into())
            .bind::<sql_types::SmallInt, SqlU8>(
                policy.mode.target_servers().into(),
            )
            .bind::<sql_types::SmallInt, SqlU8>(
                policy.mode.target_keepers().into(),
            )
            .bind::<sql_types::Timestamptz, _>(policy.time_created)
            .bind::<sql_types::BigInt, SqlU32>(prev_version.into())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
        };

        match num_inserted {
            0 => Err(Error::invalid_request(format!(
                "policy version {} is not the most recent",
                policy.version
            ))),
            1 => Ok(()),
            // This is impossible because we are explicitly inserting only one
            // row with a unique primary key.
            _ => unreachable!("query inserted more than one row"),
        }
    }

    /// Insert the first clickhouse policy in the database at version 1.
    ///
    /// Only insert this policy if no other policy exists yet.
    ///
    /// Return the number of inserted rows or an error.
    async fn clickhouse_policy_insert_first_policy(
        &self,
        opctx: &OpContext,
        policy: &ClickhousePolicy,
    ) -> Result<usize, Error> {
        sql_query(
            r"INSERT INTO clickhouse_policy 
              (version, clickhouse_mode, clickhouse_cluster_target_servers, 
               clickhouse_cluster_target_keepers, time_created) 
             SELECT $1, $2, $3, $4, $5 
             WHERE NOT EXISTS (SELECT * FROM clickhouse_policy)",
        )
        .bind::<sql_types::BigInt, SqlU32>(policy.version.into())
        .bind::<ClickhouseModeEnum, DbClickhouseMode>((&policy.mode).into())
        .bind::<sql_types::SmallInt, SqlU8>(policy.mode.target_servers().into())
        .bind::<sql_types::SmallInt, SqlU8>(policy.mode.target_keepers().into())
        .bind::<sql_types::Timestamptz, _>(policy.time_created)
        .execute_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_inventory::now_db_precision;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::ClickhouseMode;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_clickhouse_policy_basic() {
        // Setup
        let logctx = dev::test_setup_log("test_clickhouse_policy_basic");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Listing an empty table should return an empty vec

        assert!(datastore
            .clickhouse_policy_list(&opctx, &DataPageParams::max_page())
            .await
            .unwrap()
            .is_empty());

        // Fail to insert a policy with version 0
        let mut policy = ClickhousePolicy {
            version: 0,
            mode: ClickhouseMode::SingleNodeOnly,
            time_created: now_db_precision(),
        };

        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_err());

        // Inserting version 2 before version 1 should not work
        policy.version = 2;
        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_err());

        // Inserting version 1 should work
        policy.version = 1;
        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_ok());

        // Inserting version 2 should work
        policy.version = 2;
        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_ok());

        // Inserting version 4 should not work, since the prior version is 2
        policy.version = 4;
        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_err());

        // Inserting version 3 should work
        policy.version = 3;
        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_ok());

        // Inserting version 4 should work
        policy.version = 4;
        policy.mode =
            ClickhouseMode::Both { target_servers: 3, target_keepers: 5 };
        assert!(datastore
            .clickhouse_policy_insert_latest_version(&opctx, &policy)
            .await
            .is_ok());

        let history = datastore
            .clickhouse_policy_list(&opctx, &DataPageParams::max_page())
            .await
            .unwrap();

        println!("{:#?}", history);

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
