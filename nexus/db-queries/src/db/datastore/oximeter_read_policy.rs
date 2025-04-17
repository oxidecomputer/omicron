// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries related to the oximeter_read policy

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::error::ErrorHandler;
use crate::db::error::public_error_from_diesel;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::dsl::sql_query;
use diesel::expression::SelectableHelper;
use diesel::sql_types;
use nexus_db_model::DbOximeterReadMode;
use nexus_db_model::OximeterReadPolicy as DbOximeterReadPolicy;
use nexus_db_model::SqlU32;
use nexus_db_schema::enums::OximeterReadModeEnum;
use nexus_types::deployment::OximeterReadPolicy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    /// Return a list of all oximeter_read policies
    pub async fn oximeter_read_policy_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> ListResultVec<OximeterReadPolicy> {
        use nexus_db_schema::schema::oximeter_read_policy;

        opctx
            .authorize(authz::Action::ListChildren, &authz::BLUEPRINT_CONFIG)
            .await?;

        let policies = paginated(
            oximeter_read_policy::table,
            oximeter_read_policy::version,
            pagparams,
        )
        .select(DbOximeterReadPolicy::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(policies.into_iter().map(OximeterReadPolicy::from).collect())
    }

    /// Return the clickhouse policy with the highest version
    pub async fn oximeter_read_policy_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<OximeterReadPolicy, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::oximeter_read_policy::dsl;

        let latest_policy = dsl::oximeter_read_policy
            .order_by(dsl::version.desc())
            .first_async::<DbOximeterReadPolicy>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest_policy.into())
    }

    /// Insert the current version of the policy in the database
    ///
    /// Only succeeds if the prior version is the latest version currently
    /// in the `oximeter_read_policy` table.
    pub async fn oximeter_read_policy_insert_latest_version(
        &self,
        opctx: &OpContext,
        policy: &OximeterReadPolicy,
    ) -> Result<(), Error> {
        // We pre-populate the database with version 1, so any new
        // version must be greater
        if policy.version < 2 {
            return Err(Error::invalid_request(
                "policy version must be greater than 1",
            ));
        }
        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        let num_inserted = self
            .oximeter_read_policy_insert_next_policy(opctx, &policy)
            .await?;
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

    /// Insert the next version of the policy in the database
    ///
    /// Only succeeds if the prior version is the latest version currently
    /// in the `oximeter_read_policy` table.
    ///
    /// Panics if `policy.version <= 1`;
    async fn oximeter_read_policy_insert_next_policy(
        &self,
        opctx: &OpContext,
        policy: &OximeterReadPolicy,
    ) -> Result<usize, Error> {
        assert!(policy.version > 1);
        let prev_version = policy.version - 1;

        sql_query(
                r"INSERT INTO oximeter_read_policy
                     (version, oximeter_read_mode, time_created)
                     SELECT $1, $2, $3
                      FROM oximeter_read_policy WHERE version = $4 AND version IN
                       (SELECT version FROM oximeter_read_policy
                        ORDER BY version DESC LIMIT 1)",
            )
            .bind::<sql_types::BigInt, SqlU32>(policy.version.into())
            .bind::<OximeterReadModeEnum, DbOximeterReadMode>((&policy.mode).into())
            .bind::<sql_types::Timestamptz, _>(policy.time_created)
            .bind::<sql_types::BigInt, SqlU32>(prev_version.into())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_inventory::now_db_precision;
    use nexus_types::deployment::OximeterReadMode;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_oximeter_read_policy_basic() {
        // Setup
        let logctx = dev::test_setup_log("test_oximeter_read_policy_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Listing the table should return a single initial entry
        assert!(
            datastore
                .oximeter_read_policy_list(opctx, &DataPageParams::max_page())
                .await
                .unwrap()
                .len()
                == 1
        );

        // Fail to insert a policy with version 0
        let mut policy = OximeterReadPolicy {
            version: 1,
            mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
        };

        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .unwrap_err()
                .to_string()
                .contains("policy version must be greater than 1")
        );

        // Inserting version 3 before version 2 should not work
        policy.version = 3;
        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .unwrap_err()
                .to_string()
                .contains("policy version 3 is not the most recent")
        );

        // Inserting version 2 should work
        policy.version = 2;
        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .is_ok()
        );

        // Inserting version 3 should work
        policy.version = 3;
        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .is_ok()
        );

        // Inserting version 5 should not work, since the prior version is 3
        policy.version = 5;
        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .unwrap_err()
                .to_string()
                .contains("policy version 5 is not the most recent")
        );

        // Inserting version 4 should work
        policy.version = 4;
        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .is_ok()
        );

        // Inserting version 4 should work
        policy.version = 5;
        policy.mode = OximeterReadMode::Cluster;
        assert!(
            datastore
                .oximeter_read_policy_insert_latest_version(opctx, &policy)
                .await
                .is_ok()
        );

        let history = datastore
            .oximeter_read_policy_list(opctx, &DataPageParams::max_page())
            .await
            .unwrap();

        for i in 1..=5 {
            let policy = &history[i - 1];
            assert_eq!(policy.version, i as u32);
            if i != 5 {
                assert!(matches!(policy.mode, OximeterReadMode::SingleNode));
            } else {
                assert!(matches!(policy.mode, OximeterReadMode::Cluster));
            }
        }

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
