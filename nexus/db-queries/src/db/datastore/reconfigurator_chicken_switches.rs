// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries related to reconfigurator chicken switches

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::dsl::sql_query;
use diesel::expression::SelectableHelper;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::ReconfiguratorChickenSwitches as DbReconfiguratorChickenSwitches;
use nexus_db_model::SqlU32;
use nexus_types::deployment::ReconfiguratorChickenSwitches;
use nexus_types::deployment::ReconfiguratorChickenSwitchesParam;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    pub async fn reconfigurator_chicken_switches_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> ListResultVec<ReconfiguratorChickenSwitches> {
        use nexus_db_schema::schema::reconfigurator_chicken_switches;

        opctx
            .authorize(authz::Action::ListChildren, &authz::BLUEPRINT_CONFIG)
            .await?;

        let switches = paginated(
            reconfigurator_chicken_switches::table,
            reconfigurator_chicken_switches::version,
            pagparams,
        )
        .select(DbReconfiguratorChickenSwitches::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(switches
            .into_iter()
            .map(ReconfiguratorChickenSwitches::from)
            .collect())
    }

    pub async fn reconfigurator_chicken_switches_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<ReconfiguratorChickenSwitches>, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        use nexus_db_schema::schema::reconfigurator_chicken_switches::dsl;

        let latest = dsl::reconfigurator_chicken_switches
            .order_by(dsl::version.desc())
            .first_async::<DbReconfiguratorChickenSwitches>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest.map(Into::into))
    }

    pub async fn reconfigurator_chicken_switches_get(
        &self,
        opctx: &OpContext,
        version: u32,
    ) -> Result<Option<ReconfiguratorChickenSwitches>, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        use nexus_db_schema::schema::reconfigurator_chicken_switches::dsl;

        let latest = dsl::reconfigurator_chicken_switches
            .filter(dsl::version.eq(SqlU32::new(version)))
            .select(DbReconfiguratorChickenSwitches::as_select())
            .get_result_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest.map(Into::into))
    }

    /// Insert the current version of the chicken switches in the database
    ///
    /// Only succeeds if the prior version is the latest version currently in
    /// the `reconfigurator_chicken_switches` table. If there are no versions
    /// currently in the table, then the current swtiches must be at version 1.
    pub async fn reconfigurator_chicken_switches_insert_latest_version(
        &self,
        opctx: &OpContext,
        switches: ReconfiguratorChickenSwitchesParam,
    ) -> Result<(), Error> {
        let ReconfiguratorChickenSwitchesParam {
            version,
            planner_enabled,
            planner_switches,
        } = switches;
        let switches = ReconfiguratorChickenSwitches {
            version,
            planner_enabled,
            planner_switches,
            time_modified: chrono::Utc::now(),
        };

        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        let num_inserted =
            self.insert_latest_version_internal(opctx, &switches).await?;

        match num_inserted {
            0 => Err(Error::invalid_request(format!(
                "version {} is not the most recent",
                switches.version
            ))),
            1 => Ok(()),
            // This is impossible because we are explicitly inserting only one
            // row with a unique primary key.
            _ => unreachable!("query inserted more than one row"),
        }
    }

    /// Insert the next version of the chicken switches in the database
    ///
    /// Only succeeds if the prior version is the latest version currently
    /// in the `reconfigurator_chicken_switches` table.
    async fn insert_latest_version_internal(
        &self,
        opctx: &OpContext,
        switches: &ReconfiguratorChickenSwitches,
    ) -> Result<usize, Error> {
        if switches.version < 1 {
            return Err(Error::invalid_request(
                "version must be greater than 0",
            ));
        }

        sql_query(
            r"INSERT INTO reconfigurator_chicken_switches
                (version, planner_enabled, time_modified,
                 add_zones_with_mupdate_override)
              SELECT $1, $2, $3, $4
              WHERE $1 - 1 IN (
                  SELECT COALESCE(MAX(version), 0)
                  FROM reconfigurator_chicken_switches
              )",
        )
        .bind::<sql_types::BigInt, SqlU32>(switches.version.into())
        .bind::<sql_types::Bool, _>(switches.planner_enabled)
        .bind::<sql_types::Timestamptz, _>(switches.time_modified)
        .bind::<sql_types::Bool, _>(
            switches.planner_switches.add_zones_with_mupdate_override,
        )
        .execute_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::deployment::PlannerChickenSwitches;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_reconfigurator_chicken_switches_basic() {
        let logctx =
            dev::test_setup_log("test_reconfigurator_chicken_switches_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Listing an empty table should return an empty vec

        assert!(
            datastore
                .reconfigurator_chicken_switches_list(
                    opctx,
                    &DataPageParams::max_page()
                )
                .await
                .unwrap()
                .is_empty()
        );

        // Fail to insert a swtiches with version 0
        let mut switches = ReconfiguratorChickenSwitchesParam {
            version: 0,
            planner_enabled: false,
            planner_switches: PlannerChickenSwitches::default(),
        };

        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("version must be greater than 0")
        );

        // Inserting version 2 before version 1 should not work
        switches.version = 2;
        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("version 2 is not the most recent")
        );

        // Inserting version 1 should work
        switches.version = 1;
        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .is_ok()
        );

        // Inserting version 2 should work
        switches.version = 2;
        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .is_ok()
        );

        // Inserting version 4 should not work, since the prior version is 2
        switches.version = 4;
        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("version 4 is not the most recent")
        );

        // Inserting version 3 should work
        switches.version = 3;
        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .is_ok()
        );

        // Inserting version 4 should work
        switches.version = 4;
        switches.planner_enabled = true;
        assert!(
            datastore
                .reconfigurator_chicken_switches_insert_latest_version(
                    opctx, switches
                )
                .await
                .is_ok()
        );

        // Getting the latest version should return version 4
        let read = datastore
            .reconfigurator_chicken_switches_get_latest(opctx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(switches.version, read.version);
        assert_eq!(switches.planner_enabled, read.planner_enabled);

        // Getting version 4 should work
        let read = datastore
            .reconfigurator_chicken_switches_get(opctx, 4)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(switches.version, read.version);
        assert_eq!(switches.planner_enabled, read.planner_enabled);

        // Getting version 5 should fail, as it doesn't exist
        assert!(
            datastore
                .reconfigurator_chicken_switches_get(opctx, 5)
                .await
                .unwrap()
                .is_none()
        );

        let history = datastore
            .reconfigurator_chicken_switches_list(
                opctx,
                &DataPageParams::max_page(),
            )
            .await
            .unwrap();

        for i in 1..=4 {
            let switches = &history[i - 1];
            assert_eq!(switches.version, i as u32);
            if i != 4 {
                assert_eq!(switches.planner_enabled, false);
            } else {
                assert_eq!(switches.planner_enabled, true);
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
