// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Datastore methods involving CockroachDB cluster settings.

use super::DataStore;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::raw_query_builder::QueryBuilder;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::deserialize::Queryable;
use diesel::sql_types;
use nexus_types::deployment::CockroachDbSettings;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;

impl DataStore {
    /// Get the current cluster settings.
    pub async fn cluster_settings(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<CockroachDbSettings> {
        #[derive(Debug, Queryable)]
        struct QueryOutput {
            version: String,
            preserve_downgrade_option: String,
        }
        type QueryRow = (sql_types::Text, sql_types::Text);

        const QUERY: &str = r#"
            SELECT * FROM
                [SHOW CLUSTER SETTING version],
                [SHOW CLUSTER SETTING cluster.preserve_downgrade_option]
        "#;

        let conn = self.pool_connection_authorized(opctx).await?;
        let output: QueryOutput = QueryBuilder::new()
            .sql(QUERY)
            .query::<QueryRow>()
            .get_result_async(&*conn)
            .await
            .map_err(|err| {
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        Ok(CockroachDbSettings {
            version: output.version,
            preserve_downgrade_option: Some(output.preserve_downgrade_option)
                .filter(|x| !x.is_empty()),
        })
    }

    /// Set (or reset) the `cluster.preserve_downgrade_option` cluster setting.
    ///
    /// When set (`version` is `Some`), it prevents CockroachDB from
    /// auto-finalizing future major version upgrades and preserve a downgrade
    /// path back to the current major version.
    ///
    /// When reset (`version` is `None`), it allows CockroachDB to auto-finalize
    /// the current major version, **destroying** the downgrade path to the
    /// previous major version.
    ///
    /// `version` must be the current cluster version; CockroachDB will reject
    /// any other value.
    ///
    /// This cannot be run in a multi-statement transaction.
    ///
    /// WARNING: The caller to this function must know what the _correct_
    /// value for `version` is without asking CockroachDB directly (that is,
    /// from some other metadata Nexus is aware of). During finalization,
    /// the `version` cluster setting reflects an in-between state, and the
    /// `cluster.preserve_downgrade_option` setting can possibly be set to this
    /// "upgrading-to" internal version identifier.
    pub async fn cluster_setting_preserve_downgrade(
        &self,
        opctx: &OpContext,
        version: Option<String>,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let mut query = QueryBuilder::new()
            .sql("SET CLUSTER SETTING cluster.preserve_downgrade_option = ");
        query = match version {
            Some(version) => {
                query.param().bind::<diesel::sql_types::Text, _>(version)
            }
            None => query.sql("DEFAULT"),
        };
        query.query::<()>().execute_async(&*conn).await.map_err(|err| {
            public_error_from_diesel(err, ErrorHandler::Server)
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{CockroachDbSettings, OpContext};
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    const COCKROACHDB_VERSION: &str =
        include_str!("../../../../../tools/cockroachdb_version");

    #[tokio::test]
    async fn test_preserve_downgrade_option() {
        let logctx = dev::test_setup_log("test_preserve_downgrade_option");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::test_utils::datastore_test(&logctx, &db)
                .await;
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), Arc::clone(&datastore));

        let version = COCKROACHDB_VERSION
            .trim_start_matches('v')
            .rsplit_once('.')
            .unwrap()
            .0;

        // With a fresh cluster, this is the expected state
        assert_eq!(
            datastore.cluster_settings(&opctx).await.unwrap(),
            CockroachDbSettings {
                version: version.to_string(),
                preserve_downgrade_option: None,
            }
        );

        // Test setting it (twice, to verify doing it again doesn't trigger
        // an error)
        for _ in 0..2 {
            datastore
                .cluster_setting_preserve_downgrade(
                    &opctx,
                    Some(version.to_string()),
                )
                .await
                .unwrap();
            assert_eq!(
                datastore.cluster_settings(&opctx).await.unwrap(),
                CockroachDbSettings {
                    version: version.to_string(),
                    preserve_downgrade_option: Some(version.to_string()),
                }
            );
        }

        // Test resetting it (twice, same reason)
        for _ in 0..2 {
            datastore
                .cluster_setting_preserve_downgrade(&opctx, None)
                .await
                .unwrap();
            assert_eq!(
                datastore.cluster_settings(&opctx).await.unwrap(),
                CockroachDbSettings {
                    version: version.to_string(),
                    preserve_downgrade_option: None,
                }
            );
        }

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
