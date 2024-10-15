// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Datastore methods involving CockroachDB settings, which are managed by the
//! Reconfigurator.

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

/// This bit of SQL calculates a "state fingerprint" for the CockroachDB
/// cluster. `DataStore::cockroachdb_settings` calculates the fingerprint and
/// returns it to the caller. `DataStore::cockroach_setting_set_*` requires the
/// caller send the fingerprint, and it verifies it against the current state of
/// the cluster.
///
/// This is done to help prevent TOCTOU-class bugs that arise from blueprint
/// planning taking place before blueprint execution. Here are the ones we're
/// aware of, which guide the contents of this fingerprint:
///
/// - If the cluster version has changed, we are probably in the middle of
///   an upgrade. We should not be setting any settings and should re-plan.
///   (`crdb_internal.active_version()`)
/// - If the major version of CockroachDB has changed, we should not trust
///   the blueprint's value for the `cluster.preserve_downgrade_option`
///   setting; if set to an empty string and we've just upgraded the software
///   to the next major version, this will result in unwanted finalization.
///   (`crdb_internal.node_executable_version()`)
///
/// Because these are run as part of a gadget that allows CockroachDB to verify
/// the fingerprint during a `SET CLUSTER SETTING` statement, which cannot
/// be run as part of a multi-transaction statement or CTE, we are limited to
/// values that can be returned from built-in functions and operators.
///
/// This fingerprint should return a STRING value. It is safe to modify how this
/// fingerprint is calculated between Nexus releases; the stale fingerprint in
/// the previous blueprint will be rejected.
const STATE_FINGERPRINT_SQL: &str = r#"
    encode(digest(
        crdb_internal.active_version()
        || crdb_internal.node_executable_version()
    , 'sha1'), 'hex')
"#;

impl DataStore {
    /// Get the current CockroachDB settings.
    pub async fn cockroachdb_settings(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<CockroachDbSettings> {
        #[derive(Debug, Queryable)]
        struct QueryOutput {
            state_fingerprint: String,
            version: String,
            preserve_downgrade: String,
        }
        type QueryRow = (sql_types::Text, sql_types::Text, sql_types::Text);

        let conn = self.pool_connection_authorized(opctx).await?;
        let output: QueryOutput = QueryBuilder::new()
            .sql("SELECT ")
            .sql(STATE_FINGERPRINT_SQL)
            .sql(", * FROM ")
            .sql("[SHOW CLUSTER SETTING version], ")
            .sql("[SHOW CLUSTER SETTING cluster.preserve_downgrade_option]")
            .query::<QueryRow>()
            .get_result_async(&*conn)
            .await
            .map_err(|err| {
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        Ok(CockroachDbSettings {
            state_fingerprint: output.state_fingerprint,
            version: output.version,
            preserve_downgrade: output.preserve_downgrade,
        })
    }

    /// Set a CockroachDB setting with a `String` value.
    ///
    /// This cannot be run in a multi-statement transaction.
    pub async fn cockroachdb_setting_set_string(
        &self,
        opctx: &OpContext,
        state_fingerprint: String,
        setting: &'static str,
        value: String,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        QueryBuilder::new()
            .sql("SET CLUSTER SETTING ")
            .sql(setting)
            // `CASE` is the one conditional statement we get out of the
            // CockroachDB grammar for `SET CLUSTER SETTING`.
            .sql(" = CASE ")
            .sql(STATE_FINGERPRINT_SQL)
            .sql(" = ")
            .param()
            .sql(" WHEN TRUE THEN ")
            .param()
            // This is the gadget that allows us to reject changing a setting
            // if the fingerprint doesn't match. CockroachDB settings are typed,
            // but none of them are nullable, and NULL cannot be coerced into
            // any of them, so this branch returns an error if it's hit (tested
            // below in `test_state_fingerprint`).
            .sql(" ELSE NULL END")
            .bind::<sql_types::Text, _>(state_fingerprint)
            .bind::<sql_types::Text, _>(value.clone())
            .query::<()>()
            .execute_async(&*conn)
            .await
            .map_err(|err| {
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        info!(
            opctx.log,
            "set cockroachdb setting";
            "setting" => setting,
            "value" => &value,
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{CockroachDbSettings, OpContext};
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::CockroachDbClusterVersion;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_preserve_downgrade() {
        let logctx = dev::test_setup_log("test_preserve_downgrade");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::test_utils::datastore_test(&logctx, &db)
                .await;
        let opctx = OpContext::for_tests(
            logctx.log.new(o!()),
            Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        let settings = datastore.cockroachdb_settings(&opctx).await.unwrap();
        let version: CockroachDbClusterVersion =
            settings.version.parse().expect("unexpected cluster version");
        if settings.preserve_downgrade == "" {
            // This is the expected value while running tests normally.
            assert_eq!(version, CockroachDbClusterVersion::NEWLY_INITIALIZED);
        } else if settings.preserve_downgrade == version.to_string() {
            // This is the expected value if the cluster was created on a
            // previous version and `cluster.preserve_downgrade_option` was set.
            assert_eq!(version, CockroachDbClusterVersion::POLICY);
        } else {
            panic!(
                "`cluster.preserve_downgrade_option` is {:?},
                but it should be empty or \"{}\"",
                settings.preserve_downgrade, version
            );
        }

        // Verify that if a fingerprint is wrong, we get the expected SQL error
        // back.
        let Err(Error::InternalError { internal_message }) = datastore
            .cockroachdb_setting_set_string(
                &opctx,
                String::new(),
                "cluster.preserve_downgrade_option",
                version.to_string(),
            )
            .await
        else {
            panic!("should have returned an internal error");
        };
        assert_eq!(
            internal_message,
            "unexpected database error: \
            cannot use unknown tree.dNull value for string setting"
        );
        // And ensure that the state didn't change.
        assert_eq!(
            settings,
            datastore.cockroachdb_settings(&opctx).await.unwrap()
        );

        // Test setting it (twice, to verify doing it again doesn't trigger
        // an error)
        for _ in 0..2 {
            datastore
                .cockroachdb_setting_set_string(
                    &opctx,
                    settings.state_fingerprint.clone(),
                    "cluster.preserve_downgrade_option",
                    version.to_string(),
                )
                .await
                .unwrap();
            assert_eq!(
                datastore.cockroachdb_settings(&opctx).await.unwrap(),
                CockroachDbSettings {
                    state_fingerprint: settings.state_fingerprint.clone(),
                    version: version.to_string(),
                    preserve_downgrade: version.to_string(),
                }
            );
        }

        // Test resetting it (twice, same reason)
        for _ in 0..2 {
            datastore
                .cockroachdb_setting_set_string(
                    &opctx,
                    settings.state_fingerprint.clone(),
                    "cluster.preserve_downgrade_option",
                    String::new(),
                )
                .await
                .unwrap();
            let settings =
                datastore.cockroachdb_settings(&opctx).await.unwrap();
            if version == CockroachDbClusterVersion::NEWLY_INITIALIZED {
                assert_eq!(
                    settings,
                    CockroachDbSettings {
                        state_fingerprint: settings.state_fingerprint.clone(),
                        version: version.to_string(),
                        preserve_downgrade: String::new(),
                    }
                );
            } else {
                // Resetting it permits auto-finalization, so the state
                // fingerprint and version are not predictable until that
                // completes, but we can still verify that the variable was
                // reset.
                assert!(settings.preserve_downgrade.is_empty());
            }
        }

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
