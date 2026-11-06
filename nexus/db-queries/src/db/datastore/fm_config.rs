// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries related to fault management runtime configuration.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::RunnableQuery;
use crate::db::true_or_cast_error::TrueOrCastError;
use crate::db::true_or_cast_error::matches_sentinel;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::QueryResult;
use diesel::define_sql_function;
use diesel::pg::Pg;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::SqlU32;
use nexus_db_schema::schema::fm_config::dsl;
use nexus_types::fm::FmConfigParam;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;

/// Uncastable sentinel used to detect an attempt to insert a config version
/// that is not exactly 1 greater than the current latest version.
const VERSION_NOT_CURRENT: &str = "version-not-current";

impl DataStore {
    /// Read the current FM configuration override, or `None` if no overrides
    /// exist in the database.
    pub async fn fm_config_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<db::model::fm::FmConfig>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FM_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        dsl::fm_config
            .order_by(dsl::version.desc())
            .first_async::<db::model::fm::FmConfig>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Read the FM configuration override at a specific version, or `None` if
    /// no override exists at that version.
    pub async fn fm_config_get(
        &self,
        opctx: &OpContext,
        version: NonZeroU32,
    ) -> Result<Option<db::model::fm::FmConfig>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FM_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        dsl::fm_config
            .filter(dsl::version.eq(SqlU32::new(version.get())))
            .first_async::<db::model::fm::FmConfig>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a new version of the FM configuration override in the database.
    ///
    /// This only succeeds if the provided version is exactly one greater than
    /// the latest version currently in the `fm_config` table, or 1 if the
    /// table contains no overrides.
    pub async fn fm_config_insert_latest_version(
        &self,
        opctx: &OpContext,
        config: FmConfigParam,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FM_CONFIG).await?;

        // Validate the config and convert it to its database representation.
        // This rejects invalid configs with client errors, rather than relying
        // on the corresponding CHECK constraints on the `fm_config` table,
        // whose violations would surface as opaque internal errors.
        let config = db::model::fm::FmConfig::new(config)?;
        let version = config.version;

        Self::insert_latest_version_query(config)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_| ())
            .map_err(|e| {
                match e {
                    // The query's check that the version is exactly 1 greater
                    // than the current max version failed!
                    e if matches_sentinel(&e, &[VERSION_NOT_CURRENT])
                        .is_some() =>
                    {
                        Error::invalid_value(
                            "version",
                            format!("{version} is not the most recent"),
                        )
                    }
                    // If the UNIQUE constraint on the primary key was violated,
                    // we raced with another insert.
                    DieselError::DatabaseError(
                        DatabaseErrorKind::UniqueViolation,
                        _,
                    ) => Error::conflict(format!(
                        "version {version} has already been created",
                    )),
                    // Note: we could probably also turn CHECK violations into
                    // something less opaque than an `internal_error` here, but
                    // we already validated the config above to try and return a
                    // more friendly error message if it was invalid, and those
                    // *should* always line up.
                    e => public_error_from_diesel(e, ErrorHandler::Server),
                }
            })
    }

    /// Returns a query for inserting a new record into the `fm_config` table,
    /// if and only if the inserted version is exactly 1 more than the largest
    /// version currently in the table (or 1 if the table is empty). This is
    /// used by [`Self::fm_config_insert_latest_version`].
    ///
    /// ## Why's It Like That?
    ///
    /// This query is structured as a CTE which determines the maximum version
    /// currently in the table, and checks if the inserted
    /// [`db::model::fm::FmConfig`]'s version is exactly one greater, using the
    /// [`TrueOrCastError`] pattern to fail the query if it is not. We do it
    /// this way, rather than using an `INSERT ... FROM SELECT ... WHERE ...` to
    /// perform the check, because I wanted to be able to use the `Insertable`
    /// implementation for `FmConfig` to generate the insert part of the query.
    /// This felt nicer because it will always work if new fields are added to
    /// the table and to the model struct without requiring the query itself to
    /// also be updated. But, using `Insertable` means we have to do it like
    /// this rather than using a `WHERE` clause, because it cannot be used to
    /// generate a SELECT statement, only a complete
    /// `INSERT INTO ... VALUES (...)` statement.
    ///
    /// Callers must check for [`VERSION_NOT_CURRENT`] to handle the case where
    /// the check has failed.
    ///
    /// The SQL generated here is output to
    /// `tests/output/fm_config_insert_latest_version.sql`.
    fn insert_latest_version_query(
        config: db::model::fm::FmConfig,
    ) -> impl RunnableQuery<i64> {
        use diesel::query_builder::AstPass;
        use diesel::query_builder::Query;
        use diesel::query_builder::QueryFragment;
        use diesel::query_builder::QueryId;

        // This struct exists so that we can use `QueryFramgent::walk_ast` to
        // interpolate the bits of the query we'd rather build using diesel into
        // the broader CTE. However, we return `impl RunnableQuery` rather than
        // a named `ConfigInsertQuery` because I *really* didn't want to figure
        // out the actual type names of the diesel blobs that this is generic
        // over. :)
        struct ConfigInsertQuery<C, I> {
            check_version: C,
            insert_config: I,
        }

        impl<C, I> QueryId for ConfigInsertQuery<C, I> {
            type QueryId = ();
            const HAS_STATIC_QUERY_ID: bool = false;
        }

        impl<C, I> Query for ConfigInsertQuery<C, I> {
            type SqlType = sql_types::BigInt;
        }

        impl<C, I> diesel::RunQueryDsl<DbConnection> for ConfigInsertQuery<C, I> {}

        impl<C, I> QueryFragment<Pg> for ConfigInsertQuery<C, I>
        where
            C: QueryFragment<Pg>,
            I: QueryFragment<Pg>,
        {
            fn walk_ast<'b>(
                &'b self,
                mut out: AstPass<'_, 'b, Pg>,
            ) -> QueryResult<()> {
                // `MATERIALIZED` is necessary here to ensure the version check
                // part does not get optimized out by the query planner, since
                // nothing else references it.
                out.push_sql("WITH check_version AS MATERIALIZED (SELECT ");
                self.check_version.walk_ast(out.reborrow())?;
                out.push_sql("), inserted_config AS (");
                self.insert_config.walk_ast(out.reborrow())?;
                // All CTE arms must return something, so we'll return the
                // number of inserted rows (which should always be 1, but
                // whatever).
                out.push_sql(
                    " RETURNING version) \
                     SELECT count(*) FROM inserted_config",
                );
                Ok(())
            }
        }

        define_sql_function! {
            fn coalesce(
                x: sql_types::Nullable<sql_types::BigInt>,
                y: sql_types::BigInt,
            ) -> sql_types::BigInt;
        }

        // The check part
        let check_version = {
            // Select the highest version number currently in the table.
            // COALESCE nulls to 0 in the case where the table is empty.
            let current_version = coalesce(
                dsl::fm_config
                    .select(diesel::dsl::max(dsl::version))
                    .single_value(),
                0,
            );

            // `version - 1` cannot underflow, since `config.version` is a
            // `NonZeroU32`.
            let prev_version = SqlU32::new(u32::from(config.version) - 1);

            TrueOrCastError::new(
                current_version.eq(prev_version),
                VERSION_NOT_CURRENT,
            )
        };

        // The INSERT. This is the easy bit, but it's also the bit where I
        // *really* wanted to make sure we were using diesel's `Insertable`
        // implementation.
        let insert_config = diesel::insert_into(dsl::fm_config).values(config);

        ConfigInsertQuery { check_version, insert_config }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use nexus_types::fm::FmConfig;
    use nexus_types::fm::FmConfigSource;
    use nexus_types::fm::FmConfigView;
    use nexus_types::fm::config::Setting;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;

    // Generate a test config whose values will never change, for the
    // expectorate tests.
    fn test_config() -> db::model::fm::FmConfig {
        db::model::fm::FmConfig {
            version: SqlU32::new(1),
            comment: "my cool test config".to_string(),
            analysis_enabled: Some(true),
            sitrep_limit: Some(SqlU32::new(2500)),
            history_pruning_threshold: Some(SqlU32::new(2000)),
            certificate_expiry_warning_days: Some(SqlU32::new(30)),
            time_modified: chrono::DateTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn expectorate_insert_latest_version_query() {
        let query = DataStore::insert_latest_version_query(test_config());
        expectorate_query_contents(
            &query,
            "tests/output/fm_config_insert_latest_version.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_insert_latest_version_query() {
        let logctx = dev::test_setup_log("explain_insert_latest_version_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::insert_latest_version_query(test_config());

        // Before trying to explain the query, let's start by making sure it's
        // valid SQL...
        let q = diesel::debug_query::<Pg, _>(&query).to_string();
        match dev::db::format_sql(&q).await {
            Ok(q) => eprintln!("query: {q}"),
            Err(e) => panic!("query is malformed: {e}\n{q}"),
        }

        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {explanation}",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_fm_config_basic() {
        let logctx = dev::test_setup_log("test_fm_config_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // With no override rows in the table, there is no config to read.
        let read = datastore.fm_config_get_latest(opctx).await.unwrap();
        assert!(read.is_none(), "got {read:?}");

        // Inserting version 2 should fail. No overrides exist yet, so the
        // first must be version 1.
        let mut config = FmConfigParam {
            version: NonZeroU32::new(2).unwrap(),
            comment: "first override".to_string(),
            config: FmConfig {
                analysis_enabled: Setting::new(true),
                sitrep_limit: Setting::new(NonZeroU32::new(5).unwrap()),
                history_pruning_threshold: Setting::new(
                    NonZeroU32::new(4).unwrap(),
                ),
                certificate_expiry_warning_days: Setting::Default,
            },
        };
        assert!(
            dbg!(
                datastore
                    .fm_config_insert_latest_version(
                        opctx,
                        dbg!(&config).clone()
                    )
                    .await
            )
            .unwrap_err()
            .to_string()
            .contains("2 is not the most recent")
        );

        // Inserting version 1 should work.
        config.version = NonZeroU32::new(1).unwrap();

        dbg!(
            datastore
                .fm_config_insert_latest_version(opctx, dbg!(&config).clone())
                .await
        )
        .expect("inserting version 1 should succeed");

        // Getting the latest config should now return the version 1 override.
        let read = dbg!(datastore.fm_config_get_latest(opctx).await)
            .unwrap()
            .expect("an override was inserted");
        let read = FmConfigView::try_from(read).unwrap();
        let FmConfigSource::Override { version, ref comment, .. } = read.source
        else {
            panic!("expected an override source, got {:?}", read.source);
        };
        assert_eq!(version.get(), 1);
        assert_eq!(comment, "first override");
        assert_eq!(read.config.sitrep_limit.value().get(), 5);
        assert_eq!(read.config.history_pruning_threshold.value().get(), 4);
        assert_eq!(
            read.config.certificate_expiry_warning_days,
            Setting::Default
        );

        // An invalid config is rejected with an invalid value error.
        // (Validation is tested exhaustively in `nexus-types`; this just
        // checks that invalid configs are rejected on the insert path.)
        config.version = NonZeroU32::new(2).unwrap();
        config.config.sitrep_limit =
            Setting::new(NonZeroU32::new(100).unwrap());
        config.config.history_pruning_threshold =
            Setting::new(NonZeroU32::new(100).unwrap());
        assert!(
            dbg!(
                datastore
                    .fm_config_insert_latest_version(
                        opctx,
                        dbg!(&config).clone()
                    )
                    .await
            )
            .unwrap_err()
            .to_string()
            .contains("must be less than the total sitrep limit")
        );

        // An empty comment is also rejected on the insert path.
        config.config.sitrep_limit =
            Setting::new(NonZeroU32::new(500).unwrap());
        config.config.history_pruning_threshold =
            Setting::new(NonZeroU32::new(400).unwrap());
        config.comment = String::new();
        assert!(
            dbg!(
                datastore
                    .fm_config_insert_latest_version(
                        opctx,
                        dbg!(&config).clone()
                    )
                    .await
            )
            .unwrap_err()
            .to_string()
            .contains("non-empty, non-whitespace comment")
        );

        // Inserting version 2 with a valid config should work.
        config.comment = "second override".to_string();
        config.config.analysis_enabled = Setting::new(false);
        config.config.certificate_expiry_warning_days =
            Setting::new(NonZeroU32::new(60).unwrap());
        dbg!(
            datastore
                .fm_config_insert_latest_version(opctx, dbg!(config))
                .await
        )
        .expect("inserting version 2 should succeed");

        // Getting the latest config should return the version 2 override.
        let read = dbg!(datastore.fm_config_get_latest(opctx).await)
            .unwrap()
            .expect("an override was inserted");
        let read = FmConfigView::try_from(read).unwrap();
        let FmConfigSource::Override { version, ref comment, .. } = read.source
        else {
            panic!("expected an override source, got {:?}", read.source);
        };
        assert_eq!(version.get(), 2);
        assert_eq!(comment, "second override");
        assert!(!read.config.analysis_enabled.value());
        assert_eq!(read.config.sitrep_limit.value().get(), 500);
        assert_eq!(read.config.history_pruning_threshold.value().get(), 400);
        assert_eq!(
            read.config.certificate_expiry_warning_days.value().get(),
            60
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
