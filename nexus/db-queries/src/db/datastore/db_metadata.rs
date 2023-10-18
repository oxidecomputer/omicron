// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Database Metadata.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::TransactionError;
use async_bb8_diesel::{
    AsyncConnection, AsyncRunQueryDsl, AsyncSimpleConnection,
};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use omicron_common::api::external::SemverVersion;
use omicron_common::nexus_config::SchemaConfig;
use slog::Logger;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::str::FromStr;

pub const EARLIEST_SUPPORTED_VERSION: &'static str = "1.0.0";

/// Reads a "version directory" and reads all SQL changes into
/// a result Vec.
///
/// Any file that starts with "up" and ends with "sql" is considered
/// part of the migration, and fully read to a string.
///
/// These are sorted lexicographically.
pub async fn all_sql_for_version_migration<P: AsRef<Utf8Path>>(
    path: P,
) -> Result<Vec<String>, String> {
    let target_dir = path.as_ref();
    let mut up_sqls = vec![];
    let entries = target_dir
        .read_dir_utf8()
        .map_err(|e| format!("Failed to readdir {target_dir}: {e}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("Invalid entry: {err}"))?;
        let pathbuf = entry.into_path();
        let is_up = pathbuf
            .file_name()
            .map(|name| name.starts_with("up"))
            .unwrap_or(false);
        let is_sql = matches!(pathbuf.extension(), Some("sql"));
        if is_up && is_sql {
            up_sqls.push(pathbuf);
        }
    }
    up_sqls.sort();

    let mut result = vec![];
    for path in up_sqls.into_iter() {
        result.push(
            tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| format!("Cannot read {path}: {e}"))?,
        );
    }
    Ok(result)
}

impl DataStore {
    // Ensures that the database schema matches "desired_version".
    //
    // - Updating the schema makes the database incompatible with older
    // versions of Nexus, which are not running "desired_version".
    // - This is a one-way operation that cannot be undone.
    // - The caller is responsible for ensuring that the new version is valid,
    // and that all running Nexus instances can understand the new schema
    // version.
    //
    // TODO: This function assumes that all concurrently executing Nexus
    // instances on the rack are operating on the same version of software.
    // If that assumption is broken, nothing would stop a "new deployment"
    // from making a change that invalidates the queries used by an "old
    // deployment". This is fixable, but it requires slightly more knowledge
    // about the deployment and liveness of Nexus services within the rack.
    pub async fn ensure_schema(
        &self,
        log: &Logger,
        desired_version: SemverVersion,
        config: Option<&SchemaConfig>,
    ) -> Result<(), String> {
        let mut current_version = match self.database_schema_version().await {
            Ok(current_version) => {
                // NOTE: We could run with a less tight restriction.
                //
                // If we respect the meaning of the semver version, it should be possible
                // to use subsequent versions, as long as they do not introduce breaking changes.
                //
                // However, at the moment, we opt for conservatism: if the database does not
                // exactly match the schema version, we refuse to continue without modification.
                if current_version == desired_version {
                    info!(log, "Compatible database schema: {current_version}");
                    return Ok(());
                }
                let observed = &current_version.0;
                warn!(log, "Database schema {observed} does not match expected {desired_version}");
                current_version
            }
            Err(e) => {
                return Err(format!("Cannot read schema version: {e}"));
            }
        };

        let Some(config) = config else {
            return Err(
                "Not configured to automatically update schema".to_string()
            );
        };

        if current_version > desired_version {
            return Err("Nexus older than DB version: automatic downgrades are unsupported".to_string());
        }

        // If we're here, we know the following:
        //
        // - The schema does not match our expected version (or at least, it
        // didn't when we read it moments ago).
        // - We should attempt to automatically upgrade the schema.
        //
        // We do the following:
        // - Look in the schema directory for all the changes, in-order, to
        // migrate from our current version to the desired version.

        info!(log, "Reading schemas from {}", config.schema_dir.display());
        let mut dir = tokio::fs::read_dir(&config.schema_dir)
            .await
            .map_err(|e| format!("Failed to read schema config dir: {e}"))?;
        let mut all_versions = BTreeSet::new();
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read schema dir: {e}"))?
        {
            if entry.file_type().await.map_err(|e| e.to_string())?.is_dir() {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "Non-unicode schema dir".to_string())?;
                if let Ok(observed_version) = name.parse::<SemverVersion>() {
                    all_versions.insert(observed_version);
                } else {
                    let err_msg =
                        format!("Failed to parse {name} as a semver version");
                    warn!(log, "{err_msg}");
                    return Err(err_msg);
                }
            }
        }

        if !all_versions.contains(&current_version) {
            return Err(format!(
                "Current DB version {current_version} was not found in {}",
                config.schema_dir.display()
            ));
        }
        // TODO: Test this?
        if !all_versions.contains(&desired_version) {
            return Err(format!(
                "Target DB version {desired_version} was not found in {}",
                config.schema_dir.display()
            ));
        }

        let target_versions = all_versions.range((
            Bound::Excluded(&current_version),
            Bound::Included(&desired_version),
        ));

        for target_version in target_versions.into_iter() {
            info!(
                log,
                "Attempting to upgrade schema";
                "current_version" => current_version.to_string(),
                "target_version" => target_version.to_string(),
            );

            let target_dir = Utf8PathBuf::from_path_buf(
                config.schema_dir.join(target_version.to_string()),
            )
            .map_err(|e| format!("Invalid schema path: {}", e.display()))?;

            let up_sqls = all_sql_for_version_migration(&target_dir).await?;

            // Confirm the current version, set the "target_version"
            // column to indicate that a schema update is in-progress.
            //
            // Sets the following:
            // - db_metadata.target_version = new version
            self.prepare_schema_update(&current_version, &target_version)
                .await
                .map_err(|e| e.to_string())?;

            info!(
                log,
                "Marked schema upgrade as prepared";
                "current_version" => current_version.to_string(),
                "target_version" => target_version.to_string(),
            );

            for sql in &up_sqls {
                // Perform the schema change.
                self.apply_schema_update(
                    &current_version,
                    &target_version,
                    &sql,
                )
                .await
                .map_err(|e| e.to_string())?;
            }

            info!(
                log,
                "Applied schema upgrade";
                "current_version" => current_version.to_string(),
                "target_version" => target_version.to_string(),
            );

            // NOTE: We could execute the schema change in a background task,
            // and let it propagate, while observing it with the following
            // snippet of SQL:
            //
            // WITH
            //   x AS (SHOW JOBS)
            // SELECT * FROM x WHERE
            //   job_type = 'SCHEMA CHANGE' AND
            //   status != 'succeeded';
            //
            // This would enable concurrent operations to happen on the database
            // while we're mid-update. However, there is subtlety here around
            // the visibility of renamed / deleted fields, unique indices, etc,
            // so in the short-term we simply block on this job performing the
            // update.
            //
            // NOTE: If we wanted to back-fill data manually, we could do so
            // here.

            // Now that the schema change has completed, set the following:
            // - db_metadata.version = new version
            // - db_metadata.target_version = NULL
            self.finalize_schema_update(&current_version, &target_version)
                .await
                .map_err(|e| e.to_string())?;

            info!(
                log,
                "Finalized schema upgrade";
                "current_version" => current_version.to_string(),
                "target_version" => target_version.to_string(),
            );

            current_version = target_version.clone();
        }

        Ok(())
    }

    pub async fn database_schema_version(
        &self,
    ) -> Result<SemverVersion, Error> {
        use db::schema::db_metadata::dsl;

        let version: String = dsl::db_metadata
            .filter(dsl::singleton.eq(true))
            .select(dsl::version)
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        SemverVersion::from_str(&version).map_err(|e| {
            Error::internal_error(&format!("Invalid schema version: {e}"))
        })
    }

    // Updates the DB metadata to indicate that a transition from
    // `from_version` to `to_version` is occuring.
    //
    // This is only valid if the current version matches `from_version`.
    //
    // NOTE: This function should be idempotent -- if Nexus crashes mid-update,
    // a new Nexus instance should be able to re-call this function and
    // make progress.
    async fn prepare_schema_update(
        &self,
        from_version: &SemverVersion,
        to_version: &SemverVersion,
    ) -> Result<(), Error> {
        use db::schema::db_metadata::dsl;

        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                // Either we're updating to the same version, or no update is
                // in-progress.
                .filter(
                    dsl::target_version
                        .eq(Some(to_version.to_string()))
                        .or(dsl::target_version.is_null()),
                ),
        )
        .set((
            dsl::time_modified.eq(Utc::now()),
            dsl::target_version.eq(Some(to_version.to_string())),
        ))
        .execute_async(&*self.pool_connection_unauthorized().await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if rows_updated != 1 {
            return Err(Error::internal_error(
                "Failed to prepare schema for update",
            ));
        }
        Ok(())
    }

    // Applies a schema update, using raw SQL read from a caller-supplied
    // configuration file.
    async fn apply_schema_update(
        &self,
        current: &SemverVersion,
        target: &SemverVersion,
        sql: &String,
    ) -> Result<(), Error> {
        let result = self.pool_connection_unauthorized().await?.transaction_async(|conn| async move {
            if target.to_string() != EARLIEST_SUPPORTED_VERSION {
                let validate_version_query = format!("SELECT CAST(\
                        IF(\
                            (\
                                SELECT version = '{current}' and target_version = '{target}'\
                                FROM omicron.public.db_metadata WHERE singleton = true\
                            ),\
                            'true',\
                            'Invalid starting version for schema change'\
                        ) AS BOOL\
                    );");
                conn.batch_execute_async(&validate_version_query).await?;
            }
            conn.batch_execute_async(&sql).await?;
            Ok::<_, TransactionError<()>>(())
        }).await;

        match result {
            Ok(()) => Ok(()),
            Err(TransactionError::CustomError(())) => panic!("No custom error"),
            Err(TransactionError::Database(e)) => {
                Err(public_error_from_diesel(e, ErrorHandler::Server))
            }
        }
    }

    // Completes a schema migration, upgrading to the new version.
    async fn finalize_schema_update(
        &self,
        from_version: &SemverVersion,
        to_version: &SemverVersion,
    ) -> Result<(), Error> {
        use db::schema::db_metadata::dsl;

        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                .filter(dsl::target_version.eq(to_version.to_string())),
        )
        .set((
            dsl::time_modified.eq(Utc::now()),
            dsl::version.eq(to_version.to_string()),
            dsl::target_version.eq(None as Option<String>),
        ))
        .execute_async(&*self.pool_connection_unauthorized().await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if rows_updated != 1 {
            return Err(Error::internal_error(
                &format!("Failed to finalize schema update from version {from_version} to {to_version}"),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_db_model::schema::SCHEMA_VERSION;
    use nexus_test_utils::db as test_db;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    // Confirms that calling the internal "ensure_schema" function can succeed
    // when the database is already at that version.
    #[tokio::test]
    async fn ensure_schema_is_current_version() {
        let logctx = dev::test_setup_log("ensure_schema_is_current_version");
        let mut crdb = test_db::test_setup_database(&logctx.log).await;

        let cfg = db::Config { url: crdb.pg_config().clone() };
        let pool = Arc::new(db::Pool::new(&logctx.log, &cfg));
        let datastore =
            Arc::new(DataStore::new(&logctx.log, pool, None).await.unwrap());

        datastore
            .ensure_schema(&logctx.log, SCHEMA_VERSION, None)
            .await
            .expect("Failed to ensure schema");

        crdb.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Confirms that calling ensure_schema from concurrent Nexus instances
    // only permit the latest schema migration, rather than re-applying old
    // schema updates.
    #[tokio::test]
    async fn concurrent_nexus_instances_only_move_forward() {
        let logctx =
            dev::test_setup_log("concurrent_nexus_instances_only_move_forward");
        let log = &logctx.log;
        let mut crdb = test_db::test_setup_database(&logctx.log).await;

        let cfg = db::Config { url: crdb.pg_config().clone() };
        let pool = Arc::new(db::Pool::new(&logctx.log, &cfg));
        let conn = pool.pool().get().await.unwrap();

        // Mimic the layout of "schema/crdb".
        let config_dir = tempfile::TempDir::new().unwrap();

        // Helper to create the version directory and "up.sql".
        let add_upgrade = |version: SemverVersion, sql: String| {
            let config_dir_path = config_dir.path();
            async move {
                let dir = config_dir_path.join(version.to_string());
                tokio::fs::create_dir_all(&dir).await.unwrap();

                tokio::fs::write(dir.join("up.sql"), sql).await.unwrap();
            }
        };

        // Create the old version directory, and also update the on-disk "current version" to
        // this value.
        //
        // Nexus will decide to upgrade to, at most, the version that its own binary understands.
        //
        // To trigger this action within a test, we manually set the "known to DB" version.
        let v0 = SemverVersion::new(0, 0, 0);
        use db::schema::db_metadata::dsl;
        diesel::update(dsl::db_metadata.filter(dsl::singleton.eq(true)))
            .set(dsl::version.eq(v0.to_string()))
            .execute_async(&*conn)
            .await
            .expect("Failed to set version back to 0.0.0");

        let v1 = SemverVersion::new(0, 0, 1);
        let v2 = SCHEMA_VERSION;

        assert!(v0 < v1);
        assert!(v1 < v2);

        // This version must exist so Nexus can see the sequence of updates from
        // v0 to v1 to v2, but it doesn't need to re-apply it.
        add_upgrade(v0.clone(), "SELECT true;".to_string()).await;

        // This version adds a new table, but it takes a little while.
        //
        // This delay is intentional, so that some Nexus instances issuing
        // the update act quickly, while others lag behind.
        add_upgrade(
            v1.clone(),
            "SELECT pg_sleep(RANDOM() / 10); \
             CREATE TABLE IF NOT EXISTS widget(); \
             SELECT pg_sleep(RANDOM() / 10);"
                .to_string(),
        )
        .await;

        // The table we just created is deleted by a subsequent update.
        add_upgrade(v2.clone(), "DROP TABLE IF EXISTS widget;".to_string())
            .await;

        // Show that the datastores can be created concurrently.
        let config =
            SchemaConfig { schema_dir: config_dir.path().to_path_buf() };
        let _ = futures::future::join_all((0..10).map(|_| {
            let log = log.clone();
            let pool = pool.clone();
            let config = config.clone();
            tokio::task::spawn(async move {
                let datastore = DataStore::new(&log, pool, Some(&config)).await?;

                // This is the crux of this test: confirm that, as each
                // migration completes, it's not possible to see any artifacts
                // of the "v1" migration (namely: the 'Widget' table should not
                // exist).
                let result = diesel::select(
                        diesel::dsl::sql::<diesel::sql_types::Bool>(
                            "EXISTS (SELECT * FROM pg_tables WHERE tablename = 'widget')"
                        )
                    )
                    .get_result_async::<bool>(&*datastore.pool_connection_for_tests().await.unwrap())
                    .await
                    .expect("Failed to query for table");
                assert_eq!(result, false, "The 'widget' table should have been deleted, but it exists.\
                    This failure means an old update was re-applied after a newer update started.");

                Ok::<_, String>(datastore)
            })
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<Result<DataStore, _>>, _>>()
        .expect("Failed to await datastore creation task")
        .into_iter()
        .collect::<Result<Vec<DataStore>, _>>()
        .expect("Failed to create datastore");

        crdb.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
