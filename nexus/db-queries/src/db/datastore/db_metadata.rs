// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Database Metadata.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use omicron_common::api::external::SemverVersion;
use omicron_common::nexus_config::SchemaConfig;
use slog::Logger;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::str::FromStr;
use uuid::Uuid;

/// Represents the ability of this Nexus to update the database schema.
///
/// Should not be constructed unless this Nexus instance successfully modified
/// the db_metadata table to identify that a schema migration is in progress.
pub struct SchemaUpdateLease {
    our_id: Uuid,
}

impl DataStore {
    // Ensures that the database schema matches "desired_version".
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
                warn!(log, "Incompatible database schema: Saw {observed}, expected {desired_version}");
                current_version
            }
            Err(e) => {
                return Err(format!("Cannot read schema version: {e}"));
            }
        };

        let Some(config) = config else {
            return Err("Not configured to automatically update schema".to_string());
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
                    warn!(log, err_msg);
                    return Err(err_msg);
                }
            }
        }

        if !all_versions.contains(&current_version) {
            return Err(format!(
                "Current DB version {current_version} was not found in {}",
                config.schema_dir
            ));
        }

        let target_versions = all_versions.range((
            Bound::Excluded(&current_version),
            Bound::Included(&desired_version),
        ));

        for target_version in target_versions.into_iter() {
            info!(
                log,
                "Attempting to grab lease for upgrade to version {}",
                target_version
            );

            // Before we attempt to perform an update, grab a lease to allow
            // this Nexus instance to perform the update.
            //
            // If another instance is performing an update, we will fail, and
            // let them to the work on our behalf.
            let lease = self
                .grab_schema_update_lease(&current_version, &target_version)
                .await
                .map_err(|e| e.to_string())?;

            info!(
                log,
                "Acquired lease, applying upgrade to version {}",
                target_version
            );

            // Start the schema change to the new version.
            let up = config
                .schema_dir
                .join(target_version.to_string())
                .join("up.sql");
            let sql = tokio::fs::read_to_string(&up).await.map_err(|e| {
                format!("Cannot read {up}: {e}", up = up.display())
            })?;
            self.apply_schema_update(&lease, &sql)
                .await
                .map_err(|e| e.to_string())?;

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

            // NOTE: If we wanted to back-fill data manually, we could do so
            // here.

            self.release_schema_update_lease(
                lease,
                &current_version,
                &target_version,
            )
            .await
            .map_err(|e| e.to_string())?;
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
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        SemverVersion::from_str(&version).map_err(|e| {
            Error::internal_error(&format!("Invalid schema version: {e}"))
        })
    }

    /// Grants a "lease" to the currently running Nexus instance if the
    /// following is true:
    /// - The current DB version is `from_version`
    /// - No other Nexus instances are attempting to perform an update.
    // CockroachDB documentation strongly advises against performing updates
    // within transactions:
    //
    // <https://www.cockroachlabs.com/docs/stable/online-schema-changes#schema-changes-within-transactions>
    //
    // However, with multiple concurrent instantiations of Nexus, this means
    // that a "really slow Nexus", seeing version N of the database, could
    // attempt to apply the N -> N + 1 migration after we've already applied
    // several subsequent migrations. In the case that we "add a column, and
    // later remove it", this could put the database schema in an inconsistent
    // state.
    //
    // Unfortunately, without transactions (or CTEs) at our disposal, it's
    // difficult to coordinate between multiple Nexus nodes.
    //
    // To mitigate, we use the concept of a "lease": A single Nexus instance
    // granted the ability to modify the database schema for a limited period
    // of time.
    pub async fn grab_schema_update_lease(
        &self,
        from_version: &SemverVersion,
        to_version: &SemverVersion,
    ) -> Result<SchemaUpdateLease, Error> {
        use db::schema::db_metadata::dsl;

        let our_id = Uuid::new_v4();
        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                .filter(dsl::nexus_upgrade_driver.is_null()),
        )
        .set((
            dsl::time_modified.eq(Utc::now()),
            dsl::target_version.eq(Some(to_version.to_string())),
            dsl::nexus_upgrade_driver.eq(Some(our_id)),
        ))
        .execute_async(self.pool())
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        if rows_updated != 1 {
            return Err(Error::internal_error(
                "Failed to grab schema update lease",
            ));
        }
        Ok(SchemaUpdateLease { our_id })
    }

    /// Applies a schema update, using raw SQL read from a caller-supplied
    /// configuration file.
    ///
    /// Calling this function requires the caller to have successfully invoked
    /// [Self::grab_schema_update_lease].
    pub async fn apply_schema_update(
        &self,
        _: &SchemaUpdateLease,
        sql: &String,
    ) -> Result<(), Error> {
        self.pool().batch_execute_async(&sql).await.map_err(|e| {
            Error::internal_error(&format!("Failed to execute upgrade: {e}"))
        })?;
        Ok(())
    }

    /// Completes a schema migration, releasing the lease, and updating the
    /// DB schema version.
    pub async fn release_schema_update_lease(
        &self,
        lease: SchemaUpdateLease,
        from_version: &SemverVersion,
        to_version: &SemverVersion,
    ) -> Result<(), Error> {
        use db::schema::db_metadata::dsl;

        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                .filter(dsl::target_version.eq(to_version.to_string()))
                .filter(dsl::nexus_upgrade_driver.eq(Some(lease.our_id))),
        )
        .set((
            dsl::time_modified.eq(Utc::now()),
            dsl::version.eq(to_version.to_string()),
            dsl::target_version.eq(None as Option<String>),
            dsl::nexus_upgrade_driver.eq(None as Option<Uuid>),
        ))
        .execute_async(self.pool())
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        if rows_updated != 1 {
            return Err(Error::internal_error(
                "Failed to release schema update lease",
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

    // Confirms that calling ensure_schema from concurrent Nexus instances only
    // lets a single Nexus perform the update operation.
    #[tokio::test]
    async fn concurrent_nexus_instances_only_update_schema_once() {
        let logctx = dev::test_setup_log(
            "concurrent_nexus_instances_only_update_schema_once",
        );
        let log = &logctx.log;
        let mut crdb = test_db::test_setup_database(&logctx.log).await;

        let cfg = db::Config { url: crdb.pg_config().clone() };
        let pool = Arc::new(db::Pool::new(&logctx.log, &cfg));

        // Mimic the layout of "schema/crdb".
        let config_dir = tempfile::TempDir::new().unwrap();

        // Create the old version directory, and also update the on-disk "current version" to
        // this value.
        //
        // Nexus will decide to ugprade to, at most, the version that its own binary understands.
        //
        // To trigger this action within a test, we manually set the "known to DB" version back
        // one.
        let old_version = SemverVersion::new(0, 0, 0);
        use db::schema::db_metadata::dsl;
        diesel::update(dsl::db_metadata.filter(dsl::singleton.eq(true)))
            .set(dsl::version.eq(old_version.to_string()))
            .execute_async(pool.pool())
            .await
            .expect("Failed to set version back");

        let current_version_dir =
            config_dir.path().join(old_version.to_string());
        tokio::fs::create_dir_all(&current_version_dir).await.unwrap();

        // Create the current version directory.
        let next_version = SCHEMA_VERSION;
        let next_version_dir = config_dir.path().join(next_version.to_string());
        tokio::fs::create_dir_all(&next_version_dir).await.unwrap();

        // This upgrade is not idempotent.
        //
        // Normally that's a bug, but here, we exploit that fact to validate that
        // when we create multiple datastores, only one actually issues the upgrade
        // operation, but all datastores are regardless constructed successfully.
        tokio::fs::write(
            next_version_dir.join("up.sql"),
            "CREATE TABLE Widget ( Thing INT );",
        )
        .await
        .unwrap();

        let config =
            SchemaConfig { schema_dir: config_dir.path().to_path_buf() };

        // Show that the datastores can be created concurrently.
        let _ = futures::future::join_all((0..10).map(|_| {
            let log = log.clone();
            let pool = pool.clone();
            let config = config.clone();
            tokio::task::spawn(async move {
                DataStore::new(&log, pool, Some(&config)).await
            })
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<Result<DataStore, _>>, _>>()
        .expect("Failed to await datastore creation task")
        .into_iter()
        .collect::<Result<Vec<DataStore>, _>>()
        .expect("Failed to create datastore: {e}");

        crdb.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
