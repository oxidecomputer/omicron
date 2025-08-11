// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Database Metadata.

use super::DataStore;
use anyhow::{Context, anyhow};
use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::AllSchemaVersions;
use nexus_db_model::DbMetadata;
use nexus_db_model::DbMetadataBase;
use nexus_db_model::DbMetadataUpdate;
use nexus_db_model::EARLIEST_SUPPORTED_VERSION;
use nexus_db_model::SchemaUpgradeStep;
use omicron_common::api::external::Error;
use semver::Version;
use slog::{Logger, error, info, o};
use std::ops::Bound;
use std::str::FromStr;

// A SchemaVersion which uses a pre-release value to indicate
// "incremental progress".
//
// Although we organize our schema changes into higher-level versions,
// transaction semantics of CockroachDB do not allow multiple DDL statements
// to be applied together. As a result, an upgrade from "1.0.0" to "2.0.0"
// may involve multiple smaller "step" versions, each separated by
// an auto-generated pre-release version.
#[derive(Clone)]
struct StepSemverVersion {
    version: Version,
    i: usize,
}

impl StepSemverVersion {
    // Durably store our progress in the target version as the
    // pre-release extension of "target_version".
    //
    // Note that the format of "step.{i}" is load-bearing, see:
    // https://docs.rs/semver/1.0.22/semver/struct.Prerelease.html#examples
    //
    // By using "dot number" notation, we can order the pre-release
    // steps, which matters while comparing their order.
    fn new(target_version: &Version, i: usize) -> anyhow::Result<Self> {
        let mut target_step_version = target_version.clone();
        target_step_version.pre = semver::Prerelease::new(&format!("step.{i}"))
            .context("Cannot parse step as semver pre-release")?;
        Ok(Self { version: target_step_version, i })
    }

    // Drops the pre-release information about this target version.
    //
    // This is the version we are upgrading to, using these incremental steps.
    fn without_prerelease(&self) -> Version {
        let mut target_version = self.version.clone();
        target_version.pre = semver::Prerelease::EMPTY;
        target_version
    }

    // Returns the 'previous' step before this one, if such a step exists.
    fn previous(&self) -> Option<Self> {
        if self.i == 0 {
            return None;
        }
        Self::new(&self.version, self.i - 1).ok()
    }
}

// Identifies if we have already completed the "target_step_version", by
// comparing with the `target_version` value stored in the DB as
// "found_target_version".
fn skippable_version(
    log: &Logger,
    target_step_version: &Version,
    found_target_version: &Option<Version>,
) -> bool {
    if let Some(found_target_version) = found_target_version.as_ref() {
        info!(
            log,
            "Considering found target version";
            "found_target_version" => ?found_target_version,
        );

        // This case only occurs if an upgrade failed and needed to
        // restart, for whatever reason. We skip all the incremental
        // steps that we know have completed.
        if found_target_version > target_step_version {
            warn!(
                log,
                "Observed target version greater than this upgrade step. Skipping.";
                "found_target_version" => %found_target_version,
            );
            return true;
        }
    }
    return false;
}

/// Reports how the schema version deployed in the database compares to what
/// this Nexus expects.
///
/// When we say "what Nexus expects", we're referring to the
/// [nexus_db_model::SCHEMA_VERSION] which this particular Nexus binary was
/// compiled with. During an update, multiple Nexuses with different
/// values of `SCHEMA_VERSION` may be running at the same time, and handoff
/// between them must be carefully coordinated.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaVersionStatus {
    /// The database version matches and we're not quiescing
    DatabaseMatchesReady,
    /// The database version matches but we should be quiescing
    DatabaseMatchesQuiescing,
    /// Database is actually on a newer version, or database is on the same
    /// version but `quiesce_completed` is true
    DatabaseIsNewer,
    /// The database is on an older version and quiesce has not completed
    DatabaseIsOlderUnquiesced,
    /// The database is on an older version and quiesce has completed
    DatabaseIsOlderQuiesced,
}

impl SchemaVersionStatus {
    /// Interpret how the current db metadata compares to our desired version
    pub fn interpret(
        db_metadata: &DbMetadata,
        desired_version: &Version,
    ) -> SchemaVersionStatus {
        use std::cmp::Ordering;

        let found_version: Version = db_metadata.version().clone().into();
        let quiesce_started = db_metadata.quiesce_started();
        let quiesce_completed = db_metadata.quiesce_completed();

        match found_version.cmp(desired_version) {
            Ordering::Greater => SchemaVersionStatus::DatabaseIsNewer,
            Ordering::Equal => {
                if quiesce_completed {
                    SchemaVersionStatus::DatabaseIsNewer
                } else if quiesce_started {
                    SchemaVersionStatus::DatabaseMatchesQuiescing
                } else {
                    SchemaVersionStatus::DatabaseMatchesReady
                }
            }
            Ordering::Less => {
                if quiesce_completed {
                    SchemaVersionStatus::DatabaseIsOlderQuiesced
                } else {
                    SchemaVersionStatus::DatabaseIsOlderUnquiesced
                }
            }
        }
    }
}

/// A reason why [SchemaAction::Refuse] would be returned.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaRefuseReason {
    /// The database appears out-of-date, and our policy will not allow
    /// us to perform an upgrade
    OutOfDate,

    /// The database schema appears too new
    TooNew,
}

/// Describes how to respond to a [SchemaVersionStatus]
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaAction {
    /// Normal operation: use the database normally
    Ready,

    /// Start quiescing Nexus
    ///
    /// It's expected that this will not be an instantaneous operation;
    /// we may need to complete sagas to finish quiescing.
    Quiesce,

    /// Wait for either the schema to be updated or (if willing to update it
    /// ourselves but we want to wait for quiesce to complete) for quiescing to
    /// finish (do not use the database, do not try to upgrade it)
    Wait,

    /// Start a schema update
    Update,

    /// Do not touch the database - refuse the schema for the supplied reason.
    Refuse { reason: SchemaRefuseReason },
}

/// Determines how the consumer wants to behave when the schema doesn't match
/// what they expect
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ConsumerPolicy {
    /// Throw an error if the schema does not exactly match
    FailOnMismatch,
    /// Update if we're ready to update, otherwise identify that we should wait
    UpdateGracefully,
    /// Force an update regardless of quiesce state
    UpdateForcefully,
}

impl SchemaAction {
    pub fn new(
        schema_version_status: SchemaVersionStatus,
        consumer_policy: ConsumerPolicy,
    ) -> SchemaAction {
        use ConsumerPolicy::*;
        use SchemaAction::*;
        use SchemaRefuseReason::*;
        use SchemaVersionStatus::*;

        match (schema_version_status, consumer_policy) {
            // Database is newer than what we expect
            (DatabaseIsNewer, _) => Refuse { reason: TooNew },

            // Database matches and we're ready
            (DatabaseMatchesReady, _) => Ready,

            // Database matches but we're quiescing
            (DatabaseMatchesQuiescing, FailOnMismatch) => Quiesce,
            (DatabaseMatchesQuiescing, UpdateGracefully) => Quiesce,
            (DatabaseMatchesQuiescing, UpdateForcefully) => Ready,

            // Database is older and unquiesced
            (DatabaseIsOlderUnquiesced, FailOnMismatch) => {
                Refuse { reason: OutOfDate }
            }
            (DatabaseIsOlderUnquiesced, UpdateGracefully) => Wait,
            (DatabaseIsOlderUnquiesced, UpdateForcefully) => Update,

            // Database is older and quiesced (ready for update)
            (DatabaseIsOlderQuiesced, FailOnMismatch) => {
                Refuse { reason: OutOfDate }
            }
            (DatabaseIsOlderQuiesced, UpdateGracefully) => Update,
            (DatabaseIsOlderQuiesced, UpdateForcefully) => Update,
        }
    }
}

/// Reasons why [DataStore::ensure_schema] might fail
#[derive(thiserror::Error, Debug)]
pub enum EnsureSchemaError {
    /// The schema cannot be ensured, because our interpretation
    /// of the policy and database state identifies that we should
    /// not proceed with database usage or a schema update.
    #[error("Unexpected action for schema update: {action:?}")]
    UnexpectedAction { action: SchemaAction },

    /// During schema upgrade, we couldn't find the necessary upgrade steps
    #[error("Schema update from {found} -> {desired} missing step: {missing}")]
    SchemaUpdateStepMissing {
        found: Version,
        desired: Version,
        missing: Version,
    },

    /// Any other reason
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl DataStore {
    /// Check the current schema version status compared to the desired version
    pub async fn check_schema_version(
        &self,
        desired_version: &Version,
    ) -> Result<SchemaVersionStatus, Error> {
        let db_metadata = self.database_metadata().await?;
        Ok(SchemaVersionStatus::interpret(&db_metadata, desired_version))
    }

    /// Validates that the desired version of the schema is up-to-date,
    /// according to the supplied [ConsumerPolicy], which possibly causes a
    /// schema update.
    ///
    /// This function internally calls the equivalent of
    /// [Self::check_schema_version], and verifies that the [SchemaAction]
    /// is either "Ready" or "Update".
    ///
    /// Returns errors if the schema does not match or cannot be made
    /// to match.
    pub async fn ensure_schema(
        &self,
        log: &Logger,
        desired_version: Version,
        consumer_policy: ConsumerPolicy,
        all_versions: Option<&AllSchemaVersions>,
    ) -> Result<(), EnsureSchemaError> {
        // Observe current status
        let db_metadata = self
            .database_metadata()
            .await
            .context("Cannot read database schema version")?;
        let status =
            SchemaVersionStatus::interpret(&db_metadata, &desired_version);

        // Determine what action to take based on policy
        let action = SchemaAction::new(status.clone(), consumer_policy);

        // Execute the determined action
        match action {
            SchemaAction::Ready => {
                info!(log, "Database schema version is up to date");
                Ok(())
            }
            SchemaAction::Update => {
                match all_versions {
                    Some(versions) => {
                        info!(log, "Starting schema update");
                        // Inline the actual schema update logic to avoid recursion
                        let current_version =
                            db_metadata.version().clone().into();
                        let target_version = db_metadata
                            .target_version()
                            .map(|v| v.clone().into());
                        self.perform_schema_update(
                            log,
                            current_version,
                            target_version,
                            desired_version,
                            versions,
                        )
                        .await
                    }
                    None => {
                        error!(
                            log,
                            "Schema update requested but no version information provided"
                        );
                        Err(EnsureSchemaError::Other(anyhow!(
                            "No schema versions provided for update"
                        )))
                    }
                }
            }
            _ => Err(EnsureSchemaError::UnexpectedAction { action }),
        }
    }

    /// Performs the actual schema update from current_version to desired_version
    async fn perform_schema_update(
        &self,
        log: &Logger,
        current_version: Version,
        found_target_version: Option<Version>,
        desired_version: Version,
        all_versions: &AllSchemaVersions,
    ) -> Result<(), EnsureSchemaError> {
        if !all_versions.contains_version(&current_version) {
            return Err(EnsureSchemaError::SchemaUpdateStepMissing {
                found: current_version.clone(),
                desired: desired_version.clone(),
                missing: current_version.clone(),
            });
        }

        if !all_versions.contains_version(&desired_version) {
            return Err(EnsureSchemaError::SchemaUpdateStepMissing {
                found: current_version.clone(),
                desired: desired_version.clone(),
                missing: desired_version.clone(),
            });
        }

        let target_versions: Vec<_> = all_versions
            .versions_range((
                Bound::Excluded(&current_version),
                Bound::Included(&desired_version),
            ))
            .collect();

        // Iterate over each of the higher-level user-defined versions.
        //
        // These are the user-defined `KNOWN_VERSIONS` defined in
        // nexus/db-model/src/schema_versions.rs.
        let mut current_version = current_version;
        for target_version in target_versions.into_iter() {
            let log = log.new(o!(
                "current_version" => current_version.to_string(),
                "target_version" => target_version.semver().to_string(),
            ));
            info!(log, "Attempting to upgrade schema");

            // For the rationale here, see: StepSemverVersion::new.
            if target_version.semver().pre != semver::Prerelease::EMPTY {
                return Err(anyhow!(
                    "Cannot upgrade to version which includes pre-release"
                )
                .into());
            }

            // Iterate over each individual file that comprises a schema change.
            //
            // For example, this contains "up01.sql", "up02.sql", etc.
            //
            // While this update happens, the "current_version" will remain
            // the same (unless another Nexus concurrently completes the
            // update), but the "target_version" will keep shifting on each
            // incremental step.
            let mut last_step_version = None;

            for (i, step) in target_version.upgrade_steps().enumerate() {
                let target_step =
                    StepSemverVersion::new(target_version.semver(), i)?;
                let log = log.new(o!("target_step.version" => target_step.version.to_string()));

                self.apply_step_version_update(
                    &log,
                    &step,
                    &target_step,
                    &current_version,
                    &found_target_version,
                )
                .await
                .context("Failed to apply schema update step")?;

                last_step_version = Some(target_step.clone());
            }

            info!(log, "Applied schema upgrade");

            let last_step_version = last_step_version
                .ok_or_else(|| anyhow!("Missing final step version"))?;

            // We will keep "quiesced" set to true up until the last update.
            //
            // This means that if we crash and reboot partway through an update,
            // we'll be able to resume it - Nexuses are still quiesced, and
            // should not need to re-negotiate.
            let clear_quiesced = *target_version.semver() == desired_version;
            self.finalize_schema_update(
                &current_version,
                &last_step_version,
                clear_quiesced,
            )
            .await
            .context("Failed to finalize schema update")?;

            info!(log, "Finalized schema upgrade");
            current_version = target_version.semver().clone();
        }

        info!(log, "Schema update complete");
        Ok(())
    }

    // Executes (or skips, if unnecessary) a single "step" of a schema upgrade.
    //
    // - `step`: The schema upgrade step under consideration.
    // - `target_step`: The target version, indicating the step by pre-release.
    // - `current_version`: The last-known value of `db_metadata.version`.
    // - `found_target_version`: The last-known value of
    // `db_metadata.target_version`.
    async fn apply_step_version_update(
        &self,
        log: &Logger,
        step: &SchemaUpgradeStep,
        target_step: &StepSemverVersion,
        current_version: &Version,
        found_target_version: &Option<Version>,
    ) -> Result<(), anyhow::Error> {
        if skippable_version(&log, &target_step.version, &found_target_version)
        {
            return Ok(());
        }

        info!(
            log,
            "Marking schema upgrade as prepared";
        );

        // Confirm the current version, set the "target_version"
        // column to indicate that a schema update is in-progress.
        //
        // Sets the following:
        // - db_metadata.target_version = new version
        self.prepare_schema_update(&current_version, &target_step)
            .await
            .context("Failed to prepare schema change")?;

        info!(
            log,
            "Marked schema upgrade as prepared";
        );

        // Perform the schema change.
        self.apply_schema_update(
            &current_version,
            &target_step.version,
            step.sql(),
        )
        .await
        .with_context(|| {
            format!(
                "update to {}, applying step {:?}",
                target_step.version,
                step.label()
            )
        })?;

        info!(
            log,
            "Applied subcomponent of schema upgrade";
        );
        Ok(())
    }

    pub async fn database_metadata(&self) -> Result<DbMetadata, Error> {
        use nexus_db_schema::schema::db_metadata::dsl;

        // The "quiesced" fields do not exist on all deployed schemas,
        // so we read "DbMetadataBase" before trying to read the rest of the
        // columns.
        //
        // If we don't have these fields yet, read the more stable portions of
        // db_metadata, and treat the system as "already quiesced". This is
        // necessary for some of our backwards-compatibility testing.
        //
        // TODO: Once we've sufficiently updated in-field devices, we should
        // be able to read "DbMetadata" entirely, and avoid reading
        // "DbMetadataBase" altogether.
        let base = dsl::db_metadata
            .filter(dsl::singleton.eq(true))
            .select(DbMetadataBase::as_select())
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if base.version().0 < nexus_db_model::QUIESCE_VERSION {
            // TODO(https://github.com/oxidecomputer/omicron/issues/8751):
            // Remove this backwards compatibility layer.
            //
            // Why are we treating this case as "quiesced = true"?
            //
            // - If Nexus wants to be running on a schema older than
            // QUIESCE_VERSION, it's older than this code. As a part of our
            // operator-driven update process, these Nexuses will be stopped
            // manually by an operator during MUPdate.
            // - If Nexus wants to be running on a schema newer than or equal to
            // QUIESCE_VERSION, but it sees an older schema in the DB, then it
            // is trying to perform an update (as in our tests) or waiting
            // for an operator to run the schema-updater. In either of these
            // cases, Nexus is assuming no older versions of Nexus are running.
            //
            // Note that once we've upgraded past QUIESCE_VERSION, this
            // conditional becomes irrelevant, and we can rely on a more
            // sophisticated model of "old Nexus" -> "new Nexus" handoff.
            let quiesced = true;
            return Ok(DbMetadata::from_base(base, quiesced));
        }

        dsl::db_metadata
            .filter(dsl::singleton.eq(true))
            .select(DbMetadata::as_select())
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn database_schema_version(
        &self,
    ) -> Result<(Version, Option<Version>), Error> {
        use nexus_db_schema::schema::db_metadata::dsl;

        let (version, target): (String, Option<String>) = dsl::db_metadata
            .filter(dsl::singleton.eq(true))
            .select((dsl::version, dsl::target_version))
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let version = Version::from_str(&version).map_err(|e| {
            Error::internal_error(&format!("Invalid schema version: {e}"))
        })?;

        if let Some(target) = target {
            let target = Version::from_str(&target).map_err(|e| {
                Error::internal_error(&format!("Invalid schema version: {e}"))
            })?;
            return Ok((version, Some(target)));
        };

        Ok((version, None))
    }

    // Updates the DB metadata to indicate that a transition from
    // `from_version` to `to_version` is occurring.
    //
    // This is only valid if the current version matches `from_version`,
    // and the prior `target_version` is either:
    // - None (no update in-progress)
    // - target_step.version (another Nexus attempted to prepare this same update).
    // - target_step.previous() (we are incrementally working through a
    // multi-stage update).
    //
    // NOTE: This function should be idempotent -- if Nexus crashes mid-update,
    // a new Nexus instance should be able to re-call this function and
    // make progress.
    async fn prepare_schema_update(
        &self,
        from_version: &Version,
        target_step: &StepSemverVersion,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::db_metadata::dsl;

        let mut valid_prior_targets = vec![target_step.version.to_string()];
        if let Some(previous) = target_step.previous() {
            valid_prior_targets.push(previous.version.to_string());
        };

        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                // Either we're updating to the same version, or no update is
                // in-progress.
                .filter(
                    dsl::target_version
                        .eq_any(valid_prior_targets)
                        .or(dsl::target_version.is_null()),
                ),
        )
        .set((
            dsl::time_modified.eq(Utc::now()),
            dsl::target_version.eq(Some(target_step.version.to_string())),
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
        current: &Version,
        target: &Version,
        sql: &str,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_unauthorized().await?;

        let result = self.transaction_retry_wrapper("apply_schema_update")
            .transaction(&conn, |conn| async move {
                if *target != EARLIEST_SUPPORTED_VERSION {
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
                Ok(())
            }).await;

        match result {
            Ok(()) => Ok(()),
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    // Completes a schema migration, upgrading to the new version.
    //
    // This also allows clearing the "quiesce started/completed" state, as
    // Nexuses on the of the *new* version are not quiescing.
    //
    // - from_version: What we expect "version" must be to proceed
    // - last_step: What we expect "target_version" must be to proceed.
    async fn finalize_schema_update(
        &self,
        from_version: &Version,
        last_step: &StepSemverVersion,
        clear_quiesced: bool,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::db_metadata::dsl;

        let to_version = last_step.without_prerelease();
        let mut updates =
            DbMetadataUpdate::update_to_version(to_version.clone());

        // We only try to clear the "quiesced" fields (setting them to false)
        // if we think the schema knows what those fields are.
        if clear_quiesced && to_version >= nexus_db_model::QUIESCE_VERSION {
            updates.clear_quiesce();
        }

        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                .filter(dsl::target_version.eq(last_step.version.to_string())),
        )
        .set(updates)
        .execute_async(&*self.pool_connection_unauthorized().await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if rows_updated != 1 {
            return Err(Error::internal_error(&format!(
                "Failed to finalize schema update from version \
                {from_version} to {to_version}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use nexus_db_model::SCHEMA_VERSION;
    use omicron_test_utils::dev;

    // Confirms that calling the internal "ensure_schema" function can succeed
    // when the database is already at that version.
    #[tokio::test]
    async fn ensure_schema_is_current_version() {
        let logctx = dev::test_setup_log("ensure_schema_is_current_version");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let datastore = db.datastore();

        datastore
            .ensure_schema(
                &logctx.log,
                SCHEMA_VERSION,
                ConsumerPolicy::FailOnMismatch,
                None,
            )
            .await
            .expect("Failed to ensure schema");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Helper to create the version directory and "up.sql".
    async fn add_upgrade<S: AsRef<str>>(
        config_dir_path: &Utf8Path,
        version: Version,
        sql: S,
    ) {
        let dir = config_dir_path.join(version.to_string());
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("up.sql"), sql.as_ref()).await.unwrap();
    }

    async fn add_upgrade_subcomponent<S: AsRef<str>>(
        config_dir_path: &Utf8Path,
        version: Version,
        sql: S,
        i: usize,
    ) {
        let dir = config_dir_path.join(version.to_string());
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join(format!("up{i}.sql")), sql.as_ref())
            .await
            .unwrap();
    }

    // Confirms that calling ensure_schema from concurrent Nexus instances
    // only permit the latest schema migration, rather than re-applying old
    // schema updates.
    #[tokio::test]
    async fn concurrent_nexus_instances_only_move_forward() {
        let logctx =
            dev::test_setup_log("concurrent_nexus_instances_only_move_forward");
        let log = &logctx.log;
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        // Mimic the layout of "schema/crdb".
        let config_dir = Utf8TempDir::new().unwrap();

        // Create the old version directory, and also update the on-disk
        // "current version" to this value.
        //
        // Nexus will decide to upgrade to, at most, the version that its own
        // binary understands.
        //
        // To trigger this action within a test, we manually set the "known to
        // DB" version.
        let v0 = Version::new(0, 0, 0);
        use nexus_db_schema::schema::db_metadata::dsl;
        diesel::update(dsl::db_metadata.filter(dsl::singleton.eq(true)))
            .set(dsl::version.eq(v0.to_string()))
            .execute_async(&*conn)
            .await
            .expect("Failed to set version back to 0.0.0");

        let v1 = Version::new(0, 0, 1);
        let v2 = SCHEMA_VERSION;

        assert!(v0 < v1);
        assert!(v1 < v2);

        // This version must exist so Nexus can see the sequence of updates from
        // v0 to v1 to v2, but it doesn't need to re-apply it.
        add_upgrade(config_dir.path(), v0.clone(), "SELECT true;").await;

        // This version adds a new table, but it takes a little while.
        //
        // This delay is intentional, so that some Nexus instances issuing
        // the update act quickly, while others lag behind.
        add_upgrade(
            config_dir.path(),
            v1.clone(),
            "SELECT pg_sleep(RANDOM() / 10); \
             CREATE TABLE IF NOT EXISTS widget(); \
             SELECT pg_sleep(RANDOM() / 10);",
        )
        .await;

        // The table we just created is deleted by a subsequent update.
        add_upgrade(
            config_dir.path(),
            v2.clone(),
            "DROP TABLE IF EXISTS widget;",
        )
        .await;

        // Show that the datastores can be created concurrently.
        let all_versions = AllSchemaVersions::load_specific_legacy_versions(
            config_dir.path(),
            [&v0, &v1, &v2].into_iter(),
        )
        .expect("failed to load schema");

        // Mark quiescing as completed to allow the migration to proceed
        let quiesce_started = true;
        let quiesce_completed = true;
        let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
        set_quiesce(&datastore, quiesce_started, quiesce_completed).await;

        let _ = futures::future::join_all((0..10).map(|_| {
            let all_versions = all_versions.clone();
            let log = log.clone();
            let pool = pool.clone();
            tokio::task::spawn(async move {
                let datastore =
                    DataStore::new(&log, pool, Some(&all_versions)).await?;

                // This is the crux of this test: confirm that, as each
                // migration completes, it's not possible to see any artifacts
                // of the "v1" migration (namely: the 'Widget' table should not
                // exist).
                let result = diesel::select(diesel::dsl::sql::<
                    diesel::sql_types::Bool,
                >(
                    "EXISTS (SELECT * FROM pg_tables WHERE \
                            tablename = 'widget')",
                ))
                .get_result_async::<bool>(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .expect("Failed to query for table");
                assert_eq!(
                    result, false,
                    "The 'widget' table should have been deleted, but it \
                    exists.  This failure means an old update was re-applied \
                    after a newer update started."
                );

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

        datastore.terminate().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn schema_version_subcomponents_save_progress() {
        let logctx =
            dev::test_setup_log("schema_version_subcomponents_save_progress");
        let log = &logctx.log;
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        // Mimic the layout of "schema/crdb".
        let config_dir = Utf8TempDir::new().unwrap();

        // Create the old version directory, and also update the on-disk "current version" to
        // this value.
        //
        // Nexus will decide to upgrade to, at most, the version that its own binary understands.
        //
        // To trigger this action within a test, we manually set the "known to DB" version.
        let v0 = Version::new(0, 0, 0);
        use nexus_db_schema::schema::db_metadata::dsl;
        diesel::update(dsl::db_metadata.filter(dsl::singleton.eq(true)))
            .set(dsl::version.eq(v0.to_string()))
            .execute_async(&*conn)
            .await
            .expect("Failed to set version back to 0.0.0");

        let v1 = Version::new(1, 0, 0);
        let v2 = SCHEMA_VERSION;

        assert!(v0 < v1);
        assert!(v1 < v2);

        // This version must exist so Nexus can see the sequence of updates from
        // v0 to v1 to v2, but it doesn't need to re-apply it.
        add_upgrade(config_dir.path(), v0.clone(), "SELECT true;").await;

        // Add a new version with a table that we'll populate in V2.
        add_upgrade(
            config_dir.path(),
            v1.clone(),
            "CREATE TABLE t(id INT PRIMARY KEY, data TEXT NOT NULL);",
        )
        .await;

        // Populate the table in a few incremental steps that might fail.
        let add = |sql: &str, i: usize| {
            let config_dir_path = config_dir.path();
            let v2 = v2.clone();
            let sql = sql.to_string();
            async move {
                add_upgrade_subcomponent(&config_dir_path, v2.clone(), &sql, i)
                    .await
            }
        };

        // This is just:
        //   data = 'abcd',
        // but with some spurious errors thrown in for good measure.
        //
        // Note that if we "retry the steps from the beginning", this would
        // likely append the wrong string. This is intentional! If this occurs,
        // we'd like to catch such a case.
        //
        // Normally, we expect that each of these steps should be idempotent (by
        // themselves) but in this case, we're controlling the test environment,
        // and we should not expect any of these steps to be replayed once they
        // succeed.
        add("INSERT INTO t (id, data) VALUES (1, '')", 1).await;
        add("UPDATE t SET data = data || 'a' WHERE id = 1", 2).await;
        add("SELECT CAST(IF ((SELECT RANDOM() < 0.5), 'true', 'failure') AS BOOL)", 3).await;
        add("UPDATE t SET data = data || 'b' WHERE id = 1", 4).await;
        add("SELECT CAST(IF ((SELECT RANDOM() < 0.5), 'true', 'failure') AS BOOL)", 5).await;
        add("SELECT CAST(IF ((SELECT RANDOM() < 0.5), 'true', 'failure') AS BOOL)", 6).await;
        add("UPDATE t SET data = data || 'c' WHERE id = 1", 7).await;
        add("SELECT CAST(IF ((SELECT RANDOM() < 0.5), 'true', 'failure') AS BOOL)", 8).await;
        add("SELECT CAST(IF ((SELECT RANDOM() < 0.5), 'true', 'failure') AS BOOL)", 9).await;
        add("SELECT CAST(IF ((SELECT RANDOM() < 0.5), 'true', 'failure') AS BOOL)", 10).await;
        add("UPDATE t SET data = data || 'd' WHERE id = 1", 11).await;

        // Create the datastore, which should apply the update on boot
        let all_versions = AllSchemaVersions::load_specific_legacy_versions(
            config_dir.path(),
            [&v0, &v1, &v2].into_iter(),
        )
        .expect("failed to load schema");

        // Manually construct the datastore to avoid the backoff timeout.
        // We want to trigger errors, but have no need to wait.
        let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
        let quiesce_started = true;
        let quiesce_completed = true;
        set_quiesce(&datastore, quiesce_started, quiesce_completed).await;
        while let Err(e) = datastore
            .ensure_schema(
                &log,
                SCHEMA_VERSION,
                ConsumerPolicy::UpdateGracefully,
                Some(&all_versions),
            )
            .await
        {
            warn!(log, "Failed to ensure schema"; "err" => %e);
            continue;
        }
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Let's validate a couple things:
        // 1. The version is what we expect
        use diesel::dsl::sql;
        use diesel::sql_types::Text;
        let version = sql::<Text>("SELECT version FROM omicron.public.db_metadata WHERE singleton = true")
            .get_result_async::<String>(&*conn)
            .await
            .expect("Failed to get DB version");
        assert_eq!(version, SCHEMA_VERSION.to_string());
        // 2. We only applied each incremental step once
        let data = sql::<Text>("SELECT data FROM t WHERE id = 1")
            .get_result_async::<String>(&*conn)
            .await
            .expect("Failed to get data");
        assert_eq!(data, "abcd");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn set_quiesce(
        datastore: &DataStore,
        started: bool,
        completed: bool,
    ) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::db_metadata::dsl;

        diesel::update(dsl::db_metadata)
            .filter(dsl::singleton.eq(true))
            .set((
                dsl::quiesce_started.eq(started),
                dsl::quiesce_completed.eq(completed),
            ))
            .execute_async(&*conn)
            .await
            .expect("Failed to update metadata");
    }

    // This test validates what happens when an old Nexus interacts with a
    // database that has already started (or completed) going through a
    // schema migration.
    #[tokio::test]
    async fn ensure_schema_with_old_nexus() {
        let logctx = dev::test_setup_log("ensure_schema_with_old_nexus");
        let log = &logctx.log;
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();

        // The contents don't really matter here - just make the schema appear
        // older than the current Nexus expects.
        let mut old_schema = SCHEMA_VERSION;
        old_schema.major -= 1;

        // Case: Nexus version matches the DB, and quiesce has started.
        {
            let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
            let quiesce_started = true;
            let quiesce_completed = false;
            set_quiesce(&datastore, quiesce_started, quiesce_completed).await;

            let Err(err) = datastore
                .ensure_schema(
                    &log,
                    SCHEMA_VERSION,
                    ConsumerPolicy::FailOnMismatch,
                    None,
                )
                .await
            else {
                panic!("Ensuring schema mid-quiesce should fail");
            };
            assert!(
                matches!(
                    err,
                    EnsureSchemaError::UnexpectedAction {
                        action: SchemaAction::Quiesce
                    }
                ),
                "Unexpected error: {err:?} (expected: Should Quiesce)",
            );
        }

        // Case: Nexus version matches the DB, and quiesce has completed.
        {
            let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
            let quiesce_started = true;
            let quiesce_completed = true;
            set_quiesce(&datastore, quiesce_started, quiesce_completed).await;

            let Err(err) = datastore
                .ensure_schema(
                    &log,
                    SCHEMA_VERSION,
                    ConsumerPolicy::FailOnMismatch,
                    None,
                )
                .await
            else {
                panic!("Ensuring schema post-quiesce should fail");
            };
            assert!(
                matches!(
                    err,
                    EnsureSchemaError::UnexpectedAction {
                        action: SchemaAction::Refuse {
                            reason: SchemaRefuseReason::TooNew
                        }
                    },
                ),
                "Unexpected error: {err:?} (expected: Database too new)",
            );
        }

        // Case: The schema we request is older than what's in the database.
        //
        // This is what would happen if an old Nexus booted after a schema
        // migration already completed.
        {
            let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
            let quiesce_started = false;
            let quiesce_completed = false;
            set_quiesce(&datastore, quiesce_started, quiesce_completed).await;

            let Err(err) = datastore
                .ensure_schema(
                    &log,
                    old_schema,
                    ConsumerPolicy::FailOnMismatch,
                    None,
                )
                .await
            else {
                panic!("Ensuring old schema should fail");
            };
            assert!(
                matches!(
                    err,
                    EnsureSchemaError::UnexpectedAction {
                        action: SchemaAction::Refuse {
                            reason: SchemaRefuseReason::TooNew
                        }
                    },
                ),
                "Unexpected error: {err:?} (expected: Database too new)",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_schema_waits_for_quiesce() {
        let logctx = dev::test_setup_log("ensure_schema_waits_for_quiesce");
        let log = &logctx.log;
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();

        let old_schema = SCHEMA_VERSION;
        let mut new_schema = SCHEMA_VERSION;
        new_schema.major += 1;

        let config_dir = Utf8TempDir::new().unwrap();
        add_upgrade(config_dir.path(), old_schema.clone(), "SELECT true;")
            .await;
        add_upgrade(config_dir.path(), new_schema.clone(), "SELECT true;")
            .await;
        let all_versions = AllSchemaVersions::load_specific_legacy_versions(
            config_dir.path(),
            [&old_schema, &new_schema].into_iter(),
        )
        .expect("failed to load schema");

        // Load the new version, before quiescing started
        //
        // This is the case where a new Nexus has been started, but old
        // Nexuses have not yet started quiescing.
        let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
        let quiesce_started = false;
        let quiesce_completed = false;
        set_quiesce(&datastore, quiesce_started, quiesce_completed).await;

        let Err(err) = datastore
            .ensure_schema(
                &log,
                new_schema.clone(),
                ConsumerPolicy::UpdateGracefully,
                Some(&all_versions),
            )
            .await
        else {
            panic!("Ensuring schema pre-quiesce should fail");
        };
        assert!(
            matches!(
                err,
                EnsureSchemaError::UnexpectedAction {
                    action: SchemaAction::Wait
                }
            ),
            "Unexpected error: {err:?} (expected: Wait for Quiesce)",
        );

        // Load the new version, but while quiesce is in-progress
        let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
        let quiesce_started = true;
        let quiesce_completed = false;
        set_quiesce(&datastore, quiesce_started, quiesce_completed).await;

        let Err(err) = datastore
            .ensure_schema(
                &log,
                new_schema.clone(),
                ConsumerPolicy::UpdateGracefully,
                Some(&all_versions),
            )
            .await
        else {
            panic!("Ensuring schema mid-quiesce should fail");
        };
        assert!(
            matches!(
                err,
                EnsureSchemaError::UnexpectedAction {
                    action: SchemaAction::Wait
                }
            ),
            "Unexpected error: {err:?} (expected: Wait for Quiesce)",
        );

        // Let's suppose that later on, quiesce completes.
        //
        // This should let us proceed with the update.
        let quiesce_started = true;
        let quiesce_completed = true;
        set_quiesce(&datastore, quiesce_started, quiesce_completed).await;
        datastore
            .ensure_schema(
                &log,
                new_schema.clone(),
                ConsumerPolicy::UpdateGracefully,
                Some(&all_versions),
            )
            .await
            .expect("Ensuring schema should have succeeded");

        let db_metadata = datastore
            .database_metadata()
            .await
            .expect("Should be able to read database metadata post-update");

        // Since the update succeeded, we should be able to see:
        // - We're using the new version
        // - There is no "target version"
        // - Quiescing state is cleared (set to "false")
        assert_eq!(
            semver::Version::from(db_metadata.version().clone()),
            new_schema
        );
        assert_eq!(db_metadata.target_version(), None);
        assert_eq!(db_metadata.quiesce_started(), false);
        assert_eq!(db_metadata.quiesce_completed(), false);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_schema_without_upgrade() {
        let logctx = dev::test_setup_log("ensure_schema_without_upgrade");
        let log = &logctx.log;
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();

        let mut new_schema = SCHEMA_VERSION;
        new_schema.major += 1;

        // Load the new version, but don't enable update.
        let datastore = DataStore::new_unchecked(log.clone(), pool.clone());
        let Err(err) = datastore
            .ensure_schema(
                &log,
                new_schema,
                ConsumerPolicy::FailOnMismatch,
                None,
            )
            .await
        else {
            panic!("Ensuring schema without enabling update should fail");
        };
        assert!(
            matches!(
                err,
                EnsureSchemaError::UnexpectedAction {
                    action: SchemaAction::Refuse {
                        reason: SchemaRefuseReason::OutOfDate
                    }
                }
            ),
            "Unexpected error: {err:?} (expected: Schema out-of-date)",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests for the new policy-based architecture
    #[tokio::test]
    async fn test_schema_version_status_interpret() {
        // Test all combinations of version comparisons and quiesce states
        let logctx =
            dev::test_setup_log("test_schema_version_status_interpret");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let current_version = SCHEMA_VERSION;
        let mut newer_version = SCHEMA_VERSION;
        newer_version.major += 1;
        let mut older_version = SCHEMA_VERSION;
        older_version.major = older_version.major.saturating_sub(1);

        // Test: Database matches, no quiesce
        set_quiesce(&datastore, false, false).await;
        let metadata = datastore.database_metadata().await.unwrap();
        let status =
            SchemaVersionStatus::interpret(&metadata, &current_version);
        assert_eq!(status, SchemaVersionStatus::DatabaseMatchesReady);

        // Test: Database matches, quiesce started
        set_quiesce(&datastore, true, false).await;
        let metadata = datastore.database_metadata().await.unwrap();
        let status =
            SchemaVersionStatus::interpret(&metadata, &current_version);
        assert_eq!(status, SchemaVersionStatus::DatabaseMatchesQuiescing);

        // Test: Database matches, quiesce completed
        set_quiesce(&datastore, true, true).await;
        let metadata = datastore.database_metadata().await.unwrap();
        let status =
            SchemaVersionStatus::interpret(&metadata, &current_version);
        assert_eq!(status, SchemaVersionStatus::DatabaseIsNewer);

        // Test: Database is newer
        let metadata = datastore.database_metadata().await.unwrap();
        let status = SchemaVersionStatus::interpret(&metadata, &older_version);
        assert_eq!(status, SchemaVersionStatus::DatabaseIsNewer);

        // We can't easily test older database versions without more setup,
        // but the logic is straightforward from the implementation

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_schema_action_policy_combinations() {
        // Test the big match in SchemaAction::new for all meaningful combinations

        // DatabaseIsNewer should always result in "Refuse + TooNew"
        for policy in [
            ConsumerPolicy::FailOnMismatch,
            ConsumerPolicy::UpdateGracefully,
            ConsumerPolicy::UpdateForcefully,
        ] {
            let action =
                SchemaAction::new(SchemaVersionStatus::DatabaseIsNewer, policy);
            assert_eq!(
                action,
                SchemaAction::Refuse { reason: SchemaRefuseReason::TooNew }
            );
        }

        // DatabaseMatchesReady should always result in "Ready"
        for policy in [
            ConsumerPolicy::FailOnMismatch,
            ConsumerPolicy::UpdateGracefully,
            ConsumerPolicy::UpdateForcefully,
        ] {
            let action = SchemaAction::new(
                SchemaVersionStatus::DatabaseMatchesReady,
                policy,
            );
            assert_eq!(action, SchemaAction::Ready);
        }

        // DatabaseMatchesQuiescing behavior varies by policy
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseMatchesQuiescing,
                ConsumerPolicy::FailOnMismatch
            ),
            SchemaAction::Quiesce
        );
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseMatchesQuiescing,
                ConsumerPolicy::UpdateGracefully
            ),
            SchemaAction::Quiesce
        );
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseMatchesQuiescing,
                ConsumerPolicy::UpdateForcefully
            ),
            SchemaAction::Ready
        );

        // DatabaseIsOlderUnquiesced behavior varies by policy
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseIsOlderUnquiesced,
                ConsumerPolicy::FailOnMismatch
            ),
            SchemaAction::Refuse { reason: SchemaRefuseReason::OutOfDate }
        );
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseIsOlderUnquiesced,
                ConsumerPolicy::UpdateGracefully
            ),
            SchemaAction::Wait
        );
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseIsOlderUnquiesced,
                ConsumerPolicy::UpdateForcefully
            ),
            SchemaAction::Update
        );

        // DatabaseIsOlderQuiesced behavior varies by policy
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseIsOlderQuiesced,
                ConsumerPolicy::FailOnMismatch
            ),
            SchemaAction::Refuse { reason: SchemaRefuseReason::OutOfDate }
        );
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseIsOlderQuiesced,
                ConsumerPolicy::UpdateGracefully
            ),
            SchemaAction::Update
        );
        assert_eq!(
            SchemaAction::new(
                SchemaVersionStatus::DatabaseIsOlderQuiesced,
                ConsumerPolicy::UpdateForcefully
            ),
            SchemaAction::Update
        );
    }

    #[tokio::test]
    async fn test_check_schema_version() {
        let logctx = dev::test_setup_log("test_check_schema_version");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        // Test the current version and no quiesce
        set_quiesce(&datastore, false, false).await;
        let status =
            datastore.check_schema_version(&SCHEMA_VERSION).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseMatchesReady);

        // Test the current version with quiesce started
        set_quiesce(&datastore, true, false).await;
        let status =
            datastore.check_schema_version(&SCHEMA_VERSION).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseMatchesQuiescing);

        // Test the current version with quiesce completed
        //
        // (Once quiesce completes, the "current version" is equivalent
        // to an old schema)
        set_quiesce(&datastore, true, true).await;
        let status =
            datastore.check_schema_version(&SCHEMA_VERSION).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseIsNewer);

        // Test an older version without quiesce
        let mut old_version = SCHEMA_VERSION;
        old_version.major -= 1;
        set_quiesce(&datastore, false, false).await;
        let status =
            datastore.check_schema_version(&old_version).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseIsNewer);

        // Test a newer schema with quiesce unfinished
        let mut new_version = SCHEMA_VERSION;
        new_version.major += 1;
        set_quiesce(&datastore, false, false).await;
        let status =
            datastore.check_schema_version(&new_version).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseIsOlderUnquiesced);
        set_quiesce(&datastore, true, false).await;
        let status =
            datastore.check_schema_version(&new_version).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseIsOlderUnquiesced);

        // Test a newer schema with quiesce finished
        set_quiesce(&datastore, true, true).await;
        let status =
            datastore.check_schema_version(&new_version).await.unwrap();
        assert_eq!(status, SchemaVersionStatus::DatabaseIsOlderQuiesced);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
