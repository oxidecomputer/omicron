// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Database Metadata.

use super::{DataStore, DbConnection};
use crate::authz;
use crate::context::OpContext;

use anyhow::{Context, bail, ensure};
use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
use chrono::Utc;
use diesel::prelude::*;
use futures::FutureExt;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::AllSchemaVersions;
use nexus_db_model::DbMetadataNexus;
use nexus_db_model::DbMetadataNexusState;
use nexus_db_model::EARLIEST_SUPPORTED_VERSION;
use nexus_db_model::SchemaUpgradeStep;
use nexus_db_model::SchemaVersion;
use nexus_types::deployment::BlueprintZoneDisposition;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
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
    // deployment".
    pub async fn ensure_schema(
        &self,
        log: &Logger,
        desired_version: Version,
        all_versions: Option<&AllSchemaVersions>,
    ) -> Result<(), anyhow::Error> {
        let (found_version, found_target_version) = self
            .database_schema_version()
            .await
            .context("Cannot read database schema version")?;

        let log = log.new(o!(
            "found_version" => found_version.to_string(),
            "desired_version" => desired_version.to_string(),
        ));

        // NOTE: We could run with a less tight restriction.
        //
        // If we respect the meaning of the semver version, it should be
        // possible to use subsequent versions, as long as they do not introduce
        // breaking changes.
        //
        // However, at the moment, we opt for conservatism: if the database does
        // not exactly match the schema version, we refuse to continue without
        // modification.
        if found_version == desired_version {
            info!(log, "Database schema version is up to date");
            return Ok(());
        }

        if found_version > desired_version {
            error!(
                log,
                "Found schema version is newer than desired schema version";
            );
            bail!(
                "Found schema version ({}) is newer than desired schema \
                version ({})",
                found_version,
                desired_version,
            )
        }

        let Some(all_versions) = all_versions else {
            error!(
                log,
                "Database schema version is out of date, but automatic update \
                is disabled",
            );
            bail!("Schema is out of date but automatic update is disabled");
        };

        // If we're here, we know the following:
        //
        // - The schema does not match our expected version (or at least, it
        //   didn't when we read it moments ago).
        // - We should attempt to automatically upgrade the schema.
        info!(log, "Database schema is out of date.  Attempting upgrade.");
        ensure!(
            all_versions.contains_version(&found_version),
            "Found schema version {found_version} was not found",
        );

        // TODO: Test this?
        ensure!(
            all_versions.contains_version(&desired_version),
            "Desired version {desired_version} was not found",
        );

        let target_versions: Vec<&SchemaVersion> = all_versions
            .versions_range((
                Bound::Excluded(&found_version),
                Bound::Included(&desired_version),
            ))
            .collect();

        // Iterate over each of the higher-level user-defined versions.
        //
        // These are the user-defined `KNOWN_VERSIONS` defined in
        // nexus/db-model/src/schema_versions.rs.
        let mut current_version = found_version;
        for target_version in target_versions.into_iter() {
            let log = log.new(o!(
                "current_version" => current_version.to_string(),
                "target_version" => target_version.semver().to_string(),
            ));
            info!(log, "Attempting to upgrade schema");

            // For the rationale here, see: StepSemverVersion::new.
            if target_version.semver().pre != semver::Prerelease::EMPTY {
                bail!("Cannot upgrade to version which includes pre-release");
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
                    StepSemverVersion::new(&target_version.semver(), i)?;
                let log = log.new(o!("target_step.version" => target_step.version.to_string()));

                self.apply_step_version_update(
                    &log,
                    &step,
                    &target_step,
                    &current_version,
                    &found_target_version,
                )
                .await?;

                last_step_version = Some(target_step.clone());
            }

            info!(
                log,
                "Applied schema upgrade";
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
            let last_step_version = last_step_version
                .ok_or_else(|| anyhow::anyhow!("Missing final step version"))?;
            self.finalize_schema_update(&current_version, &last_step_version)
                .await
                .context("Failed to finalize schema update")?;

            info!(
                log,
                "Finalized schema upgrade";
            );

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

    // Returns the access this Nexus has to the database
    #[cfg(test)]
    async fn database_nexus_access(
        &self,
        nexus_id: OmicronZoneUuid,
    ) -> Result<Option<DbMetadataNexus>, Error> {
        use nexus_db_schema::schema::db_metadata_nexus::dsl;

        let nexus_access: Option<DbMetadataNexus> = dsl::db_metadata_nexus
            .filter(
                dsl::nexus_id.eq(nexus_db_model::to_db_typed_uuid(nexus_id)),
            )
            .first_async(&*self.pool_connection_unauthorized().await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(nexus_access)
    }

    // Checks if any db_metadata_nexus records exist in the database using an
    // existing connection
    async fn database_nexus_access_any_exist_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<bool, Error> {
        use nexus_db_schema::schema::db_metadata_nexus::dsl;

        let exists: bool = diesel::select(diesel::dsl::exists(
            dsl::db_metadata_nexus.select(dsl::nexus_id),
        ))
        .get_result_async(conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(exists)
    }

    /// Deletes the "db_metadata_nexus" record for a Nexus ID, if it exists.
    pub async fn database_nexus_access_delete(
        &self,
        opctx: &OpContext,
        nexus_id: OmicronZoneUuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::db_metadata_nexus::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(&opctx).await?;

        diesel::delete(
            dsl::db_metadata_nexus
                .filter(dsl::nexus_id.eq(nexus_id.into_untyped_uuid())),
        )
        .execute_async(conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Propagate the nexus records to the database if and only if
    /// the blueprint is the current target.
    ///
    /// If any of these records already exist, they are unmodified.
    pub async fn database_nexus_access_create(
        &self,
        opctx: &OpContext,
        blueprint: &nexus_types::deployment::Blueprint,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // TODO: Without https://github.com/oxidecomputer/omicron/pull/8863, we
        // treat all Nexuses as active. Some will become "not_yet", depending on
        // the Nexus Generation, once it exists.
        let active_nexus_zones = blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_sled, zone_cfg)| {
                if zone_cfg.zone_type.is_nexus() {
                    Some(zone_cfg)
                } else {
                    None
                }
            });
        let new_nexuses = active_nexus_zones
            .map(|z| DbMetadataNexus::new(z.id, DbMetadataNexusState::Active))
            .collect::<Vec<_>>();

        let conn = &*self.pool_connection_authorized(&opctx).await?;
        self.transaction_if_current_blueprint_is(
            &conn,
            "database_nexus_access_create",
            opctx,
            blueprint.id,
            |conn| {
                let new_nexuses = new_nexuses.clone();
                async move {
                    use nexus_db_schema::schema::db_metadata_nexus::dsl;

                    diesel::insert_into(dsl::db_metadata_nexus)
                        .values(new_nexuses)
                        .on_conflict(dsl::nexus_id)
                        .do_nothing()
                        .execute_async(conn)
                        .await?;
                    Ok(())
                }
                .boxed()
            },
        )
        .await
    }

    // Registers a Nexus instance as having active access to the database
    #[cfg(test)]
    async fn database_nexus_access_insert(
        &self,
        nexus_id: OmicronZoneUuid,
        state: DbMetadataNexusState,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::db_metadata_nexus::dsl;

        let new_nexus = DbMetadataNexus::new(nexus_id, state);

        diesel::insert_into(dsl::db_metadata_nexus)
            .values(new_nexus)
            .on_conflict(dsl::nexus_id)
            .do_update()
            .set(dsl::state.eq(diesel::upsert::excluded(dsl::state)))
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Initializes Nexus database access records from a blueprint using an
    /// existing connection
    ///
    /// This function finds all Nexus zones in the given blueprint and creates
    /// active database access records for them. Used during RSS rack setup.
    ///
    /// Returns an error if:
    /// - Any db_metadata_nexus records already exist (should only be called
    /// during initial setup)
    pub async fn initialize_nexus_access_from_blueprint_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        nexus_zone_ids: Vec<OmicronZoneUuid>,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::db_metadata_nexus::dsl;

        // Ensure no db_metadata_nexus records already exist
        let any_records_exist =
            Self::database_nexus_access_any_exist_on_connection(conn).await?;
        if any_records_exist {
            return Err(Error::conflict(
                "Cannot initialize Nexus access from blueprint: \
                db_metadata_nexus records already exist. This function should \
                only be called during initial rack setup.",
            ));
        }

        // Create db_metadata_nexus records for all Nexus zones
        let new_nexuses: Vec<DbMetadataNexus> = nexus_zone_ids
            .iter()
            .map(|&nexus_id| {
                DbMetadataNexus::new(nexus_id, DbMetadataNexusState::Active)
            })
            .collect();

        diesel::insert_into(dsl::db_metadata_nexus)
            .values(new_nexuses)
            .execute_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
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
    // - from_version: What we expect "version" must be to proceed
    // - last_step: What we expect "target_version" must be to proceed.
    async fn finalize_schema_update(
        &self,
        from_version: &Version,
        last_step: &StepSemverVersion,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::db_metadata::dsl;

        let to_version = last_step.without_prerelease();
        let rows_updated = diesel::update(
            dsl::db_metadata
                .filter(dsl::singleton.eq(true))
                .filter(dsl::version.eq(from_version.to_string()))
                .filter(dsl::target_version.eq(last_step.version.to_string())),
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
    use id_map::IdMap;
    use nexus_db_model::SCHEMA_VERSION;
    use nexus_inventory::now_db_precision;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use nexus_types::deployment::BlueprintSledConfig;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use nexus_types::deployment::OximeterReadMode;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::PlanningReport;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::NetworkInterface;
    use nexus_types::inventory::NetworkInterfaceKind;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeMap;

    // Confirms that calling the internal "ensure_schema" function can succeed
    // when the database is already at that version.
    #[tokio::test]
    async fn ensure_schema_is_current_version() {
        let logctx = dev::test_setup_log("ensure_schema_is_current_version");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let datastore = db.datastore();

        datastore
            .ensure_schema(&logctx.log, SCHEMA_VERSION, None)
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
        while let Err(e) = datastore
            .ensure_schema(&log, SCHEMA_VERSION, Some(&all_versions))
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
    fn create_test_blueprint(
        nexus_zones: Vec<(OmicronZoneUuid, BlueprintZoneDisposition)>,
    ) -> Blueprint {
        let blueprint_id = BlueprintUuid::new_v4();
        let sled_id = SledUuid::new_v4();

        let zones: IdMap<BlueprintZoneConfig> = nexus_zones
            .into_iter()
            .map(|(zone_id, disposition)| BlueprintZoneConfig {
                disposition,
                id: zone_id,
                filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
                zone_type: BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address: "[::1]:0".parse().unwrap(),
                    external_dns_servers: Vec::new(),
                    external_ip: nexus_types::deployment::OmicronZoneExternalFloatingIp {
                        id: ExternalIpUuid::new_v4(),
                        ip: std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                    },
                    external_tls: true,
                    nic: NetworkInterface {
                        id: uuid::Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: zone_id.into_untyped_uuid(),
                        },
                        name: "test-nic".parse().unwrap(),
                        ip: "192.168.1.1".parse().unwrap(),
                        mac: MacAddr::random_system(),
                        subnet: ipnetwork::IpNetwork::V4(
                            "192.168.1.0/24".parse().unwrap()
                        ).into(),
                        vni: Vni::try_from(100).unwrap(),
                        primary: true,
                        slot: 0,
                        transit_ips: Vec::new(),
                    },
                }),
                image_source: BlueprintZoneImageSource::InstallDataset,
            })
            .collect();

        let mut sleds = BTreeMap::new();
        sleds.insert(
            sled_id,
            BlueprintSledConfig {
                state: SledState::Active,
                sled_agent_generation: Generation::new(),
                zones,
                disks: IdMap::new(),
                datasets: IdMap::new(),
                remove_mupdate_override: None,
                host_phase_2: BlueprintHostPhase2DesiredSlots::current_contents(
                ),
            },
        );

        Blueprint {
            id: blueprint_id,
            sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            parent_blueprint_id: None,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            target_release_minimum_generation: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            clickhouse_cluster_config: None,
            oximeter_read_mode: OximeterReadMode::SingleNode,
            oximeter_read_version: Generation::new(),
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            report: PlanningReport::new(blueprint_id),
        }
    }

    #[tokio::test]
    async fn test_database_nexus_access_create() {
        let logctx = dev::test_setup_log("test_database_nexus_access_create");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create a blueprint with two in-service Nexus zones,
        // and one expunged Nexus.
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let expunged_nexus = OmicronZoneUuid::new_v4();
        let blueprint = create_test_blueprint(vec![
            (nexus1_id, BlueprintZoneDisposition::InService),
            (nexus2_id, BlueprintZoneDisposition::InService),
            (
                expunged_nexus,
                BlueprintZoneDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: true,
                },
            ),
        ]);

        // Insert the blueprint and make it the target
        datastore
            .blueprint_insert(&opctx, &blueprint)
            .await
            .expect("Failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set blueprint target");

        // Create nexus access records
        datastore
            .database_nexus_access_create(&opctx, &blueprint)
            .await
            .expect("Failed to create nexus access");

        // Verify records were created with Active state
        let nexus1_access = datastore
            .database_nexus_access(nexus1_id)
            .await
            .expect("Failed to get nexus1 access");
        let nexus2_access = datastore
            .database_nexus_access(nexus2_id)
            .await
            .expect("Failed to get nexus2 access");
        let expunged_access = datastore
            .database_nexus_access(expunged_nexus)
            .await
            .expect("Failed to get expunged access");

        assert!(nexus1_access.is_some(), "nexus1 should have access record");
        assert!(nexus2_access.is_some(), "nexus2 should have access record");
        assert!(
            expunged_access.is_none(),
            "expunged nexus should not have access record"
        );

        let nexus1_record = nexus1_access.unwrap();
        let nexus2_record = nexus2_access.unwrap();
        assert_eq!(nexus1_record.state(), DbMetadataNexusState::Active);
        assert_eq!(nexus2_record.state(), DbMetadataNexusState::Active);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_nexus_access_create_idempotent() {
        let logctx =
            dev::test_setup_log("test_database_nexus_access_create_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create a blueprint with one Nexus zone
        let nexus_id = OmicronZoneUuid::new_v4();
        let blueprint = create_test_blueprint(vec![(
            nexus_id,
            BlueprintZoneDisposition::InService,
        )]);

        // Insert the blueprint and make it the target
        datastore
            .blueprint_insert(&opctx, &blueprint)
            .await
            .expect("Failed to insert blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set blueprint target");

        // Create nexus access records (first time)
        datastore
            .database_nexus_access_create(&opctx, &blueprint)
            .await
            .expect("Failed to create nexus access (first time)");

        // Verify record was created
        async fn confirm_state(
            datastore: &DataStore,
            nexus_id: OmicronZoneUuid,
            expected_state: DbMetadataNexusState,
        ) {
            let state = datastore
                .database_nexus_access(nexus_id)
                .await
                .expect("Failed to get nexus access after first create")
                .expect("Entry for Nexus should have been inserted");
            assert_eq!(state.state(), expected_state);
        }

        confirm_state(datastore, nexus_id, DbMetadataNexusState::Active).await;

        // Creating the record again: not an error.
        datastore
            .database_nexus_access_create(&opctx, &blueprint)
            .await
            .expect("Failed to create nexus access (first time)");
        confirm_state(datastore, nexus_id, DbMetadataNexusState::Active).await;

        // Manually make the record "Quiesced".
        use nexus_db_schema::schema::db_metadata_nexus::dsl;
        diesel::update(dsl::db_metadata_nexus)
            .filter(dsl::nexus_id.eq(nexus_id.into_untyped_uuid()))
            .set(dsl::state.eq(DbMetadataNexusState::Quiesced))
            .execute_async(
                &*datastore.pool_connection_unauthorized().await.unwrap(),
            )
            .await
            .expect("Failed to update record");
        confirm_state(datastore, nexus_id, DbMetadataNexusState::Quiesced)
            .await;

        // Create nexus access records another time - should be idempotent,
        // but should be "on-conflict, ignore".
        datastore
            .database_nexus_access_create(&opctx, &blueprint)
            .await
            .expect("Failed to create nexus access (second time)");
        confirm_state(datastore, nexus_id, DbMetadataNexusState::Quiesced)
            .await;

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_nexus_access_create_fails_wrong_target_blueprint() {
        let logctx = dev::test_setup_log(
            "test_database_nexus_access_create_fails_wrong_target_blueprint",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create two different blueprints
        let nexus_id = OmicronZoneUuid::new_v4();
        let target_blueprint = create_test_blueprint(vec![(
            nexus_id,
            BlueprintZoneDisposition::InService,
        )]);
        let non_target_blueprint = create_test_blueprint(vec![(
            nexus_id,
            BlueprintZoneDisposition::InService,
        )]);

        // Insert both blueprints
        datastore
            .blueprint_insert(&opctx, &target_blueprint)
            .await
            .expect("Failed to insert target blueprint");
        datastore
            .blueprint_insert(&opctx, &non_target_blueprint)
            .await
            .expect("Failed to insert non-target blueprint");

        // Set the first blueprint as the target
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: target_blueprint.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("Failed to set target blueprint");

        // Try to create nexus access records using the non-target blueprint.
        // This should fail because the transaction should check if the
        // blueprint is the current target
        let result = datastore
            .database_nexus_access_create(&opctx, &non_target_blueprint)
            .await;
        assert!(
            result.is_err(),
            "Creating nexus access with wrong target blueprint should fail"
        );

        // Verify no records were created for the nexus
        let access = datastore
            .database_nexus_access(nexus_id)
            .await
            .expect("Failed to get nexus access");
        assert!(
            access.is_none(),
            "No access record should exist when wrong blueprint is used"
        );

        // Verify that using the correct target blueprint works
        datastore
            .database_nexus_access_create(&opctx, &target_blueprint)
            .await
            .expect(
                "Creating nexus access with correct blueprint should succeed",
            );

        let access_after_correct = datastore
            .database_nexus_access(nexus_id)
            .await
            .expect("Failed to get nexus access after correct blueprint");
        assert!(
            access_after_correct.is_some(),
            "Access record should exist after using correct target blueprint"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_nexus_access_delete() {
        let logctx = dev::test_setup_log("test_database_nexus_access_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = db.opctx();

        // Create test nexus IDs
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();

        // Insert records directly using the test method
        datastore
            .database_nexus_access_insert(
                nexus1_id,
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert nexus1 access");
        datastore
            .database_nexus_access_insert(
                nexus2_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus2 access");

        // Verify records were created
        let nexus1_before = datastore
            .database_nexus_access(nexus1_id)
            .await
            .expect("Failed to get nexus1 access");
        let nexus2_before = datastore
            .database_nexus_access(nexus2_id)
            .await
            .expect("Failed to get nexus2 access");
        assert!(nexus1_before.is_some(), "nexus1 should have access record");
        assert!(nexus2_before.is_some(), "nexus2 should have access record");

        // Delete nexus1 record
        datastore
            .database_nexus_access_delete(&opctx, nexus1_id)
            .await
            .expect("Failed to delete nexus1 access");

        // Verify nexus1 record was deleted, nexus2 record remains
        let nexus1_after = datastore
            .database_nexus_access(nexus1_id)
            .await
            .expect("Failed to get nexus1 access after delete");
        let nexus2_after = datastore
            .database_nexus_access(nexus2_id)
            .await
            .expect("Failed to get nexus2 access after delete");
        assert!(
            nexus1_after.is_none(),
            "nexus1 should not have access record after delete"
        );
        assert!(
            nexus2_after.is_some(),
            "nexus2 should still have access record"
        );

        // Delete nexus2 record
        datastore
            .database_nexus_access_delete(&opctx, nexus2_id)
            .await
            .expect("Failed to delete nexus2 access");

        // Verify nexus2 record was also deleted
        let nexus2_final = datastore
            .database_nexus_access(nexus2_id)
            .await
            .expect("Failed to get nexus2 access after final delete");
        assert!(
            nexus2_final.is_none(),
            "nexus2 should not have access record after delete"
        );

        // Confirm deletion is idempotent
        datastore
            .database_nexus_access_delete(&opctx, nexus1_id)
            .await
            .expect("Failed to delete nexus1 access idempotently");

        // This also means deleting non-existent records should be fine
        datastore
            .database_nexus_access_delete(&opctx, OmicronZoneUuid::new_v4())
            .await
            .expect("Failed to delete non-existent record");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
