// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Database Metadata.

use super::{DataStore, DbConnection, IdentityCheckPolicy};
use anyhow::{Context, bail, ensure};
use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::AllSchemaVersions;
use nexus_db_model::DB_METADATA_NEXUS_SCHEMA_VERSION;
use nexus_db_model::DbMetadataNexus;
use nexus_db_model::DbMetadataNexusState;
use nexus_db_model::EARLIEST_SUPPORTED_VERSION;
use nexus_db_model::SchemaUpgradeStep;
use nexus_db_model::SchemaVersion;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use semver::Version;
use slog::{Logger, error, info, o};
use std::ops::Bound;
use std::str::FromStr;
use uuid::Uuid;

/// Errors that can occur during handoff operations
#[derive(Debug, thiserror::Error)]
pub enum HandoffError {
    #[error(
        "Cannot perform handoff: \
         {active_count} Nexus instance(s) are still active. \
         All instances must be quiesced or not_yet before handoff can proceed."
    )]
    ActiveNexusInstancesExist { active_count: u32 },

    #[error(
        "Cannot perform handoff: \
         Nexus {nexus_id} does not have a record in db_metadata_nexus table. \
         This Nexus must be registered before it can become active."
    )]
    NexusNotRegistered { nexus_id: OmicronZoneUuid },

    #[error(
        "Cannot perform handoff: \
         Nexus {nexus_id} is in state {current_state:?}. \
         Must be in 'not_yet' state to become active."
    )]
    NexusInWrongState {
        nexus_id: OmicronZoneUuid,
        current_state: DbMetadataNexusState,
    },
}

impl From<HandoffError> for Error {
    fn from(err: HandoffError) -> Self {
        use HandoffError::*;
        match err {
            // These conditions are all errors that may occur transiently, with
            // handoff from old -> new Nexus, or with multiple Nexuses
            // concurrently attempting to perform the handoff operation.
            //
            // As a result, each returns a "503" error indicating that a retry
            // should be attempted.
            ActiveNexusInstancesExist { .. }
            | NexusNotRegistered { .. }
            | NexusInWrongState { .. } => Error::unavail(&err.to_string()),
        }
    }
}

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

/// Describes the state of the database access with respect this Nexus
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NexusAccess {
    /// Nexus does not yet have access to the database.
    DoesNotHaveAccessYet,

    /// Nexus has been explicitly locked out of the database.
    LockedOut,

    /// Nexus should have normal access to the database
    ///
    /// We have a record of this Nexus, and it should have access.
    HasExplicitAccess,

    /// Nexus should have normal access to the database
    ///
    /// We may or may not have a record of this Nexus, but it should have
    /// access.
    HasImplicitAccess,
}

/// Describes the state of the schema with respect this Nexus
#[derive(Debug, Copy, Clone, PartialEq)]
enum SchemaStatus {
    /// The database schema matches what we want
    UpToDate,

    /// The database schema is newer than what we want
    NewerThanDesired,

    /// The database schema is older than what we want
    OlderThanDesired,

    /// The database schema is older than what we want, and it's
    /// so old, it does not know about the "db_metadata_nexus" table.
    ///
    /// We should avoid accessing the "db_metadata_nexus" tables to check
    /// access, because the schema for these tables may not exist.
    ///
    /// TODO: This may be removed, once we're confident deployed systems
    /// have upgraded past DB_METADATA_NEXUS_SCHEMA_VERSION.
    OlderThanDesiredSkipAccessCheck,
}

/// Describes what should be done with a schema
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SchemaAction {
    /// Normal operation: The database is ready for usage
    Ready,

    /// Not ready for usage yet
    ///
    /// The database may be ready for usage once handoff has completed.
    NeedsHandoff,

    /// Start a schema update
    Update,

    /// Refuse to use the database
    Refuse,
}

/// Committment that the database is willing to perform a [SchemaAction]
/// to a desired schema [Version].
///
/// Can be created through [DataStore::check_schema_and_access]
#[derive(Clone)]
pub struct ValidatedSchemaAction {
    action: SchemaAction,
    desired: Version,
}

impl ValidatedSchemaAction {
    pub fn action(&self) -> &SchemaAction {
        &self.action
    }

    pub fn desired_version(&self) -> &Version {
        &self.desired
    }
}

impl SchemaAction {
    // Interprets the combination of access and status to decide what action
    // should be taken.
    fn new(access: NexusAccess, status: SchemaStatus) -> Self {
        use NexusAccess::*;
        use SchemaStatus::*;

        match (access, status) {
            // Nexus has been explicitly locked-out of using the database
            (LockedOut, _) => Self::Refuse,

            // The schema updated beyond what we want, do not use it.
            (_, NewerThanDesired) => Self::Refuse,

            // If we don't have access yet, but could do something once handoff
            // occurs, then handoff is needed
            (
                DoesNotHaveAccessYet,
                UpToDate | OlderThanDesired | OlderThanDesiredSkipAccessCheck,
            ) => Self::NeedsHandoff,

            // This is the most "normal" case: Nexus should have access to the
            // database, and the schema matches what it wants.
            (HasExplicitAccess | HasImplicitAccess, UpToDate) => Self::Ready,

            // If this Nexus is allowed to access the schema, but it looks
            // older than what we expect, we'll need to update the schema to
            // use it.
            (
                HasExplicitAccess | HasImplicitAccess,
                OlderThanDesired | OlderThanDesiredSkipAccessCheck,
            ) => Self::Update,
        }
    }
}

impl DataStore {
    // Checks if the specified Nexus has access to the database.
    async fn check_nexus_access(
        &self,
        nexus_id: OmicronZoneUuid,
    ) -> Result<NexusAccess, anyhow::Error> {
        // Check if any "db_metadata_nexus" rows exist.
        // If they don't exist, treat the database as having access.
        //
        // This handles the case for:
        // - Fresh deployments where RSS hasn't populated the table yet (we need
        // access to finish "rack_initialization").
        // - Systems that haven't been migrated to include nexus access control
        // (we need access to the database to backfill these records).
        //
        // After initialization/migration, this conditional should never trigger
        // again.
        let any_records_exist = self.database_nexus_access_any_exist().await?;
        if !any_records_exist {
            warn!(
                &self.log,
                "No db_metadata_nexus records exist - skipping access check";
                "nexus_id" => ?nexus_id,
                "explanation" => "This is expected during initial deployment \
                                  or before migration"
            );
            return Ok(NexusAccess::HasImplicitAccess);
        }

        // Records exist, so enforce the access control check
        let Some(state) =
            self.database_nexus_access(nexus_id).await?.map(|s| s.state())
        else {
            let msg = "Nexus does not have access to the database (no \
                       db_metadata_nexus record)";
            warn!(&self.log, "{msg}"; "nexus_id" => ?nexus_id);
            return Ok(NexusAccess::DoesNotHaveAccessYet);
        };

        let status = match state {
            DbMetadataNexusState::Active => {
                info!(
                    &self.log,
                    "Nexus has access to the database";
                    "nexus_id" => ?nexus_id
                );
                NexusAccess::HasExplicitAccess
            }
            DbMetadataNexusState::NotYet => {
                info!(
                    &self.log,
                    "Nexus does not yet have access to the database";
                    "nexus_id" => ?nexus_id
                );
                NexusAccess::DoesNotHaveAccessYet
            }
            DbMetadataNexusState::Quiesced => {
                let msg = "Nexus locked out of database access (quiesced)";
                error!(&self.log, "{msg}"; "nexus_id" => ?nexus_id);
                NexusAccess::LockedOut
            }
        };
        Ok(status)
    }

    // Checks the schema against a desired version.
    async fn check_schema(
        &self,
        desired_version: Version,
    ) -> Result<SchemaStatus, anyhow::Error> {
        let (found_version, _found_target_version) = self
            .database_schema_version()
            .await
            .context("Cannot read database schema version")?;

        let log = self.log.new(o!(
            "found_version" => found_version.to_string(),
            "desired_version" => desired_version.to_string(),
        ));

        use std::cmp::Ordering;
        match found_version.cmp(&desired_version) {
            Ordering::Less => {
                warn!(log, "Found schema version is older than desired");
                if found_version < DB_METADATA_NEXUS_SCHEMA_VERSION {
                    Ok(SchemaStatus::OlderThanDesiredSkipAccessCheck)
                } else {
                    Ok(SchemaStatus::OlderThanDesired)
                }
            }
            Ordering::Equal => {
                info!(log, "Database schema version is up to date");
                Ok(SchemaStatus::UpToDate)
            }
            Ordering::Greater => {
                error!(log, "Found schema version is newer than desired");
                Ok(SchemaStatus::NewerThanDesired)
            }
        }
    }

    /// Compares the state of the schema with the expectations of the
    /// currently running Nexus.
    ///
    /// - `identity_check`: Describes whether or not the identity of the
    /// calling Nexus should be validated before returning database access
    /// - `desired_version`: The version of the database schema this
    /// Nexus wants.
    pub async fn check_schema_and_access(
        &self,
        identity_check: IdentityCheckPolicy,
        desired_version: Version,
    ) -> Result<ValidatedSchemaAction, anyhow::Error> {
        let schema_status = self.check_schema(desired_version.clone()).await?;

        let nexus_access = match identity_check {
            IdentityCheckPolicy::CheckAndTakeover { nexus_id } => {
                match schema_status {
                    // If we don't think the "db_metadata_nexus" tables exist in
                    // the schema yet, treat them as implicitly having access.
                    //
                    // TODO: This may be removed, once we're confident deployed
                    // systems have upgraded past
                    // DB_METADATA_NEXUS_SCHEMA_VERSION.
                    SchemaStatus::OlderThanDesiredSkipAccessCheck => {
                        NexusAccess::HasImplicitAccess
                    }
                    _ => self.check_nexus_access(nexus_id).await?,
                }
            }
            IdentityCheckPolicy::DontCare => {
                // If a "nexus_id" was not supplied, skip the check, and treat it
                // as having access.
                //
                // This is necessary for tools which access the schema without a
                // running Nexus, such as the schema-updater binary.
                NexusAccess::HasImplicitAccess
            }
        };

        Ok(ValidatedSchemaAction {
            action: SchemaAction::new(nexus_access, schema_status),
            desired: desired_version,
        })
    }

    /// Ensures that the database schema matches `desired_version`.
    ///
    /// - `validated_action`: A [ValidatedSchemaAction], indicating that
    /// [Self::check_schema_and_access] has already been called.
    /// - `all_versions`: A description of all schema versions between
    /// "whatever is in the DB" and `desired_version`, instructing
    /// how to perform an update.
    pub async fn update_schema(
        &self,
        validated_action: ValidatedSchemaAction,
        all_versions: Option<&AllSchemaVersions>,
    ) -> Result<(), anyhow::Error> {
        let action = validated_action.action();

        match action {
            SchemaAction::Ready => bail!("No schema update is necessary"),
            SchemaAction::Update => (),
            _ => bail!("Not ready for schema update"),
        }

        let desired_version = validated_action.desired_version().clone();
        let (found_version, found_target_version) = self
            .database_schema_version()
            .await
            .context("Cannot read database schema version")?;

        let log = self.log.new(o!(
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

    /// Returns the access this Nexus has to the database
    pub async fn database_nexus_access(
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

    /// Checks if any db_metadata_nexus records exist in the database
    pub async fn database_nexus_access_any_exist(&self) -> Result<bool, Error> {
        let conn = self.pool_connection_unauthorized().await?;
        Self::database_nexus_access_any_exist_on_connection(&conn).await
    }

    /// Checks if any db_metadata_nexus records exist in the database using an existing connection
    pub async fn database_nexus_access_any_exist_on_connection(
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

    /// Registers a Nexus instance as having active access to the database
    pub async fn database_nexus_access_insert(
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
            .set(dsl::state.eq(excluded(dsl::state)))
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
        blueprint_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_model::ZoneType;
        use nexus_db_schema::schema::bp_omicron_zone::dsl as zone_dsl;
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

        // Query all Nexus zones from the blueprint
        let nexus_zone_ids: Vec<Uuid> = zone_dsl::bp_omicron_zone
            .filter(zone_dsl::blueprint_id.eq(blueprint_id))
            .filter(zone_dsl::zone_type.eq(ZoneType::Nexus))
            .select(zone_dsl::id)
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Create db_metadata_nexus records for all Nexus zones
        let new_nexuses: Vec<DbMetadataNexus> = nexus_zone_ids
            .iter()
            .map(|&nexus_id| {
                DbMetadataNexus::new(
                    OmicronZoneUuid::from_untyped_uuid(nexus_id),
                    DbMetadataNexusState::Active,
                )
            })
            .collect();

        diesel::insert_into(dsl::db_metadata_nexus)
            .values(new_nexuses)
            .execute_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    // Implementation function for attempt_handoff that runs within a
    // transaction
    //
    // This function must be executed from a transaction context to be safe.
    async fn attempt_handoff_impl(
        conn: async_bb8_diesel::Connection<DbConnection>,
        nexus_id: OmicronZoneUuid,
        err: OptionalError<HandoffError>,
    ) -> Result<(), diesel::result::Error> {
        use nexus_db_schema::schema::db_metadata_nexus::dsl;

        // Before proceeding, all records must be in the "quiesced" or "not_yet"
        // states.
        //
        // We explicitly look for any records violating this, rather than
        // explicitly looking for "active" records, as to protect ourselves from
        // future states being added over time.
        let active_count: nexus_db_model::SqlU32 = dsl::db_metadata_nexus
            .filter(
                dsl::state
                    .ne(DbMetadataNexusState::Quiesced)
                    .and(dsl::state.ne(DbMetadataNexusState::NotYet)),
            )
            .count()
            .get_result_async(&conn)
            .await?;
        let active_count: u32 = active_count.0;
        if active_count > 0 {
            return Err(err.bail(HandoffError::ActiveNexusInstancesExist {
                active_count,
            }));
        }

        // Check that our nexus has a "not_yet" record
        //
        // Only read the "state" field to avoid reading the rest of the struct,
        // in case additional columns are added over time.
        let our_nexus_state: Option<DbMetadataNexusState> =
            dsl::db_metadata_nexus
                .filter(
                    dsl::nexus_id
                        .eq(nexus_db_model::to_db_typed_uuid(nexus_id)),
                )
                .select(dsl::state)
                .get_result_async(&conn)
                .await
                .optional()?;
        let Some(our_state) = our_nexus_state else {
            return Err(err.bail(HandoffError::NexusNotRegistered { nexus_id }));
        };
        if our_state != DbMetadataNexusState::NotYet {
            return Err(err.bail(HandoffError::NexusInWrongState {
                nexus_id,
                current_state: our_state,
            }));
        }

        // Update all "not_yet" records to "active"
        diesel::update(dsl::db_metadata_nexus)
            .filter(dsl::state.eq(DbMetadataNexusState::NotYet))
            .set(dsl::state.eq(DbMetadataNexusState::Active))
            .execute_async(&conn)
            .await?;

        Ok(())
    }

    /// Attempts to perform a handoff to activate this Nexus for database
    /// access.
    ///
    /// This function checks that:
    /// 1. ALL records in db_metadata_nexus are in "not_yet" or "quiesced"
    ///    states
    /// 2. The specified nexus_id has a record which is "not_yet"
    ///
    /// If both conditions are met, it updates ALL "not_yet" records to
    /// "active". These operations are performed transactionally.
    ///
    /// Returns an error if:
    /// - Any record is in "active" state
    /// - The specified nexus_id doesn't have a "not_yet" record
    /// - Database transaction fails
    pub async fn attempt_handoff(
        &self,
        nexus_id: OmicronZoneUuid,
    ) -> Result<(), Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_unauthorized().await?;

        self.transaction_retry_wrapper("attempt_handoff")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::attempt_handoff_impl(conn, nexus_id, err).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err.into()
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
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
    use crate::db::datastore::IdentityCheckPolicy;
    use crate::db::pub_test_utils::TestDatabase;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use nexus_db_model::SCHEMA_VERSION;
    use omicron_test_utils::dev;

    // Confirms that calling the internal "ensure_schema" function can succeed
    // when the database is already at that version.
    #[tokio::test]
    async fn check_schema_is_current_version() {
        let logctx = dev::test_setup_log("check_schema_is_current_version");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let datastore = db.datastore();

        let checked_action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::DontCare,
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");

        assert!(
            matches!(checked_action.action(), SchemaAction::Ready),
            "Unexpected action: {:?}",
            checked_action.action(),
        );
        assert_eq!(
            checked_action.desired_version(),
            &SCHEMA_VERSION,
            "Unexpected desired version: {}",
            checked_action.desired_version()
        );

        datastore.update_schema(checked_action, None).await.expect_err(
            "Should not be able to update schema that's already up-to-date",
        );

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
                let datastore = DataStore::new(
                    &log,
                    pool,
                    Some(&all_versions),
                    IdentityCheckPolicy::DontCare,
                )
                .await?;

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
        let checked_action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::DontCare,
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");

        // This needs to be in a loop because we constructed a schema change
        // that will intentionally fail sometimes when doing this work.
        //
        // This isn't a normal behavior! But we're trying to test the
        // intermediate steps of a schema change here.
        while let Err(e) = datastore
            .update_schema(checked_action.clone(), Some(&all_versions))
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

    #[tokio::test]
    async fn test_attempt_handoff_with_active_records() {
        let logctx =
            dev::test_setup_log("test_attempt_handoff_with_active_records");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        // Set up test data: create some nexus records, including one active
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let nexus3_id = OmicronZoneUuid::new_v4();

        // Insert records: one active, one not_yet, one quiesced
        datastore
            .database_nexus_access_insert(
                nexus1_id,
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert active nexus");
        datastore
            .database_nexus_access_insert(
                nexus2_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert not_yet nexus");
        datastore
            .database_nexus_access_insert(
                nexus3_id,
                DbMetadataNexusState::Quiesced,
            )
            .await
            .expect("Failed to insert quiesced nexus");

        // Attempt handoff with nexus2 - should fail because nexus1 is active
        let result = datastore.attempt_handoff(nexus2_id).await;
        assert!(result.is_err());
        let error_msg = format!("{}", result.unwrap_err());
        assert!(
            error_msg.contains("1 Nexus instance(s) are still active"),
            "Expected error about active instances, got: {}",
            error_msg
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attempt_handoff_nexus_not_registered() {
        let logctx =
            dev::test_setup_log("test_attempt_handoff_nexus_not_registered");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        // Set up test data: create some other nexus records but not the one we're trying to handoff
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let unregistered_nexus_id = OmicronZoneUuid::new_v4();

        datastore
            .database_nexus_access_insert(
                nexus1_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus1");
        datastore
            .database_nexus_access_insert(
                nexus2_id,
                DbMetadataNexusState::Quiesced,
            )
            .await
            .expect("Failed to insert nexus2");

        // Attempt handoff with unregistered nexus - should fail
        let result = datastore.attempt_handoff(unregistered_nexus_id).await;
        assert!(result.is_err());
        let error_msg = format!("{}", result.unwrap_err());
        assert!(
            error_msg
                .contains("does not have a record in db_metadata_nexus table"),
            "Expected error about unregistered nexus, got: {}",
            error_msg
        );
        assert!(
            error_msg.contains(&unregistered_nexus_id.to_string()),
            "Expected error to contain nexus ID {}, got: {}",
            unregistered_nexus_id,
            error_msg
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attempt_handoff_nexus_wrong_state() {
        let logctx =
            dev::test_setup_log("test_attempt_handoff_nexus_wrong_state");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        // Set up test data: create nexus records where our target is in wrong state
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let quiesced_nexus_id = OmicronZoneUuid::new_v4();

        datastore
            .database_nexus_access_insert(
                nexus1_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus1");
        datastore
            .database_nexus_access_insert(
                nexus2_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus2");
        datastore
            .database_nexus_access_insert(
                quiesced_nexus_id,
                DbMetadataNexusState::Quiesced,
            )
            .await
            .expect("Failed to insert quiesced nexus");

        // Attempt handoff with quiesced nexus - should fail
        let result = datastore.attempt_handoff(quiesced_nexus_id).await;
        assert!(result.is_err());
        let error_msg = format!("{}", result.unwrap_err());
        assert!(
            error_msg.contains("is in state Quiesced"),
            "Expected error about wrong state, got: {}",
            error_msg
        );
        assert!(
            error_msg.contains("Must be in 'not_yet' state to become active"),
            "Expected error to mention required state, got: {}",
            error_msg
        );
        assert!(
            error_msg.contains(&quiesced_nexus_id.to_string()),
            "Expected error to contain nexus ID {}, got: {}",
            quiesced_nexus_id,
            error_msg
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attempt_handoff_success() {
        let logctx = dev::test_setup_log("test_attempt_handoff_success");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        // Set up test data: create multiple nexus records in not_yet and quiesced states
        let nexus1_id = OmicronZoneUuid::new_v4();
        let nexus2_id = OmicronZoneUuid::new_v4();
        let nexus3_id = OmicronZoneUuid::new_v4();

        datastore
            .database_nexus_access_insert(
                nexus1_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus1");
        datastore
            .database_nexus_access_insert(
                nexus2_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus2");
        datastore
            .database_nexus_access_insert(
                nexus3_id,
                DbMetadataNexusState::Quiesced,
            )
            .await
            .expect("Failed to insert nexus3");

        // Verify initial state: all not_yet or quiesced
        let nexus1_before = datastore
            .database_nexus_access(nexus1_id)
            .await
            .expect("Failed to get nexus1")
            .expect("nexus1 should exist");
        let nexus2_before = datastore
            .database_nexus_access(nexus2_id)
            .await
            .expect("Failed to get nexus2")
            .expect("nexus2 should exist");
        let nexus3_before = datastore
            .database_nexus_access(nexus3_id)
            .await
            .expect("Failed to get nexus3")
            .expect("nexus3 should exist");

        assert_eq!(nexus1_before.state(), DbMetadataNexusState::NotYet);
        assert_eq!(nexus2_before.state(), DbMetadataNexusState::NotYet);
        assert_eq!(nexus3_before.state(), DbMetadataNexusState::Quiesced);

        // Attempt handoff with nexus2 - should succeed
        let result = datastore.attempt_handoff(nexus2_id).await;
        if let Err(ref e) = result {
            panic!("Handoff should succeed but got error: {}", e);
        }
        assert!(result.is_ok());

        // Verify final state: all not_yet records should now be active, quiesced should remain quiesced
        let nexus1_after = datastore
            .database_nexus_access(nexus1_id)
            .await
            .expect("Failed to get nexus1")
            .expect("nexus1 should exist");
        let nexus2_after = datastore
            .database_nexus_access(nexus2_id)
            .await
            .expect("Failed to get nexus2")
            .expect("nexus2 should exist");
        let nexus3_after = datastore
            .database_nexus_access(nexus3_id)
            .await
            .expect("Failed to get nexus3")
            .expect("nexus3 should exist");

        assert_eq!(nexus1_after.state(), DbMetadataNexusState::Active);
        assert_eq!(nexus2_after.state(), DbMetadataNexusState::Active);
        assert_eq!(nexus3_after.state(), DbMetadataNexusState::Quiesced); // Should remain unchanged

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // This test covers two cases:
    //
    // 1. New systems: We use RSS to initialize Nexus, but no db_metadata_nexus entries
    //    exist.
    // 2. Deployed systems: We have a deployed system which updates to have this
    //    "db_metadata_nexus"-handling code, but has no rows in that table.
    //
    // Both of these cases must be granted database access to self-populate later.
    #[tokio::test]
    async fn test_check_schema_and_access_empty_table_permits_access() {
        let logctx = dev::test_setup_log(
            "test_check_schema_and_access_empty_table_permits_access",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let nexus_id = OmicronZoneUuid::new_v4();

        // With an empty table, even explicit nexus ID should get access
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::Ready);

        // Add a record to the table, now explicit nexus ID should NOT get access
        datastore
            .database_nexus_access_insert(
                OmicronZoneUuid::new_v4(), // Different nexus
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert nexus record");

        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::NeedsHandoff);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validates the case where a Nexus ID is explicitly requested or omitted.
    //
    // The omission case is important for the "schema-updater" binary to keep working.
    #[tokio::test]
    async fn test_check_schema_and_access_nexus_id() {
        let logctx =
            dev::test_setup_log("test_check_schema_and_access_nexus_id");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        // Add an active record, for some Nexus ID.
        datastore
            .database_nexus_access_insert(
                OmicronZoneUuid::new_v4(),
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert nexus record");

        // Using 'DontCare' as a nexus ID should get access (schema updater case)
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::DontCare,
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::Ready);

        // Explicit CheckAndTakeover with a Nexus ID that doesn't exist should
        // not get access
        let nexus_id = OmicronZoneUuid::new_v4();
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::NeedsHandoff);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validates that an explicit db_metadata_nexus record can lock-out Nexuses which should not be
    // able to access the database.
    #[tokio::test]
    async fn test_check_schema_and_access_lockout_refuses_access() {
        let logctx = dev::test_setup_log(
            "test_check_schema_and_access_lockout_refuses_access",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let nexus_id = OmicronZoneUuid::new_v4();

        // Insert our nexus as quiesced (locked out)
        datastore
            .database_nexus_access_insert(
                nexus_id,
                DbMetadataNexusState::Quiesced,
            )
            .await
            .expect("Failed to insert nexus record");

        // Should refuse access
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::Refuse);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validates that if a Nexus with an "old desired schema" boots, it cannot access the
    // database under any conditions.
    //
    // This is the case where the database has upgraded beyond what Nexus can understand.
    //
    // In practice, the db_metadata_nexus records should prevent this situation from occurring,
    // but it's still a useful property to reject old schemas while the "schema-updater" binary
    // exists.
    #[tokio::test]
    async fn test_check_schema_and_access_schema_too_new() {
        let logctx =
            dev::test_setup_log("test_check_schema_and_access_schema_too_new");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let nexus_id = OmicronZoneUuid::new_v4();

        // Insert our nexus as active
        datastore
            .database_nexus_access_insert(
                nexus_id,
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert nexus record");

        // Try to access with an older version than what's in the database
        let older_version = Version::new(SCHEMA_VERSION.major - 1, 0, 0);

        // Explicit Nexus ID: Rejected
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                older_version.clone(),
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::Refuse);

        // Implicit Access: Rejected
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::DontCare,
                older_version.clone(),
            )
            .await
            .expect("Failed to check schema and access");
        assert_eq!(action.action(), &SchemaAction::Refuse);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validates that the schema + access combinations identify we should wait for handoff
    // when we have a "NotYet" record that could become compatible with the database.
    #[tokio::test]
    async fn test_check_schema_and_access_wait_for_handoff() {
        let logctx = dev::test_setup_log(
            "test_check_schema_and_access_wait_for_handoff",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let nexus_id = OmicronZoneUuid::new_v4();

        // Insert our nexus as not_yet (doesn't have access yet)
        datastore
            .database_nexus_access_insert(
                nexus_id,
                DbMetadataNexusState::NotYet,
            )
            .await
            .expect("Failed to insert nexus record");

        // We should wait for handoff if the versions match, or if our desired
        // version is newer than what exists in the database.
        let current_version = SCHEMA_VERSION;
        let newer_version = Version::new(SCHEMA_VERSION.major + 1, 0, 0);
        let versions = [current_version, newer_version];

        for version in &versions {
            // Should wait for handoff when schema is up-to-date
            let action = datastore
                .check_schema_and_access(
                    IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                    version.clone(),
                )
                .await
                .expect("Failed to check schema and access");
            assert_eq!(action.action(), &SchemaAction::NeedsHandoff);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validates the "normal case", where a Nexus has access and the schema already matches.
    #[tokio::test]
    async fn test_check_schema_and_access_normal_use() {
        let logctx =
            dev::test_setup_log("test_check_schema_and_access_normal_use");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let nexus_id = OmicronZoneUuid::new_v4();

        // Insert our nexus as active
        datastore
            .database_nexus_access_insert(
                nexus_id,
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert nexus record");

        // With current schema version, should be ready for normal use
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                SCHEMA_VERSION,
            )
            .await
            .expect("Failed to check schema and access");

        assert_eq!(action.action(), &SchemaAction::Ready);
        assert_eq!(action.desired_version(), &SCHEMA_VERSION);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validates that when a Nexus is active with a newer-than-database desired
    // version, it will request an update
    #[tokio::test]
    async fn test_check_schema_and_access_update_now() {
        let logctx =
            dev::test_setup_log("test_check_schema_and_access_update_now");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let datastore =
            DataStore::new_unchecked(logctx.log.clone(), db.pool().clone());

        let nexus_id = OmicronZoneUuid::new_v4();

        // Insert our nexus as active
        datastore
            .database_nexus_access_insert(
                nexus_id,
                DbMetadataNexusState::Active,
            )
            .await
            .expect("Failed to insert nexus record");

        let newer_version = Version::new(SCHEMA_VERSION.major + 1, 0, 0);

        // With a newer desired version, should request update
        let action = datastore
            .check_schema_and_access(
                IdentityCheckPolicy::CheckAndTakeover { nexus_id },
                newer_version.clone(),
            )
            .await
            .expect("Failed to check schema and access");

        assert_eq!(action.action(), &SchemaAction::Update);
        assert_eq!(action.desired_version(), &newer_version);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
