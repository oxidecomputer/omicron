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
