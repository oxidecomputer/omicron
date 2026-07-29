// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries related to fault management runtime configuration.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::QueryDsl;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::SqlU32;
use nexus_db_schema::schema::fm_config::dsl;
use nexus_types::fm::FmConfigParam;
use nexus_types::fm::FmConfigView;
use omicron_common::api::external::Error;

impl DataStore {
    /// Read the current FM configuration (the one with the highest version
    /// number).
    ///
    /// Unlike most "latest version" queries, this does not return an
    /// `Option`: the `fm_config` table is seeded with a default configuration
    /// at version 1 when the database is populated, so a missing row
    /// indicates a broken database and is reported as an internal error.
    pub async fn fm_config_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<FmConfigView, Error> {
        opctx.authorize(authz::Action::Read, &authz::FM_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let latest = dsl::fm_config
            .order_by(dsl::version.desc())
            .first_async::<db::model::fm::FmConfig>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        latest.try_into()
    }

    /// Insert a new version of the FM configuration in the database.
    ///
    /// This only succeeds if the provided version is exactly one greater than
    /// the latest version currently in the `fm_config` table.
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
        let config = db::model::fm::FmConfig::try_from(config)?;

        // The largest version currently in the table. `MAX` over an empty
        // table is NULL, in which case the comparison below is also NULL and
        // no row is inserted; that's the desired behavior, since an empty
        // `fm_config` table means the database seed data is missing.
        let current_version = dsl::fm_config
            .select(diesel::dsl::max(dsl::version))
            .single_value();

        // This exhaustive destructuring exists to trigger compilation
        // errors when the database model changes, so that people are
        // prompted to update the query below accordingly.
        let db::model::fm::FmConfig {
            version,
            sitrep_limit,
            sitrep_deletion_threshold,
            time_modified,
        } = config;

        // SELECT exactly the values we're trying to INSERT, but only if the new
        // version is exactly one greater than the current latest version.
        // Unfortunately, we cannot use the generated `Insertable`
        // implementation for `db::model::FmConfig` in an INSERT-from-SELECT
        // query, so we must manually construct the select clause here *and* the
        // list of columns in the `into_columns` below. If the exhaustive
        // destructuring of the `db::model::FmConfig` struct changes, both parts
        // of the query must be updated accordingly!
        let new_row = diesel::dsl::select((
            version.into_sql::<sql_types::BigInt>(),
            sitrep_limit.into_sql::<sql_types::BigInt>(),
            sitrep_deletion_threshold.into_sql::<sql_types::BigInt>(),
            time_modified.into_sql::<sql_types::Timestamptz>(),
        ))
        // `version - 1` cannot underflow, as the `TryFrom` conversion above
        // rejects a zero version
        .filter(current_version.eq(SqlU32::new(u32::from(version) - 1)));

        let num_inserted = diesel::insert_into(dsl::fm_config)
            .values(new_row)
            .into_columns((
                dsl::version,
                dsl::sitrep_limit,
                dsl::sitrep_deletion_threshold,
                dsl::time_modified,
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match num_inserted {
            0 => Err(Error::invalid_request(format!(
                "version {version} is not the most recent",
            ))),
            1 => Ok(()),
            // This is impossible because we are explicitly inserting only one
            // row with a unique primary key.
            _ => unreachable!("query inserted more than one row"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn test_fm_config_basic() {
        let logctx = dev::test_setup_log("test_fm_config_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // The table is seeded with a default config at version 1 by
        // dbinit.sql.
        let seeded = datastore.fm_config_get_latest(opctx).await.unwrap();
        assert_eq!(seeded.config.version, NonZeroU32::new(1).unwrap());

        // Inserting version 0 should fail: config versions must be nonzero.
        let mut config = FmConfigParam {
            version: 0,
            sitrep_limit: 5,
            sitrep_deletion_threshold: 4,
        };
        assert!(
            datastore
                .fm_config_insert_latest_version(opctx, config)
                .await
                .unwrap_err()
                .to_string()
                .contains("must be greater than 0")
        );

        // Inserting version 1 should fail: the seeded version 1 is already
        // the latest.
        config.version = 1;
        assert!(
            datastore
                .fm_config_insert_latest_version(opctx, config)
                .await
                .unwrap_err()
                .to_string()
                .contains("version 1 is not the most recent")
        );

        // Inserting version 3 should fail, since the latest version is 1.
        config.version = 3;
        assert!(
            datastore
                .fm_config_insert_latest_version(opctx, config)
                .await
                .unwrap_err()
                .to_string()
                .contains("version 3 is not the most recent")
        );

        // An invalid config is rejected with an invalid value error.
        // (Validation is tested exhaustively in `nexus-types`; this just
        // checks that invalid configs are rejected on the insert path.)
        config.version = 2;
        config.sitrep_limit = 100;
        config.sitrep_deletion_threshold = 100;
        assert!(
            datastore
                .fm_config_insert_latest_version(opctx, config)
                .await
                .unwrap_err()
                .to_string()
                .contains("must be less than the sitrep limit")
        );

        // Inserting version 2 with a valid config should work.
        config.sitrep_limit = 500;
        config.sitrep_deletion_threshold = 400;
        datastore
            .fm_config_insert_latest_version(opctx, config)
            .await
            .expect("inserting version 2 should succeed");

        // Getting the latest version should return version 2.
        let read = datastore.fm_config_get_latest(opctx).await.unwrap();
        assert_eq!(read.config.version.get(), 2);
        assert_eq!(read.config.sitrep_limit.get(), 500);
        assert_eq!(read.config.sitrep_deletion_threshold.get(), 400);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
