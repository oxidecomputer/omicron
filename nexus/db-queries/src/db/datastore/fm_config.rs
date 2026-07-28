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
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::define_sql_function;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::SqlU32;
use nexus_db_schema::schema::fm_config::dsl;
use nexus_types::fm::FmConfigParam;
use nexus_types::fm::FmConfigView;
use omicron_common::api::external::Error;

define_sql_function! {
    fn coalesce(
        x: sql_types::Nullable<sql_types::BigInt>,
        y: sql_types::BigInt,
    ) -> sql_types::BigInt;
}

impl DataStore {
    /// Read the current FM configuration override, or `None` if no overrides
    /// exist in the database.
    pub async fn fm_config_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<FmConfigView>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FM_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let latest = dsl::fm_config
            .order_by(dsl::version.desc())
            .first_async::<db::model::fm::FmConfig>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        latest.map(TryInto::try_into).transpose()
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
        let config = db::model::fm::FmConfig::try_from(config)?;

        // The largest version currently in the table. `MAX` over an empty
        // table is NULL; coalescing it to 0 allows the first override to be
        // inserted at version 1.
        let current_version = coalesce(
            dsl::fm_config
                .select(diesel::dsl::max(dsl::version))
                .single_value(),
            0,
        );

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
        // `version - 1` cannot underflow: `FmConfigParam::version` is a
        // `NonZeroU32`.
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
    use nexus_types::fm::FmConfigSource;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn test_fm_config_basic() {
        let logctx = dev::test_setup_log("test_fm_config_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // With no override rows in the table, there is no config to read.
        let read = datastore.fm_config_get_latest(opctx).await.unwrap();
        assert_eq!(read, None);

        // Inserting version 2 should fail: no overrides exist yet, so the
        // first must be version 1.
        let mut config = FmConfigParam {
            version: NonZeroU32::new(2).unwrap(),
            sitrep_limit: 5,
            sitrep_deletion_threshold: 4,
        };
        assert!(
            datastore
                .fm_config_insert_latest_version(opctx, config)
                .await
                .unwrap_err()
                .to_string()
                .contains("version 2 is not the most recent")
        );

        // Inserting version 1 should work.
        config.version = NonZeroU32::new(1).unwrap();
        datastore
            .fm_config_insert_latest_version(opctx, config)
            .await
            .expect("inserting version 1 should succeed");

        // Getting the latest config should now return the version 1 override.
        let read = datastore
            .fm_config_get_latest(opctx)
            .await
            .unwrap()
            .expect("an override was inserted");
        let FmConfigSource::Override { version, .. } = read.source else {
            panic!("expected an override source, got {:?}", read.source);
        };
        assert_eq!(version.get(), 1);
        assert_eq!(read.config.sitrep_limit.get(), 5);
        assert_eq!(read.config.sitrep_deletion_threshold.get(), 4);

        // An invalid config is rejected with an invalid value error.
        // (Validation is tested exhaustively in `nexus-types`; this just
        // checks that invalid configs are rejected on the insert path.)
        config.version = NonZeroU32::new(2).unwrap();
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

        // Getting the latest config should return the version 2 override.
        let read = datastore
            .fm_config_get_latest(opctx)
            .await
            .unwrap()
            .expect("an override was inserted");
        let FmConfigSource::Override { version, .. } = read.source else {
            panic!("expected an override source, got {:?}", read.source);
        };
        assert_eq!(version.get(), 2);
        assert_eq!(read.config.sitrep_limit.get(), 500);
        assert_eq!(read.config.sitrep_deletion_threshold.get(), 400);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
