// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_model::{BootstoreConfig, BootstoreKeys};
use omicron_common::api::external::{CreateResult, LookupResult};
use sled_agent_types::rack_init::rack_init_bootstore_generation;

impl DataStore {
    /// Bump the bootstore generation for `key` conditionally on the current
    /// gen number being `expected`.
    ///
    /// Returns the new generation, or `None` if the generation wasn't
    /// `expected`. This is a "compare-and-swap" approach to help synchronize
    /// writes to sled agent's bootstore. This guarantees that only one caller
    /// writes for each gen number and will prevent dueling if the following is
    /// guaranteed:
    /// - Writers read the gen number before or in the same transaction that
    /// they use to read the config from the DB they wish to write.
    /// - Writers only write if they receive the new gen from calling this
    /// function.
    pub async fn try_bump_bootstore_generation(
        &self,
        opctx: &OpContext,
        key: String,
        expected: Option<i64>,
    ) -> LookupResult<Option<i64>> {
        use nexus_db_schema::schema::bootstore_keys;
        use nexus_db_schema::schema::bootstore_keys::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        match expected {
            Some(expected) => diesel::update(dsl::bootstore_keys)
                .filter(dsl::key.eq(key))
                .filter(dsl::generation.eq(expected)) // Where we actually check
                .set(dsl::generation.eq(dsl::generation + 1))
                .returning(dsl::generation)
                .get_result_async::<i64>(&*conn)
                .await
                .optional(),
            None => diesel::insert_into(dsl::bootstore_keys)
                .values(BootstoreKeys {
                    key,
                    generation: rack_init_bootstore_generation::NEXUS_INITIAL,
                })
                .on_conflict(bootstore_keys::key) // Doing nothing on conflict is what prevents this race
                .do_nothing()
                .returning(dsl::generation)
                .get_result_async::<i64>(&*conn)
                .await
                .optional(),
        }
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn ensure_bootstore_config(
        &self,
        opctx: &OpContext,
        config: BootstoreConfig,
    ) -> CreateResult<()> {
        use nexus_db_schema::schema::bootstore_config::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::insert_into(dsl::bootstore_config)
            .values(config)
            .on_conflict((dsl::key, dsl::generation))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn get_latest_bootstore_config(
        &self,
        opctx: &OpContext,
        key: String,
    ) -> LookupResult<Option<BootstoreConfig>> {
        use nexus_db_schema::schema::bootstore_config::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let result = dsl::bootstore_config
            .filter(dsl::key.eq(key))
            .select(BootstoreConfig::as_select())
            .order(dsl::generation.desc())
            .limit(1)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if let Some(nat_entry) = result.first() {
            Ok(Some(nat_entry.clone()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::NETWORK_KEY;
    use omicron_test_utils::dev;
    use rack_init_bootstore_generation::NEXUS_INITIAL;
    use sled_agent_types::rack_init::rack_init_bootstore_generation;

    #[tokio::test]
    async fn test_try_bump_bootstore_generation() {
        let logctx = dev::test_setup_log("test_try_bump_bootstore_generation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let bump = |expected| {
            datastore.try_bump_bootstore_generation(
                opctx,
                NETWORK_KEY.into(),
                expected,
            )
        };

        // Before the first bump, only a caller expecting `None` can bump.
        assert_eq!(bump(Some(NEXUS_INITIAL)).await.unwrap(), None);
        assert_eq!(bump(None).await.unwrap(), Some(NEXUS_INITIAL));

        // After that, only a caller expecting the current generation can bump.
        assert_eq!(bump(None).await.unwrap(), None);
        assert_eq!(bump(Some(NEXUS_INITIAL - 1)).await.unwrap(), None);
        assert_eq!(
            bump(Some(NEXUS_INITIAL)).await.unwrap(),
            Some(NEXUS_INITIAL + 1)
        );
        assert_eq!(bump(Some(NEXUS_INITIAL)).await.unwrap(), None);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
