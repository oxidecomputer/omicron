use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use nexus_db_model::{BootstoreConfig, BootstoreKeys};
use omicron_common::api::external::{CreateResult, LookupResult};

impl DataStore {
    pub async fn bump_bootstore_generation(
        &self,
        opctx: &OpContext,
        key: String,
    ) -> LookupResult<i64> {
        use db::schema::bootstore_keys;
        use db::schema::bootstore_keys::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let bks = diesel::insert_into(dsl::bootstore_keys)
            .values(BootstoreKeys {
                key: key.clone(),
                generation: 2, // RSS starts with a generation of 1
            })
            .on_conflict(bootstore_keys::key)
            .do_update()
            .set(bootstore_keys::generation.eq(dsl::generation + 1))
            .returning(BootstoreKeys::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(bks.generation)
    }

    pub async fn ensure_bootstore_config(
        &self,
        opctx: &OpContext,
        config: BootstoreConfig,
    ) -> CreateResult<()> {
        use db::schema::bootstore_config::dsl;

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
        use db::schema::bootstore_config::dsl;

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
