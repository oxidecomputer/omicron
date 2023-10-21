use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use nexus_db_model::BootstoreKeys;
use omicron_common::api::external::LookupResult;

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
}
