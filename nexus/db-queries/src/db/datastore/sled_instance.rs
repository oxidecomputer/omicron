use super::DataStore;

use crate::authz;
use crate::context::OpContext;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::SledInstance;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use uuid::Uuid;

impl DataStore {
    pub async fn sled_instance_list(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SledInstance> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::sled_instance::dsl;
        paginated(dsl::sled_instance, dsl::id, &pagparams)
            .filter(dsl::active_sled_id.eq(to_db_typed_uuid(authz_sled.id())))
            .select(SledInstance::as_select())
            .load_async::<SledInstance>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
