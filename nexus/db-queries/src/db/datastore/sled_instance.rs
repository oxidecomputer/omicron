use super::DataStore;

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::InvSledAgent;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_model::DbTypedUuid;
use nexus_db_model::SledInstance;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledKind;
use uuid::Uuid;

impl DataStore {
    pub async fn sled_instance_list(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SledInstance> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::sled_instance::dsl;
        paginated(dsl::sled_instance, dsl::id, &pagparams)
            .filter(dsl::active_sled_id.eq(authz_sled.id()))
            .select(SledInstance::as_select())
            .load_async::<SledInstance>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn sled_instance_list_by_sled_agent(
        &self,
        opctx: &OpContext,
        collection: CollectionUuid,
        pagparams: &DataPageParams<'_, DbTypedUuid<SledKind>>,
    ) -> ListResultVec<(InvSledAgent, SledInstance)> {
        // TODO(eliza): should probably paginate this?
        use crate::db::schema::{inv_sled_agent, sled_instance::dsl};
        opctx.authorize(authz::Action::Read, &authz::INVENTORY).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let result = paginated(
            inv_sled_agent::dsl::inv_sled_agent,
            inv_sled_agent::dsl::sled_id,
            pagparams,
        )
        // Only list sled agents from the latest collection.
        .filter(
            inv_sled_agent::dsl::inv_collection_id
                .eq(collection.into_untyped_uuid()),
        )
        .inner_join(
            dsl::sled_instance
                .on(dsl::active_sled_id.eq(inv_sled_agent::dsl::sled_id)),
        )
        .select((InvSledAgent::as_select(), SledInstance::as_select()))
        .load_async::<(InvSledAgent, SledInstance)>(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(result)
    }
}
