use super::DataStore;

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_model::ApplySledFilterExt;
use nexus_db_model::SledInstance;
use nexus_types::deployment::SledFilter;
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
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<(Sled, SledInstance)> {
        // TODO(eliza): should probably paginate this?
        use crate::db::schema::{sled::dsl as sled_dsl, sled_instance::dsl};
        opctx.authorize(authz::Action::Read, &authz::INVENTORY).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let result = paginated(sled_dsl::sled, sled_dsl::id, pagparams)
            .filter(sled_dsl::time_deleted.is_null())
            .sled_filter(SledFilter::InService)
            .inner_join(
                dsl::sled_instance.on(dsl::active_sled_id.eq(sled_dsl::id)),
            )
            .select((Sled::as_select(), SledInstance::as_select()))
            .load_async::<(Sled, SledInstance)>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(result)
    }
}
