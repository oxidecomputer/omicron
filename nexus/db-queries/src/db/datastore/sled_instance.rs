use super::DataStore;

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::lookup::SledInstance;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_model::Name;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::ListResultVec;
use ref_cast::RefCast;
use uuid::Uuid;

impl DataStore {
    pub async fn sled_instance_list(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SledInstance> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::sled_instance::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::sled_instance, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::sled_instance,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::active_sled_id.eq(sled_id))
        .select(SledInstance::as_select())
        .load_async::<SledInstance>(self.pool_authorized(opctx).await?)
        .await
        .map_error(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}
