use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Switch;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new switch in the database.
    pub async fn switch_upsert(&self, switch: Switch) -> CreateResult<Switch> {
        use db::schema::switch::dsl;

        let conn = self.pool_connection_unauthorized().await?;
        diesel::insert_into(dsl::switch)
            .values(switch.clone())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::rack_id.eq(switch.rack_id),
            ))
            .returning(Switch::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Switch,
                        &switch.id().to_string(),
                    ),
                )
            })
    }

    pub async fn switch_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Switch> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::switch::dsl;
        paginated(dsl::switch, dsl::id, pagparams)
            .select(Switch::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
