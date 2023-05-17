use crate::authz;
use crate::db;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use uuid::Uuid;

impl super::Nexus {
    pub async fn sled_instance_list(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SledInstance> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.sled_instance_list(opctx, sled_id, pagparams).await
    }
}
