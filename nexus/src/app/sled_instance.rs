use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn sled_instance_list(
        &self,
        opctx: &OpContext,
        sled_lookup: &lookup::Sled<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SledInstance> {
        let (.., authz_sled) =
            sled_lookup.lookup_for(authz::Action::Read).await?;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore
            .sled_instance_list(opctx, &authz_sled, pagparams)
            .await
    }
}
