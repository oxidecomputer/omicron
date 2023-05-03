use crate::authz;
use crate::db;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;

impl super::Nexus {
    // Switches

    pub async fn switch_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Switch> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.switch_list(&opctx, pagparams).await
    }

    pub fn switch_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        switch_selector: params::SwitchSelector,
    ) -> LookupResult<lookup::Switch<'a>> {
        Ok(LookupPath::new(opctx, &self.db_datastore)
            .switch_id(switch_selector.switch))
    }
}
