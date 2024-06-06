use nexus_db_model::Switch as ModelSwitch;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::params;
use nexus_types::internal_api::params::SwitchPutRequest;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on switches
pub struct Switch {
    datastore: Arc<db::DataStore>,
}

impl Switch {
    pub fn new(datastore: Arc<db::DataStore>) -> Switch {
        Switch { datastore }
    }

    // Switches
    pub fn lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        switch_selector: params::SwitchSelector,
    ) -> LookupResult<lookup::Switch<'a>> {
        Ok(LookupPath::new(opctx, &self.datastore)
            .switch_id(switch_selector.switch))
    }

    /// Upserts a switch into the database, updated it if it already exists.
    /// Should only be called by the internal API
    pub(crate) async fn upsert(
        &self,
        id: Uuid,
        request: SwitchPutRequest,
    ) -> Result<ModelSwitch, Error> {
        let switch = db::model::Switch::new(
            id,
            request.baseboard.serial,
            request.baseboard.part,
            request.baseboard.revision,
            request.rack_id,
        );
        self.datastore.switch_upsert(switch).await
    }

    pub(crate) async fn list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Switch> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.datastore.switch_list(&opctx, pagparams).await
    }
}
