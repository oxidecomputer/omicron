// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::Switch;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_types::external_api::sled;
use nexus_types::internal_api::params::SwitchPutRequest;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use uuid::Uuid;

impl super::Nexus {
    // Switches
    pub fn switch_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        switch_selector: sled::SwitchSelector,
    ) -> LookupResult<lookup::Switch<'a>> {
        Ok(LookupPath::new(opctx, &self.db_datastore)
            .switch_id(switch_selector.switch))
    }

    /// Upserts a switch into the database, updated it if it already exists.
    /// Should only be called by the internal API
    pub(crate) async fn switch_upsert(
        &self,
        id: Uuid,
        request: SwitchPutRequest,
    ) -> Result<Switch, Error> {
        let switch = db::model::Switch::new(
            id,
            request.baseboard.serial,
            request.baseboard.part,
            request.baseboard.revision,
            RackUuid::from_untyped_uuid(request.rack_id),
        );
        self.db_datastore.switch_upsert(switch).await
    }

    pub(crate) async fn switch_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Switch> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.switch_list(&opctx, pagparams).await
    }
}
