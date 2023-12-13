// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::sagas;
use crate::external_api::params;
use db::model::{LoopbackAddress, Name};
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, Error, InternalContext, IpNet,
    ListResultVec,
};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub fn loopback_address_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        rack_id: Uuid,
        switch_location: Name,
        address: IpNet,
    ) -> LookupResult<lookup::LoopbackAddress<'a>> {
        Ok(LookupPath::new(opctx, &self.db_datastore).loopback_address(
            rack_id,
            switch_location,
            address.into(),
        ))
    }

    pub(crate) async fn loopback_address_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::LoopbackAddressCreate,
    ) -> CreateResult<LoopbackAddress> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        validate_switch_location(params.switch_location.as_str())?;

        let saga_params = sagas::loopback_address_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            loopback_address: params.clone(),
        };

        let saga_output = self.execute_saga::<
            sagas::loopback_address_create::SagaLoopbackAddressCreate>(
                saga_params).await?;

        let value = saga_output
            .lookup_node_output::<LoopbackAddress>(
                "created_loopback_address_record",
            )
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from loopback create saga")?;

        Ok(value)
    }

    pub(crate) async fn loopback_address_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        address: IpNet,
    ) -> DeleteResult {
        let saga_params = sagas::loopback_address_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            address,
            rack_id,
            switch_location,
        };

        self.execute_saga::<
            sagas::loopback_address_delete::SagaLoopbackAddressDelete>(
                saga_params).await?;

        Ok(())
    }

    pub(crate) async fn loopback_address_list(
        self: &Arc<Self>,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<LoopbackAddress> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.loopback_address_list(opctx, pagparams).await
    }
}

pub fn validate_switch_location(switch_location: &str) -> Result<(), Error> {
    if switch_location != "switch0" && switch_location != "switch1" {
        return Err(Error::invalid_request(
            "Switch location must be switch0 or switch1",
        ));
    }
    Ok(())
}
