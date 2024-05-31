// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Switch interfaces

use crate::app::address_lot::AddressLot;
use crate::app::background::BackgroundTasks;
use crate::app::rack::Rack;
use crate::external_api::params;
use db::model::{LoopbackAddress, Name};
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
};
use oxnet::IpNet;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on switch interfaces
#[derive(Clone)]
pub struct SwitchInterface {
    datastore: Arc<db::DataStore>,
    background_tasks: Arc<BackgroundTasks>,
    rack: Rack,
    address_lot: AddressLot,
}

impl SwitchInterface {
    pub fn new(
        datastore: Arc<db::DataStore>,
        background_tasks: Arc<BackgroundTasks>,
        rack: Rack,
        address_lot: AddressLot,
    ) -> SwitchInterface {
        SwitchInterface { datastore, background_tasks, rack, address_lot }
    }
    pub fn loopback_address_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        rack_id: Uuid,
        switch_location: Name,
        address: IpNet,
    ) -> LookupResult<lookup::LoopbackAddress<'a>> {
        Ok(LookupPath::new(opctx, &self.datastore).loopback_address(
            rack_id,
            switch_location,
            address.into(),
        ))
    }

    pub(crate) async fn loopback_address_create(
        &self,
        opctx: &OpContext,
        params: params::LoopbackAddressCreate,
    ) -> CreateResult<LoopbackAddress> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        validate_switch_location(params.switch_location.as_str())?;

        // Just a check to make sure a valid rack id was passed in.
        self.rack.rack_lookup(&opctx, &params.rack_id).await?;

        let address_lot_lookup = self
            .address_lot
            .address_lot_lookup(&opctx, params.address_lot.clone())?;

        let (.., authz_address_lot) =
            address_lot_lookup.lookup_for(authz::Action::CreateChild).await?;

        let value = self
            .datastore
            .loopback_address_create(&opctx, &params, None, &authz_address_lot)
            .await?;

        // eagerly propagate changes via rpw
        self.background_tasks
            .driver
            .activate(&self.background_tasks.task_switch_port_settings_manager);

        Ok(value)
    }

    pub(crate) async fn loopback_address_delete(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        address: IpNet,
    ) -> DeleteResult {
        let loopback_address_lookup = self.loopback_address_lookup(
            &opctx,
            rack_id,
            switch_location,
            address,
        )?;

        let (.., authz_loopback_address) =
            loopback_address_lookup.lookup_for(authz::Action::Delete).await?;

        self.datastore
            .loopback_address_delete(&opctx, &authz_loopback_address)
            .await?;

        // eagerly propagate changes via rpw
        self.background_tasks
            .driver
            .activate(&self.background_tasks.task_switch_port_settings_manager);

        Ok(())
    }

    pub(crate) async fn loopback_address_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<LoopbackAddress> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.datastore.loopback_address_list(opctx, pagparams).await
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
