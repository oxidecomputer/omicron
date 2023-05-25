// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::authn;
use crate::authz;
use crate::db::model::{LoopbackAddress, Name};
use crate::external_api::params;
use anyhow::{anyhow, Error};
use nexus_types::identity::Asset;
use omicron_common::api::external::{IpNet, NameOrId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub rack_id: Uuid,
    pub switch_location: Name,
    pub address: IpNet,
}

declare_saga_actions! {
    loopback_address_delete;
    DELETE_LOOPBACK_ADDRESS_RECORD -> "deleted_loopback_address_record" {
        + slc_loopback_address_delete_record
        - slc_loopback_address_undelete_record
    }
    DELETE_LOOPBACK_ADDRESS -> "delete_loopback_address" {
        + slc_loopback_address_delete
    }
}

#[derive(Debug)]
pub struct SagaLoopbackAddressDelete;
impl NexusSaga for SagaLoopbackAddressDelete {
    const NAME: &'static str = "loopback-address-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        loopback_address_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(delete_loopback_address_record_action());
        builder.append(delete_loopback_address_action());

        Ok(builder.build()?)
    }
}

async fn slc_loopback_address_delete_record(
    sagactx: NexusActionContext,
) -> Result<LoopbackAddress, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let loopback_address_lookup = nexus
        .loopback_address_lookup(
            &opctx,
            params.rack_id,
            params.switch_location,
            params.address,
        )
        .map_err(ActionError::action_failed)?;

    let (.., authz_loopback_address) = loopback_address_lookup
        .lookup_for(authz::Action::Delete)
        .await
        .map_err(ActionError::action_failed)?;

    let value = nexus
        .db_datastore
        .loopback_address_get(&opctx, &authz_loopback_address)
        .await
        .map_err(ActionError::action_failed)?;

    nexus
        .db_datastore
        .loopback_address_delete(&opctx, &authz_loopback_address)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(value)
}

async fn slc_loopback_address_undelete_record(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let value =
        sagactx.lookup::<LoopbackAddress>("deleted_loopback_address_record")?;

    let address_lot_id = nexus
        .db_datastore
        .address_lot_id_for_block_id(&opctx, value.address_lot_block_id)
        .await?;

    let arg = params::LoopbackAddressCreate {
        address_lot: NameOrId::Id(address_lot_id),
        rack_id: value.rack_id,
        switch_location: value
            .switch_location
            .parse()
            .map_err(|e| anyhow!("bad switch location name: {}", e))?,
        address: value.address.ip(),
        mask: value.address.prefix(),
    };

    let address_lot_lookup = nexus
        .address_lot_lookup(&opctx, arg.address_lot.clone())
        .map_err(ActionError::action_failed)?;
    let (.., authz_address_lot) = address_lot_lookup
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    // Just a check to make sure a valid rack id was passed in.
    nexus
        .rack_lookup(&opctx, &arg.rack_id)
        .await
        .map_err(ActionError::action_failed)?;

    nexus
        .db_datastore
        .loopback_address_create(
            &opctx,
            &arg,
            Some(value.id()),
            &authz_address_lot,
        )
        .await?;

    Ok(())
}

async fn slc_loopback_address_delete(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // TODO: https://github.com/oxidecomputer/omicron/issues/2629
    if let Ok(_) = std::env::var("SKIP_ASIC_CONFIG") {
        let log = sagactx.user_data().log();
        debug!(log, "SKIP_ASIC_CONFIG is set, disabling calls to dendrite");
        return Ok(());
    };

    // TODO https://github.com/oxidecomputer/omicron/issues/2760
    // how do we know this is the right dpd client? There will be at least
    // two and in multirack 2*N where N is the number of racks.
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    match &params.address {
        IpNet::V4(a) => dpd_client.loopback_ipv4_delete(&a.ip()).await,
        IpNet::V6(a) => dpd_client.loopback_ipv6_delete(&a.ip()).await,
    }
    .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(())
}
