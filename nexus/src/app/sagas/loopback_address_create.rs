// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NEXUS_DPD_TAG};
use crate::app::sagas::retry_until_known_result;
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::external_api::params;
use anyhow::Error;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::db::model::LoopbackAddress;
use omicron_common::api::internal::shared::SwitchLocation;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use steno::ActionError;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub loopback_address: params::LoopbackAddressCreate,
}

declare_saga_actions! {
    loopback_address_create;
    CREATE_LOOPBACK_ADDRESS_RECORD -> "created_loopback_address_record" {
        + slc_loopback_address_create_record
        - slc_loopback_address_delete_record
    }
    CREATE_LOOPBACK_ADDRESS -> "create_loopback_address" {
        + slc_loopback_address_create
    }
}

#[derive(Debug)]
pub(crate) struct SagaLoopbackAddressCreate;
impl NexusSaga for SagaLoopbackAddressCreate {
    const NAME: &'static str = "loopback-address-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        loopback_address_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(create_loopback_address_record_action());
        builder.append(create_loopback_address_action());

        Ok(builder.build()?)
    }
}

async fn slc_loopback_address_create_record(
    sagactx: NexusActionContext,
) -> Result<LoopbackAddress, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let address_lot_lookup = nexus
        .address_lot_lookup(&opctx, params.loopback_address.address_lot.clone())
        .map_err(ActionError::action_failed)?;
    let (.., authz_address_lot) = address_lot_lookup
        .lookup_for(authz::Action::CreateChild)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    // Just a check to make sure a valid rack id was passed in.
    nexus
        .rack_lookup(&opctx, &params.loopback_address.rack_id)
        .await
        .map_err(ActionError::action_failed)?;

    // If there is a failure down the road, this record will get cleaned up by
    // the unwind action slc_loopback_address_delete_record. In the case that
    // the saga is retried, we will just retry with a new id, returning that to
    // the caller if the saga retry is successful. Having intermediate ids here
    // is ok.
    let value = nexus
        .db_datastore
        .loopback_address_create(
            &opctx,
            &params.loopback_address,
            None,
            &authz_address_lot,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(value)
}

async fn slc_loopback_address_delete_record(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let loopback_address_lookup = nexus.loopback_address_lookup(
        &opctx,
        params.loopback_address.rack_id,
        params.loopback_address.switch_location.clone().into(),
        params.loopback_address.address.into(),
    )?;

    let (.., authz_loopback_address) =
        loopback_address_lookup.lookup_for(authz::Action::Delete).await?;

    nexus
        .db_datastore
        .loopback_address_delete(&opctx, &authz_loopback_address)
        .await?;

    Ok(())
}

async fn slc_loopback_address_create(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let log = sagactx.user_data().log();

    let dpd_client: Arc<dpd_client::Client> =
        select_dendrite_client(&sagactx).await?;

    retry_until_known_result(log, || async {
        dpd_client
            .ensure_loopback_created(
                log,
                params.loopback_address.address,
                NEXUS_DPD_TAG,
            )
            .await
    })
    .await
    .map_err(|e| ActionError::action_failed(e.to_string()))
}

pub(crate) async fn select_dendrite_client(
    sagactx: &NexusActionContext,
) -> Result<Arc<dpd_client::Client>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let switch_location: SwitchLocation = params
        .loopback_address
        .switch_location
        .as_str()
        .parse()
        .map_err(ActionError::action_failed)?;
    let dpd_client: Arc<dpd_client::Client> = osagactx
        .nexus()
        .dpd_clients
        .get(&switch_location)
        .ok_or_else(|| {
            ActionError::action_failed(format!(
                "requested switch not available: {switch_location}"
            ))
        })?
        .clone();
    Ok(dpd_client)
}
