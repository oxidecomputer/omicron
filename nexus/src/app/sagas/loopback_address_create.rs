// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NEXUS_DPD_TAG};
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::authn;
use crate::db::model::LoopbackAddress;
use crate::external_api::params;
use anyhow::Error;
use dpd_client::types::{Ipv4Entry, Ipv6Entry};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::sync::Arc;
use steno::ActionError;

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
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
pub struct SagaLoopbackAddressCreate;
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

    // If there is a failure down the road, this record will get cleaned up by
    // the unwind action slc_loopback_address_delete_record. In the case that
    // the saga is retried, we will just retry with a new id, returning that to
    // the caller if the saga retry is successful. Having intermediate ids here
    // is ok.
    let value = nexus
        .db_datastore
        .loopback_address_create(&opctx, &params.loopback_address, None)
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

    let selector = params::LoopbackAddressSelector {
        rack_id: params.loopback_address.rack_id,
        switch_location: params.loopback_address.switch_location.clone(),
        address: params.loopback_address.address.into(),
    };

    nexus.db_datastore.loopback_address_delete(&opctx, &selector).await?;

    Ok(())
}

async fn slc_loopback_address_create(
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

    //TODO how do we know this is the right dpd client? There will be at least
    //two and in multirack 2*N where N is the number of racks.
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let result = match &params.loopback_address.address {
        IpAddr::V4(a) => {
            dpd_client
                .loopback_ipv4_create(&Ipv4Entry {
                    addr: *a,
                    tag: NEXUS_DPD_TAG.into(),
                })
                .await
        }
        IpAddr::V6(a) => {
            dpd_client
                .loopback_ipv6_create(&Ipv6Entry {
                    addr: *a,
                    tag: NEXUS_DPD_TAG.into(),
                })
                .await
        }
    };

    if let Err(e) = result {
        match e {
            sled_agent_client::Error::ErrorResponse(ref er) => {
                match er.status() {
                    http::StatusCode::CONFLICT => Ok(()),
                    _ => Err(ActionError::action_failed(e.to_string())),
                }
            }
            _ => Err(ActionError::action_failed(e.to_string())),
        }?;
    }

    Ok(())
}
