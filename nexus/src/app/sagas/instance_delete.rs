// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use crate::context::OpContext;
use crate::db;
use crate::{authn, authz};
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// instance delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
}

// instance delete saga: actions

declare_saga_actions! {
    instance_delete;
    INSTANCE_DELETE_RECORD -> "no_result1" {
        + sid_delete_instance_record
    }
    DELETE_NETWORK_INTERFACES -> "no_result2" {
        + sid_delete_network_interfaces
    }
    DEALLOCATE_EXTERNAL_IP -> "no_result3" {
        + sid_deallocate_external_ip
    }
    RESOURCES_ACCOUNT -> "no_reult4" {
        + sid_account_resources
    }
}

// instance delete saga: definition

#[derive(Debug)]
pub struct SagaInstanceDelete;
impl NexusSaga for SagaInstanceDelete {
    const NAME: &'static str = "instance-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(instance_delete_record_action());
        builder.append(delete_network_interfaces_action());
        builder.append(deallocate_external_ip_action());
        builder.append(resources_account_action());
        Ok(builder.build()?)
    }
}

// instance delete saga: action implementations

async fn sid_delete_instance_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .project_delete_instance(&opctx, &params.authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_delete_network_interfaces(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .instance_delete_all_network_interfaces(&opctx, &params.authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_deallocate_external_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .deallocate_external_ip_by_instance_id(
            &opctx,
            params.authz_instance.id(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sid_account_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);

    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            params.instance.id(),
            params.instance.project_id,
            -i64::from(params.instance.runtime_state.ncpus.0 .0),
            -i64::try_from(params.instance.runtime_state.memory.to_bytes())
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "updating resource provisioning: {e}"
                    ))
                })
                .map_err(ActionError::action_failed)?,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
