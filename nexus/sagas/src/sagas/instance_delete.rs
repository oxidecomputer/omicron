use std::collections::HashSet;

use nexus_auth::authn;
use nexus_auth::authz;
use nexus_db_lookup::LookupPath;
use nexus_saga_interface::NexusAction;
use nexus_saga_interface::NexusActionContext;
use nexus_saga_interface::NexusSaga2;
use nexus_saga_interface::SagaInitError;
use nexus_saga_interface::declare_saga_actions;
use nexus_saga_interface::op_context_for_saga_action;
use omicron_common::api::internal::shared::SwitchLocation;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// instance delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_instance: authz::Instance,
    pub instance: nexus_db_model::Instance,
    pub boundary_switches: HashSet<SwitchLocation>,
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
    INSTANCE_DELETE_NAT -> "no_result4" {
        + sid_delete_nat
    }
}

// instance delete saga: definition

#[derive(Debug)]
pub struct SagaInstanceDelete;
impl NexusSaga2 for SagaInstanceDelete {
    const NAME: &'static str = "instance-delete";
    type Params = Params;

    fn actions() -> Vec<NexusAction> {
        instance_delete_list_actions()
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(instance_delete_nat_action());
        builder.append(instance_delete_record_action());
        builder.append(delete_network_interfaces_action());
        builder.append(deallocate_external_ip_action());
        Ok(builder.build()?)
    }
}

// instance delete saga: action implementations

async fn sid_delete_instance_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = op_context_for_saga_action(&sagactx, &params.serialized_authn);
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
    let nexus = osagactx.nexus();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = op_context_for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .instance_delete_all_network_interfaces(&opctx, &params.authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    let background_tasks = nexus.background_tasks();
    background_tasks.activate(&background_tasks.task_v2p_manager);
    Ok(())
}

async fn sid_delete_nat(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let instance_id = params.authz_instance.id();
    let osagactx = sagactx.user_data();
    let opctx = op_context_for_saga_action(&sagactx, &params.serialized_authn);

    let (.., authz_instance) = LookupPath::new(&opctx, osagactx.datastore())
        .instance_id(instance_id)
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sid_deallocate_external_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = op_context_for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .deallocate_external_ip_by_instance_id(
            &opctx,
            params.authz_instance.id(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    osagactx
        .datastore()
        .detach_floating_ips_by_instance_id(&opctx, params.authz_instance.id())
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
