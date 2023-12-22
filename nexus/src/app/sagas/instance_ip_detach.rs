// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::instance_common::{
    instance_ip_add_nat, instance_ip_add_opte, instance_ip_get_instance_state,
    instance_ip_move_state, instance_ip_remove_nat, instance_ip_remove_opte,
    InstanceStateForIp,
};
use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use futures::TryFutureExt;
use nexus_db_model::{ExternalIp, IpAttachState, IpKind};
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::views;
use omicron_common::api::external::{Error, InstanceState};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// rough sequence of evts:
// - take temp ownership of instance while interacting w/ sled agent
//  -> mark instance migration id as Some(0) if None
// - Withdraw routes
//  -> ensure_dpd... (?) Do we actually need to?
//  -> must precede OPTE: host may change its sending
//     behaviour prematurely
// - Deregister addr in OPTE
//  -> Put addr in sled-agent endpoint
// - Detach and Delete EIP iff. Ephemeral
//   -> why so late? Risk that we can't recover our IP in an unwind.
// - free up migration_id of instance.
//  -> mark instance migration id as None

declare_saga_actions! {
    instance_ip_detach;
    DETACH_EXTERNAL_IP -> "target_ip" {
        + siid_begin_detach_ip
        - siid_begin_detach_ip_undo
    }

    INSTANCE_STATE -> "instance_state" {
        + siid_get_instance_state
    }

    REMOVE_NAT -> "no_result0" {
        + siid_nat
        - siid_nat_undo
    }

    REMOVE_OPTE_PORT -> "no_result1" {
        + siid_update_opte
        - siid_update_opte_undo
    }

    COMPLETE_ATTACH -> "output" {
        + siid_complete_attach
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub delete_params: params::ExternalIpDelete,
    pub authz_instance: authz::Instance,
    pub project_id: Uuid,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn siid_begin_detach_ip(
    sagactx: NexusActionContext,
) -> Result<ExternalIp, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    match params.delete_params {
        params::ExternalIpDelete::Ephemeral => {
            let eips = datastore
                .instance_lookup_external_ips(
                    &opctx,
                    params.authz_instance.id(),
                )
                .await
                .map_err(ActionError::action_failed)?;

            let eph_ip = eips
                .iter()
                .find(|e| e.kind == IpKind::Ephemeral)
                .ok_or_else(|| {
                    ActionError::action_failed(Error::invalid_request(
                    "instance does not have an attached ephemeral IP address"
                ))
                })?;

            datastore
                .begin_deallocate_ephemeral_ip(
                    &opctx,
                    eph_ip.id,
                    params.authz_instance.id(),
                )
                .await
                .map_err(ActionError::action_failed)
        }
        params::ExternalIpDelete::Floating { ref floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.project_id)
                .floating_ip_name(&floating_ip_name)
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            datastore
                .floating_ip_begin_detach(
                    &opctx,
                    &authz_fip,
                    params.authz_instance.id(),
                    false,
                )
                .await
                .map_err(ActionError::action_failed)
        }
    }
}

async fn siid_begin_detach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    warn!(log, "siid_begin_detach_ip_undo: Reverting attached->detaching");
    let params = sagactx.saga_params::<Params>()?;
    if !instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Detaching,
        IpAttachState::Attached,
    )
    .await?
    {
        error!(log, "siid_begin_detach_ip_undo: external IP was deleted")
    }

    Ok(())
}

async fn siid_get_instance_state(
    sagactx: NexusActionContext,
) -> Result<InstanceStateForIp, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_get_instance_state(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
        "detach",
    )
    .await
}

async fn siid_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_remove_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
    )
    .await
}

async fn siid_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_add_nat(
        &sagactx,
        &params.serialized_authn,
        &params.authz_instance,
    )
    .await?;

    Ok(())
}

async fn siid_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_remove_opte(&sagactx, &params.authz_instance).await
}

async fn siid_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let params = sagactx.saga_params::<Params>()?;
    instance_ip_add_opte(&sagactx, &params.authz_instance).await?;
    Ok(())
}

async fn siid_complete_attach(
    sagactx: NexusActionContext,
) -> Result<views::ExternalIp, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let initial_state =
        sagactx.lookup::<InstanceStateForIp>("instance_state")?.state;
    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    let update_occurred = instance_ip_move_state(
        &sagactx,
        &params.serialized_authn,
        IpAttachState::Detaching,
        IpAttachState::Detached,
    )
    .await?;

    // TODO: explain why it is safe to not back out on state change.
    match (update_occurred, initial_state) {
        // Allow failure here on stopped because the instance_delete saga
        // may have been concurrently fired off and removed the row.
        (false, InstanceState::Stopped) | (true, _) => {
            target_ip.try_into().map_err(ActionError::action_failed)
        }
        _ => Err(Error::internal_error("failed to complete IP detach"))
            .map_err(ActionError::action_failed),
    }
}

#[derive(Debug)]
pub struct SagaInstanceIpDetach;
impl NexusSaga for SagaInstanceIpDetach {
    const NAME: &'static str = "external-ip-detach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_ip_detach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(detach_external_ip_action());
        builder.append(instance_state_action());
        builder.append(remove_nat_action());
        builder.append(remove_opte_port_action());
        builder.append(complete_attach_action());
        Ok(builder.build()?)
    }
}

#[cfg(test)]
pub(crate) mod test {

    use nexus_test_utils_macros::nexus_test;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        _cptestctx: &ControlPlaneTestContext,
    ) {
        todo!()
    }
}
