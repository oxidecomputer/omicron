// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use futures::TryFutureExt;
use nexus_db_model::{ExternalIp, IpKind};
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
    LOCK_MIGRATION -> "sled_id" {
        + siid_migration_lock
        - siid_migration_lock_undo
    }

    DETACH_EXTERNAL_IP -> "target_ip" {
        + siid_begin_detach_ip
        - siid_begin_detach_ip_undo
    }

    REMOVE_NAT -> "no_result0" {
        + siid_nat
        - siid_nat_undo
    }

    REMOVE_OPTE_PORT -> "no_result1" {
        + siid_update_opte
        - siid_update_opte_undo
    }

    UNLOCK_MIGRATION -> "output" {
        + siid_migration_unlock
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub delete_params: params::ExternalIpDelete,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn siid_migration_lock(
    sagactx: NexusActionContext,
) -> Result<Option<Uuid>, ActionError> {
    // TODO: do this.
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let inst_and_vmm = datastore
        .instance_fetch_with_vmm(&opctx, &params.authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    if inst_and_vmm.instance().runtime_state.migration_id.is_some() {
        return Err(ActionError::action_failed(Error::ServiceUnavailable {
            internal_message: "target instance is migrating".into(),
        }));
    }

    let valid_instance_states = [
        InstanceState::Running,
        InstanceState::Stopped,
        // InstanceState::Rebooting is safe in principle, but likely
        // to trip up when backing out iff. state change.
    ];

    let state = inst_and_vmm.instance().runtime_state.nexus_state.0;
    if !valid_instance_states.contains(&state) {
        return Err(ActionError::action_failed(Error::ServiceUnavailable {
            internal_message: "instance must be 'Running' or 'Stopped'".into(),
        }));
    }

    // TODO: actually lock?
    // TODO: fail out in a user-friendly way if migrating?

    Ok(inst_and_vmm.sled_id())
}

async fn siid_migration_lock_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO: do this iff. we implement migration lock.
    Ok(())
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
                .instance_lookup_external_ips(&opctx, params.instance.id())
                .await
                .map_err(ActionError::action_failed)?;

            let eph_ip = eips.iter().find(|e| e.kind == IpKind::Ephemeral)
                .ok_or_else(|| ActionError::action_failed(Error::invalid_request("instance does not have an attached ephemeral IP address")))?;

            datastore
                .begin_deallocate_ephemeral_ip(
                    &opctx,
                    eph_ip.id,
                    params.instance.id(),
                )
                .await
                .map_err(ActionError::action_failed)
        }
        params::ExternalIpDelete::Floating { ref floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.instance.project_id)
                .floating_ip_name(&floating_ip_name)
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            datastore
                .floating_ip_begin_detach(
                    &opctx,
                    &authz_fip,
                    params.instance.id(),
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
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    let n_rows = datastore
        .external_ip_complete_op(
            &opctx,
            target_ip.id,
            target_ip.kind,
            nexus_db_model::IpAttachState::Detaching,
            nexus_db_model::IpAttachState::Attached,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siid_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // No physical sled? Don't push NAT.
    if sagactx.lookup::<Option<Uuid>>("sled_id")?.is_none() {
        return Ok(());
    }

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    // Currently getting an unfortunate error from here since 'detach'
    // comes so late.
    // Possible soln: use states, capture logic in 'begin_detach/attach'
    // and call early?

    osagactx
        .nexus()
        .instance_delete_dpd_config(
            &opctx,
            &params.authz_instance,
            Some(target_ip.id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siid_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // NOTE: mostly copied from instance_start.

    // If we didn't push NAT before, don't undo it.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            params.instance.id(),
            &sled.address(),
            Some(target_ip.id),
        )
        .await?;

    Ok(())
}

async fn siid_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // No physical sled? Don't inform OPTE.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;
    let sled_agent_body =
        target_ip.try_into().map_err(ActionError::action_failed)?;

    // TODO: disambiguate the various sled agent errors etc.
    osagactx
        .nexus()
        .sled_client(&sled_uuid)
        .await
        .map_err(ActionError::action_failed)?
        .instance_delete_external_ip(&params.instance.id(), &sled_agent_body)
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::invalid_request("hmm"))
        })?;

    Ok(())
}

async fn siid_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // If we didn't push OPTE before, don't undo it.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;
    let sled_agent_body =
        target_ip.try_into().map_err(ActionError::action_failed)?;

    // TODO: disambiguate the various sled agent errors etc.
    osagactx
        .nexus()
        .sled_client(&sled_uuid)
        .await
        .map_err(ActionError::action_failed)?
        .instance_put_external_ip(&params.instance.id(), &sled_agent_body)
        .await?;

    Ok(())
}

async fn siid_migration_unlock(
    sagactx: NexusActionContext,
) -> Result<views::ExternalIp, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    // TODO: do this iff. we implement migration lock.
    // TODO: Backtrack if there's an unexpected change to runstate?

    let target_ip = sagactx.lookup::<ExternalIp>("target_ip")?;

    let n_rows = datastore
        .external_ip_complete_op(
            &opctx,
            target_ip.id,
            target_ip.kind,
            nexus_db_model::IpAttachState::Detaching,
            nexus_db_model::IpAttachState::Detached,
        )
        .await
        .map_err(ActionError::action_failed)?;

    target_ip.try_into().map_err(ActionError::action_failed)
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
        builder.append(lock_migration_action());
        builder.append(detach_external_ip_action());
        builder.append(remove_nat_action());
        builder.append(remove_opte_port_action());
        builder.append(unlock_migration_action());
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
