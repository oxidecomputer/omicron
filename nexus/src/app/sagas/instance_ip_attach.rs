// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use crate::external_api::params;
use nexus_db_model::ExternalIp;
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
// - Attach+alloc EIP to instance
// - Register routes
//  -> ensure_dpd...
//  -> must precede OPTE: host may change its sending
//     behaviour prematurely
// - Register addr in OPTE
//  -> Put addr in sled-agent endpoint
// - free up migration_id of instance.
//  -> mark instance migration id as None

declare_saga_actions! {
    instance_ip_attach;
    LOCK_MIGRATION -> "sled_id" {
        + siia_migration_lock
        - siia_migration_lock_undo
    }

    ATTACH_EXTERNAL_IP -> "new_ip" {
        + siia_begin_attach_ip
        - siia_begin_attach_ip_undo
    }

    REGISTER_NAT -> "no_result0" {
        + siia_nat
        - siia_nat_undo
    }

    ENSURE_OPTE_PORT -> "no_result1" {
        + siia_update_opte
        - siia_update_opte_undo
    }

    UNLOCK_MIGRATION -> "output" {
        + siia_migration_unlock
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub create_params: params::ExternalIpCreate,
    pub authz_instance: authz::Instance,
    pub instance: db::model::Instance,
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,
}

async fn siia_migration_lock(
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

    // TODO: Currently stop if there's a migration. This may be a good case
    //       for RPW'ing ext_ip_state -> { NAT RPW, sled-agent } in future.
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

    Ok(inst_and_vmm.sled_id())
}

async fn siia_migration_lock_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO: do this iff. we implement migration lock.
    Ok(())
}

// TODO: factor this out for attach, detach, and instance create
//       to share an impl.

async fn siia_begin_attach_ip(
    sagactx: NexusActionContext,
) -> Result<ExternalIp, ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    match params.create_params {
        // Allocate a new IP address from the target, possibly default, pool
        params::ExternalIpCreate::Ephemeral { ref pool_name } => {
            let pool_name =
                pool_name.as_ref().map(|name| db::model::Name(name.clone()));
            datastore
                .allocate_instance_ephemeral_ip(
                    &opctx,
                    Uuid::new_v4(),
                    params.instance.id(),
                    pool_name,
                )
                .await
                .map_err(ActionError::action_failed)
        }
        // Set the parent of an existing floating IP to the new instance's ID.
        params::ExternalIpCreate::Floating { ref floating_ip_name } => {
            let floating_ip_name = db::model::Name(floating_ip_name.clone());
            let (.., authz_fip) = LookupPath::new(&opctx, &datastore)
                .project_id(params.instance.project_id)
                .floating_ip_name(&floating_ip_name)
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(ActionError::action_failed)?;

            datastore
                .floating_ip_begin_attach(
                    &opctx,
                    &authz_fip,
                    params.instance.id(),
                )
                .await
                .map_err(ActionError::action_failed)
        }
    }
}

async fn siia_begin_attach_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    let n_rows = datastore
        .external_ip_complete_op(
            &opctx,
            new_ip.id,
            new_ip.kind,
            nexus_db_model::IpAttachState::Attaching,
            nexus_db_model::IpAttachState::Detached,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siia_nat(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // NOTE: mostly copied from instance_start.

    // No physical sled? Don't push NAT.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    // Querying sleds requires fleet access; use the instance allocator context
    // for this.
    let (.., sled) = LookupPath::new(&osagactx.nexus().opctx_alloc, &datastore)
        .sled_id(sled_uuid)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(
            &opctx,
            params.instance.id(),
            &sled.address(),
            Some(new_ip.id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siia_nat_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // If we didn't push NAT before, don't undo it.
    if sagactx.lookup::<Option<Uuid>>("sled_id")?.is_none() {
        return Ok(());
    }

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    osagactx
        .nexus()
        .instance_delete_dpd_config(
            &opctx,
            &params.authz_instance,
            Some(new_ip.id),
        )
        .await?;

    Ok(())
}

async fn siia_update_opte(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // No physical sled? Don't inform OPTE.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;
    let sled_agent_body =
        new_ip.try_into().map_err(ActionError::action_failed)?;

    // TODO: disambiguate the various sled agent errors etc.
    osagactx
        .nexus()
        .sled_client(&sled_uuid)
        .await
        .map_err(ActionError::action_failed)?
        .instance_put_external_ip(&params.instance.id(), &sled_agent_body)
        .await
        .map_err(|_| {
            ActionError::action_failed(Error::invalid_request("hmm"))
        })?;

    Ok(())
}

async fn siia_update_opte_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // If we didn't push OPTE before, don't undo it.
    let Some(sled_uuid) = sagactx.lookup::<Option<Uuid>>("sled_id")? else {
        return Ok(());
    };

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;
    let sled_agent_body = new_ip.try_into()?;

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

async fn siia_migration_unlock(
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

    let new_ip = sagactx.lookup::<ExternalIp>("new_ip")?;

    let n_rows = datastore
        .external_ip_complete_op(
            &opctx,
            new_ip.id,
            new_ip.kind,
            nexus_db_model::IpAttachState::Attaching,
            nexus_db_model::IpAttachState::Attached,
        )
        .await
        .map_err(ActionError::action_failed)?;

    new_ip.try_into().map_err(ActionError::action_failed)
}

// TODO: backout changes if run state changed illegally?

#[derive(Debug)]
pub struct SagaInstanceIpAttach;
impl NexusSaga for SagaInstanceIpAttach {
    const NAME: &'static str = "external-ip-attach";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_ip_attach_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(lock_migration_action());
        builder.append(attach_external_ip_action());
        builder.append(register_nat_action());
        builder.append(ensure_opte_port_action());
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
