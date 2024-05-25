// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::Params;
use super::STATE;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::ActionError;
use nexus_db_model::Generation;
use nexus_db_model::Instance;
use nexus_db_model::InstanceRuntimeState;
use nexus_db_model::Vmm;
use nexus_db_queries::db::datastore::InstanceAndVmms;
use nexus_db_queries::db::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use slog::info;

// instance update VMM destroyed subsaga: actions

// This subsaga is responsible for handling an instance update where the
// instance's active VMM has entered the `Destroyed` state. This requires
// deallocating resources assigned to the instance, updating the instance's
// records in the database, and marking the VMM as deleted.
declare_saga_actions! {
    instance_update_destroyed;

    // Deallocate physical sled resources reserved for the destroyed VMM, as it
    // is no longer using them.
    RELEASE_SLED_RESOURCES -> "no_result1" {
        + siud_release_sled_resources
    }

    // Deallocate virtual provisioning resources reserved by the instance, as it
    // is no longer running.
    RELEASE_VIRTUAL_PROVISIONING -> "no_result2" {
        + siud_release_virtual_provisioning
    }

    // Unassign the instance's Oximeter producer.
    UNASSIGN_OXIMETER_PRODUCER -> "no_result3" {
        + siud_unassign_oximeter_producer
    }

    DELETE_V2P_MAPPINGS -> "no_result4" {
        + siud_delete_v2p_mappings
    }

    DELETE_NAT_ENTRIES -> "no_result5" {
        + siud_delete_nat_entries
    }

    UPDATE_INSTANCE -> "no_result6" {
        + siud_update_instance
    }

    MARK_VMM_DELETED -> "no_result7" {
        + siud_mark_vmm_deleted
    }
}

#[derive(Debug)]
pub(crate) struct SagaVmmDestroyed;
impl NexusSaga for SagaVmmDestroyed {
    const NAME: &'static str = "instance-update-vmm-destroyed";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_update_destroyed_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(release_sled_resources_action());
        builder.append(release_virtual_provisioning_action());
        builder.append(unassign_oximeter_producer_action());
        builder.append(delete_v2p_mappings_action());
        builder.append(delete_nat_entries_action());
        builder.append(update_instance_action());
        builder.append(mark_vmm_deleted_action());

        Ok(builder.build()?)
    }
}

fn get_destroyed_vmm(
    sagactx: &NexusActionContext,
) -> Result<Option<(Instance, Vmm)>, ActionError> {
    let state = sagactx.lookup::<InstanceAndVmms>(STATE)?;
    match state.active_vmm {
        Some(vmm) if vmm.runtime.state.state() == &InstanceState::Destroyed => {
            Ok(Some((state.instance, vmm)))
        }
        _ => Ok(None),
    }
}

async fn siud_release_sled_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((_, vmm)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };

    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (active VMM destroyed): deallocating sled resource reservation";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm.id,
        "instance_update" => %"VMM destroyed",
    );

    osagactx
        .datastore()
        .sled_reservation_delete(&opctx, vmm.id)
        .await
        .or_else(|err| {
            // Necessary for idempotency
            match err {
                Error::ObjectNotFound { .. } => Ok(()),
                _ => Err(err),
            }
        })
        .map_err(ActionError::action_failed)
}

async fn siud_release_virtual_provisioning(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((instance, vmm)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };

    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): deallocating virtual provisioning resources";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm.id,
        "instance_update" => %"VMM destroyed",
    );

    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            authz_instance.id(),
            instance.project_id,
            i64::from(instance.ncpus.0 .0),
            instance.memory,
            i64::try_from(&instance.runtime_state.gen.0).unwrap(),
        )
        .await
        .map(|_| ())
        .or_else(|err| {
            // Necessary for idempotency
            match err {
                Error::ObjectNotFound { .. } => Ok(()),
                _ => Err(ActionError::action_failed(err)),
            }
        })
}

async fn siud_unassign_oximeter_producer(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((_, _)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    crate::app::oximeter::unassign_producer(
        osagactx.datastore(),
        osagactx.log(),
        &opctx,
        &authz_instance.id(),
    )
    .await
    .map_err(ActionError::action_failed)
}

async fn siud_delete_v2p_mappings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((instance, vmm)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };
    let osagactx = sagactx.user_data();
    info!(
        osagactx.log(),
        "instance update (VMM destroyed): deleting V2P mappings";
        "instance_id" => %instance.id(),
        "propolis_id" => %vmm.id,
        "instance_update" => %"VMM destroyed",
    );

    let nexus = osagactx.nexus();
    nexus.background_tasks.activate(&nexus.background_tasks.task_v2p_manager);
    Ok(())
}

async fn siud_delete_nat_entries(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((_, vmm)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): deleting NAT entries";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm.id,
        "instance_update" => %"VMM destroyed",
    );

    osagactx
        .nexus()
        .instance_delete_dpd_config(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn siud_update_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((instance, vmm)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };
    let osagactx = sagactx.user_data();
    let new_runtime = InstanceRuntimeState {
        propolis_id: None,
        nexus_state: external::InstanceState::Stopped.into(),
        gen: Generation(instance.runtime_state.gen.0.next()),
        ..instance.runtime_state
    };

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): updating runtime state";
        "instance_id" => %instance.id(),
        "propolis_id" => %vmm.id,
        "new_runtime_state" => ?new_runtime,
        "instance_update" => %"VMM destroyed",
    );

    // It's okay for this to fail, it just means that the active VMM ID has changed.
    let _ = osagactx
        .datastore()
        .instance_update_runtime(&instance.id(), &new_runtime)
        .await;
    Ok(())
}

async fn siud_mark_vmm_deleted(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Some((instance, vmm)) = get_destroyed_vmm(&sagactx)? else {
        // if the update we are handling is not an active VMM destroyed update,
        // bail --- there's nothing to do here.
        return Ok(());
    };
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): marking VMM record deleted";
        "instance_id" => %instance.id(),
        "propolis_id" => %vmm.id,
        "instance_update" => %"VMM destroyed",
    );

    osagactx
        .datastore()
        .vmm_mark_deleted(&opctx, &vmm.id)
        .await
        .map(|_| ())
        .map_err(ActionError::action_failed)
}
