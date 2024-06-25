// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use super::RealParams;
use super::DESTROYED_VMM_ID;
use crate::app::sagas::ActionError;
use chrono::Utc;
use nexus_db_model::Generation;
use nexus_db_model::InstanceRuntimeState;
use nexus_db_model::InstanceState;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;

pub(super) async fn siu_destroyed_release_sled_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;
    let vmm_id = sagactx.lookup::<PropolisUuid>(DESTROYED_VMM_ID)?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (active VMM destroyed): deallocating sled resource reservation";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm_id,
        "instance_update" => %"VMM destroyed",
    );

    osagactx
        .datastore()
        .sled_reservation_delete(&opctx, vmm_id.into_untyped_uuid())
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

pub(super) async fn siu_destroyed_release_virtual_provisioning(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref serialized_authn, ref authz_instance, state, .. } =
        sagactx.saga_params::<RealParams>()?;

    let vmm_id = sagactx.lookup::<PropolisUuid>(DESTROYED_VMM_ID)?;
    let instance = state.instance;
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // `virtual_provisioning_collection_delete_instace` will only delete virtual
    // provisioning records that are *less than* the max generation parameter,
    // not less than or equal to it --- the idea is that the generation number
    // has already been advanced when we are deallocating the virtual
    // provisioning records. This is kind of an artifact of sled-agent
    // previously owning instance runtime state generations, since the
    // sled-agent would have already advanced the instance's generation.
    //
    // However, now that the instance record is owned by Nexus, and we are
    // updating the instance in response to a VMM state update from sled-agent,
    // the instance record snapshot we are holding has not yet had its
    // generation advanced, so we want to allow deleting virtual provisioning
    // records that were created with the instance's current generation. The
    // generation will be advanced at the end of this saga, once we have updated
    // the actual instance record.
    let max_gen = instance.runtime_state.gen.next();
    let result = osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            instance.project_id,
            i64::from(instance.ncpus.0 .0),
            instance.memory,
            i64::try_from(&max_gen).unwrap(),
        )
        .await;
    match result {
        Ok(deleted) => {
            info!(
                osagactx.log(),
                "instance update (VMM destroyed): deallocated virtual \
                 provisioning resources";
                "instance_id" => %instance_id,
                "propolis_id" => %vmm_id,
                "records_deleted" => ?deleted,
                "instance_update" => %"VMM destroyed",
            );
        }
        // Necessary for idempotency --- the virtual provisioning resources may
        // have been deleted already, that's fine.
        Err(Error::ObjectNotFound { .. }) => {
            // TODO(eliza): it would be nice if we could distinguish
            // between errors returned by
            // `virtual_provisioning_collection_delete_instance` where
            // the instance ID was not found, and errors where the
            // generation number was too low...
            info!(
                osagactx.log(),
                "instance update (VMM destroyed): virtual provisioning \
                 record not found; perhaps it has already been deleted?";
                "instance_id" => %instance_id,
                "propolis_id" => %vmm_id,
                "instance_update" => %"VMM destroyed",
            );
        }
        Err(err) => return Err(ActionError::action_failed(err)),
    };

    Ok(())
}

pub(super) async fn siu_destroyed_unassign_oximeter_producer(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;

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

pub(super) async fn siu_destroyed_update_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;
    let vmm_id = sagactx.lookup::<PropolisUuid>(DESTROYED_VMM_ID)?;
    let nexus = osagactx.nexus();

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): deleting V2P mappings";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm_id,
        "instance_update" => %"VMM destroyed",
    );

    nexus.background_tasks.activate(&nexus.background_tasks.task_v2p_manager);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): deleting NAT entries";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm_id,
        "instance_update" => %"VMM destroyed",
    );

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    nexus
        .instance_delete_dpd_config(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

pub(super) async fn siu_destroyed_update_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let RealParams { ref authz_instance, state, .. } =
        sagactx.saga_params::<RealParams>()?;

    let vmm_id = sagactx.lookup::<PropolisUuid>(DESTROYED_VMM_ID)?;
    let instance = state.instance;
    let osagactx = sagactx.user_data();
    let new_runtime = InstanceRuntimeState {
        propolis_id: None,
        nexus_state: InstanceState::NoVmm,
        gen: Generation(instance.runtime_state.gen.0.next()),
        time_updated: Utc::now(),
        ..instance.runtime_state
    };

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): updating runtime state";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm_id,
        "new_runtime_state" => ?new_runtime,
        "instance_update" => %"VMM destroyed",
    );

    // It's okay for this to fail, it just means that the active VMM ID has changed.
    if let Err(e) = osagactx
        .datastore()
        .instance_update_runtime(
            &InstanceUuid::from_untyped_uuid(authz_instance.id()),
            &new_runtime,
        )
        .await
    {
        warn!(
            osagactx.log(),
            "instance update (VMM destroyed): updating runtime state failed";
            "instance_id" => %authz_instance.id(),
            "propolis_id" => %vmm_id,
            "new_runtime_state" => ?new_runtime,
            "instance_update" => %"VMM destroyed",
            "error" => %e,
        );
    }
    Ok(())
}

pub(super) async fn siu_destroyed_mark_vmm_deleted(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref authz_instance, ref serialized_authn, .. } =
        sagactx.saga_params::<RealParams>()?;
    let vmm_id = sagactx.lookup::<PropolisUuid>(DESTROYED_VMM_ID)?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): marking VMM record deleted";
        "instance_id" => %authz_instance.id(),
        "propolis_id" => %vmm_id,
        "instance_update" => %"VMM destroyed",
    );

    osagactx
        .datastore()
        .vmm_mark_deleted(&opctx, &vmm_id)
        .await
        .map(|_| ())
        .map_err(ActionError::action_failed)
}
