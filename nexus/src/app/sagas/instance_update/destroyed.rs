// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use super::RealParams;
use super::DESTROYED_VMM_ID;
use crate::app::sagas::ActionError;
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

    let result = osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            instance.project_id,
            i64::from(instance.ncpus.0 .0),
            instance.memory,
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
