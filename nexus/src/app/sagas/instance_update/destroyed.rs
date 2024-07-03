// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    declare_saga_actions, ActionRegistry, DagBuilder, NexusActionContext,
    NexusSaga, SagaInitError,
};
use crate::app::sagas::ActionError;
use nexus_db_queries::authn;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde::{Deserialize, Serialize};

// destroy VMM subsaga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Params {
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub(super) serialized_authn: authn::saga::Serialized,

    /// Instance UUID of the instance being updated. This is only just used
    /// for logging, so we just use the instance ID here instead of serializing
    /// a whole instance record.
    pub(super) instance_id: InstanceUuid,

    /// UUID of the VMM to destroy.
    pub(super) vmm_id: PropolisUuid,
}

// destroy VMM subsaga: actions

declare_saga_actions! {
    destroy_vmm;

    // Deallocate physical sled resources reserved for the destroyed VMM, as it
    // is no longer using them.
    RELEASE_SLED_RESOURCES -> "release_sled_resources" {
        + siu_destroyed_release_sled_resources
    }

    // Mark the VMM record as deleted.
    MARK_VMM_DELETED -> "mark_vmm_deleted" {
        + siu_destroyed_mark_vmm_deleted
    }
}

// destroy VMM subsaga: definition

#[derive(Debug)]
pub(super) struct SagaDestroyVmm;
impl NexusSaga for SagaDestroyVmm {
    const NAME: &'static str = "destroy-vmm";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        destroy_vmm_register_actions(registry)
    }

    fn make_saga_dag(
        _: &Self::Params,
        mut builder: DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(release_sled_resources_action());
        builder.append(mark_vmm_deleted_action());
        Ok(builder.build()?)
    }
}

async fn siu_destroyed_release_sled_resources(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, instance_id, vmm_id, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (active VMM destroyed): deallocating sled resource reservation";
        "instance_id" => %instance_id,
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

pub(super) async fn siu_destroyed_mark_vmm_deleted(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, instance_id, vmm_id, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): marking VMM record deleted";
        "instance_id" => %instance_id,
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
