// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    siu_lock_instance, siu_unlock_instance, NexusActionContext, Params,
};
use crate::app::instance_network;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::ActionError;
use nexus_db_model::Generation;
use nexus_db_model::InstanceRuntimeState;
use nexus_db_queries::db::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;

declare_saga_actions! {
    instance_update_destroyed;

    // Read the target Instance from CRDB and join with its active VMM and
    // migration target VMM records if they exist, and then acquire the
    // "instance updater" lock with this saga's ID if no other saga is currently
    // updating the instance.
    LOCK_INSTANCE -> "lock_generation" {
        + siu_lock_instance
        - siu_unlock_instance
    }

    DELETE_SLED_RESOURCE -> "no_result1" {
        + siud_delete_sled_resource
    }

    DELETE_VIRTUAL_PROVISIONING -> "no_result2" {
        + siud_delete_virtual_provisioning
    }

    DELETE_V2P_MAPPINGS -> "no_result3" {
        + siud_delete_v2p_mappings
    }

    DELETE_NAT_ENTRIES -> "no_result4" {
        + siud_delete_nat_entries
    }

    UPDATE_VMM_DESTROYED -> "no_result5" {
        + siud_instance_update_vmm_destroyed
    }

    MARK_VMM_DELETED -> "no_result6" {
        + siud_mark_vmm_deleted
    }

    UNLOCK_INSTANCE -> "no_result7" {
        + siu_unlock_instance
    }
}

async fn siud_delete_sled_resource(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref active_vmm, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let propolis_id = active_vmm
        .as_ref()
        // TODO(eliza): don't unwrap here and put it in params instead when deciding
        // what to start.
        .expect("if we started this saga there is an active propolis ID")
        .id;
    osagactx
        .datastore()
        .sled_reservation_delete(&opctx, propolis_id)
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

async fn siud_delete_virtual_provisioning(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref instance, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance.id(),
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

async fn siud_delete_v2p_mappings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref instance, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    // Per the commentary in instance_network::delete_instance_v2p_mappings`,
    // this should be idempotent.
    instance_network::delete_instance_v2p_mappings(
        osagactx.datastore(),
        osagactx.log(),
        &osagactx.nexus().opctx_alloc,
        &opctx,
        instance.id(),
    )
    .await
    .or_else(|err| {
        // Necessary for idempotency
        match err {
            Error::ObjectNotFound {
                type_name: ResourceType::Instance,
                lookup_type: _,
            } => Ok(()),
            _ => Err(ActionError::action_failed(err)),
        }
    })
}

async fn siud_delete_nat_entries(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let opctx_alloc = &osagactx.nexus().opctx_alloc;
    let resolver = osagactx.nexus().resolver().await;
    let datastore = osagactx.datastore();
    let log = osagactx.log();

    instance_network::instance_delete_dpd_config(
        datastore,
        log,
        &resolver,
        &opctx,
        opctx_alloc,
        authz_instance,
    )
    .await
    .or_else(|err|
        // Necessary for idempotency
        match err {
            Error::ObjectNotFound { .. } => Ok(()),
            _ => Err(ActionError::action_failed(err)),
        })
}

async fn siud_instance_update_vmm_destroyed(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { instance, .. } = sagactx.saga_params::<Params>()?;
    let new_runtime = InstanceRuntimeState {
        propolis_id: None,
        nexus_state: external::InstanceState::Stopped.into(),
        gen: Generation(instance.runtime_state.gen.0.next()),
        ..instance.runtime_state
    };

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
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref active_vmm, .. } =
        sagactx.saga_params::<Params>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let propolis_id = active_vmm
        .as_ref()
        // TODO(eliza): don't unwrap here and put it in params instead when deciding
        // what to start.
        .expect("if we started this saga there is an active propolis ID")
        .id;
    osagactx
        .datastore()
        .vmm_mark_deleted(&opctx, &propolis_id)
        .await
        .map(|_| ())
        .map_err(ActionError::action_failed)
}
