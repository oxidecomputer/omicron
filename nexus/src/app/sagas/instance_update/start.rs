// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// instance update start saga

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, RealParams,
    SagaDoActualInstanceUpdate, SagaInitError, UpdatesRequired,
    ACTION_GENERATE_ID, INSTANCE_LOCK, INSTANCE_LOCK_ID,
};
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::db::datastore::instance;
use nexus_db_queries::{authn, authz};
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node};
use uuid::Uuid;

/// Parameters to the start instance update saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub(crate) serialized_authn: authn::saga::Serialized,

    pub(crate) authz_instance: authz::Instance,
}

// instance update saga: actions

declare_saga_actions! {
    start_instance_update;

    // Acquire the instance updater" lock with this saga's ID if no other saga
    // is currently updating the instance.
    LOCK_INSTANCE -> "updater_lock" {
        + siu_lock_instance
        - siu_lock_instance_undo
    }

    // Fetch the instance and VMM's state, and start the "real" instance update saga.
    // N.B. that this must be performed as a separate action from
    // `LOCK_INSTANCE`, so that if the lookup fails, we will still unwind the
    // `LOCK_INSTANCE` action and release the lock.
    FETCH_STATE_AND_START_REAL_SAGA -> "state" {
        + siu_fetch_state_and_start_real_saga
    }
}

// instance update saga: definition

#[derive(Debug)]
pub(crate) struct SagaInstanceUpdate;
impl NexusSaga for SagaInstanceUpdate {
    const NAME: &'static str = "start-instance-update";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        start_instance_update_register_actions(registry);
        super::SagaDoActualInstanceUpdate::register_actions(registry);
        super::destroyed::SagaDestroyVmm::register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            INSTANCE_LOCK_ID,
            "GenerateInstanceLockId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(lock_instance_action());
        builder.append(fetch_state_and_start_real_saga_action());

        Ok(builder.build()?)
    }
}

// start instance update saga: action implementations

async fn siu_lock_instance(
    sagactx: NexusActionContext,
) -> Result<Option<instance::UpdaterLock>, ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    slog::info!(
        osagactx.log(),
        "instance update: attempting to lock instance";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %lock_id,
    );
    let locked = osagactx
        .datastore()
        .instance_updater_lock(&opctx, authz_instance, lock_id)
        .await;
    match locked {
        Ok(lock) => Ok(Some(lock)),
        // Don't return an error if we can't take the lock. This saga will
        // simply not start the real instance update saga, rather than having to unwind.
        Err(instance::UpdaterLockError::AlreadyLocked) => Ok(None),
        // Okay, that's a real error. Time to die!
        Err(instance::UpdaterLockError::Query(e)) => {
            Err(ActionError::action_failed(e))
        }
    }
}

async fn siu_lock_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    super::unlock_instance_inner(
        serialized_authn,
        authz_instance,
        &sagactx,
        None,
    )
    .await?;
    Ok(())
}

async fn siu_fetch_state_and_start_real_saga(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params { serialized_authn, authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    // Did we get the lock? If so, we can start the next saga, otherwise, just
    // exit gracefully.
    let Some(orig_lock) =
        sagactx.lookup::<Option<instance::UpdaterLock>>(INSTANCE_LOCK)?
    else {
        slog::info!(
            osagactx.log(),
            "instance update: instance is already locked! doing nothing...";
            "instance_id" => %authz_instance.id(),
            "saga_id" => %lock_id,
        );
        return Ok(());
    };

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, &serialized_authn);

    let state = osagactx
        .datastore()
        .instance_fetch_all(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    // Determine what updates are required based on the instance's current
    // state snapshot. If there are updates to perform, execute the "real"
    // update saga. Otherwise, if we don't need to do anything else, simply
    // release the lock and finish this saga.
    if let Some(update) = UpdatesRequired::for_snapshot(osagactx.log(), &state)
    {
        info!(
            osagactx.log(),
            "instance update: starting real update saga...";
            "instance_id" => %authz_instance.id(),
            "current.runtime_state" => ?state.instance.runtime(),
            "current.migration" => ?state.migration,
            "current.active_vmm" => ?state.active_vmm,
            "current.target_vmm" => ?state.target_vmm,
            "update.new_runtime_state" => ?update.new_runtime,
            "update.network_config_update" => ?update.network_config,
            "update.destroy_active_vmm" => ?update.destroy_active_vmm,
            "update.destroy_target_vmm" => ?update.destroy_target_vmm,
            "update.deprovision" => update.deprovision,
        );
        osagactx
            .nexus()
            .sagas
            .saga_execute::<SagaDoActualInstanceUpdate>(RealParams {
                serialized_authn,
                authz_instance,
                state,
                update,
                orig_lock,
            })
            .await
            .map_err(ActionError::action_failed)?;
    } else {
        info!(
            osagactx.log(),
            "instance update: no updates required, releasing lock.";
            "instance_id" => %authz_instance.id(),
            "current.runtime_state" => ?state.instance.runtime(),
            "current.migration" => ?state.migration,
            "current.active_vmm" => ?state.active_vmm,
            "current.target_vmm" => ?state.target_vmm,
        );
        super::unlock_instance_inner(
            &serialized_authn,
            &authz_instance,
            &sagactx,
            None,
        )
        .await?;
    }

    Ok(())
}
