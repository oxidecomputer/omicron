// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// instance update start saga

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, RealParams,
    SagaDoActualInstanceUpdate, SagaInitError, UpdatesRequired,
    ACTION_GENERATE_ID, INSTANCE_LOCK, INSTANCE_LOCK_ID,
};
use crate::app::saga;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::db::datastore::instance;
use nexus_db_queries::{authn, authz};
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node, SagaResultErr};
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

    info!(
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

    // If the instance lock node in the saga context was `None`, that means
    // we didn't acquire the lock, and we can die happily without having to
    // worry about unlocking the instance. It would be pretty surprising if this
    // saga unwound without having acquired the lock, but...whatever.
    if let Some(lock) =
        sagactx.lookup::<Option<instance::UpdaterLock>>(INSTANCE_LOCK)?
    {
        super::unwind_instance_lock(
            lock,
            serialized_authn,
            authz_instance,
            &sagactx,
        )
        .await;
    }

    Ok(())
}

async fn siu_fetch_state_and_start_real_saga(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params { serialized_authn, authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let instance_id = authz_instance.id();
    let log = osagactx.log();

    // Did we get the lock? If so, we can start the next saga, otherwise, just
    // exit gracefully.
    let Some(orig_lock) =
        sagactx.lookup::<Option<instance::UpdaterLock>>(INSTANCE_LOCK)?
    else {
        info!(
            log,
            "instance update: instance is already locked! doing nothing...";
            "instance_id" => %instance_id,
            "saga_id" => %lock_id,
        );
        return Ok(());
    };

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, &serialized_authn);
    let datastore = osagactx.datastore();
    let nexus = osagactx.nexus();

    let state = datastore
        .instance_fetch_all(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    // Determine what updates are required based on the instance's current
    // state snapshot. If there are updates to perform, execute the "real"
    // update saga. Otherwise, if we don't need to do anything else, simply
    // release the lock and finish this saga.
    if let Some(update) = UpdatesRequired::for_instance(log, &state) {
        info!(
            log,
            "instance update: starting real update saga...";
            "instance_id" => %instance_id,
            "current.runtime_state" => ?state.instance.runtime(),
            "current.migration" => ?state.migration,
            "current.active_vmm" => ?state.active_vmm,
            "current.target_vmm" => ?state.target_vmm,
            "update.new_runtime_state" => ?update.new_runtime,
            "update.network_config_update" => ?update.network_config,
            "update.destroy_active_vmm" => ?update.destroy_active_vmm,
            "update.destroy_target_vmm" => ?update.destroy_target_vmm,
            "update.deprovision" => update.deprovision.is_some(),
        );
        // Prepare the child saga.
        //
        // /!\ WARNING /!\ This is really finicky: whether or not the start saga
        // should unwind depends on *whether the child `instance-update` saga
        // has advanced far enough to have inherited the lock or not. If the
        // child has not inherited the lock, we *must* unwind to ensure the lock
        // is dropped.
        //
        // Note that we *don't* use `SagaExecutor::saga_execute`, which prepares
        // the child saga and waits for it to complete. That function wraps all
        // the errors returned by this whole process in an external API error,
        // which makes it difficult for us to figure out *why* the child saga
        // failed, and whether we should unwind or not.

        let dag =
            saga::create_saga_dag::<SagaDoActualInstanceUpdate>(RealParams {
                serialized_authn,
                authz_instance,
                update,
                orig_lock,
            })
            // If we can't build a DAG for the child saga, we should unwind, so
            // that we release the lock.
            .map_err(|e| {
                nexus.background_tasks.task_instance_updater.activate();
                ActionError::action_failed(e)
            })?;
        let child_result = nexus
            .sagas
            .saga_prepare(dag)
            .await
            // Similarly, if we can't prepare the child saga, we need to unwind
            // and release the lock.
            .map_err(|e| {
                nexus.background_tasks.task_instance_updater.activate();
                ActionError::action_failed(e)
            })?
            .start()
            .await
            // And, if we can't start it, we need to unwind.
            .map_err(|e| {
                nexus.background_tasks.task_instance_updater.activate();
                ActionError::action_failed(e)
            })?
            .wait_until_stopped()
            .await
            .into_raw_result();
        match child_result.kind {
            Ok(_) => {
                debug!(
                    log,
                    "instance update: child saga completed successfully";
                    "instance_id" => %instance_id,
                    "child_saga_id" => %child_result.saga_id,
                )
            }
            // Check if the child saga failed to inherit the updater lock from
            // this saga.
            Err(SagaResultErr {
                error_node_name,
                error_source: ActionError::ActionFailed { source_error },
                ..
            }) if error_node_name.as_ref() == super::INSTANCE_LOCK => {
                if let Ok(instance::UpdaterLockError::AlreadyLocked) =
                    serde_json::from_value(source_error)
                {
                    // If inheriting the lock failed because the lock was held by another
                    // saga. If this is the case, that's fine: this action must have
                    // executed more than once, and created multiple child sagas. No big deal.
                    return Ok(());
                } else {
                    // Otherwise, the child saga could not inherit the lock for
                    // some other reason. That means we MUST unwind to ensure
                    // the lock is released.
                    return Err(ActionError::action_failed(
                        "child saga failed to inherit lock".to_string(),
                    ));
                }
            }
            Err(error) => {
                warn!(
                    log,
                    "instance update: child saga failed, unwinding...";
                    "instance_id" => %instance_id,
                    "child_saga_id" => %child_result.saga_id,
                    "error" => ?error,
                );

                // If the real saga failed, kick the background task. If the real
                // saga failed because this action was executed twice and the second
                // child saga couldn't lock the instance, that's fine, because the
                // background task will only start new sagas for instances whose DB
                // state actually *needs* an update.
                nexus.background_tasks.task_instance_updater.activate();
                return Err(error.error_source);
            }
        }
    } else {
        info!(
            log,
            "instance update: no updates required, releasing lock.";
            "instance_id" => %authz_instance.id(),
            "current.runtime_state" => ?state.instance.runtime(),
            "current.migration" => ?state.migration,
            "current.active_vmm" => ?state.active_vmm,
            "current.target_vmm" => ?state.target_vmm,
        );
        datastore
            .instance_updater_unlock(&opctx, &authz_instance, &orig_lock)
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}
