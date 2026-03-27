// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// instance update start saga

use super::{
    ACTION_GENERATE_ID, ActionRegistry, INSTANCE_LOCK, INSTANCE_LOCK_ID,
    NexusActionContext, NexusSaga, RETRY_WARN_AFTER, RealParams,
    SagaDoActualInstanceUpdate, SagaInitError, UpdatesRequired,
};
use crate::app::saga;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::db::datastore::instance;
use nexus_db_queries::{authn, authz};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_common::backoff;
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
    let instance_id = authz_instance.id();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    debug!(
        osagactx.log(),
        "instance update: attempting to lock instance";
        "instance_id" => %instance_id,
        "saga_id" => %lock_id,
    );

    // /!\ EXTREMELY IMPORTANT WARNING /!\
    //
    // We are about to attempt to acquire the instance record's updater lock. If
    // this succeeds, this saga node should output `Some(UpdaterLock)`, which
    // will result in an actual update saga being started with that lock. If the
    // instance record is already locked by another saga, we will instead return
    // `Ok(None)`, and this start saga will complete without starting a real
    // update saga.
    //
    // However, there is a third possible outcome of the query that attempts to
    // lock the instance record: we may encounter a database error that does NOT
    // indicate that the lock is already held. In that case, this action MUST
    // continue retrying the lock operation forever until it either succeeds or
    // indicates that another saga has the lock. Otherwise, a particularly
    // unlucky sequence of a Nexus crash followed by a transient database error
    // could leave the instance record permanently locked by this (now failed)
    // saga. The scenario in which this would occur is as follows:
    //
    // 1. A Nexus starts executing this action, successfully locks the instance
    //    record, and then crashes *before* marking the saga node as having
    //    completed.
    // 2. Subsequently, a new Nexus resumes executing the saga and runs this
    //    action again. It hits a query failure trying to lock the instance
    //    record, returns an `ActionFailed` error, and unwinds.
    // 3. Because the saga node has not *completed*, our undo action
    //    (`siu_instance_lock_undo()`), will *not* execute, so the instance
    //    record remains locked, but this saga has now failed, so no one will
    //    ever unlock the instance.
    //
    // See https://github.com/oxidecomputer/omicron/issues/10166 for more
    // details.
    //
    // Due to this potential danger, we shall retry the lock operation forever
    // until it either succeeds or indicates that the instance has already been
    // locked by another saga. Because the lock operation is idempotent if *our*
    // updater ID is the one inside the lock, it is fine if this node executes
    // multiple times. Retrying indefinitely is reasonable here based on the
    // assumption that if we can't talk to the database, our only options are to
    // keep retrying or unwind, and unwinding *also* requires that we be able to
    // talk to the database, so we may as well retry. We will complain loudly if
    // we've been retrying for a long time, or if the error seems to be "our
    // fault" (a client error).
    backoff::retry_notify_ext(
        // This is an internal service query to CockroachDB.
        backoff::retry_policy_internal_service(),
        || async {
            let result = osagactx
                .datastore()
                .instance_updater_lock(&opctx, authz_instance, lock_id)
                .await;
            match result {
                Ok(lock) => {
                    info!(
                        osagactx.log(),
                        "instance update: lock acquired!";
                        "instance_id" => %instance_id,
                        "saga_id" => %lock_id,
                        "lock" => ?lock,
                    );
                    Ok(Some(lock))
                }
                // Don't return an error if we can't take the lock. This
                // saga will simply not start the real instance update
                // saga, rather than having to unwind.
                Err(instance::UpdaterLockError::AlreadyLocked) => {
                    info!(
                        osagactx.log(),
                        "instance update: instance already locked, giving up";
                        "instance_id" => %instance_id,
                        "saga_id" => %lock_id,
                    );
                    Ok(None)
                }
                // Retry database errors that *don't* indicate the lock
                // is already held forever.
                Err(instance::UpdaterLockError::Query(e)) => {
                    Err(backoff::BackoffError::transient(e))
                }
            }
        },
        |error, call_count, total_duration| {
            let http_error = dropshot::HttpError::from(error.clone());
            if http_error.status_code.is_client_error() {
                // A "client error" here indicates that we probably sent a query
                // to the database that will never succeed. This should
                // hopefully never happen, and probably indicates indicate a
                // programmer error in the lock operation. However, if we don't
                // keep retrying, the instance may remain locked forever, so
                // we shall just keep seeing if it will ever succeed.
                error!(
                    osagactx.log(),
                    "instance update: client error while trying to lock \
                     instance (likely requires operator intervention), \
                     retrying anyway...";
                     "instance_id" => %instance_id,
                     "saga_id" => %lock_id,
                    "error" => &error,
                    "call_count" => call_count,
                    "total_duration" => ?total_duration,
                );
            } else if total_duration > RETRY_WARN_AFTER {
                // This error was probably not our fault, but man, we've been
                // retrying for kind of a while now. Better complain about it.
                warn!(
                    osagactx.log(),
                    "instance update: server error while attempting to lock \
                     instance, retrying...";
                    "instance_id" => %instance_id,
                    "saga_id" => %lock_id,
                    "error" => &error,
                    "call_count" => call_count,
                    "total_duration" => ?total_duration,
                );
            } else {
                info!(
                    osagactx.log(),
                    "instance update: server error while attempting to lock \
                     instance, retrying...";
                    "instance_id" => %instance_id,
                    "saga_id" => %lock_id,
                    "error" => &error,
                    "call_count" => call_count,
                    "total_duration" => ?total_duration,
                );
            }
        },
    )
    .await
    .map_err(saga_action_failed)
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
        .map_err(saga_action_failed)?;

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
                saga_action_failed(e)
            })?;
        let child_result = nexus
            .sagas
            .saga_prepare(dag)
            .await
            // Similarly, if we can't prepare the child saga, we need to unwind
            // and release the lock.
            .map_err(|e| {
                nexus.background_tasks.task_instance_updater.activate();
                saga_action_failed(e)
            })?
            .start()
            .await
            // And, if we can't start it, we need to unwind.
            .map_err(|e| {
                nexus.background_tasks.task_instance_updater.activate();
                saga_action_failed(e)
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
                    return Err(saga_action_failed(Error::internal_error(
                        "child saga failed to inherit lock",
                    )));
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
            .map_err(saga_action_failed)?;
        // If we're releasing the lock, check if we should activate the
        // instance reincarnation background task to reincarnate this
        // instance. A previous activation may have not been able to
        // reincarnate this instance because we held the lock, so poking the
        // reincarnation task here should help ensure it sees a failed instance
        // in a timely manner.
        //
        // This doesn't matter a whole lot in production systems, where the task
        // will activate periodically regardless, but it should make the tests
        // less flakey, and hopefully also decrease the latency with which
        // failed instances are reincarnated a bit, maybe?
        super::reincarnate_if_needed(osagactx, &state);
    }

    Ok(())
}
