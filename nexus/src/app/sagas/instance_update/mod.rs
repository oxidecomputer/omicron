// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::InstanceSnapshot;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz};
use omicron_common::api::external::InstanceState;
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node, SagaName};
use uuid::Uuid;

mod destroyed;

/// Parameters to the start instance update saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    pub authz_instance: authz::Instance,
}

/// Parameters to the "real" instance update saga.
#[derive(Debug, Deserialize, Serialize)]
struct RealParams {
    serialized_authn: authn::saga::Serialized,

    authz_instance: authz::Instance,

    state: InstanceSnapshot,
}

const INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";

// instance update saga: actions

declare_saga_actions! {
    instance_update;

    // Acquire the instance updater" lock with this saga's ID if no other saga
    // is currently updating the instance.
    LOCK_INSTANCE -> "saga_instance_lock_gen" {
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

    // Become the instance updater
    BECOME_UPDATER -> "generation" {
        + siu_become_updater
        - siu_unbecome_updater
    }

    UNLOCK_INSTANCE -> "unlocked" {
        + siu_unlock_instance
    }
}

// instance update saga: definition

#[derive(Debug)]
pub(crate) struct SagaInstanceUpdate;
impl NexusSaga for SagaInstanceUpdate {
    const NAME: &'static str = "start-instance-update";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_update_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
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

struct SagaRealInstanceUpdate;

impl NexusSaga for SagaRealInstanceUpdate {
    const NAME: &'static str = "instance-update";
    type Params = RealParams;

    fn register_actions(registry: &mut ActionRegistry) {
        instance_update_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            INSTANCE_LOCK_ID,
            "GenerateInstanceLockId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(become_updater_action());

        // determine which subsaga(s) to execute based on the state of the instance
        // and the VMMs associated with it.
        if let Some(ref active_vmm) = params.state.active_vmm {
            // If the active VMM is `Destroyed`, schedule the active VMM
            // destroyed subsaga.
            if active_vmm.runtime.state.state() == &InstanceState::Destroyed {
                const DESTROYED_SUBSAGA_PARAMS: &str =
                    "params_for_vmm_destroyed_subsaga";
                let subsaga_params = destroyed::Params {
                    serialized_authn: params.serialized_authn.clone(),
                    authz_instance: params.authz_instance.clone(),
                    vmm_id: active_vmm.id,
                    instance: params.state.instance.clone(),
                };
                let subsaga_dag = {
                    let subsaga_builder = DagBuilder::new(SagaName::new(
                        destroyed::SagaVmmDestroyed::NAME,
                    ));
                    destroyed::SagaVmmDestroyed::make_saga_dag(
                        &subsaga_params,
                        subsaga_builder,
                    )?
                };

                builder.append(Node::constant(
                    DESTROYED_SUBSAGA_PARAMS,
                    serde_json::to_value(&subsaga_params).map_err(|e| {
                        SagaInitError::SerializeError(
                            DESTROYED_SUBSAGA_PARAMS.to_string(),
                            e,
                        )
                    })?,
                ));

                builder.append(Node::subsaga(
                    "vmm_destroyed_subsaga_no_result",
                    subsaga_dag,
                    DESTROYED_SUBSAGA_PARAMS,
                ));
            }
        }

        builder.append(unlock_instance_action());
        Ok(builder.build()?)
    }
}

// instance update saga: action implementations

async fn siu_lock_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
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
    osagactx
        .datastore()
        .instance_updater_lock(&opctx, authz_instance, lock_id)
        .await
        .map_err(ActionError::action_failed)
        .map(|_| ())
}

async fn siu_fetch_state_and_start_real_saga(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let Params { serialized_authn, authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, &serialized_authn);

    let state = osagactx
        .datastore()
        .instance_fetch_all(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;
    osagactx
        .nexus()
        .execute_saga::<SagaRealInstanceUpdate>(RealParams {
            serialized_authn,
            authz_instance,
            state,
        })
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siu_become_updater(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let RealParams {
        ref serialized_authn, ref authz_instance, ref state, ..
    } = sagactx.saga_params::<RealParams>()?;

    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    slog::debug!(
        osagactx.log(),
        "instance update: trying to become instance updater...";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %lock_id,
        "parent_id" => ?state.instance.runtime_state.updater_id,
    );

    osagactx
        .datastore()
        .instance_updater_inherit_lock(&opctx, &state.instance, &lock_id)
        .await
        .map_err(ActionError::action_failed)?;

    slog::info!(
        osagactx.log(),
        "instance update: became instance updater";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %lock_id,
        "parent_id" => ?state.instance.runtime_state.updater_id,
    );

    Ok(())
}

async fn siu_unbecome_updater(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;
    unlock_instance_inner(serialized_authn, authz_instance, &sagactx).await?;

    Ok(())
}

async fn siu_unlock_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;
    unlock_instance_inner(serialized_authn, authz_instance, &sagactx).await
}

// N.B. that this has to be a separate function just because the undo action
// must return `anyhow::Error` rather than `ActionError`.
async fn siu_lock_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    unlock_instance_inner(serialized_authn, authz_instance, &sagactx).await?;
    Ok(())
}

async fn unlock_instance_inner(
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    sagactx: &NexusActionContext,
) -> Result<(), ActionError> {
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    slog::info!(
        osagactx.log(),
        "instance update: unlocking instance";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %lock_id,
    );

    osagactx
        .datastore()
        .instance_updater_unlock(&opctx, authz_instance, &lock_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
