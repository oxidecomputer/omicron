// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// instance update start saga

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID, INSTANCE_LOCK_ID,
};
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz};
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node};
use uuid::Uuid;

/// Parameters to the start instance update saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    pub authz_instance: authz::Instance,
}

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
        .instance_updater_lock(&opctx, authz_instance, &lock_id)
        .await
        .map_err(ActionError::action_failed)
        .map(|_| ())
}

async fn siu_lock_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    super::unlock_instance_inner(serialized_authn, authz_instance, &sagactx)
        .await?;
    Ok(())
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
        .execute_saga::<super::SagaDoActualInstanceUpdate>(super::RealParams {
            serialized_authn,
            authz_instance,
            state,
        })
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
