// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::InstanceAndVmms;
use crate::app::db::lookup::LookupPath;
use crate::app::sagas::declare_saga_actions;
use nexus_db_model::Generation;
use nexus_db_queries::{authn, authz};
use nexus_types::identity::Resource;
use omicron_common::api::external::InstanceState;
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node, SagaName};
use uuid::Uuid;

mod destroyed;
/// Parameters to the instance update saga.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    /// Authentication context to use to fetch the instance's current state from
    /// the database.
    pub serialized_authn: authn::saga::Serialized,

    pub authz_instance: authz::Instance,
}

const INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";
const STATE: &str = "state";

// instance update saga: actions

declare_saga_actions! {
    instance_update;

    // Acquire the instance updater" lock with this saga's ID if no other saga
    // is currently updating the instance.
    LOCK_INSTANCE -> "saga_instance_lock_gen" {
        + siu_lock_instance
        - siu_lock_instance_undo
    }

    // Fetch the instance and VMM's state.
    // N.B. that this must be performed as a separate action from
    // `LOCK_INSTANCE`, so that if the lookup fails, we will still unwind the
    // `LOCK_INSTANCE` action and release the lock.
    FETCH_STATE -> "state" {
        + siu_fetch_state
    }

    UNLOCK_INSTANCE -> "no_result7" {
        + siu_unlock_instance
    }
}

// instance update saga: definition

#[derive(Debug)]
pub(crate) struct SagaInstanceUpdate;
impl NexusSaga for SagaInstanceUpdate {
    const NAME: &'static str = "instance-update";
    type Params = Params;

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
        builder.append(lock_instance_action());
        builder.append(fetch_state_action());

        // determine which subsaga to execute based on the state of the instance
        // and the VMMs associated with it.
        const DESTROYED_SUBSAGA_PARAMS: &str =
            "params_for_vmm_destroyed_subsaga";
        let subsaga_dag = {
            let subsaga_builder = DagBuilder::new(SagaName::new(
                destroyed::SagaVmmDestroyed::NAME,
            ));
            destroyed::SagaVmmDestroyed::make_saga_dag(
                &params,
                subsaga_builder,
            )?
        };

        builder.append(Node::constant(
            DESTROYED_SUBSAGA_PARAMS,
            serde_json::to_value(&params).map_err(|e| {
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

        builder.append(unlock_instance_action());

        Ok(builder.build()?)
    }
}

// instance update saga: action implementations

async fn siu_lock_instance(
    sagactx: NexusActionContext,
) -> Result<Generation, ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let datastore = osagactx.datastore();
    let log = osagactx.log();
    slog::info!(
        osagactx.log(),
        "instance update: attempting to lock instance";
        "instance_id" => %instance.id(),
        "saga_id" => %lock_id,
    );
    osagactx
        .datastore()
        .instance_updater_lock(&opctx, authz_instance, &lock_id)
        .await
        .map_err(ActionError::action_failed)
        .map(|_| ())
}

async fn siu_fetch_state(
    sagactx: NexusActionContext,
) -> Result<InstanceAndVmms, ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    osagactx
        .datastore()
        .instance_fetch_with_vmms(&opctx, authz_instance)
        .await
        .map_err(ActionError::action_failed)
}

async fn siu_unlock_instance(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    osagactx
        .datastore()
        .instance_updater_unlock(&opctx, &authz_instance, &lock_id)
        .await?;
    Ok(())
}

// N.B. that this has to be a separate function just because the undo action
// must return `anyhow::Error` rather than `ActionError`.
async fn siu_lock_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let datastore = osagactx.datastore();

    slog::info!(
        osagactx.log(),
        "instance update: unlocking instance on unwind";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %lock_id,
    );

    datastore.instance_updater_unlock(&opctx, authz_instance, &lock_id).await?;

    Ok(())
}
