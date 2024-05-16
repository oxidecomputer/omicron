// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::InstanceSnapshot;
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

    pub state: InstanceSnapshot,
}

const INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";
const INSTANCE_LOCK_GEN: &str = "saga_instance_lock_gen";

// instance update saga: actions

declare_saga_actions! {
    instance_update;

    // Read the target Instance from CRDB and join with its active VMM and
    // migration target VMM records if they exist, and then acquire the
    // "instance updater" lock with this saga's ID if no other saga is currently
    // updating the instance.
    LOCK_INSTANCE -> "saga_instance_lock_gen" {
        + siu_lock_instance
        - siu_lock_instance_undo
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

        // determine which subsaga to execute based on the state of the instance
        // and the VMMs associated with it.
        match params.state {
            // VMM destroyed subsaga
            InstanceSnapshot {
                instance, active_vmm: Some(ref vmm), ..
            } if vmm.runtime.state.state() == &VmmState::Destroyed => {
                const DESTROYED_SUBSAGA_PARAMS: &str =
                    "params_for_vmm_destroyed_subsaga";
                let subsaga_params = destroyed::Params {
                    serialized_authn: params.serialized_authn.clone(),
                    instance: instance.clone(),
                    vmm: vmm.clone(),
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
            _ => {
                // TODO(eliza): other subsagas
            }
        };

        builder.append(unlock_instance_action());

        Ok(builder.build()?)
    }
}

// instance update saga: action implementations

async fn siu_lock_instance(
    sagactx: NexusActionContext,
) -> Result<Generation, ActionError> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref state, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let datastore = osagactx.datastore();

    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(state.instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    // try to acquire the instance updater lock
    datastore
        .instance_updater_try_lock(
            &opctx,
            &authz_instance,
            state.instance.runtime_state.updater_gen,
            &lock_id,
        )
        .await
        .map_err(ActionError::action_failed)?
        .ok_or_else(|| {
            ActionError::action_failed(
                serde_json::json!({"error": "can't get ye lock"}),
            )
        })
}

async fn siu_unlock_instance(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref state, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let gen = sagactx.lookup::<Generation>(INSTANCE_LOCK_GEN)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    osagactx
        .datastore()
        .instance_updater_unlock(&opctx, &authz_instance, &lock_id)
        .await?;
    Ok(())
}

// this is different from "lock instance" lol
async fn siu_lock_instance_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let Params { ref serialized_authn, ref state, .. } =
        sagactx.saga_params::<Params>()?;
    let lock_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let datastore = osagactx.datastore();

    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(state.instance.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    let updater_gen = state.instance.runtime_state.updater_gen.next().into();
    datastore
        .instance_updater_unlock(&opctx, &authz_instance, &lock_id, updater_gen)
        .await?;
    Ok(())
}
