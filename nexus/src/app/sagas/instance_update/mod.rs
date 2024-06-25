// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::instance;
use crate::app::db::datastore::InstanceSnapshot;
use crate::app::db::lookup::LookupPath;
use crate::app::db::model::Generation;
use crate::app::db::model::VmmState;
use crate::app::db::model::{Migration, MigrationState};
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_queries::{authn, authz};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node, SagaName};
use uuid::Uuid;

mod destroyed;

// The public interface to this saga is actually a smaller saga that starts the
// "real" update saga, which inherits the lock from the start saga. This is
// because the decision of which subsaga(s) to run depends on the state of the
// instance record read from the database *once the lock has been acquired*,
// and the saga DAG for the "real" instance update saga may be constructed only
// after the instance state has been fetched. However, since the the instance
// state must be read inside the lock, that *also* needs to happen in a saga,
// so that the lock is always dropped when unwinding. Thus, we have a second,
// smaller saga which starts our real saga, and then the real saga, which
// decides what DAG to build based on the instance fetched by the start saga.
//
// Don't worry, this won't be on the test.
mod start;
pub(crate) use self::start::{Params, SagaInstanceUpdate};

/// Parameters to the "real" instance update saga.
#[derive(Debug, Deserialize, Serialize)]
struct RealParams {
    serialized_authn: authn::saga::Serialized,

    authz_instance: authz::Instance,

    state: InstanceSnapshot,

    orig_lock: instance::UpdaterLock,
}

const INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";
const INSTANCE_LOCK: &str = "updater_lock";
const MIGRATION: &str = "migration";

// instance update saga: actions

declare_saga_actions! {
    instance_update;

    // Become the instance updater
    BECOME_UPDATER -> "updater_lock" {
        + siu_become_updater
        - siu_unbecome_updater
    }

    // Update the instance record to reflect a migration event. If the
    // migration has completed on the target VMM, or if the migration has
    // failed, this will clear the migration IDs, allowing the instance to
    // migrate again. If the migration has completed on either VMM, the target
    // VMM becomes the active VMM.
    MIGRATION_UPDATE_INSTANCE -> "migration_update_instance" {
        + siu_migration_update_instance
    }

    // Update network configuration to point to the new active VMM.
    MIGRATION_UPDATE_NETWORK_CONFIG -> "migration_update_network_config" {
        + siu_migration_update_network_config
    }

    UNLOCK_INSTANCE -> "unlocked" {
        + siu_unlock_instance
    }
}

// instance update saga: definition
struct SagaDoActualInstanceUpdate;

impl NexusSaga for SagaDoActualInstanceUpdate {
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

        fn const_node(
            name: &'static str,
            value: &impl serde::Serialize,
        ) -> Result<steno::Node, super::SagaInitError> {
            let value = serde_json::to_value(value).map_err(|e| {
                SagaInitError::SerializeError(name.to_string(), e)
            })?;
            Ok(Node::constant(name, value))
        }

        // determine which subsaga(s) to execute based on the state of the instance
        // and the VMMs associated with it.
        if let Some(ref active_vmm) = params.state.active_vmm {
            // If the active VMM is `Destroyed`, schedule the active VMM
            // destroyed subsaga.
            if active_vmm.runtime.state == VmmState::Destroyed {
                const DESTROYED_SUBSAGA_PARAMS: &str =
                    "params_for_vmm_destroyed_subsaga";
                let subsaga_params = destroyed::Params {
                    serialized_authn: params.serialized_authn.clone(),
                    authz_instance: params.authz_instance.clone(),
                    vmm_id: PropolisUuid::from_untyped_uuid(active_vmm.id),
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

        // Next, determine what to do with the migration. A migration update
        // saga needs to be scheduled if (and only if) the instance's migration
        // ID currently points to a migration. The `instance_fetch_all` query
        // will only return a migration if it is the instance's currently active
        // migration, so if we have one here, that means that there's a
        // migration.
        if let Some(ref migration) = params.state.migration {
            // If either side of the migration reports a terminal state, update
            // the instance to reflect that.
            if migration.is_terminal() {
                builder.append(const_node(MIGRATION, migration)?);
                // TODO(eliza): perhaps we could determine the final state in
                // `make_saga_dag` and push a constant node for it, and then
                // only have one `update_instance` action that's run regardless
                // of which path through the saga we build...
                builder.append(migration_update_instance_action());
                builder.append(migration_update_network_config_action());
            }
        }

        builder.append(unlock_instance_action());
        Ok(builder.build()?)
    }
}

async fn siu_become_updater(
    sagactx: NexusActionContext,
) -> Result<instance::UpdaterLock, ActionError> {
    let RealParams {
        ref serialized_authn, ref authz_instance, orig_lock, ..
    } = sagactx.saga_params::<RealParams>()?;
    let saga_id = sagactx.lookup::<Uuid>(INSTANCE_LOCK_ID)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();

    slog::debug!(
        osagactx.log(),
        "instance update: trying to become instance updater...";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %saga_id,
        "parent_lock" => ?orig_lock,
    );

    let lock = osagactx
        .datastore()
        .instance_updater_inherit_lock(
            &opctx,
            &authz_instance,
            orig_lock,
            saga_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    slog::info!(
        osagactx.log(),
        "Now, I am become Updater, the destroyer of VMMs.";
        "instance_id" => %authz_instance.id(),
        "saga_id" => %saga_id,
    );

    Ok(lock)
}

async fn siu_unbecome_updater(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;
    unlock_instance_inner(serialized_authn, authz_instance, &sagactx).await?;

    Ok(())
}

async fn siu_migration_update_instance(
    sagactx: NexusActionContext,
) -> Result<PropolisUuid, ActionError> {
    let RealParams { ref authz_instance, ref state, .. } =
        sagactx.saga_params()?;
    let migration = sagactx.lookup::<Migration>(MIGRATION)?;

    let osagactx = sagactx.user_data();
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let mut new_runtime = state.instance.runtime().clone();
    new_runtime.gen = Generation(new_runtime.gen.next());
    new_runtime.time_updated = Utc::now();

    // Determine how to update the instance record to reflect the current
    // migration state.
    let failed = migration.either_side_failed();
    // If the migration has failed, or if the target reports that the migration
    // has completed, clear the instance record's migration IDs so that a new
    // migration can begin.
    if failed || migration.target_state == MigrationState::COMPLETED {
        info!(
            osagactx.log(),
            "instance update (migration {}): clearing migration IDs",
            if failed { "failed" } else { "target_completed" };
            "instance_id" => %instance_id,
            "migration_id" => %migration.id,
            "src_propolis_id" => %migration.source_propolis_id,
            "target_propolis_id" => %migration.target_propolis_id,
            "instance_update" => %"migration",
        );
        new_runtime.migration_id = None;
        new_runtime.dst_propolis_id = None;
    }

    // If either side reports that the migration has completed, move the target
    // Propolis ID to the active position.
    let new_propolis_id = if !failed && migration.either_side_completed() {
        info!(
            osagactx.log(),
            "instance update (migration completed): setting active VMM ID to target";
            "instance_id" => %instance_id,
            "migration_id" => %migration.id,
            "src_propolis_id" => %migration.source_propolis_id,
            "target_propolis_id" => %migration.target_propolis_id,
            "instance_update" => %"migration",
        );
        new_runtime.propolis_id = Some(migration.target_propolis_id);
        migration.target_propolis_id
    } else {
        migration.source_propolis_id
    };

    osagactx
        .datastore()
        .instance_update_runtime(&instance_id, &new_runtime)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(PropolisUuid::from_untyped_uuid(new_propolis_id))
}

async fn siu_migration_update_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params()?;

    let migration = sagactx.lookup::<Migration>(MIGRATION)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    // Either the instance moved from one sled to another, or it attempted
    // to migrate and failed. Ensure the correct networking configuration
    // exists for its current home.
    //
    // TODO(#3107) This is necessary even if the instance didn't move,
    // because registering a migration target on a sled creates OPTE ports
    // for its VNICs, and that creates new V2P mappings on that sled that
    // place the relevant virtual IPs on the local sled. Once OPTE stops
    // creating these mappings, this path only needs to be taken if an
    // instance has changed sleds.

    // Look up the ID of the sled that the instance now resides on, so that we
    // can look up its address.
    let active_propolis_id =
        sagactx.lookup::<PropolisUuid>("update_instance_record")?;
    let new_sled_id = match osagactx
        .datastore()
        .vmm_fetch(&opctx, authz_instance, &active_propolis_id)
        .await
    {
        Ok(vmm) => vmm.sled_id,

        // A VMM in the active position should never be destroyed. If the
        // sled sending this message is the owner of the instance's last
        // active VMM and is destroying it, it should also have retired that
        // VMM.
        Err(Error::ObjectNotFound { .. }) => {
            error!(osagactx.log(), "instance's active vmm unexpectedly not found";
                    "instance_id" => %instance_id,
                    "propolis_id" => %active_propolis_id);

            return Ok(());
        }
        Err(e) => return Err(ActionError::action_failed(e)),
    };

    info!(
        osagactx.log(),
        "instance update (migration): ensuring updated instance network config";
        "instance_id" => %instance_id,
        "migration_id" => %migration.id,
        "src_propolis_id" => %migration.source_propolis_id,
        "target_propolis_id" => %migration.target_propolis_id,
        "active_propolis_id" => %active_propolis_id,
        "sled_id" => %new_sled_id,
        "migration_failed" => migration.either_side_failed(),
    );

    if let Err(e) = osagactx.nexus().v2p_notification_tx.send(()) {
        error!(
            osagactx.log(),
            "error notifying background task of v2p change";
            "error" => ?e
        )
    };

    let (.., sled) = LookupPath::new(&opctx, osagactx.datastore())
        .sled_id(new_sled_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    osagactx
        .nexus()
        .instance_ensure_dpd_config(&opctx, instance_id, &sled.address(), None)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn siu_unlock_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;
    unlock_instance_inner(serialized_authn, authz_instance, &sagactx).await
}

async fn unlock_instance_inner(
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    sagactx: &NexusActionContext,
) -> Result<(), ActionError> {
    let lock = sagactx.lookup::<instance::UpdaterLock>(INSTANCE_LOCK)?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    slog::info!(
        osagactx.log(),
        "instance update: unlocking instance";
        "instance_id" => %authz_instance.id(),
        "lock" => ?lock,
    );

    let did_unlock = osagactx
        .datastore()
        .instance_updater_unlock(&opctx, authz_instance, lock)
        .await
        .map_err(ActionError::action_failed)?;

    slog::info!(
        osagactx.log(),
        "instance update: unlocked instance";
        "instance_id" => %authz_instance.id(),
        "did_unlock" => ?did_unlock,
    );

    Ok(())
}
