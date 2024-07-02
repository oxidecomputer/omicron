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
use crate::app::db::model::InstanceRuntimeState;
use crate::app::db::model::InstanceState;
use crate::app::db::model::MigrationState;
use crate::app::db::model::VmmState;
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_queries::{authn, authz};
use nexus_types::identity::Resource;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node};
use uuid::Uuid;

mod destroyed;
use destroyed::*;

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

#[derive(Debug, Deserialize, Serialize)]
struct UpdatesRequired {
    /// The new runtime state that must be written back to the database.
    new_runtime: InstanceRuntimeState,

    /// If `true`, this VMM must be destroyed.
    destroy_vmm: Option<DestroyedVmm>,

    network_config: Option<NetworkConfigUpdate>,
}

#[derive(Debug, Deserialize, Serialize)]
enum NetworkConfigUpdate {
    Delete,
    Update(PropolisUuid),
}

/// An active VMM has been destroyed.
#[derive(Debug, Deserialize, Serialize)]
struct DestroyedVmm {
    /// The UUID of the destroyed active VMM.
    id: PropolisUuid,
    /// If `true`, the virtual provisioning records for this instance should be
    /// deallocated.
    ///
    /// This occurs when the instance's VMM is destroyed *without* migrating out.
    /// If the instance's current active VMM has been destroyed because the
    /// instance has successfully migrated out, the virtual provisioning records
    /// are left in place, as the instance is still consuming those virtual
    /// resources on its new sled.
    deprovision: bool,
}

impl UpdatesRequired {
    fn for_snapshot(
        log: &slog::Logger,
        snapshot: &InstanceSnapshot,
    ) -> Option<Self> {
        let mut new_runtime = snapshot.instance.runtime().clone();
        new_runtime.gen = Generation(new_runtime.gen.next());
        new_runtime.time_updated = Utc::now();
        let instance_id = snapshot.instance.id();

        let mut update_required = false;
        let mut network_config = None;
        let mut deprovision = true;

        // Has the active VMM been destroyed?
        let destroy_vmm = snapshot.active_vmm.as_ref().and_then(|active_vmm| {
            if active_vmm.runtime.state == VmmState::Destroyed {
                // Unlink the active VMM ID. If the active VMM was destroyed
                // because a migration out completed, the next block, which
                // handles migration updates, will set this to the new VMM's ID,
                // instead.
                new_runtime.propolis_id = None;
                new_runtime.nexus_state = InstanceState::NoVmm;
                update_required = true;
                network_config = Some(NetworkConfigUpdate::Delete);
                Some(PropolisUuid::from_untyped_uuid(active_vmm.id))
            } else {
                None
            }
        });

        // Determine what to do with the migration.
        if let Some(ref migration) = snapshot.migration {
            // Determine how to update the instance record to reflect the current
            // migration state.
            let failed = migration.either_side_failed();
            // If the migration has failed, or if the target reports that the migration
            // has completed, clear the instance record's migration IDs so that a new
            // migration can begin.
            if failed || migration.target_state == MigrationState::COMPLETED {
                info!(
                    log,
                    "instance update (migration {}): clearing migration IDs",
                    if failed { "failed" } else { "target completed" };
                    "instance_id" => %instance_id,
                    "migration_id" => %migration.id,
                    "src_propolis_id" => %migration.source_propolis_id,
                    "target_propolis_id" => %migration.target_propolis_id,
                );
                new_runtime.migration_id = None;
                new_runtime.dst_propolis_id = None;
                update_required = true;
            }

            // If the active VMM was destroyed, the network config must be
            // deleted (which was determined above). Otherwise, if the
            // migration failed but the active VMM was still there, we must
            // still ensure the correct networking configuration
            // exists for its current home.
            //
            // TODO(#3107) This is necessary even if the instance didn't move,
            // because registering a migration target on a sled creates OPTE ports
            // for its VNICs, and that creates new V2P mappings on that sled that
            // place the relevant virtual IPs on the local sled. Once OPTE stops
            // creating these mappings, this path only needs to be taken if an
            // instance has changed sleds.
            if failed && destroy_vmm.is_none() {
                network_config = Some(NetworkConfigUpdate::Update(
                    PropolisUuid::from_untyped_uuid(
                        migration.source_propolis_id,
                    ),
                ));
                update_required = true;
            }

            // If either side reports that the migration has completed, move the target
            // Propolis ID to the active position.
            if !failed && migration.either_side_completed() {
                info!(
                    log,
                    "instance update (migration completed): setting active VMM ID to target";
                    "instance_id" => %instance_id,
                    "migration_id" => %migration.id,
                    "src_propolis_id" => %migration.source_propolis_id,
                    "target_propolis_id" => %migration.target_propolis_id,
                );

                network_config = Some(NetworkConfigUpdate::Update(
                    PropolisUuid::from_untyped_uuid(
                        migration.target_propolis_id,
                    ),
                ));
                new_runtime.propolis_id = Some(migration.target_propolis_id);
                // Even if the active VMM was destroyed (and we set the
                // instance's state to `NoVmm` above), it has successfully
                // migrated, so leave it in the VMM state.
                new_runtime.nexus_state = InstanceState::Vmm;
                new_runtime.dst_propolis_id = None;
                // If the active VMM has also been destroyed, don't delete
                // virtual provisioning records while cleaning it up.
                deprovision = false;
                update_required = true;
            }
        }

        if !update_required {
            return None;
        }
        Some(Self {
            new_runtime,
            destroy_vmm: destroy_vmm.map(|id| DestroyedVmm { id, deprovision }),
            network_config,
        })
    }
}

/// Parameters to the "real" instance update saga.
#[derive(Debug, Deserialize, Serialize)]
struct RealParams {
    serialized_authn: authn::saga::Serialized,

    authz_instance: authz::Instance,

    state: InstanceSnapshot,

    update: UpdatesRequired,

    orig_lock: instance::UpdaterLock,
}

const INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";
const INSTANCE_LOCK: &str = "updater_lock";
const DESTROYED_VMM_ID: &str = "destroyed_vmm_id";
const NETWORK_CONFIG_UPDATE: &str = "network_config_update";

// instance update saga: actions

declare_saga_actions! {
    instance_update;

    // Become the instance updater
    BECOME_UPDATER -> "updater_lock" {
        + siu_become_updater
        - siu_unbecome_updater
    }

    // Update network configuration.
    UPDATE_NETWORK_CONFIG -> "update_network_config" {
        + siu_update_network_config
    }

    // Release the lock and write back the new instance record.
    UPDATE_AND_UNLOCK_INSTANCE -> "unlocked" {
        + siu_update_and_unlock_instance
    }

    // === active VMM destroyed actions ===

    // Deallocate physical sled resources reserved for the destroyed VMM, as it
    // is no longer using them.
    DESTROYED_RELEASE_SLED_RESOURCES -> "destroyed_vmm_release_sled_resources" {
        + siu_destroyed_release_sled_resources
    }

    // Deallocate virtual provisioning resources reserved by the instance, as it
    // is no longer running.
    DESTROYED_RELEASE_VIRTUAL_PROVISIONING -> "destroyed_vmm_release_virtual_provisioning" {
        + siu_destroyed_release_virtual_provisioning
    }

    // Unassign the instance's Oximeter producer.
    DESTROYED_UNASSIGN_OXIMETER_PRODUCER -> "destroyed_vmm_unassign_oximeter" {
        + siu_destroyed_unassign_oximeter_producer
    }

    DESTROYED_MARK_VMM_DELETED -> "destroyed_mark_vmm_deleted" {
        + siu_destroyed_mark_vmm_deleted
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
        fn const_node(
            name: &'static str,
            value: &impl serde::Serialize,
        ) -> Result<steno::Node, super::SagaInitError> {
            let value = serde_json::to_value(value).map_err(|e| {
                SagaInitError::SerializeError(name.to_string(), e)
            })?;
            Ok(Node::constant(name, value))
        }

        builder.append(Node::action(
            INSTANCE_LOCK_ID,
            "GenerateInstanceLockId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(become_updater_action());

        // If the active VMM has been destroyed, clean up after it.
        // TODO(eliza): if we also wished to delete destroyed target VMMs after
        // a failed migration, we could move all the "VMM destroyed" actions into
        // a subsaga that we can push twice...
        if let Some(DestroyedVmm { ref id, deprovision }) =
            params.update.destroy_vmm
        {
            builder.append(const_node(DESTROYED_VMM_ID, id)?);
            builder.append(destroyed_release_sled_resources_action());
            // If the instance hasn't migrated out of the destroyed VMM, also release virtual
            // provisioning records and unassign the Oximeter producer.
            if deprovision {
                builder.append(destroyed_release_virtual_provisioning_action());
                builder.append(destroyed_unassign_oximeter_producer_action());
            }
        }

        // If a network config update is required, do that.
        if let Some(ref update) = params.update.network_config {
            builder.append(const_node(NETWORK_CONFIG_UPDATE, update)?);
            builder.append(update_network_config_action());
        }

        builder.append(update_and_unlock_instance_action());

        // Delete the active VMM only *after* the instance record is
        // updated, to avoid creating a "dangling pointer" where the instance
        // record's active VMM ID points to a VMM record that has now been
        // deleted.
        if params.update.destroy_vmm.is_some() {
            builder.append(destroyed_mark_vmm_deleted_action());
        }
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
    unlock_instance_inner(serialized_authn, authz_instance, &sagactx, None)
        .await?;

    Ok(())
}

async fn siu_update_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params()?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let update =
        sagactx.lookup::<NetworkConfigUpdate>(NETWORK_CONFIG_UPDATE)?;

    match update {
        NetworkConfigUpdate::Delete => {
            info!(
                osagactx.log(),
                "instance update: deleting network config";
                "instance_id" => %instance_id,
            );
            nexus
                .instance_delete_dpd_config(&opctx, authz_instance)
                .await
                .map_err(ActionError::action_failed)?;
        }
        NetworkConfigUpdate::Update(active_propolis_id) => {
            // Look up the ID of the sled that the instance now resides on, so that we
            // can look up its address.
            let new_sled_id = osagactx
                .datastore()
                .vmm_fetch(&opctx, authz_instance, &active_propolis_id)
                .await
                .map_err(ActionError::action_failed)?
                .sled_id;

            info!(
                osagactx.log(),
                "instance update: ensuring updated instance network config";
                "instance_id" => %instance_id,
                "active_propolis_id" => %active_propolis_id,
                "new_sled_id" => %new_sled_id,
            );

            let (.., sled) = LookupPath::new(&opctx, osagactx.datastore())
                .sled_id(new_sled_id)
                .fetch()
                .await
                .map_err(ActionError::action_failed)?;

            nexus
                .instance_ensure_dpd_config(
                    &opctx,
                    instance_id,
                    &sled.address(),
                    None,
                )
                .await
                .map_err(ActionError::action_failed)?;
        }
    }

    // Make sure the V2P manager background task runs to ensure the V2P mappings
    // for this instance are up to date.
    nexus.background_tasks.activate(&nexus.background_tasks.task_v2p_manager);

    Ok(())
}

async fn siu_update_and_unlock_instance(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let RealParams {
        ref serialized_authn, ref authz_instance, ref update, ..
    } = sagactx.saga_params::<RealParams>()?;
    unlock_instance_inner(
        serialized_authn,
        authz_instance,
        &sagactx,
        Some(&update.new_runtime),
    )
    .await
}

async fn unlock_instance_inner(
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    sagactx: &NexusActionContext,
    new_runtime: Option<&InstanceRuntimeState>,
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
        .instance_updater_unlock(&opctx, authz_instance, lock, new_runtime)
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
