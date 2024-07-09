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
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde::{Deserialize, Serialize};
use steno::{ActionError, DagBuilder, Node};
use uuid::Uuid;

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

mod destroyed;

#[derive(Debug, Deserialize, Serialize)]
struct UpdatesRequired {
    /// The new runtime state that must be written back to the database.
    new_runtime: InstanceRuntimeState,

    destroy_active_vmm: Option<PropolisUuid>,
    destroy_target_vmm: Option<PropolisUuid>,
    deprovision: bool,
    network_config: Option<NetworkConfigUpdate>,
}

#[derive(Debug, Deserialize, Serialize)]
enum NetworkConfigUpdate {
    Delete,
    Update { active_propolis_id: PropolisUuid, new_sled_id: Uuid },
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
        let mut deprovision = false;

        // Has the active VMM been destroyed?
        let destroy_active_vmm =
            snapshot.active_vmm.as_ref().and_then(|active_vmm| {
                if active_vmm.runtime.state == VmmState::Destroyed {
                    // Unlink the active VMM ID. If the active VMM was destroyed
                    // because a migration out completed, the next block, which
                    // handles migration updates, will set this to the new VMM's ID,
                    // instead.
                    new_runtime.propolis_id = None;
                    new_runtime.nexus_state = InstanceState::NoVmm;
                    update_required = true;
                    // If and only if the active VMM was destroyed *and* we did
                    // not successfully migrate out of it, the instance's
                    // virtual provisioning records and oximeter producer must
                    // be cleaned up.
                    //
                    // If the active VMM was destroyed as a result of a
                    // successful migration out, the subsequent code for
                    // determining what to do with the migration will change
                    // this back.
                    deprovision = true;
                    // Similarly, if the active VMM was destroyed and the
                    // instance has not migrated out of it, we must delete the
                    // instance's network configuration. Again, if there was a
                    // migration out, the subsequent migration-handling code
                    // will change this to a network config update if the
                    // instance is now living somewhere else.
                    network_config = Some(NetworkConfigUpdate::Delete);
                    Some(PropolisUuid::from_untyped_uuid(active_vmm.id))
                } else {
                    None
                }
            });

        let destroy_target_vmm =
            snapshot.target_vmm.as_ref().and_then(|target_vmm| {
                if target_vmm.runtime.state == VmmState::Destroyed {
                    // Unlink the target VMM ID.
                    new_runtime.dst_propolis_id = None;
                    update_required = true;
                    Some(PropolisUuid::from_untyped_uuid(target_vmm.id))
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
            if failed && destroy_active_vmm.is_none() {
                network_config = Some(NetworkConfigUpdate::Update {
                    active_propolis_id: PropolisUuid::from_untyped_uuid(
                        migration.source_propolis_id,
                    ),
                    new_sled_id: snapshot
                        .active_vmm
                        .as_ref()
                        .expect("if we're here, there must be an active VMM")
                        .sled_id,
                });
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

                network_config = Some(NetworkConfigUpdate::Update {
                    active_propolis_id: PropolisUuid::from_untyped_uuid(
                        migration.target_propolis_id,
                    ),
                    new_sled_id: snapshot
                        .target_vmm
                        .as_ref()
                        .expect("if we're here, there must be a target VMM")
                        .sled_id,
                });
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
            destroy_active_vmm,
            destroy_target_vmm,
            deprovision,
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

    // Deallocate virtual provisioning resources reserved by the instance, as it
    // is no longer running.
    RELEASE_VIRTUAL_PROVISIONING -> "release_virtual_provisioning" {
        + siu_release_virtual_provisioning
    }

    // Unassign the instance's Oximeter producer.
    UNASSIGN_OXIMETER_PRODUCER -> "unassign_oximeter_producer" {
        + siu_unassign_oximeter_producer
    }

    // Release the lock and write back the new instance record.
    UPDATE_AND_UNLOCK_INSTANCE -> "unlocked" {
        + siu_update_and_unlock_instance
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
    ) -> Result<steno::Dag, SagaInitError> {
        // Helper function for constructing a constant node.
        fn const_node(
            name: impl AsRef<str>,
            value: &impl serde::Serialize,
        ) -> Result<steno::Node, SagaInitError> {
            let value = serde_json::to_value(value).map_err(|e| {
                SagaInitError::SerializeError(name.as_ref().to_string(), e)
            })?;
            Ok(Node::constant(name, value))
        }

        // Generate a new ID and attempt to inherit the lock from the start saga.
        builder.append(Node::action(
            INSTANCE_LOCK_ID,
            "GenerateInstanceLockId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(become_updater_action());

        // If a network config update is required, do that.
        if let Some(ref update) = params.update.network_config {
            builder.append(const_node(NETWORK_CONFIG_UPDATE, update)?);
            builder.append(update_network_config_action());
        }

        // If the instance now has no active VMM, release its virtual
        // provisioning resources and unassign its Oximeter producer.
        if params.update.deprovision {
            builder.append(release_virtual_provisioning_action());
            builder.append(unassign_oximeter_producer_action());
        }

        // Once we've finished mutating everything owned by the instance, we can
        // write ck the updated state and release the instance lock.
        builder.append(update_and_unlock_instance_action());

        // If either VMM linked to this instance has been destroyed, append
        // subsagas to clean up the VMMs resources and mark them as deleted.
        //
        // Note that we must not mark the VMMs as deleted until *after* we have
        // written back the updated instance record. Otherwise, if we mark a VMM
        // as deleted while the instance record still references its ID, we will
        // have created a state where the instance record contains a "dangling
        // pointer" (database version) where the foreign key points to a record
        // that no longer exists. Other consumers of the instance record may be
        // unpleasantly surprised by this, so we avoid marking these rows as
        // deleted until they've been unlinked from the instance by the
        // `update_and_unlock_instance` action.
        let mut append_destroyed_vmm_subsaga =
            |vmm_id: PropolisUuid, which_vmm: &'static str| {
                let params = destroyed::Params {
                    vmm_id,
                    instance_id: InstanceUuid::from_untyped_uuid(
                        params.authz_instance.id(),
                    ),
                    serialized_authn: params.serialized_authn.clone(),
                };
                let name = format!("destroy_{which_vmm}_vmm");

                let subsaga = destroyed::SagaDestroyVmm::make_saga_dag(
                    &params,
                    DagBuilder::new(steno::SagaName::new(&name)),
                )?;

                let params_name = format!("{name}_params");
                builder.append(const_node(&params_name, &params)?);

                let output_name = format!("{which_vmm}_vmm_destroyed");
                builder.append(Node::subsaga(
                    output_name.as_str(),
                    subsaga,
                    &params_name,
                ));

                Ok::<(), SagaInitError>(())
            };

        if let Some(vmm_id) = params.update.destroy_active_vmm {
            append_destroyed_vmm_subsaga(vmm_id, "active")?;
        }

        if let Some(vmm_id) = params.update.destroy_target_vmm {
            append_destroyed_vmm_subsaga(vmm_id, "target")?;
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
        NetworkConfigUpdate::Update { active_propolis_id, new_sled_id } => {
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

pub(super) async fn siu_release_virtual_provisioning(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref serialized_authn, ref authz_instance, state, .. } =
        sagactx.saga_params::<RealParams>()?;

    let instance = state.instance;
    let vmm_id = {
        let id = instance
            .runtime()
            .propolis_id
            .expect("a `release_virtual_provisioning` action should not have been pushed if there is no active VMM ID");
        PropolisUuid::from_untyped_uuid(id)
    };
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    let result = osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            instance.project_id,
            i64::from(instance.ncpus.0 .0),
            instance.memory,
        )
        .await;
    match result {
        Ok(deleted) => {
            info!(
                osagactx.log(),
                "instance update (VMM destroyed): deallocated virtual \
                 provisioning resources";
                "instance_id" => %instance_id,
                "propolis_id" => %vmm_id,
                "records_deleted" => ?deleted,
                "instance_update" => %"active VMM destroyed",
            );
        }
        // Necessary for idempotency --- the virtual provisioning resources may
        // have been deleted already, that's fine.
        Err(Error::ObjectNotFound { .. }) => {
            info!(
                osagactx.log(),
                "instance update (VMM destroyed): virtual provisioning \
                 record not found; perhaps it has already been deleted?";
                "instance_id" => %instance_id,
                "propolis_id" => %vmm_id,
                "instance_update" => %"active VMM destroyed",
            );
        }
        Err(err) => return Err(ActionError::action_failed(err)),
    };

    Ok(())
}

pub(super) async fn siu_unassign_oximeter_producer(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params::<RealParams>()?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    info!(
        osagactx.log(),
        "instance update (VMM destroyed): unassigning oximeter producer";
        "instance_id" => %authz_instance.id(),
        "instance_update" => %"active VMM destroyed",
    );
    crate::app::oximeter::unassign_producer(
        osagactx.datastore(),
        osagactx.log(),
        &opctx,
        &authz_instance.id(),
    )
    .await
    .map_err(ActionError::action_failed)
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
