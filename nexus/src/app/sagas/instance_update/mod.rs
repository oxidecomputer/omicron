// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance Update Saga
//!
//! # Theory of Operation
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
use crate::app::db::model::Vmm;
use crate::app::db::model::VmmState;
use crate::app::sagas::declare_saga_actions;
use anyhow::Context;
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

        // If there's an active migration, determine how to update the instance
        // record to reflect the current migration state.
        if let Some(ref migration) = snapshot.migration {
            if migration.either_side_failed() {
                // If the migration has failed, clear the instance record's
                // migration IDs so that a new migration can begin.
                info!(
                    log,
                    "instance update (migration failed): clearing migration IDs";
                    "instance_id" => %instance_id,
                    "migration_id" => %migration.id,
                    "src_propolis_id" => %migration.source_propolis_id,
                    "target_propolis_id" => %migration.target_propolis_id,
                );
                new_runtime.migration_id = None;
                new_runtime.dst_propolis_id = None;
                update_required = true;
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
                if destroy_active_vmm.is_none() {
                    if let Some(ref active_vmm) = snapshot.active_vmm {
                        info!(
                            log,
                            "instance update (migration failed): pointing network \
                             config back at current VMM";
                            "instance_id" => %instance_id,
                            "migration_id" => %migration.id,
                            "src_propolis_id" => %migration.source_propolis_id,
                            "target_propolis_id" => %migration.target_propolis_id,
                        );
                        network_config =
                            Some(NetworkConfigUpdate::to_vmm(active_vmm));
                    } else {
                        // Otherwise, the active VMM has already been destroyed,
                        // and the target is reporting a failure because of
                        // that. Just delete the network config.
                    }
                }
            } else if migration.either_side_completed() {
                // If either side reports that the migration has completed, set
                // the instance record's active Propolis ID to point at the new
                // VMM, and update the network configuration to point at that VMM.
                if new_runtime.propolis_id != Some(migration.target_propolis_id)
                {
                    info!(
                        log,
                        "instance update (migration completed): setting active \
                         VMM ID to target and updating network config";
                        "instance_id" => %instance_id,
                        "migration_id" => %migration.id,
                        "src_propolis_id" => %migration.source_propolis_id,
                        "target_propolis_id" => %migration.target_propolis_id,
                    );
                    let new_vmm = snapshot.target_vmm.as_ref().expect(
                        "if we have gotten here, there must be a target VMM",
                    );
                    debug_assert_eq!(new_vmm.id, migration.target_propolis_id);
                    new_runtime.propolis_id =
                        Some(migration.target_propolis_id);
                    update_required = true;
                    network_config = Some(NetworkConfigUpdate::to_vmm(new_vmm));
                }

                // If the target reports that the migration has completed,
                // unlink the migration (allowing a new one to begin). This has
                // to wait until the target has reported completion to ensure a
                // migration out of the target can't start until the migration
                // in has definitely finished.
                if migration.target_state == MigrationState::COMPLETED {
                    info!(
                        log,
                        "instance update (migration target completed): \
                         clearing migration IDs";
                        "instance_id" => %instance_id,
                        "migration_id" => %migration.id,
                        "src_propolis_id" => %migration.source_propolis_id,
                        "target_propolis_id" => %migration.target_propolis_id,
                    );
                    new_runtime.migration_id = None;
                    new_runtime.dst_propolis_id = None;
                    update_required = true;
                }

                // Even if the active VMM was destroyed (and we set the
                // instance's state to `NoVmm` above), it has successfully
                // migrated, so leave it in the VMM state.
                new_runtime.nexus_state = InstanceState::Vmm;
                // If the active VMM has also been destroyed, don't delete
                // virtual provisioning records while cleaning it up.
                deprovision = false;
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

impl NetworkConfigUpdate {
    fn to_vmm(vmm: &Vmm) -> Self {
        Self::Update {
            active_propolis_id: PropolisUuid::from_untyped_uuid(vmm.id),
            new_sled_id: vmm.sled_id,
        }
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

    // Write back the new instance record, releasing the instance updater lock,
    // and re-fetch the VMM and migration states. If they have changed in a way
    // that requires an additional update saga, attempt to execute an additional
    // update saga immediately.
    COMMIT_INSTANCE_UPDATES -> "commit_instance_updates" {
        + siu_commit_instance_updates
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
        // write back the updated state and release the instance lock.
        builder.append(commit_instance_updates_action());

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

async fn siu_commit_instance_updates(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let RealParams { serialized_authn, authz_instance, ref update, .. } =
        sagactx.saga_params::<RealParams>()?;
    unlock_instance_inner(
        &serialized_authn,
        &authz_instance,
        &sagactx,
        Some(&update.new_runtime),
    )
    .await?;
    let instance_id = authz_instance.id();

    // Check if the VMM or migration state has changed while the update saga was
    // running and whether an additional update saga is now required. If one is
    // required, try to start it.
    //
    // TODO(eliza): it would be nice if we didn't release the lock, determine
    // the needed updates, and then start a new start-instance-update saga that
    // re-locks the instance --- instead, perhaps we could keep the lock, and
    // try to start a new "actual" instance update saga that inherits our lock.
    // This way, we could also avoid computing updates required twice.
    // But, I'm a bit sketched out by the implications of not committing update
    // and dropping the lock in the same operation. This deserves more thought...
    if let Err(error) =
        chain_update_saga(&sagactx, authz_instance, serialized_authn).await
    {
        let osagactx = sagactx.user_data();
        // If starting the new update saga failed, DO NOT unwind this saga and
        // undo all the work we've done successfully! Instead, just kick the
        // instance-updater background task to try and start a new saga
        // eventually, and log a warning.
        warn!(
            osagactx.log(),
            "instance update: failed to start successor saga!";
            "instance_id" => %instance_id,
            "error" => %error,
        );
        osagactx.nexus().background_tasks.task_instance_updater.activate();
    }

    Ok(())
}

async fn chain_update_saga(
    sagactx: &NexusActionContext,
    authz_instance: authz::Instance,
    serialized_authn: authn::saga::Serialized,
) -> Result<(), anyhow::Error> {
    let opctx =
        crate::context::op_context_for_saga_action(sagactx, &serialized_authn);
    let osagactx = sagactx.user_data();
    let instance_id = authz_instance.id();

    // Fetch the state from the database again to see if we should immediately
    // run a new saga.
    let new_state = osagactx
        .datastore()
        .instance_fetch_all(&opctx, &authz_instance)
        .await
        .context("failed to fetch latest snapshot for instance")?;

    if let Some(update) =
        UpdatesRequired::for_snapshot(osagactx.log(), &new_state)
    {
        debug!(
            osagactx.log(),
            "instance update: additional updates required, preparing a \
             successor update saga...";
            "instance_id" => %instance_id,
            "update.new_runtime_state" => ?update.new_runtime,
            "update.network_config_update" => ?update.network_config,
            "update.destroy_active_vmm" => ?update.destroy_active_vmm,
            "update.destroy_target_vmm" => ?update.destroy_target_vmm,
            "update.deprovision" => update.deprovision,
        );
        let saga_dag = SagaInstanceUpdate::prepare(&Params {
            serialized_authn,
            authz_instance,
        })
        .context("failed to build new update saga DAG")?;
        let saga = osagactx
            .nexus()
            .sagas
            .saga_prepare(saga_dag)
            .await
            .context("failed to prepare new update saga")?;
        saga.start().await.context("failed to start successor update saga")?;
        // N.B. that we don't wait for the successor update saga to *complete*
        // here. We just want to make sure it starts.
        info!(
            osagactx.log(),
            "instance update: successor update saga started!";
            "instance_id" => %instance_id,
        );
    }

    Ok(())
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::db::model::Instance;
    use crate::app::db::model::VmmRuntimeState;
    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::test_helpers;
    use crate::external_api::params;
    use chrono::Utc;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
    use nexus_db_queries::db::lookup::LookupPath;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_test_utils::dev::poll;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PropolisUuid;
    use std::time::Duration;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "test-project";
    const INSTANCE_NAME: &str = "test-instance";

    // Most Nexus sagas have test suites that follow a simple formula: there's
    // usually a `test_saga_basic_usage_succeeds` that just makes sure the saga
    // basically works, and then a `test_actions_succeed_idempotently` test that
    // does the same thing, but runs every action twice. Then, there's usually a
    // `test_action_failures_can_unwind` test, and often also a
    // `test_action_failures_can_unwind_idempotently` test.
    //
    // For the instance-update saga, the test suite is a bit more complicated.
    // This saga will do a number of different things depending on the ways in
    // which the instance's migration and VMM records have changed since the
    // last update. Therefore, we want to test all of the possible branches
    // through the saga:
    //
    // 1. active VMM destroyed
    // 2. migration source completed
    // 3. migration target completed
    // 4. migration source VMM completed and was destroyed,
    // 5. migration target failed
    // 6. migration source failed

    async fn setup_test_project(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
        let project = create_project(&client, PROJECT_NAME).await;
        project.identity.id
    }

    async fn create_instance(
        client: &ClientTestContext,
    ) -> omicron_common::api::external::Instance {
        use omicron_common::api::external::{
            ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
        };
        let instances_url = format!("/v1/instances?project={}", PROJECT_NAME);
        object_create(
            client,
            &instances_url,
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: INSTANCE_NAME.parse().unwrap(),
                    description: format!("instance {:?}", INSTANCE_NAME),
                },
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_gibibytes_u32(2),
                hostname: INSTANCE_NAME.parse().unwrap(),
                user_data: b"#cloud-config".to_vec(),
                ssh_public_keys: Some(Vec::new()),
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::None,
                external_ips: vec![],
                disks: vec![],
                start: true,
            },
        )
        .await
    }

    /// Wait for an update saga to complete for the provided `instance`.
    async fn wait_for_update(
        cptestctx: &ControlPlaneTestContext,
        instance: &Instance,
    ) -> InstanceAndActiveVmm {
        // I'd be pretty surprised if an update saga takes longer than a minute
        // to complete in a unit test. If a saga hasn't run, failing the test in
        // a timely manner is helpful, so making this *too* long could be annoying...
        const MAX_WAIT: Duration = Duration::from_secs(60);

        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let initial_gen = instance.updater_gen;
        let log = &cptestctx.logctx.log;

        info!(
            log,
            "waiting for instance update to complete...";
            "instance_id" => %instance_id,
            "initial_gen" => ?initial_gen,
        );

        poll::wait_for_condition(
            || async {
                let state =
                    test_helpers::instance_fetch(cptestctx, instance_id).await;
                let instance = state.instance();
                if instance.updater_gen > initial_gen
                    && instance.updater_id.is_none()
                {
                    info!(
                        log,
                        "instance update completed!";
                        "instance_id" => %instance_id,
                        "initial_gen" => ?initial_gen,
                        "current_gen" => ?instance.updater_gen,
                    );
                    return Ok(state);
                }

                info!(
                    log,
                    "instance update has not yet completed...";
                    "instance_id" => %instance_id,
                    "initial_gen" => ?initial_gen,
                    "current_gen" => ?instance.updater_gen,
                    "current_updater" => ?instance.updater_id,
                );
                Err(poll::CondCheckError::NotYet::<()>)
            },
            // A lot can happen in one second...
            &Duration::from_secs(1),
            &MAX_WAIT,
        )
        .await
        .unwrap()
    }

    #[track_caller]
    fn assert_instance_unlocked(instance: &Instance) {
        assert_eq!(
            instance.updater_id, None,
            "instance updater lock should have been released"
        )
    }

    // === Active VMM destroyed tests ====

    #[nexus_test(server = crate::Server)]
    async fn test_active_vmm_destroyed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let (state, params) = setup_active_vmm_destroyed_test(cptestctx).await;

        // Run the instance-update saga.
        let nexus = &cptestctx.server.server_context().nexus;
        nexus
            .sagas
            .saga_execute::<SagaInstanceUpdate>(params)
            .await
            .expect("update saga should succeed");

        // Assert that the saga properly cleaned up the active VMM's resources.
        verify_active_vmm_destroyed(cptestctx, state.instance().id()).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_active_vmm_destroyed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let (state, params) = setup_active_vmm_destroyed_test(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let dag = create_saga_dag::<SagaInstanceUpdate>(params).unwrap();

        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            &cptestctx.server.server_context().nexus,
            dag,
        )
        .await;

        // Assert that the saga properly cleaned up the active VMM's resources.
        verify_active_vmm_destroyed(cptestctx, state.instance().id()).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_active_vmm_destroyed_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let (state, Params { serialized_authn, authz_instance }) =
            setup_active_vmm_destroyed_test(cptestctx).await;
        let instance_id =
            InstanceUuid::from_untyped_uuid(state.instance().id());

        test_helpers::action_failure_can_unwind::<SagaInstanceUpdate, _, _>(
            &cptestctx.server.server_context().nexus,
            || {
                Box::pin(async {
                    Params {
                        serialized_authn: serialized_authn.clone(),
                        authz_instance: authz_instance.clone(),
                    }
                })
            },
            || {
                Box::pin({
                    async {
                        let state = test_helpers::instance_fetch(
                            cptestctx,
                            instance_id,
                        )
                        .await;
                        // Unlike most other sagas, we actually don't unwind the
                        // work performed by an update saga, as we would prefer
                        // that at least some of it succeeds. The only thing
                        // that *needs* to be rolled back when an
                        // instance-update saga fails is that the updater lock
                        // *MUST* be released so that a subsequent saga can run.
                        assert_instance_unlocked(state.instance());
                    }
                })
            },
            &cptestctx.logctx.log,
        )
        .await;
    }

    // --- test helpers ---

    async fn setup_active_vmm_destroyed_test(
        cptestctx: &ControlPlaneTestContext,
    ) -> (InstanceAndActiveVmm, Params) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore().clone();
        let _project_id = setup_test_project(&client).await;

        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        // Poke the instance to get it into the Running state.
        let state = test_helpers::instance_fetch(cptestctx, instance_id).await;
        test_helpers::instance_simulate(cptestctx, &instance_id).await;
        // Wait for the instance update saga triggered by a transition to
        // Running to complete.
        let state = wait_for_update(cptestctx, state.instance()).await;
        // The instance should have an active VMM.
        let instance_runtime = state.instance().runtime();
        assert_eq!(instance_runtime.nexus_state, InstanceState::Vmm);
        assert!(instance_runtime.propolis_id.is_some());
        // Once we destroy the active VMM, we'll assert that the virtual
        // provisioning and sled resource records it owns have been deallocated.
        // In order to ensure we're actually testing the correct thing, let's
        // make sure that those records exist now --- if not, the assertions
        // later won't mean anything!
        assert!(
            !test_helpers::no_virtual_provisioning_resource_records_exist(
                cptestctx
            )
            .await,
            "we can't assert that a destroyed VMM instance update deallocates \
             virtual provisioning records if none exist!",
        );
        assert!(
            !test_helpers::no_virtual_provisioning_collection_records_using_instances(cptestctx)
                .await,
            "we can't assert that a destroyed VMM instance update deallocates \
             virtual provisioning records if none exist!",
        );
        assert!(
            !test_helpers::no_sled_resource_instance_records_exist(cptestctx)
                .await,
            "we can't assert that a destroyed VMM instance update deallocates \
             sled resource records if none exist!"
        );

        // Now, destroy the active VMM
        let vmm = state.vmm().as_ref().unwrap();
        let vmm_id = PropolisUuid::from_untyped_uuid(vmm.id);
        datastore
            .vmm_update_runtime(
                &vmm_id,
                &VmmRuntimeState {
                    time_state_updated: Utc::now(),
                    gen: Generation(vmm.runtime.gen.0.next()),
                    state: VmmState::Destroyed,
                },
            )
            .await
            .unwrap();

        let (_, _, authz_instance, ..) = LookupPath::new(&opctx, &datastore)
            .instance_id(instance_id.into_untyped_uuid())
            .fetch()
            .await
            .expect("test instance should be present in datastore");
        let params = Params {
            authz_instance,
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
        };
        (state, params)
    }

    async fn verify_active_vmm_destroyed(
        cptestctx: &ControlPlaneTestContext,
        instance_id: Uuid,
    ) {
        let state = test_helpers::instance_fetch(
            cptestctx,
            InstanceUuid::from_untyped_uuid(instance_id),
        )
        .await;

        // The instance's active VMM has been destroyed, so its state should
        // transition to `NoVmm`, and its active VMM ID should be unlinked. The
        // virtual provisioning and sled resources allocated to the instance
        // should be deallocated.
        assert_instance_unlocked(state.instance());
        assert!(state.vmm().is_none());
        let instance_runtime = state.instance().runtime();
        assert_eq!(instance_runtime.nexus_state, InstanceState::NoVmm);
        assert!(instance_runtime.propolis_id.is_none());
        assert!(
            test_helpers::no_virtual_provisioning_resource_records_exist(
                cptestctx
            )
            .await
        );
        assert!(test_helpers::no_virtual_provisioning_collection_records_using_instances(cptestctx).await);
        assert!(
            test_helpers::no_sled_resource_instance_records_exist(cptestctx)
                .await
        );
    }
}
