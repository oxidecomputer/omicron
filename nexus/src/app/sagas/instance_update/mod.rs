// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance Update Saga
//!
//! ## Background
//!
//! The state of a VM instance, as understood by Nexus, consists of a
//! combination of database tables:
//!
//! - The `instance` table, owned exclusively by Nexus itself, represents the
//!   user-facing "logical" VM instance.
//! - The `vmm` table, which represents a "physical" Propolis VMM process on
//!   which a running instance is incarnated.
//! - The `migration` table, which represents the state of an in-progress live
//!   migration of an instance between two VMMs.
//!
//! When an instance is incarnated on a sled, the `propolis_id` field in an
//! `instance` record contains a UUID foreign key into the `vmm` table that points
//! to the `vmm` record for the Propolis process on which the instance is
//! currently running. If an instance is undergoing live migration, its record
//! additionally contains a `dst_propolis_id` foreign key pointing at the `vmm`
//! row representing the *target* Propolis process that it is migrating to, and
//! a `migration_id` foreign key into the `migration` table record tracking the
//! state of that migration.
//!
//! Sled-agents report the state of the VMMs they manage to Nexus. This occurs
//! when a VMM state transition occurs and the sled-agent *pushes* an update to
//! Nexus' `cpapi_instances_put` internal API endpoint, when a Nexus'
//! `instance-watcher` background task *pulls* instance states from sled-agents
//! periodically, or as the return value of an API call from Nexus to a
//! sled-agent. When a Nexus receives a new [`SledInstanceState`] from a
//! sled-agent through any of these mechanisms, the Nexus will write any changed
//! state to the `vmm` and/or `migration` tables directly on behalf of the
//! sled-agent.
//!
//! Although Nexus is technically the party responsible for the database query
//! that writes VMM and migration state updates received from sled-agent, it is
//! the sled-agent that *logically* "owns" these records. A row in the `vmm`
//! table represents a particular Propolis process running on a particular sled,
//! and that sled's sled-agent is the sole source of truth for that process. The
//! generation number for a `vmm` record is the property of the sled-agent
//! responsible for that VMM. Similarly, a `migration` record has separate
//! generation numbers for the source and target VMM states, which are owned by
//! the sled-agents responsible for the source and target Propolis processes,
//! respectively. If a sled-agent pushes a state update to a particular Nexus
//! instance and that Nexus fails to write the state to the database, that isn't
//! the end of the world: the sled-agent can simply retry with a different
//! Nexus, and the generation number, which is incremented exclusively by the
//! sled-agent, ensures that state changes are idempotent and ordered. If a
//! faulty Nexus were to return 200 OK to a sled-agent's call to
//! `cpapi_instances_put` but choose to simply eat the received instance state
//! update rather than writing it to the database, even *that* wouldn't
//! necessarily mean that the state change was gone forever: the
//! `instance-watcher` background task on another Nexus instance would
//! eventually poll the sled-agent's state and observe any changes that were
//! accidentally missed. This is all very neat and tidy, and we should feel
//! proud of ourselves for having designed such a nice little system.
//!
//! Unfortunately, when we look beyond the `vmm` and `migration` tables, things
//! rapidly become interesting (in the "may you live in interesting times"
//! sense). The `instance` record *cannot* be owned exclusively by anyone. The
//! logical instance state it represents is a gestalt that may consist of state
//! that exists in multiple VMM processes on multiple sleds, as well as
//! control-plane operations triggered by operator inputs and performed by
//! multiple Nexus instances. This is, as they say, "hairy". The neat and tidy
//! little state updates published by sled-agents to Nexus in the previous
//! paragraph may, in some cases, represent a state transition that also
//! requires changes to the `instance` table: for instance, a live migration may
//! have completed, necessitating a change in the instance's `propolis_id` to
//! point to the new VMM.
//!
//! Oh, and one other thing: the `instance` table record in turn logically
//! "owns" other resources, such as the virtual-provisioning counters that
//! represent rack-level resources allocated to the instance, and the instance's
//! network configuration. When the instance's state changes, these resources
//! owned by the instance may also need to be updated, such as changing the
//! network configuration to point at an instance's new home after a successful
//! migration, or deallocating virtual provisioning counters when an instance is
//! destroyed. Naturally, these updates must also be performed reliably and
//! inconsistent states must be avoided.
//!
//! Thus, we arrive here, at the instance-update saga.
//!
//! ## Theory of Operation
//!
//! Some ground rules:
use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::instance;
use crate::app::db::datastore::instance::InstanceUpdateResult;
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
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::SledInstanceState;
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

/// Returns `true` if an `instance-update` saga should be executed as a result
/// of writing the provided [`SledInstanceState`] to the database with the
/// provided [`InstanceUpdateResult`].
///
/// We determine this only after actually updating the database records,
/// because we don't know whether a particular VMM or migration state is
/// *new* or not until we know whether the corresponding database record has
/// actually changed (determined by the generation number). For example, when
/// an instance has migrated into a Propolis process, Propolis will continue
/// to report the migration in in the `Completed` state as part of all state
/// updates regarding that instance, but we no longer need to act on it if
/// the migration record has already been updated to reflect that the
/// migration has completed.
///
/// Once we know what rows have been updated, we can inspect the states
/// written to the DB to determine whether an instance-update saga is
/// required to bring the instance record's state in line with the new
/// VMM/migration states.
pub fn update_saga_needed(
    log: &slog::Logger,
    instance_id: InstanceUuid,
    state: &SledInstanceState,
    result: &InstanceUpdateResult,
) -> bool {
    // Currently, an instance-update saga is required if (and only if):
    //
    // - The instance's active VMM has transitioned to `Destroyed`. We don't
    //   actually know whether the VMM whose state was updated here was the
    //   active VMM or not, so we will always attempt to run an instance-update
    //   saga if the VMM was `Destroyed`.
    let vmm_needs_update = result.vmm_updated
        && state.vmm_state.state == nexus::VmmState::Destroyed;
    // - A migration in to this VMM has transitioned to a terminal state
    //   (`Failed` or `Completed`).
    let migrations = state.migrations();
    let migration_in_needs_update = result.migration_in_updated
        && migrations
            .migration_in
            .map(|migration| migration.state.is_terminal())
            .unwrap_or(false);
    // - A migration out from this VMM has transitioned to a terminal state
    //   (`Failed` or `Completed`).
    let migration_out_needs_update = result.migration_out_updated
        && migrations
            .migration_out
            .map(|migration| migration.state.is_terminal())
            .unwrap_or(false);
    // If any of the above conditions are true, prepare an instance-update saga
    // for this instance.
    let needed = vmm_needs_update
        || migration_in_needs_update
        || migration_out_needs_update;
    if needed {
        debug!(log,
            "new VMM runtime state from sled agent requires an \
             instance-update saga";
            "instance_id" => %instance_id,
            "propolis_id" => %state.propolis_id,
            "vmm_needs_update" => vmm_needs_update,
            "migration_in_needs_update" => migration_in_needs_update,
            "migration_out_needs_update" => migration_out_needs_update,
        );
    }
    needed
}

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
                    network_config = Some(NetworkConfigUpdate::to_vmm(new_vmm));
                    update_required = true;
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

async fn siu_release_virtual_provisioning(
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

async fn siu_unassign_oximeter_producer(
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
    use crate::app::OpContext;
    use crate::external_api::params;
    use chrono::Utc;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
    use nexus_db_queries::db::lookup::LookupPath;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::internal::nexus::{
        MigrationRuntimeState, MigrationState, Migrations,
    };
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PropolisUuid;
    use omicron_uuid_kinds::SledUuid;
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

    #[track_caller]
    fn assert_instance_unlocked(instance: &Instance) {
        assert_eq!(
            instance.updater_id, None,
            "instance updater lock should have been released"
        )
    }

    async fn after_unwinding(cptestctx: &ControlPlaneTestContext) {
        let state = test_helpers::instance_fetch_by_name(
            cptestctx,
            INSTANCE_NAME,
            PROJECT_NAME,
        )
        .await;
        let instance = state.instance();

        // Unlike most other sagas, we actually don't unwind the
        // work performed by an update saga, as we would prefer
        // that at least some of it succeeds. The only thing
        // that *needs* to be rolled back when an
        // instance-update saga fails is that the updater lock
        // *MUST* be released so that a subsequent saga can run.
        assert_instance_unlocked(instance);

        // Throw away the instance so that subsequent unwinding
        // tests also operate on an instance in the correct
        // preconditions to actually run the saga path we mean
        // to test.
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        // Depending on where we got to in the update saga, the
        // sled-agent may or may not actually be willing to stop
        // the instance, so just manually update the DB record
        // into a state where we can delete it to make sure
        // everything is cleaned up for the next run.
        cptestctx
            .server
            .server_context()
            .nexus
            .datastore()
            .instance_update_runtime(
                &instance_id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    gen: Generation(instance.runtime().gen.0.next()),
                    propolis_id: None,
                    dst_propolis_id: None,
                    migration_id: None,
                    nexus_state: InstanceState::NoVmm,
                },
            )
            .await
            .unwrap();

        test_helpers::instance_delete_by_name(
            cptestctx,
            INSTANCE_NAME,
            PROJECT_NAME,
        )
        .await;
    }

    // === Active VMM destroyed tests ====

    #[nexus_test(server = crate::Server)]
    async fn test_active_vmm_destroyed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
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
        let _project_id = setup_test_project(&cptestctx.external_client).await;
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
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let nexus = &cptestctx.server.server_context().nexus;
        test_helpers::action_failure_can_unwind::<SagaInstanceUpdate, _, _>(
            nexus,
            || {
                Box::pin(async {
                    let (_, params) =
                        setup_active_vmm_destroyed_test(cptestctx).await;
                    params
                })
            },
            || Box::pin(after_unwinding(cptestctx)),
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

        let opctx = test_helpers::test_opctx(cptestctx);
        let instance = create_instance(client).await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        // Poke the instance to get it into the Running state.
        test_helpers::instance_simulate(cptestctx, &instance_id).await;

        let state = test_helpers::instance_fetch(cptestctx, instance_id).await;
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

    // === migration source completed tests ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_source_completed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        MigrationOutcome::default()
            .source(MigrationState::Completed, VmmState::Stopping)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_source_completed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .source(MigrationState::Completed, VmmState::Stopping)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_actions_succeed_idempotently_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_source_completed_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;

        let outcome = MigrationOutcome::default()
            .source(MigrationState::Completed, VmmState::Stopping);

        test_helpers::action_failure_can_unwind::<SagaInstanceUpdate, _, _>(
            nexus,
            || {
                Box::pin(async {
                    outcome
                        .setup_test(cptestctx, &other_sleds)
                        .await
                        .saga_params()
                })
            },
            || Box::pin(after_unwinding(cptestctx)),
            &cptestctx.logctx.log,
        )
        .await;
    }

    // === migration target completed tests ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_completed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Completed, VmmState::Running)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_completed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Completed, VmmState::Running)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_actions_succeed_idempotently_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_completed_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let outcome = MigrationOutcome::default()
            .target(MigrationState::Completed, VmmState::Running);

        test_helpers::action_failure_can_unwind::<SagaInstanceUpdate, _, _>(
            nexus,
            || {
                Box::pin(async {
                    outcome
                        .setup_test(cptestctx, &other_sleds)
                        .await
                        .saga_params()
                })
            },
            || Box::pin(after_unwinding(cptestctx)),
            &cptestctx.logctx.log,
        )
        .await;
    }

    // === migration completed and source destroyed tests ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_completed_source_destroyed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Completed, VmmState::Running)
            .source(MigrationState::Completed, VmmState::Destroyed)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_completed_source_destroyed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Completed, VmmState::Running)
            .source(MigrationState::Completed, VmmState::Destroyed)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_actions_succeed_idempotently_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_completed_source_destroyed_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;

        let outcome = MigrationOutcome::default()
            .target(MigrationState::Completed, VmmState::Running)
            .source(MigrationState::Completed, VmmState::Destroyed);

        test_helpers::action_failure_can_unwind::<SagaInstanceUpdate, _, _>(
            nexus,
            || {
                Box::pin(async {
                    outcome
                        .setup_test(cptestctx, &other_sleds)
                        .await
                        .saga_params()
                })
            },
            || Box::pin(after_unwinding(cptestctx)),
            &cptestctx.logctx.log,
        )
        .await;
    }

    #[derive(Clone, Copy, Default)]
    struct MigrationOutcome {
        source: Option<(MigrationState, VmmState)>,
        target: Option<(MigrationState, VmmState)>,
        failed: bool,
    }

    impl MigrationOutcome {
        fn source(self, migration: MigrationState, vmm: VmmState) -> Self {
            let failed = self.failed
                || migration == MigrationState::Failed
                || vmm == VmmState::Failed;
            Self { source: Some((migration, vmm)), failed, ..self }
        }

        fn target(self, migration: MigrationState, vmm: VmmState) -> Self {
            let failed = self.failed
                || migration == MigrationState::Failed
                || vmm == VmmState::Failed;
            Self { target: Some((migration, vmm)), failed, ..self }
        }

        async fn setup_test(
            self,
            cptestctx: &ControlPlaneTestContext,
            other_sleds: &[(SledUuid, omicron_sled_agent::sim::Server)],
        ) -> MigrationTest {
            MigrationTest::setup(self, cptestctx, other_sleds).await
        }
    }

    struct MigrationTest {
        outcome: MigrationOutcome,
        instance_id: InstanceUuid,
        initial_state: InstanceSnapshot,
        authz_instance: authz::Instance,
        opctx: OpContext,
    }

    impl MigrationTest {
        fn target_vmm_id(&self) -> Uuid {
            self.initial_state
                .target_vmm
                .as_ref()
                .expect("migrating instance must have a target VMM")
                .id
        }

        fn src_vmm_id(&self) -> Uuid {
            self.initial_state
                .active_vmm
                .as_ref()
                .expect("migrating instance must have a source VMM")
                .id
        }

        async fn setup(
            outcome: MigrationOutcome,
            cptestctx: &ControlPlaneTestContext,
            other_sleds: &[(SledUuid, omicron_sled_agent::sim::Server)],
        ) -> Self {
            use crate::app::sagas::instance_migrate;

            let client = &cptestctx.external_client;
            let nexus = &cptestctx.server.server_context().nexus;
            let datastore = nexus.datastore();

            let opctx = test_helpers::test_opctx(cptestctx);
            let instance = create_instance(client).await;
            let instance_id =
                InstanceUuid::from_untyped_uuid(instance.identity.id);

            // Poke the instance to get it into the Running state.
            let state =
                test_helpers::instance_fetch(cptestctx, instance_id).await;
            test_helpers::instance_simulate(cptestctx, &instance_id).await;

            let vmm = state.vmm().as_ref().unwrap();
            let dst_sled_id =
                test_helpers::select_first_alternate_sled(vmm, other_sleds);
            let params = instance_migrate::Params {
                serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
                instance: state.instance().clone(),
                src_vmm: vmm.clone(),
                migrate_params: params::InstanceMigrate {
                    dst_sled_id: dst_sled_id.into_untyped_uuid(),
                },
            };

            nexus
                .sagas
                .saga_execute::<instance_migrate::SagaInstanceMigrate>(params)
                .await
                .expect("Migration saga should succeed");

            // Poke the destination sled just enough to make it appear to have a VMM.
            test_helpers::instance_single_step_on_sled(
                cptestctx,
                &instance_id,
                &dst_sled_id,
            )
            .await;

            let (_, _, authz_instance, ..) =
                LookupPath::new(&opctx, &datastore)
                    .instance_id(instance_id.into_untyped_uuid())
                    .fetch()
                    .await
                    .expect("test instance should be present in datastore");
            let initial_state = datastore
                .instance_fetch_all(&opctx, &authz_instance)
                .await
                .expect("test instance should be present in datastore");

            let this = Self {
                authz_instance,
                initial_state,
                outcome,
                opctx,
                instance_id,
            };
            if let Some((migration_state, vmm_state)) = this.outcome.source {
                this.update_src_state(cptestctx, vmm_state, migration_state)
                    .await;
            }

            if let Some((migration_state, vmm_state)) = this.outcome.target {
                this.update_target_state(cptestctx, vmm_state, migration_state)
                    .await;
            }

            this
        }

        async fn run_saga_basic_usage_succeeds_test(
            &self,
            cptestctx: &ControlPlaneTestContext,
        ) {
            // Run the instance-update saga.
            let nexus = &cptestctx.server.server_context().nexus;
            nexus
                .sagas
                .saga_execute::<SagaInstanceUpdate>(self.saga_params())
                .await
                .expect("update saga should succeed");

            // Check the results
            self.verify(cptestctx).await;
        }

        async fn run_actions_succeed_idempotently_test(
            &self,
            cptestctx: &ControlPlaneTestContext,
        ) {
            // Build the saga DAG with the provided test parameters
            let dag = create_saga_dag::<SagaInstanceUpdate>(self.saga_params())
                .unwrap();

            // Run the actions-succeed-idempotently test
            crate::app::sagas::test_helpers::actions_succeed_idempotently(
                &cptestctx.server.server_context().nexus,
                dag,
            )
            .await;

            // Check the results
            self.verify(cptestctx).await;
        }

        async fn update_src_state(
            &self,
            cptestctx: &ControlPlaneTestContext,
            vmm_state: VmmState,
            migration_state: MigrationState,
        ) {
            let src_vmm = self
                .initial_state
                .active_vmm
                .as_ref()
                .expect("must have an active VMM");
            let vmm_id = PropolisUuid::from_untyped_uuid(src_vmm.id);
            let new_runtime = nexus_db_model::VmmRuntimeState {
                time_state_updated: Utc::now(),
                gen: Generation(src_vmm.runtime.gen.0.next()),
                state: vmm_state,
            };

            let migration = self
                .initial_state
                .migration
                .as_ref()
                .expect("must have an active migration");
            let migration_out = MigrationRuntimeState {
                migration_id: migration.id,
                state: migration_state,
                gen: migration.source_gen.0.next(),
                time_updated: Utc::now(),
            };
            let migrations = Migrations {
                migration_in: None,
                migration_out: Some(&migration_out),
            };

            info!(
                cptestctx.logctx.log,
                "updating source VMM state...";
                "propolis_id" => %vmm_id,
                "new_runtime" => ?new_runtime,
                "migration_out" => ?migration_out,
            );

            cptestctx
                .server
                .server_context()
                .nexus
                .datastore()
                .vmm_and_migration_update_runtime(
                    vmm_id,
                    &new_runtime,
                    migrations,
                )
                .await
                .expect("updating migration source state should succeed");
        }

        async fn update_target_state(
            &self,
            cptestctx: &ControlPlaneTestContext,
            vmm_state: VmmState,
            migration_state: MigrationState,
        ) {
            let target_vmm = self
                .initial_state
                .target_vmm
                .as_ref()
                .expect("must have a target VMM");
            let vmm_id = PropolisUuid::from_untyped_uuid(target_vmm.id);
            let new_runtime = nexus_db_model::VmmRuntimeState {
                time_state_updated: Utc::now(),
                gen: Generation(target_vmm.runtime.gen.0.next()),
                state: vmm_state,
            };

            let migration = self
                .initial_state
                .migration
                .as_ref()
                .expect("must have an active migration");
            let migration_in = MigrationRuntimeState {
                migration_id: migration.id,
                state: migration_state,
                gen: migration.target_gen.0.next(),
                time_updated: Utc::now(),
            };
            let migrations = Migrations {
                migration_in: Some(&migration_in),
                migration_out: None,
            };

            info!(
                cptestctx.logctx.log,
                "updating target VMM state...";
                "propolis_id" => %vmm_id,
                "new_runtime" => ?new_runtime,
                "migration_in" => ?migration_in,
            );

            cptestctx
                .server
                .server_context()
                .nexus
                .datastore()
                .vmm_and_migration_update_runtime(
                    vmm_id,
                    &new_runtime,
                    migrations,
                )
                .await
                .expect("updating migration target state should succeed");
        }

        fn saga_params(&self) -> Params {
            Params {
                authz_instance: self.authz_instance.clone(),
                serialized_authn: authn::saga::Serialized::for_opctx(
                    &self.opctx,
                ),
            }
        }

        async fn verify(&self, cptestctx: &ControlPlaneTestContext) {
            info!(
                cptestctx.logctx.log,
                "checking update saga results after migration";
                "source_outcome" => ?self.outcome.source.as_ref(),
                "target_outcome" => ?self.outcome.target.as_ref(),
                "migration_failed" => self.outcome.failed,
            );

            use test_helpers::*;
            let state =
                test_helpers::instance_fetch(cptestctx, self.instance_id).await;
            let instance = state.instance();
            let instance_runtime = instance.runtime();

            let active_vmm_id = instance_runtime.propolis_id;

            assert_instance_unlocked(instance);

            if self.outcome.failed {
                todo!("eliza: verify migration-failed postconditions");
            } else {
                assert_eq!(
                    active_vmm_id,
                    Some(self.target_vmm_id()),
                    "target VMM must be in the active VMM position after migration success",
                );
                assert_eq!(instance_runtime.nexus_state, InstanceState::Vmm);
                if self
                    .outcome
                    .target
                    .as_ref()
                    .map(|(state, _)| state == &MigrationState::Completed)
                    .unwrap_or(false)
                {
                    assert_eq!(
                        instance_runtime.dst_propolis_id, None,
                        "target VMM ID must be unset once target VMM reports success",
                    );
                    assert_eq!(
                        instance_runtime.migration_id, None,
                        "migration ID must be unset once target VMM reports success",
                    );
                } else {
                    assert_eq!(
                        instance_runtime.dst_propolis_id,
                        Some(self.target_vmm_id()),
                        "target VMM ID must remain set until the target VMM reports success",
                    );
                    assert_eq!(
                        instance_runtime.migration_id,
                        self.initial_state.instance.runtime().migration_id,
                        "migration ID must remain set until target VMM reports success",
                    );
                }
            }

            let src_destroyed = self
                .outcome
                .source
                .as_ref()
                .map(|(_, state)| state == &VmmState::Destroyed)
                .unwrap_or(false);
            assert_eq!(
                self.src_resource_records_exist(cptestctx).await,
                !src_destroyed,
                "source VMM should exist if and only if the source hasn't been destroyed",
            );

            let target_destroyed = self
                .outcome
                .target
                .as_ref()
                .map(|(_, state)| state == &VmmState::Destroyed)
                .unwrap_or(false);

            assert_eq!(
                self.target_resource_records_exist(cptestctx).await,
                !target_destroyed,
                "target VMM should exist if and only if the target hasn't been destroyed",
            );

            let all_vmms_destroyed = src_destroyed && target_destroyed;

            assert_eq!(
                no_virtual_provisioning_resource_records_exist(cptestctx).await,
                all_vmms_destroyed,
                "virtual provisioning resource records must exist as long as \
                 the instance has a VMM",
            );
            assert_eq!(
                no_virtual_provisioning_collection_records_using_instances(
                    cptestctx
                )
                .await,
                all_vmms_destroyed,
                "virtual provisioning collection records must exist as long \
                 as the instance has a VMM",
            );

            let instance_state = if all_vmms_destroyed {
                InstanceState::NoVmm
            } else {
                InstanceState::Vmm
            };
            assert_eq!(instance_runtime.nexus_state, instance_state);
        }

        async fn src_resource_records_exist(
            &self,
            cptestctx: &ControlPlaneTestContext,
        ) -> bool {
            test_helpers::sled_resources_exist_for_vmm(
                cptestctx,
                PropolisUuid::from_untyped_uuid(self.src_vmm_id()),
            )
            .await
        }

        async fn target_resource_records_exist(
            &self,
            cptestctx: &ControlPlaneTestContext,
        ) -> bool {
            test_helpers::sled_resources_exist_for_vmm(
                cptestctx,
                PropolisUuid::from_untyped_uuid(self.target_vmm_id()),
            )
            .await
        }
    }
}
