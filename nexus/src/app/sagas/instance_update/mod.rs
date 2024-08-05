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
//! `instance` record contains a UUID foreign key into the `vmm` table that
//! points to the `vmm` record for the Propolis process on which the instance is
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
//! In order to ensure that changes to the state of an instance are handled
//! reliably, we require that all multi-stage operations on an instance ---
//! i.e., operations which cannot be done atomically in a single database query
//! --- on an instance are performed  by a saga. The following sagas currently
//! touch the `instance` record:
//!
//! - [`instance_start`]
//! - [`instance_migrate`]
//! - [`instance_delete`]
//! - `instance_update` (this saga)
//!
//! For most of these sagas, the instance state machine itself guards against
//! potential race conditions. By considering the valid and invalid flows
//! through an instance's state machine, we arrive at some ground rules:
//!
//! - The `instance_migrate` and `instance_delete` sagas will only modify the
//!   instance record if the instance *has* an active Propolis ID.
//! - The `instance_start` and instance_delete` sagas will only modify the
//!   instance record if the instance does *not* have an active VMM.
//! - The presence of a migration ID prevents an `instance_migrate` saga from
//!   succeeding until the current migration is resolved (either completes or
//!   fails).
//! - Only the `instance_start` saga can set the instance's *active* Propolis
//!   ID, and it can only do this if there is currently no active Propolis.
//! - Only the `instance_migrate` saga can set the instance's *target* Propolis
//!   ID and migration ID, and it can only do that if these fields are unset.
//! - Only the `instance_update` saga can unset a migration ID and target
//!   Propolis ID, which it will do when handling an update from sled-agent that
//!   indicates that a migration has succeeded or failed.
//! - Only the `instance_update` saga can unset an instance's active Propolis
//!   ID, which it will do when handling an update from sled-agent that
//!   indicates that the VMM has been destroyed (peacefully or violently).
//!
//! For the most part, this state machine prevents race conditions where
//! multiple sagas mutate the same fields in the instance record, because the
//! states from which a particular transition may start limited. However, this
//! is not the case for the `instance-update` saga, which may need to run any
//! time a sled-agent publishes a new instance state. Therefore, this saga
//! ensures mutual exclusion using one of the only distributed locking schemes
//! in Omicron: the "instance updater lock".
//!
//! ### The Instance-Updater Lock, or, "Distributed RAII"
//!
//! Distributed locks [are scary][dist-locking]. One of the *scariest* things
//! about distributed locks is that a process can die[^1] while holding a lock,
//! which results in the protected resource (in this case, the `instance`
//! record) being locked forever.[^2] It would be good for that to not happen.
//! Fortunately, *if* (and only if) we promise to *only* ever acquire the
//! instance-updater lock inside of a saga, we can guarantee forward progress:
//! should a saga fail while holding the lock, it will unwind into a reverse
//! action that releases the lock. This is essentially the distributed
//! equivalent to holding a RAII guard in a Rust program: if the thread holding
//! the lock panics, it unwinds its stack, drops the [`std::sync::MutexGuard`],
//! and the rest of the system is not left in a deadlocked state. As long as we
//! ensure that the instance-updater lock is only ever acquired by sagas, and
//! that any saga holding a lock will reliably release it when it unwinds, we're
//! ... *probably* ... okay.
//!
//! When an `instance-update` saga is started, it attempts to [acquire the
//! updater lock][instance_updater_lock]. If the lock is already held by another
//! update saga, then the update saga completes immediately. Otherwise, the saga
//! then queries CRDB for the current state of the `instance` record, the active
//! and migration-target `vmm` records (if any exist), and the current
//! `migration` record (if one exists). This snapshot represents the state from
//! which the update will be applied, and must be read only after locking the
//! instance to ensure that it cannot race with another saga.
//!
//! This is where another of this saga's weird quirks shows up: the shape of the
//! saga DAG we wish to execute depends on this instance, active VMM, target
//! VMM, and migration. But, because the precondition for the saga state may
//! only be read once the lock is acquired, and --- as we discussed above ---
//! the instance-updater lock may only ever be acquired within a saga, we arrive
//! at a bit of a weird impasse: we can't determine what saga DAG to build
//! without looking at the initial state, but we can't load the state until
//! we've already started a saga. To solve this, we've split this saga into two
//! pieces: the first, `start-instance-update`, is a very small saga that just
//! tries to lock the instance, and upon doing so, loads the instance state from
//! the database and prepares and executes the "real" instance update saga. Once
//! the "real" saga starts, it "inherits" the lock from the start saga by
//! performing [the SQL equivalent equivalent of a compare-and-swap
//! operation][instance_updater_inherit_lock] with its own UUID.
//!
//! The DAG for the "real" update saga depends on the state read within the
//! lock, and since the lock was never released, that state remains valid for
//! its execution. As the final action of the update saga, the instance record's
//! new runtime state is written back to the database and the lock is released,
//! in a [single atomic operation][instance_updater_unlock]. Should the update
//! saga fail, it will release the inherited lock. And, if the unwinding update
//! saga unwinds into the start saga, that's fine, because a double-unlock is
//! prevented by the saga ID having changed in the "inherit lock" operation.
//!
//! ### Avoiding Missed Updates, or, "The `InstanceRuntimeState` Will Always Get Through"
//!
//! The lock operation we've described above is really more of a "try-lock"
//! operation: if the lock is already held, the saga trying to acquire it just
//! ends immediately, rather than waiting for the lock to be released. This begs
//! the question, "what happens if an instance update comes in while the lock is
//! held?" Do we just...leave it on the floor? Wasn't the whole point of this
//! Rube Goldberg mechanism of sagas to *prevent* instance state changes from
//! being missed?
//!
//! We solve this using an ~~even more layers of complexity~~defense-in-depth
//! approach. Together, a number of mechanisms exist to ensure that (a) an
//! instance whose VMM and migration states require an update saga will always
//! have an update saga run eventually, and (b) update sagas are run in as
//! timely a manner as possible.
//!
//! The first of these ~~layers of nonsense~~redundant systems to prevent missed
//! updates is perhaps the simplest one: _avoiding unnecessary update sagas_.
//! The `cpapi_instances_put` API endpoint and instance-watcher background tasks
//! handle changes to VMM and migration states by calling the
//! [`notify_instance_updated`] method, which writes the new states to the
//! database and (potentially) starts an update saga. Naively, this method would
//! *always* start an update saga, but remember that --- as we discussed
//! [above](#background) --- many VMM/migration state changes don't actually
//! require modifying the instance record. For example, if an instance's VMM
//! transitions from [`VmmState::Starting`] to [`VmmState::Running`], that
//! changes the instance's externally-visible effective state, but it does *not*
//! require an instance record update. By not starting an update saga unless one
//! is actually required, we reduce updater lock contention, so that the lock is
//! less likely to be held when VMM and migration states that actually *do*
//! require an update saga are published. The [`update_saga_needed`] function in
//! this module contains the logic for determining whether an update saga is
//! required.
//!
//! The second mechanism for ensuring updates are performed in a timely manner
//! is what I'm calling _saga chaining_. When the final action in an
//! instance-update saga writes back the instance record and releases the
//! updater lock, it will then perform a second query to read the instance, VMM,
//! and migration records. If the current state of the instance indicates that
//! another update saga is needed, then the completing saga will execute a new
//! start saga as its final action.
//!
//! The last line of defense is the `instance-updater` background task. This
//! task periodically queries the database to list instances which require
//! update sagas (either their active VMM is `Destroyed` or their active
//! migration has terminated) and are not currently locked by another update
//! saga. A new update saga is started for any such instances found. Because
//! this task runs periodically, it ensures that eventually, an update saga will
//! be started for any instance that requires one.[^3]
//!
//! The background task ensures that sagas are started eventually, but because
//! it only runs occasionally, update sagas started by it may be somewhat
//! delayed. To improve the timeliness of update sagas, we will also explicitly
//! activate the background task at any point where we know that an update saga
//! *should* run but we were not able to run it. If an update saga cannot be
//! started, whether by [`notify_instance_updated`], a `start-instance-update`
//! saga attempting to start its real saga, or an `instance-update` saga
//! chaining into a new one as its last action, the `instance-watcher`
//! background task is activated. Similarly, when a `start-instance-update` saga
//! fails to acquire the lock and exits, it activates the background task as
//! well. This ensures that we will attempt the update again.
//!
//! ### On Unwinding
//!
//! Typically, when a Nexus saga unwinds, each node's reverse action undoes any
//! changes made by the forward action. The `instance-update` saga, however, is
//! a bit different: most of its nodes don't have reverse actions that undo the
//! action they performed. This is because, unlike `instance-start`,
//! `instance-migrate`, or `instance-delete`, the instance-update saga is
//! **not** attempting to perform a state change for the instance that was
//! requested by a user. Instead, it is attempting to update the
//! database and networking configuration *to match a state change that has
//! already occurred.*
//!
//! Consider the following: if we run an `instance-start` saga, and the instance
//! cannot actually be started, of course we would want the unwinding saga to
//! undo any database changes it has made, because the instance was not actually
//! started. Failing to undo those changes when an `instance-start` saga unwinds
//! would mean the database is left in a state that does not reflect reality, as
//! the instance was not actually started. On the other hand, suppose an
//! instance's active VMM shuts down and we start an `instance-update` saga to
//! move it to the `Destroyed` state. Even if some action along the way fails, the
//! instance is still `Destroyed``; that state transition has *already happened*
//! on the sled, and unwinding the update saga cannot and should not un-destroy
//! the VMM.
//!
//! So, unlike other sagas, we want to leave basically anything we've
//! successfully done in place when unwinding, because even if the update is
//! incomplete, we have still brought Nexus' understanding of the instance
//! *closer* to reality. If there was something we weren't able to do, one of
//! the instance-update-related RPWs[^rpws] will start a new update saga to try
//! it again. Because saga actions are idempotent, attempting to do something
//! that was already successfully performed a second time isn't a problem, and
//! we don't need to undo it.
//!
//! The one exception to this is, as [discussed
//! above](#the-instance-updater-lock-or-distributed-raii), unwinding instance
//! update sagas MUST always release the instance-updater lock, so that a
//! subsequent saga can update the instance. Thus, the saga actions which lock
//! the instance have reverse actions that release the updater lock.
//!
//! [`instance_start`]: super::instance_start
//! [`instance_migrate`]: super::instance_migrate
//! [`instance_delete`]: super::instance_delete
//! [instance_updater_lock]:
//!     crate::app::db::datastore::DataStore::instance_updater_lock
//! [instance_updater_inherit_lock]:
//!     crate::app::db::datastore::DataStore::instance_updater_inherit_lock
//! [instance_updater_unlock]:
//!     crate::app::db::datastore::DataStore::instance_updater_unlock
//! [`notify_instance_updated`]: crate::app::Nexus::notify_instance_updated
//!
//! [dist-locking]:
//!     https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
//!
//! [^1]: And, if a process *can* die, well...we can assume it *will*.
//! [^2]: Barring human intervention.
//! [^3]: Even if the Nexus instance that processed the state update died
//!     between when it wrote the state to CRDB and when it started the
//!     requisite update saga!
//! [^rpws]: Either the `instance-updater` or `abandoned-vmm-reaper` background
//!     tasks, as appropriate.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::instance;
use crate::app::db::datastore::InstanceGestalt;
use crate::app::db::datastore::VmmStateUpdateResult;
use crate::app::db::lookup::LookupPath;
use crate::app::db::model::ByteCount;
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
/// provided [`VmmStateUpdateResult`].
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
    result: &VmmStateUpdateResult,
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

/// The set of updates to the instance and its owned resources to perform in
/// response to a VMM/migration state update.
///
/// Depending on the current state of the instance and its VMM(s) and migration,
/// an update saga may perform a variety of operations. Which operations need to
/// be performed for the current state snapshot of the instance, VMM, and
/// migration records is determined by the [`UpdatesRequired::for_snapshot`]
/// function.
#[derive(Debug, Deserialize, Serialize)]
struct UpdatesRequired {
    /// The new runtime state that must be written back to the database when the
    /// saga completes.
    new_runtime: InstanceRuntimeState,

    /// If this is [`Some`], the instance's active VMM with this UUID has
    /// transitioned to [`VmmState::Destroyed`], and its resources must be
    /// cleaned up by a [`destroyed`] subsaga.
    destroy_active_vmm: Option<PropolisUuid>,

    /// If this is [`Some`], the instance's migration target VMM with this UUID
    /// has transitioned to [`VmmState::Destroyed`], and its resources must be
    /// cleaned up by a [`destroyed`] subsaga.
    destroy_target_vmm: Option<PropolisUuid>,

    /// If this is [`Some`], the instance no longer has an active VMM, and its
    /// virtual provisioning resource records and Oximeter producer should be
    /// deallocated.
    deprovision: Option<Deprovision>,

    /// If this is [`Some`], then a network configuration update must be
    /// performed: either updating NAT configuration and V2P mappings when the
    /// instance has moved to a new sled, or deleting them if it is no longer
    /// incarnated.
    network_config: Option<NetworkConfigUpdate>,
}

#[derive(Debug, Deserialize, Serialize)]
enum NetworkConfigUpdate {
    Delete,
    Update { active_propolis_id: PropolisUuid, new_sled_id: Uuid },
}

/// Virtual provisioning counters to release when an instance no longer has a
/// VMM.
#[derive(Debug, Deserialize, Serialize)]
struct Deprovision {
    project_id: Uuid,
    cpus_diff: i64,
    ram_diff: ByteCount,
}

impl UpdatesRequired {
    fn for_snapshot(
        log: &slog::Logger,
        snapshot: &InstanceGestalt,
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
                    let id = PropolisUuid::from_untyped_uuid(active_vmm.id);
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
                    Some(id)
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
                // migrated, so leave it in the VMM state and don't deallocate
                // virtual provisioning records --- the instance is still
                // incarnated.
                new_runtime.nexus_state = InstanceState::Vmm;
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
            deprovision: deprovision.then(|| Deprovision {
                project_id: snapshot.instance.project_id,
                cpus_diff: i64::from(snapshot.instance.ncpus.0 .0),
                ram_diff: snapshot.instance.memory,
            }),
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

    update: UpdatesRequired,

    orig_lock: instance::UpdaterLock,
}

const INSTANCE_LOCK_ID: &str = "saga_instance_lock_id";
const INSTANCE_LOCK: &str = "updater_lock";
const NETWORK_CONFIG_UPDATE: &str = "network_config_update";

// instance update saga: actions

declare_saga_actions! {
    instance_update;

    // Become the instance updater.
    //
    // This action inherits the instance-updater lock from the
    // `start-instance-update` saga, which attempts to compare-and-swap in a new
    // saga UUID. This ensuring that only one child update saga is
    // actually allowed to proceed, even if the `start-instance-update` saga's
    // "fetch_instance_and_start_real_saga" executes multiple times, avoiding
    // duplicate work.
    //
    // Unwinding this action releases the updater lock. In addition, it
    // activates the `instance-updater` background task to ensure that a new
    // update saga is started in a timely manner, to perform the work that the
    // unwinding saga was *supposed* to do. Since this action only succeeds if
    // the lock was acquired, and this saga is only started if updates are
    // required, having this action activate the background task when unwinding
    // avoids unneeded activations when a saga fails just because it couldn't
    // get the lock.
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
        if params.update.deprovision.is_some() {
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
    let log = osagactx.log();

    debug!(
        log,
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

    info!(
        log,
        "instance_update: Now, I am become Updater, the destroyer of VMMs.";
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
    let lock = sagactx.lookup::<instance::UpdaterLock>(INSTANCE_LOCK)?;

    unwind_instance_lock(lock, serialized_authn, authz_instance, &sagactx)
        .await;

    // Now that we've released the lock, activate the `instance-updater`
    // background task to make sure that a new instance update saga is started
    // if the instance still needs to be updated.
    sagactx
        .user_data()
        .nexus()
        .background_tasks
        .task_instance_updater
        .activate();

    Ok(())
}

async fn siu_update_network_config(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let Params { ref serialized_authn, ref authz_instance, .. } =
        sagactx.saga_params()?;

    let update =
        sagactx.lookup::<NetworkConfigUpdate>(NETWORK_CONFIG_UPDATE)?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let log = osagactx.log();

    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    match update {
        NetworkConfigUpdate::Delete => {
            info!(
                log,
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
                log,
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

    Ok(())
}

async fn siu_release_virtual_provisioning(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams {
        ref serialized_authn, ref authz_instance, ref update, ..
    } = sagactx.saga_params::<RealParams>()?;
    let Some(Deprovision { project_id, cpus_diff, ram_diff }) =
        update.deprovision
    else {
        return Err(ActionError::action_failed(
            "a `siu_release_virtual_provisioning` action should never have \
             been added to the DAG if the update does not contain virtual \
             resources to deprovision"
                .to_string(),
        ));
    };
    let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

    let log = osagactx.log();
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, serialized_authn);

    let result = osagactx
        .datastore()
        .virtual_provisioning_collection_delete_instance(
            &opctx,
            instance_id,
            project_id,
            cpus_diff,
            ram_diff,
        )
        .await;
    match result {
        Ok(deleted) => {
            info!(
                log,
                "instance update (no VMM): deallocated virtual provisioning \
                 resources";
                "instance_id" => %instance_id,
                "records_deleted" => ?deleted,
            );
        }
        // Necessary for idempotency --- the virtual provisioning resources may
        // have been deleted already, that's fine.
        Err(Error::ObjectNotFound { .. }) => {
            info!(
                log,
                "instance update (no VMM): virtual provisioning record not \
                 found; perhaps it has already been deleted?";
                "instance_id" => %instance_id,
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
    let log = osagactx.log();

    info!(
        log,
        "instance update (no VMM): unassigning oximeter producer";
        "instance_id" => %authz_instance.id(),
    );
    crate::app::oximeter::unassign_producer(
        osagactx.datastore(),
        log,
        &opctx,
        &authz_instance.id(),
    )
    .await
    .map_err(ActionError::action_failed)
}

async fn siu_commit_instance_updates(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let RealParams { serialized_authn, authz_instance, ref update, .. } =
        sagactx.saga_params::<RealParams>()?;
    let lock = sagactx.lookup::<instance::UpdaterLock>(INSTANCE_LOCK)?;

    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, &serialized_authn);
    let log = osagactx.log();
    let nexus = osagactx.nexus();

    let instance_id = authz_instance.id();

    debug!(
        log,
        "instance update: committing new runtime state and unlocking...";
        "instance_id" => %instance_id,
        "new_runtime" => ?update.new_runtime,
        "lock" => ?lock,
    );

    let did_unlock = osagactx
        .datastore()
        .instance_commit_update(
            &opctx,
            &authz_instance,
            &lock,
            &update.new_runtime,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        log,
        "instance update: committed update new runtime state!";
        "instance_id" => %instance_id,
        "new_runtime" => ?update.new_runtime,
        "did_unlock" => ?did_unlock,
    );

    if update.network_config.is_some() {
        // If the update we performed changed networking configuration, activate
        // the V2P manager and VPC router RPWs, to ensure that the V2P mapping
        // and VPC for this instance are up to date.
        //
        // We do this here, rather than in the network config update action, so
        // that the instance's state in the database reflects the new rather
        // than the old state. Otherwise, if the networking RPW ran *before*
        // writing the new state to CRDB, it will run with the old VMM, rather
        // than the new one, and probably do nothing. Then, the networking
        // config update would be delayed until the *next* background task
        // activation. This way, we ensure that the RPW runs *after* we are in
        // the new state.

        nexus.background_tasks.task_v2p_manager.activate();
        nexus.vpc_needed_notify_sleds();
    }

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
        // If starting the new update saga failed, DO NOT unwind this saga and
        // undo all the work we've done successfully! Instead, just kick the
        // instance-updater background task to try and start a new saga
        // eventually, and log a warning.
        warn!(
            log,
            "instance update: failed to start successor saga!";
            "instance_id" => %instance_id,
            "error" => %error,
        );
        nexus.background_tasks.task_instance_updater.activate();
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
    let log = osagactx.log();

    let instance_id = authz_instance.id();

    // Fetch the state from the database again to see if we should immediately
    // run a new saga.
    let new_state = osagactx
        .datastore()
        .instance_fetch_all(&opctx, &authz_instance)
        .await
        .context("failed to fetch latest snapshot for instance")?;

    if let Some(update) = UpdatesRequired::for_snapshot(log, &new_state) {
        debug!(
            log,
            "instance update: additional updates required, preparing a \
             successor update saga...";
            "instance_id" => %instance_id,
            "update.new_runtime_state" => ?update.new_runtime,
            "update.network_config_update" => ?update.network_config,
            "update.destroy_active_vmm" => ?update.destroy_active_vmm,
            "update.destroy_target_vmm" => ?update.destroy_target_vmm,
            "update.deprovision" => ?update.deprovision,
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
            log,
            "instance update: successor update saga started!";
            "instance_id" => %instance_id,
        );
    }

    Ok(())
}

/// Unlock the instance record while unwinding.
///
/// This is factored out of the actual reverse action, because the `Params` type
/// differs between the start saga and the actual instance update sagas, both of
/// which must unlock the instance in their reverse actions.
async fn unwind_instance_lock(
    lock: instance::UpdaterLock,
    serialized_authn: &authn::saga::Serialized,
    authz_instance: &authz::Instance,
    sagactx: &NexusActionContext,
) {
    // /!\ EXTREMELY IMPORTANT WARNING /!\
    //
    // This comment is a message, and part of a system of messages. Pay
    // attention to it! The message is a warning about danger.
    //
    // The danger is still present in your time, as it was in ours. The danger
    // is to the instance record, and it can deadlock.
    //
    // When unwinding, unlocking an instance MUST succeed at all costs. This is
    // of the upmost importance. It's fine for unlocking an instance in a
    // forward action to fail, since the reverse action will still unlock the
    // instance when the saga is unwound. However, when unwinding, we must
    // ensure the instance is unlocked, no matter what. This is because a
    // failure to unlock the instance will leave the instance record in a
    // PERMANENTLY LOCKED state, since no other update saga will ever be
    // able to lock it again. If we can't unlock the instance here, our death
    // will ruin the instance record forever and it will only be able to be
    // removed by manual operator intervention. That would be...not great.
    //
    // Therefore, this action will retry the attempt to unlock the instance
    // until it either:
    //
    // - succeeds, and we know the instance is now unlocked.
    // - fails *because the instance doesn't exist*, in which case we can die
    //   happily because it doesn't matter if the instance is actually unlocked.
    use dropshot::HttpError;
    use futures::{future, TryFutureExt};
    use omicron_common::backoff;

    let osagactx = sagactx.user_data();
    let log = osagactx.log();
    let instance_id = authz_instance.id();
    let opctx =
        crate::context::op_context_for_saga_action(sagactx, &serialized_authn);

    debug!(
        log,
        "instance update: unlocking instance on unwind";
        "instance_id" => %instance_id,
        "lock" => ?lock,
    );

    const WARN_DURATION: std::time::Duration =
        std::time::Duration::from_secs(20);

    let did_unlock = backoff::retry_notify_ext(
        // This is an internal service query to CockroachDB.
        backoff::retry_policy_internal_service(),
        || {
            osagactx
            .datastore()
            .instance_updater_unlock(&opctx, authz_instance, &lock)
            .or_else(|err| future::ready(match err {
                // The instance record was not found. It's probably been
                // deleted. That's fine, we can now die happily, since we won't
                // be leaving the instance permanently locked.
                Error::ObjectNotFound { .. } => {
                    info!(
                        log,
                        "instance update: giving up on unlocking instance, \
                         as it no longer exists";
                        "instance_id" => %instance_id,
                        "lock" => ?lock,
                    );

                    Ok(false)
                },
                // All other errors should be retried.
                _ => Err(backoff::BackoffError::transient(err)),
            }))
        },
        |error, call_count, total_duration| {
            let http_error = HttpError::from(error.clone());
            if http_error.status_code.is_client_error() {
                error!(
                    log,
                    "instance update: client error while unlocking instance \
                     (likely requires operator intervention), retrying anyway";
                    "instance_id" => %instance_id,
                    "lock" => ?lock,
                    "error" => &error,
                    "call_count" => call_count,
                    "total_duration" => ?total_duration,
                );
            } else if total_duration > WARN_DURATION {
                warn!(
                    log,
                    "instance update: server error while unlocking instance,
                     retrying";
                    "instance_id" => %instance_id,
                    "lock" => ?lock,
                    "error" => &error,
                    "call_count" => call_count,
                    "total_duration" => ?total_duration,
                );
            } else {
                info!(
                    log,
                    "server error while recording saga event, retrying";
                    "instance_id" => %instance_id,
                    "lock" => ?lock,
                    "error" => &error,
                    "call_count" => call_count,
                    "total_duration" => ?total_duration,
                );
            }
        },
    )
    .await
    .expect("errors should be retried indefinitely");

    info!(
        log,
        "instance update: unlocked instance while unwinding";
        "instance_id" => %instance_id,
        "lock" => ?lock,
        "did_unlock" => did_unlock,
    );
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

    // Asserts that an instance record is in a consistent state (e.g., that all
    // state changes performed by the update saga are either applied atomically,
    // or have not been applied). This is particularly important to check when a
    // saga unwinds.
    #[track_caller]
    fn assert_instance_record_is_consistent(instance: &Instance) {
        let run_state = instance.runtime();
        match run_state.nexus_state {
            InstanceState::Vmm => assert!(
                run_state.propolis_id.is_some(),
                "if the instance record is in the `Vmm` state, it must have \
                 an active VMM\ninstance: {instance:#?}",
            ),
            state => assert_eq!(
                run_state.propolis_id, None,
                "if the instance record is in the `{state:?}` state, it must \
                 not have an active VMM\ninstance: {instance:#?}",
            ),
        }

        if run_state.dst_propolis_id.is_some() {
            assert!(
                run_state.migration_id.is_some(),
                "if the instance record has a target VMM ID, then it must \
                 also have a migration\ninstance: {instance:#?}",
            );
        }

        if run_state.migration_id.is_some() {
            assert_eq!(
                run_state.nexus_state,
                InstanceState::Vmm,
                "if an instance is migrating, it must be in the VMM state\n\
                 instance: {instance:#?}",
            );
        }
    }

    async fn after_unwinding(cptestctx: &ControlPlaneTestContext) {
        let state = test_helpers::instance_fetch_by_name(
            cptestctx,
            INSTANCE_NAME,
            PROJECT_NAME,
        )
        .await;
        let instance = state.instance();

        // Unlike most other sagas, we actually don't unwind the work performed
        // by an update saga, as we would prefer that at least some of it
        // succeeds. The only thing that *needs* to be rolled back when an
        // instance-update saga fails is that the updater lock *MUST* be
        // released so that a subsequent saga can run. See the section "on
        // unwinding" in the documentation comment at the top of the
        // instance-update module for details.
        assert_instance_unlocked(instance);
        // Additionally, we assert that the instance record is in a
        // consistent state, ensuring that all changes to the instance record
        // are atomic. This is important *because* we won't roll back changes
        // to the instance: if we're going to leave them in place, they can't
        // be partially applied, even if we unwound partway through the saga.
        assert_instance_record_is_consistent(instance);

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

    // === migration failed, target not destroyed ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_failed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Failed)
            .source(MigrationState::Failed, VmmState::Running)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_failed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Failed)
            .source(MigrationState::Failed, VmmState::Running)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_failed_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;

        let outcome = MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Failed)
            .source(MigrationState::Failed, VmmState::Running);

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

    // === migration failed, migration target destroyed tests ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_failed_destroyed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Destroyed)
            .source(MigrationState::Failed, VmmState::Running)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_failed_destroyed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Destroyed)
            .source(MigrationState::Failed, VmmState::Running)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_target_failed_destroyed_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;

        let outcome = MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Destroyed)
            .source(MigrationState::Failed, VmmState::Running);

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

    // === migration failed, migration source destroyed tests ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_source_failed_destroyed_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::InProgress, VmmState::Running)
            .source(MigrationState::Failed, VmmState::Destroyed)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_source_failed_destroyed_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::InProgress, VmmState::Running)
            .source(MigrationState::Failed, VmmState::Destroyed)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_source_failed_destroyed_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;

        let outcome = MigrationOutcome::default()
            .target(MigrationState::InProgress, VmmState::Running)
            .source(MigrationState::Failed, VmmState::Destroyed);

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

    // === migration failed, source and target both destroyed ===

    #[nexus_test(server = crate::Server)]
    async fn test_migration_failed_everyone_died_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Destroyed)
            .source(MigrationState::Failed, VmmState::Destroyed)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_failed_everyone_died_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _project_id = setup_test_project(&cptestctx.external_client).await;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;

        MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Destroyed)
            .source(MigrationState::Failed, VmmState::Destroyed)
            .setup_test(cptestctx, &other_sleds)
            .await
            .run_saga_basic_usage_succeeds_test(cptestctx)
            .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_migration_failed_everyone_died_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let other_sleds = test_helpers::add_sleds(cptestctx, 1).await;
        let _project_id = setup_test_project(&cptestctx.external_client).await;

        let outcome = MigrationOutcome::default()
            .target(MigrationState::Failed, VmmState::Destroyed)
            .source(MigrationState::Failed, VmmState::Destroyed);

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
        initial_state: InstanceGestalt,
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
                    &self.opctx,
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
                    &self.opctx,
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
            assert_instance_record_is_consistent(instance);

            if self.outcome.failed {
                assert_eq!(
                    instance_runtime.migration_id, None,
                    "migration ID must be unset when a migration has failed"
                );
                assert_eq!(
                    instance_runtime.dst_propolis_id, None,
                    "target VMM ID must be unset when a migration has failed"
                );
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

            // VThe instance has a VMM if (and only if):
            let has_vmm = if self.outcome.failed {
                // If the migration failed, the instance should have a VMM if
                // and only if the source VMM is still okay. It doesn't matter
                // whether the target is still there or not, because we didn't
                // migrate to it successfully.
                !src_destroyed
            } else {
                // Otherwise, if the migration succeeded, the instance should be
                // on the target VMM.
                true
            };

            assert_eq!(
                no_virtual_provisioning_resource_records_exist(cptestctx).await,
                !has_vmm,
                "virtual provisioning resource records must exist as long as \
                 the instance has a VMM",
            );
            assert_eq!(
                no_virtual_provisioning_collection_records_using_instances(
                    cptestctx
                )
                .await,
                !has_vmm,
                "virtual provisioning collection records must exist as long \
                 as the instance has a VMM",
            );

            let instance_state =
                if has_vmm { InstanceState::Vmm } else { InstanceState::NoVmm };
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
