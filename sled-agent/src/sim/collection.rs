// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent object collection

use super::config::SimMode;

use crate::nexus::NexusClient;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use omicron_common::api::external::Error;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use super::simulatable::Simulatable;

/// Simulates an object of type `S: Simulatable`.
///
/// Much of the simulation logic is commonized here in `SimObject` rather than
/// separately in the specific `Simulatable` types.
#[derive(Debug)]
struct SimObject<S: Simulatable> {
    /// The simulated object.
    object: S,
    /// Debug log
    log: Logger,
    /// Tx-side of a channel used to notify when async state changes begin
    channel_tx: Option<Sender<()>>,
}

/// Buffer size for channel used to communicate with each `SimObject`'s
/// background task
///
/// Messages sent on this channel trigger the task to simulate an asynchronous
/// state transition by sleeping for some interval and then updating the object
/// state.  When the background task updates the object state after sleeping, it
/// always looks at the current state to decide what to do.  As a result, we
/// never need to queue up more than one transition.  In turn, that means we
/// don't need (or want) a channel buffer larger than 1.  If we were to queue up
/// multiple messages in the buffer, the net effect would be exactly the same as
/// if just one message were queued.  (Because of what we said above, as part of
/// processing that message, the receiver will wind up handling all state
/// transitions requested up to the point where the first message is read.  If
/// another transition is requested after that point, another message will be
/// enqueued and the receiver will process that transition then.  There's no need
/// to queue more than one message.)  Even stronger: we don't want a larger
/// buffer because that would only cause extra laps through the sleep cycle,
/// which just wastes resources and increases the latency for processing the next
/// real transition request.
const SIM_CHANNEL_BUFFER_SIZE: usize = 0;

impl<S: Simulatable> SimObject<S> {
    /// Create a new `SimObject` with async state transitions automatically
    /// simulated by a background task.  The caller is expected to provide the
    /// background task that reads from the channel and advances the simulation.
    fn new_simulated_auto(
        initial_state: &S::CurrentState,
        log: Logger,
    ) -> (SimObject<S>, Receiver<()>) {
        info!(log, "created"; "initial_state" => ?initial_state);
        let (tx, rx) = futures::channel::mpsc::channel(SIM_CHANNEL_BUFFER_SIZE);
        (
            SimObject {
                object: S::new(initial_state.clone()),
                log,
                channel_tx: Some(tx),
            },
            rx,
        )
    }

    /// Create a new `SimObject` with state transitions simulated by explicit
    /// calls.  The only difference from the perspective of this struct is that
    /// we won't have a channel to which we send notifications when asynchronous
    /// state transitions begin.
    fn new_simulated_explicit(
        initial_state: &S::CurrentState,
        log: Logger,
    ) -> SimObject<S> {
        info!(log, "created"; "initial_state" => ?initial_state);
        SimObject {
            object: S::new(initial_state.clone()),
            log,
            channel_tx: None,
        }
    }

    /// Begin a transition to the requested object state `target`.  On success,
    /// returns whatever requested state change was dropped (because it was
    /// replaced), if any.  This is mainly used for testing.
    fn transition(
        &mut self,
        target: S::RequestedState,
    ) -> Result<Option<S::RequestedState>, Error> {
        let dropped = self.object.desired();
        let old_gen = self.object.generation();
        let action = self.object.request_transition(&target)?;
        if old_gen == self.object.generation() {
            info!(self.log, "noop transition"; "target" => ?target);
            return Ok(None);
        }

        info!(self.log, "transition";
            "target" => ?target,
            "dropped" => ?dropped,
            "current" => ?self.object.current(),
            "desired" => ?self.object.desired(),
            "action" => ?action,
        );

        // If this is an asynchronous transition, notify the background task to
        // simulate it.  There are a few possible error cases:
        //
        // (1) We fail to send the message because the channel's buffer is full.
        //     All we need to guarantee in the first place is that the receiver
        //     will receive a message at least once after this function is
        //     invoked.  If there's already a message in the buffer, we don't
        //     need to do anything else to achieve that.
        //
        // (2) We fail to send the message because the channel is disconnected.
        //     This would be a programmer error -- the contract between us and
        //     the receiver is that we shut down the channel first.  As a
        //     result, we panic if we find this case.
        //
        // (3) We failed to send the message for some other reason.  This
        //     appears impossible at the time of this writing.   It would be
        //     nice if the returned error type were implemented in a way that we
        //     could identify this case at compile time (e.g., using an enum),
        //     but that's not currently the case.
        if self.object.desired().is_some() {
            if let Some(ref mut tx) = self.channel_tx {
                let result = tx.try_send(());
                if let Err(error) = result {
                    assert!(!error.is_disconnected());
                    assert!(error.is_full());
                }
            }
        }

        Ok(dropped)
    }

    fn transition_finish(&mut self) {
        let current = self.object.current();
        let desired = self.object.desired();
        let action = self.object.execute_desired_transition();
        info!(self.log, "simulated transition finish";
            "state_before" => ?current,
            "requested_state" => ?desired,
            "state_after" => ?self.object.current(),
            "desired_after" => ?self.object.desired(),
            "action" => ?action,
        );
    }
}

/// The modes of operation for the "poke" operation.
pub enum PokeMode {
    /// Make the simulated object take a single step.
    SingleStep,

    /// Make the simulated object take steps until it no longer reports an
    /// explicit destination state.
    Drain,
}

/// A collection of `Simulatable` objects, each represented by a `SimObject`
///
/// This struct provides basic facilities for simulating SledAgent APIs for
/// instances and disks.
pub struct SimCollection<S: Simulatable> {
    /// handle to the Nexus API, used to notify about async transitions
    nexus_client: Arc<NexusClient>,
    /// logger for this collection
    log: Logger,
    /// simulation mode: automatic (timer-based) or explicit (using an API)
    sim_mode: SimMode,
    /// list of objects being simulated
    objects: Mutex<BTreeMap<Uuid, SimObject<S>>>,
}

impl<S: Simulatable + 'static> SimCollection<S> {
    /// Returns a new collection of simulated objects.
    pub fn new(
        nexus_client: Arc<NexusClient>,
        log: Logger,
        sim_mode: SimMode,
    ) -> SimCollection<S> {
        SimCollection {
            nexus_client,
            log,
            sim_mode,
            objects: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn size(&self) -> usize {
        self.objects.lock().await.len()
    }

    /// Body of the background task (one per `SimObject`) that simulates
    /// asynchronous transitions.  Each time we read a message from the object's
    /// channel, we sleep for a bit and then invoke `poke()` to complete whatever
    /// transition is currently outstanding.
    ///
    /// This is only used for `SimMode::Auto`.
    async fn sim_step(&self, id: Uuid, mut rx: Receiver<()>) {
        while rx.next().await.is_some() {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            self.sim_poke(id, PokeMode::Drain).await;
        }
    }

    /// Forcibly removes the object `id` from the collection without simulating
    /// any further state changes for it.
    pub async fn sim_force_remove(&self, id: Uuid) {
        let mut objects = self.objects.lock().await;
        let object = objects.remove(&id).unwrap();
        if let Some(mut tx) = object.channel_tx {
            tx.close_channel();
        }
    }

    /// Complete a desired asynchronous state transition for object `id`.
    /// This is invoked either by `sim_step()` (if the simulation mode is
    /// `SimMode::Auto`) or `instance_finish_transition` (if the simulation mode
    /// is `SimMode::Api).
    pub async fn sim_poke(&self, id: Uuid, mode: PokeMode) {
        let mut should_step = true;

        while should_step {
            let (new_state, to_destroy) = {
                // The object must be present in `objects` because it only gets
                // removed when it comes to rest in the "Destroyed" state, but
                // we can only get here if there's an asynchronous state
                // transition desired.
                //
                // We do as little as possible with the lock held.  In
                // particular, we want to finish this work before calling out to
                // notify the nexus.
                let mut objects = self.objects.lock().await;
                let mut object = objects.remove(&id).unwrap();
                object.transition_finish();
                let after = object.object.current().clone();

                if matches!(mode, PokeMode::SingleStep)
                    || object.object.desired().is_none()
                {
                    should_step = false;
                }

                if object.object.desired().is_none()
                    && object.object.ready_to_destroy()
                {
                    info!(&self.log, "object is ready to destroy";
                          "object_id" => %id);

                    (after, Some(object))
                } else {
                    objects.insert(id, object);
                    (after, None)
                }
            };

            // Notify Nexus that the object's state has changed.
            // TODO-robustness: If this fails, we need to put it on some list of
            // updates to retry later.
            S::notify(&self.nexus_client, &id, new_state).await.unwrap();

            // If the object came to rest destroyed, complete any async cleanup
            // needed now.
            // TODO-debugging It would be nice to have visibility
            // into objects that are cleaning up in case we have to debug
            // resource leaks here.
            // TODO-correctness Is it a problem that nobody
            // waits on the background task?  If we did it here, we'd deadlock,
            // since we're invoked from the background task.
            if let Some(destroyed_object) = to_destroy {
                if let Some(mut tx) = destroyed_object.channel_tx {
                    tx.close_channel();
                }
            }
        }
    }

    pub async fn sim_ensure_producer(
        self: &Arc<Self>,
        id: &Uuid,
        args: S::ProducerArgs,
    ) -> Result<(), Error> {
        self.objects
            .lock()
            .await
            .get_mut(id)
            .expect("Setting producer on object that does not exist")
            .object
            .set_producer(args)
            .await?;
        Ok(())
    }

    /// Move the object identified by `id` from its current state to the
    /// requested state `target`.  The object does not need to exist already; if
    /// not, it will be created from `current`.  (This is the only case where
    /// `current` is used.)
    ///
    /// This call is idempotent; it will take whatever actions are necessary
    /// (if any) to create the object and move it to the requested state.
    ///
    /// This function returns the updated state, but note that this may not be
    /// the requested state in the event that the transition is asynchronous.
    /// For example, if an Instance is "stopped", and the requested state is
    /// "running", the returned state will be "starting".  Subsequent
    /// asynchronous state transitions are reported via the notify() functions on
    /// the `NexusClient` object.
    pub async fn sim_ensure(
        self: &Arc<Self>,
        id: &Uuid,
        current: S::CurrentState,
        target: Option<S::RequestedState>,
    ) -> Result<S::CurrentState, Error> {
        let mut objects = self.objects.lock().await;
        let maybe_current_object = objects.remove(id);
        let (mut object, is_new) = {
            if let Some(current_object) = maybe_current_object {
                (current_object, false)
            } else {
                // Create a new SimObject
                let idc = *id;
                let log = self.log.new(o!("id" => idc.to_string()));

                if let SimMode::Auto = self.sim_mode {
                    let (object, rx) =
                        SimObject::new_simulated_auto(&current, log);
                    let selfc = Arc::clone(self);
                    tokio::spawn(async move {
                        selfc.sim_step(idc, rx).await;
                    });
                    (object, true)
                } else {
                    (SimObject::new_simulated_explicit(&current, log), true)
                }
            }
        };

        let rv = if let Some(target) = target {
            object.transition(target).map(|_| object.object.current())
        } else {
            Ok(current.clone())
        };

        if rv.is_ok() || !is_new {
            objects.insert(*id, object);
        }
        rv
    }

    pub async fn contains_key(self: &Arc<Self>, id: &Uuid) -> bool {
        self.objects.lock().await.contains_key(id)
    }
}

impl<S: Simulatable + Clone + 'static> SimCollection<S> {
    pub async fn sim_get_cloned_object(
        self: &Arc<Self>,
        id: &Uuid,
    ) -> Result<S, Error> {
        let objects = self.objects.lock().await;
        let instance = objects
            .get(id)
            .ok_or_else(|| Error::not_found_by_id(S::resource_type(), id))?;
        Ok(instance.object.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::sim::collection::SimObject;
    use crate::sim::disk::SimDisk;
    use crate::sim::instance::SimInstance;
    use crate::sim::simulatable::Simulatable;
    use chrono::Utc;
    use dropshot::test_util::LogContext;
    use futures::channel::mpsc::Receiver;
    use omicron_common::api::external::DiskState;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::nexus::DiskRuntimeState;
    use omicron_common::api::internal::nexus::SledVmmState;
    use omicron_common::api::internal::nexus::VmmRuntimeState;
    use omicron_common::api::internal::nexus::VmmState;
    use omicron_test_utils::dev::test_setup_log;
    use sled_agent_types::disk::DiskStateRequested;
    use sled_agent_types::instance::VmmStateRequested;

    fn make_instance(
        logctx: &LogContext,
    ) -> (SimObject<SimInstance>, Receiver<()>) {
        let vmm_state = VmmRuntimeState {
            state: VmmState::Starting,
            generation: Generation::new(),
            time_updated: Utc::now(),
        };

        let state =
            SledVmmState { vmm_state, migration_in: None, migration_out: None };

        SimObject::new_simulated_auto(&state, logctx.log.new(o!()))
    }

    fn make_disk(
        logctx: &LogContext,
        initial_state: DiskState,
    ) -> (SimObject<SimDisk>, Receiver<()>) {
        let initial_runtime = {
            DiskRuntimeState {
                disk_state: initial_state,
                generation: Generation::new(),
                time_updated: Utc::now(),
            }
        };

        SimObject::new_simulated_auto(&initial_runtime, logctx.log.new(o!()))
    }

    #[tokio::test]
    async fn test_sim_instance_creating_to_stop() {
        let logctx = test_setup_log("test_sim_instance_creating_to_stop");
        let (mut instance, mut rx) = make_instance(&logctx);
        let r1 = instance.object.current();

        info!(logctx.log, "new instance"; "state" => ?r1);
        assert_eq!(r1.vmm_state.state, VmmState::Starting);
        assert_eq!(r1.vmm_state.generation, Generation::new());

        // There's no asynchronous transition going on yet so a
        // transition_finish() shouldn't change anything.
        assert!(instance.object.desired().is_none());
        instance.transition_finish();
        let rnext = instance.object.current();
        assert!(instance.object.desired().is_none());
        assert_eq!(r1.vmm_state.time_updated, rnext.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, rnext.vmm_state.state);
        assert_eq!(r1.vmm_state.generation, rnext.vmm_state.generation);
        assert!(rx.try_next().is_err());

        // Stopping an instance that was never started synchronously destroys
        // its VMM.
        let rprev = r1;
        let dropped = instance.transition(VmmStateRequested::Stopped).unwrap();
        assert!(dropped.is_none());
        assert!(instance.object.desired().is_none());
        let rnext = instance.object.current();
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated >= rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, VmmState::Destroyed);
        assert!(rx.try_next().is_err());

        logctx.cleanup_successful();
    }

    /// Tests a SimInstance which transitions to running and is subsequently destroyed.
    /// This test observes an intermediate transition through "stopping" to
    /// accomplish this goal.
    #[tokio::test]
    async fn test_sim_instance_running_then_destroyed() {
        let logctx = test_setup_log("test_sim_instance_running_then_destroyed");
        let (mut instance, mut rx) = make_instance(&logctx);
        let r1 = instance.object.current();

        info!(logctx.log, "new instance"; "state" => ?r1);
        assert_eq!(r1.vmm_state.state, VmmState::Starting);
        assert_eq!(r1.vmm_state.generation, Generation::new());

        // There's no asynchronous transition going on yet so a
        // transition_finish() shouldn't change anything.
        assert!(instance.object.desired().is_none());
        instance.transition_finish();
        assert!(instance.object.desired().is_none());
        let rnext = instance.object.current();
        assert_eq!(r1.vmm_state.time_updated, rnext.vmm_state.time_updated);
        assert_eq!(r1.vmm_state.state, rnext.vmm_state.state);
        assert_eq!(r1.vmm_state.generation, rnext.vmm_state.generation);
        assert!(rx.try_next().is_err());

        // Set up a transition to Running. This has no immediate effect on the
        // simulated instance's state, but it does queue up a transition.
        let mut rprev = r1;
        assert!(rx.try_next().is_err());
        let dropped = instance.transition(VmmStateRequested::Running).unwrap();
        assert!(dropped.is_none());
        assert!(instance.object.desired().is_some());
        assert!(rx.try_next().is_err());

        // The VMM should still be Starting and its generation should not have
        // changed (the transition to Running is queued but hasn't executed).
        let rnext = instance.object.current();
        assert_eq!(rnext.vmm_state.generation, rprev.vmm_state.generation);
        assert_eq!(rnext.vmm_state.time_updated, rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, VmmState::Starting);
        rprev = rnext;

        // Now poke the instance. It should transition to Running.
        instance.transition_finish();
        let rnext = instance.object.current();
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated >= rprev.vmm_state.time_updated);
        assert!(instance.object.desired().is_none());
        assert!(rx.try_next().is_err());
        assert_eq!(rprev.vmm_state.state, VmmState::Starting);
        assert_eq!(rnext.vmm_state.state, VmmState::Running);
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        rprev = rnext;

        // There shouldn't be anything left on the queue now.
        instance.transition_finish();
        let rnext = instance.object.current();
        assert_eq!(rprev.vmm_state.generation, rnext.vmm_state.generation);

        // If we transition again to "Running", the process should complete
        // immediately.
        let dropped = instance.transition(VmmStateRequested::Running).unwrap();
        assert!(dropped.is_none());
        assert!(instance.object.desired().is_none());
        assert!(rx.try_next().is_err());
        let rnext = instance.object.current();
        assert_eq!(rnext.vmm_state.generation, rprev.vmm_state.generation);
        assert_eq!(rnext.vmm_state.time_updated, rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, rprev.vmm_state.state);
        rprev = rnext;

        // If we go back to any stopped state, we go through the async process
        // again.
        assert!(rx.try_next().is_err());
        let dropped = instance.transition(VmmStateRequested::Stopped).unwrap();
        assert!(dropped.is_none());
        assert!(instance.object.desired().is_some());
        let rnext = instance.object.current();
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated >= rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, VmmState::Stopping);
        rprev = rnext;

        // Propolis publishes its own transition to Stopping before it publishes
        // Stopped.
        instance.transition_finish();
        let rnext = instance.object.current();
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated >= rprev.vmm_state.time_updated);
        assert!(instance.object.desired().is_some());
        assert_eq!(rprev.vmm_state.state, VmmState::Stopping);
        assert_eq!(rnext.vmm_state.state, VmmState::Stopping);
        rprev = rnext;

        // The Stopping-to-Stopped transition is masked from external viewers of
        // the instance so that the instance doesn't appear to be Stopped before
        // it is ready to be started again.
        instance.transition_finish();
        let rnext = instance.object.current();
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated >= rprev.vmm_state.time_updated);
        assert!(instance.object.desired().is_some());
        assert_eq!(rprev.vmm_state.state, VmmState::Stopping);
        assert_eq!(rnext.vmm_state.state, VmmState::Stopping);
        rprev = rnext;

        // ...and Stopped (internally) goes to Destroyed. This transition is
        // hidden from external viewers of the instance by retiring the active
        // Propolis ID.
        instance.transition_finish();
        let rnext = instance.object.current();
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated >= rprev.vmm_state.time_updated);
        assert_eq!(rprev.vmm_state.state, VmmState::Stopping);
        assert_eq!(rnext.vmm_state.state, VmmState::Destroyed);
        logctx.cleanup_successful();
    }

    // Test reboot-related transitions.
    #[tokio::test]
    async fn test_sim_instance_reboot() {
        let logctx = test_setup_log("test_sim_instance_reboot");

        // Get an initial instance up to "Running".
        let (mut instance, _rx) = make_instance(&logctx);
        let r1 = instance.object.current();

        info!(logctx.log, "new instance"; "state" => ?r1);
        assert_eq!(r1.vmm_state.state, VmmState::Starting);
        assert_eq!(r1.vmm_state.generation, Generation::new());
        assert!(
            instance.transition(VmmStateRequested::Running).unwrap().is_none()
        );
        instance.transition_finish();
        let (rprev, rnext) = (r1, instance.object.current());

        // Chrono doesn't give us enough precision, so sleep a bit
        if cfg!(windows) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);

        // Now reboot the instance. This is dispatched to Propolis, which will
        // move to the Rebooting state and then back to Running.
        assert!(
            instance.transition(VmmStateRequested::Reboot).unwrap().is_none()
        );
        let (rprev, rnext) = (rnext, instance.object.current());
        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated > rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, VmmState::Rebooting);
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.object.current());

        // Chrono doesn't give us enough precision, so sleep a bit
        if cfg!(windows) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated > rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, VmmState::Rebooting);
        assert!(instance.object.desired().is_some());
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.object.current());

        // Chrono doesn't give us enough precision, so sleep a bit
        if cfg!(windows) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        assert!(rnext.vmm_state.generation > rprev.vmm_state.generation);
        assert!(rnext.vmm_state.time_updated > rprev.vmm_state.time_updated);
        assert_eq!(rnext.vmm_state.state, VmmState::Running);
        logctx.cleanup_successful();
    }

    /// Tests basic usage of `SimDisk`.  This is somewhat less exhaustive than
    /// the analogous tests for `SimInstance` because much of that functionality
    /// is implemented in `SimObject`, common to both.  So we don't bother
    /// verifying dropped state, messages sent to the background task, or some
    /// sanity checks around completion of async transitions when none is
    /// desired.
    #[tokio::test]
    async fn test_sim_disk_transition_to_detached_states() {
        let logctx =
            test_setup_log("test_sim_disk_transition_to_detached_states");
        let (mut disk, _rx) = make_disk(&logctx, DiskState::Creating);
        let r1 = disk.object.current();

        info!(logctx.log, "new disk"; "disk_state" => ?r1.disk_state);
        assert_eq!(r1.disk_state, DiskState::Creating);
        assert_eq!(r1.generation, Generation::new());

        // Try transitioning to every other detached state.
        let detached_states = vec![
            (DiskStateRequested::Detached, DiskState::Detached),
            (DiskStateRequested::Destroyed, DiskState::Destroyed),
            (DiskStateRequested::Faulted, DiskState::Faulted),
        ];
        let mut rprev = r1;
        for (requested, next) in detached_states {
            assert!(!rprev.disk_state.is_attached());
            disk.transition(requested.clone()).unwrap();
            let rnext = disk.object.current().clone();
            assert!(rnext.generation > rprev.generation);
            assert!(rnext.time_updated >= rprev.time_updated);
            assert_eq!(rnext.disk_state, next);
            rprev = rnext;
        }
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sim_disk_attach_then_destroy() {
        let logctx = test_setup_log("test_sim_disk_attach_then_destroy");
        let (mut disk, _rx) = make_disk(&logctx, DiskState::Creating);
        let r1 = disk.object.current();

        info!(logctx.log, "new disk"; "disk_state" => ?r1.disk_state);
        assert_eq!(r1.disk_state, DiskState::Creating);
        assert_eq!(r1.generation, Generation::new());

        let id = uuid::Uuid::new_v4();
        let rprev = r1;
        assert!(!rprev.disk_state.is_attached());
        assert!(
            disk.transition(DiskStateRequested::Attached(id))
                .unwrap()
                .is_none()
        );
        let rnext = disk.object.current();
        assert!(rnext.generation > rprev.generation);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert_eq!(rnext.disk_state, DiskState::Attaching(id));
        assert!(rnext.disk_state.is_attached());
        assert_eq!(id, *rnext.disk_state.attached_instance_id().unwrap());
        let rprev = rnext;

        disk.transition_finish();
        let rnext = disk.object.current();
        assert_eq!(rnext.disk_state, DiskState::Attached(id));
        assert!(rnext.generation > rprev.generation);
        assert!(rnext.time_updated >= rprev.time_updated);
        let rprev = rnext;

        disk.transition_finish();
        let rnext = disk.object.current();
        assert_eq!(rnext.generation, rprev.generation);
        assert_eq!(rnext.disk_state, DiskState::Attached(id));
        assert!(rnext.disk_state.is_attached());
        let rprev = rnext;

        // If we go straight to "Attached" again, there's nothing to do.
        assert!(
            disk.transition(DiskStateRequested::Attached(id))
                .unwrap()
                .is_none()
        );
        let rnext = disk.object.current();
        assert_eq!(rnext.generation, rprev.generation);
        let rprev = rnext;

        // It's illegal to go straight to attached to a different instance.
        let id2 = uuid::Uuid::new_v4();
        assert_ne!(id, id2);
        let error =
            disk.transition(DiskStateRequested::Attached(id2)).unwrap_err();
        if let Error::InvalidRequest { message } = error {
            assert_eq!("disk is already attached", message.external_message());
        } else {
            panic!("unexpected error type");
        }
        let rnext = disk.object.current();
        assert_eq!(rprev.generation, rnext.generation);
        let rprev = rnext;

        // If we go to a different detached state, we go through the async
        // transition again.
        disk.transition(DiskStateRequested::Detached).unwrap();
        let rnext = disk.object.current();
        assert!(rnext.generation > rprev.generation);
        assert_eq!(rnext.disk_state, DiskState::Detaching(id));
        assert!(rnext.disk_state.is_attached());
        let rprev = rnext;

        disk.transition_finish();
        let rnext = disk.object.current();
        assert_eq!(rnext.disk_state, DiskState::Detached);
        assert!(rnext.generation > rprev.generation);

        // Verify that it works fine to change directions in the middle of an
        // async transition.
        disk.transition(DiskStateRequested::Attached(id)).unwrap();
        assert_eq!(disk.object.current().disk_state, DiskState::Attaching(id));
        disk.transition(DiskStateRequested::Destroyed).unwrap();
        assert_eq!(disk.object.current().disk_state, DiskState::Detaching(id));
        disk.transition_finish();
        assert_eq!(disk.object.current().disk_state, DiskState::Destroyed);
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sim_disk_attach_then_fault() {
        let logctx = test_setup_log("test_sim_disk_attach_then_fault");
        let (mut disk, _rx) = make_disk(&logctx, DiskState::Creating);
        let r1 = disk.object.current();

        info!(logctx.log, "new disk"; "disk_state" => ?r1.disk_state);
        assert_eq!(r1.disk_state, DiskState::Creating);
        assert_eq!(r1.generation, Generation::new());

        let id = uuid::Uuid::new_v4();
        disk.transition(DiskStateRequested::Attached(id)).unwrap();
        disk.transition_finish();
        assert_eq!(disk.object.current().disk_state, DiskState::Attached(id));
        disk.transition(DiskStateRequested::Faulted).unwrap();
        assert_eq!(disk.object.current().disk_state, DiskState::Detaching(id));
        let error =
            disk.transition(DiskStateRequested::Attached(id)).unwrap_err();
        if let Error::InvalidRequest { message } = error {
            assert_eq!(
                "cannot attach from detaching",
                message.external_message()
            );
        } else {
            panic!("unexpected error type");
        }
        disk.transition_finish();
        assert_eq!(disk.object.current().disk_state, DiskState::Faulted);

        logctx.cleanup_successful();
    }
}
