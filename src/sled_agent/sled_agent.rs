/*!
 * Simulated sled agent implementation
 */

use super::ControllerClient;
use super::SimMode;

use crate::api_error::ApiError;
use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiDiskStateRequested;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceRuntimeStateRequested;
use crate::api_model::ApiInstanceState;
use async_trait::async_trait;
use chrono::Utc;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use slog::Logger;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/**
 * Simulates management of the control plane on a sled
 *
 * The current implementation simulates a server directly in this program.
 * **It's important to be careful about the interface exposed by this struct.**
 * The intent is for it to eventually be implemented using requests to a remote
 * server.  The tighter the coupling that exists now, the harder this will be to
 * move later.
 */
pub struct SledAgent {
    /** unique id for this server */
    pub id: Uuid,

    /** collection of simulated instances, indexed by instance uuid */
    instances: Arc<SimCollection<SimInstance>>,
    /** collection of simulated disks, indexed by disk uuid */
    disks: Arc<SimCollection<SimDisk>>,
}

impl SledAgent {
    /*
     * TODO-cleanup should this instantiate the ControllerClient it needs?
     * Should it take a Config object instead of separate id, sim_mode, etc?
     */
    /** Constructs a simulated SledAgent with the given uuid. */
    pub fn new_simulated_with_id(
        id: &Uuid,
        sim_mode: SimMode,
        log: Logger,
        ctlsc: Arc<ControllerClient>,
    ) -> SledAgent {
        info!(&log, "created simulated sled agent"; "sim_mode" => ?sim_mode);

        let instance_log = log.new(o!("kind" => "instances"));
        let disk_log = log.new(o!("kind" => "disks"));

        SledAgent {
            id: id.clone(),
            instances: Arc::new(SimCollection::new(
                Arc::clone(&ctlsc),
                instance_log,
                sim_mode.clone(),
            )),
            disks: Arc::new(SimCollection::new(
                Arc::clone(&ctlsc),
                disk_log,
                sim_mode.clone(),
            )),
        }
    }

    /**
     * Idempotently ensures that the given API Instance (described by
     * `api_instance`) exists on this server in the given runtime state
     * (described by `target`).
     */
    pub async fn instance_ensure(
        self: &Arc<Self>,
        instance_id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        Ok(self
            .instances
            .sim_ensure(&instance_id, initial_runtime, target)
            .await?)
    }

    /**
     * Idempotently ensures that the given API Disk (described by `api_disk`)
     * is attached (or not) as specified.  This simulates disk attach and
     * detach, similar to instance boot and halt.
     */
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_state: ApiDiskRuntimeState,
        target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        Ok(self.disks.sim_ensure(&disk_id, initial_state, target).await?)
    }

    pub async fn instance_poke(&self, id: Uuid) {
        self.instances.sim_poke(id).await;
    }

    pub async fn disk_poke(&self, id: Uuid) {
        self.disks.sim_poke(id).await;
    }
}

/**
 * Describes Oxide API objects that can be simulated here in the sled agent
 *
 * We only simulate these objects from the perspective of an API consumer, which
 * means for example accepting a request to boot it, reporting the current state
 * as "starting", and then some time later reporting that the state is
 * "running".
 *
 * This interface defines only associated functions, not constructors nor what
 * would traditionally be called "methods".  On the one hand, this approach is
 * relatively simple to reason about, since all the functions are stateless.  On
 * the other hand, the interface is a little gnarly.  That's largely because
 * this plugs into a more complex simulation mechanism (implemented by
 * `SimObject` and `SimCollection`) and this interface defines only the small
 * chunks of functionality that differ between types.  Still, this is cleaner
 * than what was here before!
 *
 * The basic idea is that for any type that we want to simulate (e.g.,
 * Instances), there's a `CurrentState` (which you could think of as "stopped",
 * "starting", "running", "stopping") and a `RequestedState` (which would only
 * be "stopped" and "running" -- you can't ask an Instance to transition to
 * "starting" or "stopping")
 *
 * (The term "state" here is a bit overloaded.  `CurrentState` refers to the
 * state of the object itself that's being simulated.  This might be an Instance
 * that is currently in state "running".  `RequestedState` refers to a requested
 * _change_ to the state of the object.  The state of the _simulated_ object
 * includes both of these: e.g., a "starting" Instance that is requested to be
 * "running".  So in most cases in the interface below, the state is represented
 * by a tuple of `(CurrentState, Option<RequestedState>)`.)
 *
 * Transitioning between states is always either synchronous (which means that
 * we make the transition immediately) or asynchronous (which means that we
 * first transition to some intermediate state and some time later finish the
 * transition to the requested state).  An Instance transition from "Stopped" to
 * "Destroyed" is synchronous.  An Instance transition from "Stopped" to
 * "Running" is asynchronous; it first goes to "Starting" and some time later
 * becomes "Running".
 *
 * It's expected that an object can begin another user-requested state
 * transition no matter what state it's in, although some particular transitions
 * may be disallowed (e.g., "reboot" from a stopped state).
 *
 * The implementor determines the set of possible states (via `CurrentState` and
 * `RequestedState`) as well as what transitions are allowed (via
 * `next_state_for_new_target()`).
 *
 * When an asynchronous state change completes, we notify the control plane via
 * the `notify()` function.
 */
/*
 * TODO-cleanup Among the awkward bits here is that the an object's state is
 * essentially represented by a tuple `(CurrentState, Option<RequestedState>)`,
 * but that's not represented anywhere.  Would it help to have that be a
 * first-class type?  Maybe it would be easier if there were a separate type
 * representing pairs of possible values here?  That sounds worse (because it
 * sounds MxN), but in practice many combinations are not legal and so it might
 * eliminate code that checks for these cases.
 */
#[async_trait]
trait Simulatable: fmt::Debug {
    /**
     * Represents a possible current runtime state of the simulated object.
     * For an Instance, you might think of the state as "starting" or "running",
     * etc., although in practice it's likely an object that includes this as
     * well as a generation counter and other metadata.
     */
    type CurrentState: Send + Clone + fmt::Debug;

    /**
     * Represents a possible requested state of the simulated object.  This is
     * often a subset of current states, since users may not be able to request
     * transitions to intermediate states.
     */
    type RequestedState: Send + Clone + fmt::Debug;

    /**
     * Given `current` (the current state of a simulated object), `pending`
     * (the requested state associated with a currently outstanding asynchronous
     * transition), and `target` (a new requested state), return a tuple
     * describing the next state and the requested state for the next
     * asynchronous transition, if any.
     *
     * If the requested transition is illegal (which should not be common),
     * return an appropriate `ApiError`.  If the requested transition can be
     * completed immediately (synchronously), the second field of the returned
     * tuple should be `None`.  If the requested transition is asynchronous, the
     * immediate next state should be returned as the first element and the
     * pending asynchronous request should be returned as the second.
     */
    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>;

    /**
     * Given `current` (the current state of a simulated object) and `pending`
     * (the requested state associated with a currently outstanding asynchronous
     * transition), return a tuple of the immediate next state and the requested
     * state for the next asynchronous transition, if any.  The return value has
     * the same semantics as for `next_state_for_new_target()`; however, this
     * function is not allowed to fail.  (Put differently, if this could fail,
     * then it should fail when the asynchronous state change was started, back
     * in next_state_for_new_target()`.)
     */
    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>);

    /**
     * Returns true if "state2" represents no meaningful change from "state1".
     * If possible, this should use a generation number or the like.
     */
    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool;

    /**
     * Returns true if the state `current` is a terminal state representing that
     * the object has been destroyed.
     */
    fn ready_to_destroy(current: &Self::CurrentState) -> bool;

    /**
     * Notifies the controller (via `csc`) about a new state (`current`) for the
     * object identified by `id`.
     */
    async fn notify(
        csc: &Arc<ControllerClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError>;
}

/**
 * Simulates an object of type `S: Simulatable`.
 *
 * Much of the simulation logic is commonized here in `SimObject` rather than
 * separately in the specific `Simulatable` types.
 */
#[derive(Debug)]
struct SimObject<S: Simulatable> {
    /** the current runtime state of the object */
    current_state: S::CurrentState,
    /** the most recently requested change to the object's runtime state */
    requested_state: Option<S::RequestedState>,
    /** debug log */
    log: Logger,
    /** tx-side of a channel used to notify when async state changes begin */
    channel_tx: Option<Sender<()>>,
}

/**
 * Buffer size for channel used to communicate with each `SimObject`'s
 * background task
 *
 * Messages sent on this channel trigger the task to simulate an asynchronous
 * state transition by sleeping for some interval and then updating the object
 * state.  When the background task updates the object state after sleeping, it
 * always looks at the current state to decide what to do.  As a result, we
 * never need to queue up more than one transition.  In turn, that means we
 * don't need (or want) a channel buffer larger than 1.  If we were to queue up
 * multiple messages in the buffer, the net effect would be exactly the same as
 * if just one message were queued.  (Because of what we said above, as part of
 * processing that message, the receiver will wind up handling all state
 * transitions requested up to the point where the first message is read.  If
 * another transition is requested after that point, another message will be
 * enqueued and the receiver will process that transition then.  There's no need
 * to queue more than one message.)  Even stronger: we don't want a larger
 * buffer because that would only cause extra laps through the sleep cycle,
 * which just wastes resources and increases the latency for processing the next
 * real transition request.
 */
const SIM_CHANNEL_BUFFER_SIZE: usize = 0;

impl<S: Simulatable> SimObject<S> {
    /**
     * Create a new `SimObject` with async state transitions automatically
     * simulated by a background task.  The caller is expected to provide the
     * background task that reads from the channel and advances the simulation.
     */
    fn new_simulated_auto(
        initial_state: &S::CurrentState,
        log: Logger,
    ) -> (SimObject<S>, Receiver<()>) {
        info!(log, "created"; "initial_state" => ?initial_state);
        let (tx, rx) = futures::channel::mpsc::channel(SIM_CHANNEL_BUFFER_SIZE);
        (
            SimObject {
                current_state: initial_state.clone(),
                requested_state: None,
                log,
                channel_tx: Some(tx),
            },
            rx,
        )
    }

    /**
     * Create a new `SimObject` with state transitions simulated by explicit
     * calls.  The only difference from the perspective of this struct is that
     * we won't have a channel to which we send notifications when asynchronous
     * state transitions begin.
     */
    fn new_simulated_explicit(
        initial_state: &S::CurrentState,
        log: Logger,
    ) -> SimObject<S> {
        info!(log, "created"; "initial_state" => ?initial_state);
        SimObject {
            current_state: initial_state.clone(),
            requested_state: None,
            log,
            channel_tx: None,
        }
    }

    /**
     * Begin a transition to the requested object state `target`.  On success,
     * returns whatever requested state change was dropped (because it was
     * replaced), if any.  This is mainly used for testing.
     */
    fn transition(
        &mut self,
        target: S::RequestedState,
    ) -> Result<Option<S::RequestedState>, ApiError> {
        let dropped = self.requested_state.clone();
        let state_before = self.current_state.clone();

        let (state_after, requested_state) =
            S::next_state_for_new_target(&state_before, &dropped, &target)?;

        if S::state_unchanged(&state_before, &state_after) {
            info!(self.log, "noop transition"; "target" => ?target);
            return Ok(None);
        }

        info!(self.log, "transition";
            "state_before" => ?state_before,
            "target" => ?target,
            "state_after" => ?state_after,
            "new_requested_state" => ?requested_state,
            "dropped" => ?dropped,
        );

        self.current_state = state_after;

        /*
         * If this is an asynchronous transition, notify the background task to
         * simulate it.  There are a few possible error cases:
         *
         * (1) We fail to send the message because the channel's buffer is full.
         *     All we need to guarantee in the first place is that the receiver
         *     will receive a message at least once after this function is
         *     invoked.  If there's already a message in the buffer, we don't
         *     need to do anything else to achieve that.
         *
         * (2) We fail to send the message because the channel is disconnected.
         *     This would be a programmer error -- the contract between us and
         *     the receiver is that we shut down the channel first.  As a
         *     result, we panic if we find this case.
         *
         * (3) We failed to send the message for some other reason.  This
         *     appears impossible at the time of this writing.   It would be
         *     nice if the returned error type were implemented in a way that we
         *     could identify this case at compile time (e.g., using an enum),
         *     but that's not currently the case.
         */
        if let Some(_) = &requested_state {
            self.requested_state = requested_state;
            if let Some(ref mut tx) = self.channel_tx {
                let result = tx.try_send(());
                if let Err(error) = result {
                    assert!(!error.is_disconnected());
                    assert!(error.is_full());
                }
            }
        } else {
            self.requested_state = None;
        }

        Ok(dropped)
    }

    fn transition_finish(&mut self) {
        if self.requested_state.is_none() {
            /*
             * Somebody must have requested a state change while we were
             * simulating a previous asynchronous one.  By definition, the new
             * one must also be asynchronous, and the first of the two calls to
             * `transition_finish()` will complete the new transition.  The
             * second one will find us here.
             * TODO-cleanup We could probably eliminate this case by not
             * sending a message to the background task if we were already in an
             * async transition.
             */
            info!(self.log, "noop transition finish"; "current" => ?self);
            return;
        }

        let requested_state = self.requested_state.take().unwrap();
        let (next_state, next_async) =
            S::next_state_for_async_transition_finish(
                &self.current_state,
                &requested_state,
            );
        info!(self.log, "simulated transition finish";
            "state_before" => ?self.current_state,
            "requested_state" => ?self.requested_state,
            "state_after" => ?next_state,
        );

        self.current_state = next_state;

        /*
         * If the async transition started another async transition, begin that
         * now.  It must be legal.
         */
        if let Some(new_target) = next_async {
            self.transition(new_target).unwrap();
        }
    }
}

/**
 * A collection of `Simulatable` objects, each represented by a `SimObject`
 *
 * This struct provides basic facilities for simulating SledAgent APIs for
 * instances and disks.
 */
struct SimCollection<S: Simulatable> {
    /** handle to the controller API, used to notify about async transitions */
    ctlsc: Arc<ControllerClient>,
    /** logger for this collection */
    log: Logger,
    /** simulation mode: automatic (timer-based) or explicit (using an API) */
    sim_mode: SimMode,
    /** list of objects being simulated */
    objects: Mutex<BTreeMap<Uuid, SimObject<S>>>,
}

impl<S: Simulatable + 'static> SimCollection<S> {
    /** Returns a new collection of simulated objects. */
    fn new(
        ctlsc: Arc<ControllerClient>,
        log: Logger,
        sim_mode: SimMode,
    ) -> SimCollection<S> {
        SimCollection {
            ctlsc,
            log,
            sim_mode,
            objects: Mutex::new(BTreeMap::new()),
        }
    }

    /**
     * Body of the background task (one per `SimObject`) that simulates
     * asynchronous transitions.  Each time we read a message from the object's
     * channel, we sleep for a bit and then invoke `poke()` to complete whatever
     * transition is currently outstanding.
     *
     * This is only used for `SimMode::Auto`.
     */
    async fn sim_step(&self, id: Uuid, mut rx: Receiver<()>) {
        while let Some(_) = rx.next().await {
            tokio::time::delay_for(Duration::from_millis(1500)).await;
            self.sim_poke(id).await;
        }
    }

    /**
     * Complete a pending asynchronous state transition for object `id`.
     * This is invoked either by `sim_step()` (if the simulation mode is
     * `SimMode::Auto`) or `instance_finish_transition` (if the simulation mode
     * is `SimMode::Api).
     */
    async fn sim_poke(&self, id: Uuid) {
        let (new_state, to_destroy) = {
            /*
             * The object must be present in `objects` because it only gets
             * removed when it comes to rest in the "Destroyed" state, but we
             * can only get here if there's an asynchronous state transition
             * pending.
             *
             * We do as little as possible with the lock held.  In particular,
             * we want to finish this work before calling out to notify the
             * controller.
             */
            let mut objects = self.objects.lock().await;
            let mut object = objects.remove(&id).unwrap();
            object.transition_finish();
            let after = object.current_state.clone();
            if object.requested_state.is_none()
                && S::ready_to_destroy(&object.current_state)
            {
                (after, Some(object))
            } else {
                objects.insert(id.clone(), object);
                (after, None)
            }
        };

        /*
         * Notify the controller that the object's state has changed.
         * TODO-robustness: If this fails, we need to put it on some list of
         * updates to retry later.
         */
        S::notify(&self.ctlsc, &id, new_state).await.unwrap();

        /*
         * If the object came to rest destroyed, complete any async cleanup
         * needed now.
         * TODO-debug It would be nice to have visibility into objects that
         * are cleaning up in case we have to debug resource leaks here.
         * TODO-correctness Is it a problem that nobody waits on the background
         * task?  If we did it here, we'd deadlock, since we're invoked from the
         * background task.
         */
        if let Some(destroyed_object) = to_destroy {
            if let Some(mut tx) = destroyed_object.channel_tx {
                tx.close_channel();
            }
        }
    }

    /**
     * Move the object identified by `id` from its current state to the
     * requested state `target`.  The object does not need to exist already; if
     * not, it will be created from `current`.  (This is the only case where
     * `current` is used.)
     *
     * This call is idempotent; it will take whatever actions are necessary
     * (if any) to create the object and move it to the requested state.
     *
     * This function returns the updated state, but note that this may not be
     * the requested state in the event that the transition is asynchronous.
     * For example, if an Instance is "stopped", and the requested state is
     * "running", the returned state will be "starting".  Subsequent
     * asynchronous state transitions are reported via the notify() functions on
     * the `ControllerClient` object.
     */
    async fn sim_ensure(
        self: &Arc<Self>,
        id: &Uuid,
        current: S::CurrentState,
        target: S::RequestedState,
    ) -> Result<S::CurrentState, ApiError> {
        let mut objects = self.objects.lock().await;
        let maybe_current_object = objects.remove(id);
        let (mut object, is_new) = {
            if let Some(current_object) = maybe_current_object {
                (current_object, false)
            } else {
                /* Create a new SimObject */
                let idc = id.clone();
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

        let rv = object
            .transition(target)
            .and_then(|_| Ok(object.current_state.clone()));
        if rv.is_ok() || !is_new {
            objects.insert(id.clone(), object);
        }
        rv
    }
}

/**
 * Simulated Instance (virtual machine), as created by the external Oxide API
 */
#[derive(Debug)]
struct SimInstance {}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = ApiInstanceRuntimeState;
    type RequestedState = ApiInstanceRuntimeStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        target: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        /*
         * TODO-cleanup it would be better if the type used to represent a
         * requested instance state did not allow you to even express this
         * value.
         */
        if target.reboot_wanted && target.run_state != ApiInstanceState::Running
        {
            return Err(ApiError::InvalidRequest {
                message: String::from(
                    "cannot reboot to a state other than \"running\"",
                ),
            });
        }

        let state_before = current.run_state.clone();

        /*
         * TODO-cleanup would it be possible to eliminate this possibility by
         * modifying the type used to represent the requested instance state?
         */
        if target.reboot_wanted
            && state_before != ApiInstanceState::Starting
            && state_before != ApiInstanceState::Running
            && (state_before != ApiInstanceState::Stopping
                || !current.reboot_in_progress)
        {
            return Err(ApiError::InvalidRequest {
                message: format!(
                    "cannot reboot instance in state \"{}\"",
                    state_before
                ),
            });
        }

        let mut state_after = match target.run_state {
            /*
             * For intermediate states (which don't really make sense to
             * request), just try to do the closest reasonable thing.
             * TODO-cleanup Use a different type here.
             */
            ApiInstanceState::Creating => &ApiInstanceState::Running,
            ApiInstanceState::Starting => &ApiInstanceState::Running,
            ApiInstanceState::Stopping => &ApiInstanceState::Stopped,

            /* This is the most common interesting case. */
            ref target_run_state => target_run_state,
        };

        /*
         * There's nothing to do if the current and target states are the same
         * AND either:
         *
         * - there's neither a reboot pending nor a reboot requested
         * - there's both a reboot pending and a reboot requested and
         *   the current reboot is still in the "Stopping" phase
         *
         * Otherwise, even if the states match, we may need to take action to
         * begin or cancel a reboot.
         *
         * Reboot can only be requested with a target state of "Running".
         * It doesn't make sense to reboot to any other state.
         * TODO-debug log a warning in this case or make it impossible to
         * represent?  Or validate it sooner?  TODO-cleanup if we create a
         * separate ApiInstanceStateRequested as discussed elsewhere, the
         * `Running` state could have a boolean indicating whether a reboot
         * is requested first.
         */
        let reb_pending = current.reboot_in_progress;
        let reb_wanted =
            *state_after == ApiInstanceState::Running && target.reboot_wanted;
        if *state_after == state_before
            && ((!reb_pending && !reb_wanted)
                || (reb_pending
                    && reb_wanted
                    && state_before == ApiInstanceState::Stopping))
        {
            let next_state = current.clone();
            let next_async = pending.clone();
            return Ok((next_state, next_async));
        }

        /*
         * If we're doing a reboot, then we've already verified that the target
         * run state is "Running", but for the rest of this function we'll treat
         * it like a transition to "Stopped" (with an extra bit telling us later
         * to transition again to Running).
         */
        if reb_wanted {
            state_after = &ApiInstanceState::Stopped;
        }

        /*
         * Depending on what state we're in and what state we're going to, we
         * may need to transition to an intermediate state before we can get to
         * the requested state.  In that case, we'll asynchronously simulate the
         * transition.
         */
        let (immed_next_state, need_async) =
            if state_before.is_stopped() && !state_after.is_stopped() {
                (&ApiInstanceState::Starting, true)
            } else if !state_before.is_stopped() && state_after.is_stopped() {
                (&ApiInstanceState::Stopping, true)
            } else {
                (state_after, false)
            };

        let next_state = ApiInstanceRuntimeState {
            run_state: immed_next_state.clone(),
            reboot_in_progress: reb_wanted,
            sled_uuid: current.sled_uuid.clone(),
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };

        let next_async = if need_async {
            Some(ApiInstanceRuntimeStateRequested {
                run_state: state_after.clone(),
                reboot_wanted: reb_wanted,
            })
        } else {
            None
        };

        Ok((next_state, next_async))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        /*
         * As documented above, `self.requested_state` is only non-None when
         * there's an asynchronous (simulated) transition in progress, and the
         * only such transitions start at "Starting" or "Stopping" and go to
         * "Running" or one of several stopped states, respectively.  Since we
         * checked `self.requested_state` above, we know we're in one of
         * these two transitions and assert that here.
         */
        let run_state_before = current.run_state.clone();
        let run_state_after = &pending.run_state;
        match run_state_before {
            ApiInstanceState::Starting => {
                assert_eq!(*run_state_after, ApiInstanceState::Running);
                assert!(!pending.reboot_wanted);
            }
            ApiInstanceState::Stopping => {
                assert!(run_state_after.is_stopped());
                assert_eq!(pending.reboot_wanted, current.reboot_in_progress);
                assert!(
                    !pending.reboot_wanted
                        || *run_state_after == ApiInstanceState::Stopped
                );
            }
            _ => panic!("async transition started for unexpected state"),
        };

        /*
         * Having verified all that, we can update the Instance's state.
         */
        let next_state = ApiInstanceRuntimeState {
            run_state: run_state_after.clone(),
            reboot_in_progress: pending.reboot_wanted,
            sled_uuid: current.sled_uuid.clone(),
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };

        let next_async = if next_state.reboot_in_progress {
            assert_eq!(*run_state_after, ApiInstanceState::Stopped);
            Some(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            })
        } else {
            None
        };

        (next_state, next_async)
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        return state1.gen == state2.gen;
    }

    fn ready_to_destroy(current: &Self::CurrentState) -> bool {
        current.run_state == ApiInstanceState::Destroyed
    }

    async fn notify(
        csc: &Arc<ControllerClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        /*
         * Notify the controller that the instance state has changed.  The
         * sled agent is authoritative for the runtime state, and we use a
         * generation number here so that calls processed out of order do not
         * settle on the wrong value.
         */
        csc.notify_instance_updated(id, &current).await
    }
}

/**
 * Simulated Disk (network block device), as created by the external Oxide API
 *
 * See `Simulatable` for how this works.
 */
#[derive(Debug)]
struct SimDisk {}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = ApiDiskRuntimeState;
    type RequestedState = ApiDiskStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        let state_before = &current.disk_state;
        let state_after = next;

        let to_do = match (state_before, state_after) {
            /*
             * It's conceivable that we'd be asked to transition from a state to
             * itself, in which case we also don't need to do anything.
             */
            (
                ApiDiskState::Attached(id1),
                ApiDiskStateRequested::Attached(id2),
            ) => {
                /*
                 * It's not legal to ask us to go from attached to one instance
                 * to attached to another.
                 */
                assert!(pending.is_none());
                if id1 != id2 {
                    return Err(ApiError::InvalidRequest {
                        message: format!("disk is already attached"),
                    });
                }

                None
            }
            (ApiDiskState::Detached, ApiDiskStateRequested::Detached) => {
                assert!(pending.is_none());
                None
            }
            (ApiDiskState::Destroyed, ApiDiskStateRequested::Destroyed) => {
                assert!(pending.is_none());
                None
            }
            (ApiDiskState::Faulted, ApiDiskStateRequested::Faulted) => {
                assert!(pending.is_none());
                None
            }

            /*
             * If we're going from any unattached state to "Attached" (the only
             * requestable attached state), the appropriate next state is
             * "Attaching", and it will be an asynchronous transition to
             * "Attached".  This is allowed even for "Destroyed" -- the caller
             * is responsible for disallowing this if that's what's intended.
             */
            (ApiDiskState::Creating, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(id.clone()), Some(state_after)))
            }
            (ApiDiskState::Detached, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(id.clone()), Some(state_after)))
            }
            (ApiDiskState::Destroyed, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(id.clone()), Some(state_after)))
            }
            (ApiDiskState::Faulted, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(id.clone()), Some(state_after)))
            }

            /*
             * If we're currently attaching, it's only legal to try to attach to
             * the same thing (in which case it's a noop).
             * TODO-cleanup would it be more consistent with our intended
             * interface (which is to let the controller just say what it wants
             * and have us do the work) to have this work and go through
             * detaching first?
             */
            (
                ApiDiskState::Attaching(id1),
                ApiDiskStateRequested::Attached(id2),
            ) => {
                if id1 != id2 {
                    return Err(ApiError::InvalidRequest {
                        message: format!("disk is already attached"),
                    });
                }

                Some((ApiDiskState::Attaching(id2.clone()), Some(state_after)))
            }

            (
                ApiDiskState::Detaching(_),
                ApiDiskStateRequested::Attached(_),
            ) => {
                return Err(ApiError::InvalidRequest {
                    message: format!("cannot attach while detaching"),
                });
            }

            /*
             * If we're going from any attached state to any detached state,
             * then we'll go straight to "Detaching" en route to the new state.
             */
            (from_state, to_state)
                if from_state.is_attached() && !to_state.is_attached() =>
            {
                let id = from_state.attached_instance_id().unwrap();
                Some((ApiDiskState::Detaching(id.clone()), Some(to_state)))
            }

            /*
             * The only remaining options are transitioning from one detached
             * state to a different one, in which case we can go straight there
             * with no need for an asynchronous transition.
             */
            (from_state, ApiDiskStateRequested::Destroyed) => {
                assert!(!from_state.is_attached());
                Some((ApiDiskState::Destroyed, None))
            }

            (from_state, ApiDiskStateRequested::Detached) => {
                assert!(!from_state.is_attached());
                Some((ApiDiskState::Detached, None))
            }

            (from_state, ApiDiskStateRequested::Faulted) => {
                assert!(!from_state.is_attached());
                Some((ApiDiskState::Faulted, None))
            }
        };

        if to_do.is_none() {
            return Ok((current.clone(), pending.clone()));
        }

        let (immed_next_state, next_async) = to_do.unwrap();
        let next_state = ApiDiskRuntimeState {
            disk_state: immed_next_state,
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };

        Ok((next_state, next_async.map(|s| s.clone())))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        let state_before = &current.disk_state;
        let is_detaching = match &current.disk_state {
            ApiDiskState::Attaching(_) => false,
            ApiDiskState::Detaching(_) => true,
            _ => panic!("async transition was not in progress"),
        };
        let next_state = match pending {
            ApiDiskStateRequested::Attached(id) => {
                let id = id.clone();
                assert_eq!(state_before, &ApiDiskState::Attaching(id));
                ApiDiskState::Attached(id)
            }
            ApiDiskStateRequested::Faulted => {
                assert!(is_detaching);
                ApiDiskState::Faulted
            }
            ApiDiskStateRequested::Destroyed => {
                assert!(is_detaching);
                ApiDiskState::Destroyed
            }
            ApiDiskStateRequested::Detached => {
                assert!(is_detaching);
                ApiDiskState::Detached
            }
        };
        let next_runtime = ApiDiskRuntimeState {
            disk_state: next_state,
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };
        (next_runtime, None)
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        return state1.gen == state2.gen;
    }

    fn ready_to_destroy(current: &Self::CurrentState) -> bool {
        ApiDiskState::Destroyed == current.disk_state
    }

    async fn notify(
        csc: &Arc<ControllerClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        csc.notify_disk_updated(id, &current).await
    }
}

#[cfg(test)]
mod test {
    use super::SimDisk;
    use super::SimInstance;
    use super::SimObject;
    use crate::api_error::ApiError;
    use crate::api_model::ApiDiskRuntimeState;
    use crate::api_model::ApiDiskState;
    use crate::api_model::ApiDiskStateRequested;
    use crate::api_model::ApiInstanceRuntimeState;
    use crate::api_model::ApiInstanceRuntimeStateRequested;
    use crate::api_model::ApiInstanceState;
    use crate::test_util::test_setup_log;
    use chrono::Utc;
    use dropshot::test_util::LogContext;
    use futures::channel::mpsc::Receiver;

    fn make_instance(
        logctx: &LogContext,
        initial_state: ApiInstanceState,
    ) -> (SimObject<SimInstance>, Receiver<()>) {
        let now = Utc::now();
        let initial_runtime = {
            ApiInstanceRuntimeState {
                run_state: initial_state,
                reboot_in_progress: false,
                sled_uuid: uuid::Uuid::new_v4(),
                gen: 1,
                time_updated: now,
            }
        };

        SimObject::new_simulated_auto(&initial_runtime, logctx.log.new(o!()))
    }

    fn make_disk(
        logctx: &LogContext,
        initial_state: ApiDiskState,
    ) -> (SimObject<SimDisk>, Receiver<()>) {
        let now = Utc::now();
        let initial_runtime = {
            ApiDiskRuntimeState {
                disk_state: initial_state,
                gen: 1,
                time_updated: now,
            }
        };

        SimObject::new_simulated_auto(&initial_runtime, logctx.log.new(o!()))
    }

    /**
     * Tests SimObject in general when backed by a SimDisk in particular, for
     * non-reboot-related transitions.
     */
    #[tokio::test]
    async fn test_sim_instance() {
        let logctx = test_setup_log("test_sim_instance").await;
        let (mut instance, mut rx) =
            make_instance(&logctx, ApiInstanceState::Creating);
        let r1 = instance.current_state.clone();

        info!(logctx.log, "new instance"; "run_state" => ?r1.run_state);
        assert_eq!(r1.run_state, ApiInstanceState::Creating);
        assert_eq!(r1.gen, 1);

        /*
         * There's no asynchronous transition going on yet so a
         * transition_finish() shouldn't change anything.
         */
        assert!(instance.requested_state.is_none());
        instance.transition_finish();
        assert!(instance.requested_state.is_none());
        assert_eq!(&r1.time_updated, &instance.current_state.time_updated);
        assert_eq!(&r1.run_state, &instance.current_state.run_state);
        assert_eq!(r1.gen, instance.current_state.gen);
        assert!(rx.try_next().is_err());

        /*
         * We should be able to transition immediately to any other stopped
         * state.  We can't do this for "Creating" because transition() treats
         * that as a transition to "Running".
         */
        let stopped_states = vec![
            ApiInstanceState::Stopped,
            ApiInstanceState::Repairing,
            ApiInstanceState::Failed,
            ApiInstanceState::Destroyed,
        ];
        let mut rprev = r1;
        for state in stopped_states {
            assert!(rprev.run_state.is_stopped());
            let dropped = instance
                .transition(ApiInstanceRuntimeStateRequested {
                    run_state: state.clone(),
                    reboot_wanted: false,
                })
                .unwrap();
            assert!(dropped.is_none());
            assert!(instance.requested_state.is_none());
            let rnext = instance.current_state.clone();
            assert!(rnext.gen > rprev.gen);
            assert!(rnext.time_updated >= rprev.time_updated);
            assert_eq!(rnext.run_state, state);
            assert!(rx.try_next().is_err());
            rprev = rnext;
        }

        /*
         * Now, if we transition to "Running", we must go through the async
         * process.
         */
        assert!(rprev.run_state.is_stopped());
        assert!(rx.try_next().is_err());
        let dropped = instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            })
            .unwrap();
        assert!(dropped.is_none());
        assert!(instance.requested_state.is_some());
        assert!(rx.try_next().is_ok());
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        assert!(!rnext.run_state.is_stopped());
        rprev = rnext;

        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert!(instance.requested_state.is_none());
        assert!(rx.try_next().is_err());
        assert_eq!(rprev.run_state, ApiInstanceState::Starting);
        assert_eq!(rnext.run_state, ApiInstanceState::Running);
        rprev = rnext;
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rprev.gen, rnext.gen);

        /*
         * If we transition again to "Running", the process should complete
         * immediately.
         */
        assert!(!rprev.run_state.is_stopped());
        let dropped = instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            })
            .unwrap();
        assert!(dropped.is_none());
        assert!(instance.requested_state.is_none());
        assert!(rx.try_next().is_err());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.gen, rprev.gen);
        assert_eq!(rnext.time_updated, rprev.time_updated);
        assert_eq!(rnext.run_state, rprev.run_state);
        rprev = rnext;

        /*
         * If we go back to any stopped state, we go through the async process
         * again.
         */
        assert!(!rprev.run_state.is_stopped());
        assert!(rx.try_next().is_err());
        let dropped = instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Destroyed,
                reboot_wanted: false,
            })
            .unwrap();
        assert!(dropped.is_none());
        assert!(instance.requested_state.is_some());
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        assert!(!rnext.run_state.is_stopped());
        rprev = rnext;

        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert!(instance.requested_state.is_none());
        assert_eq!(rprev.run_state, ApiInstanceState::Stopping);
        assert_eq!(rnext.run_state, ApiInstanceState::Destroyed);
        rprev = rnext;
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rprev.gen, rnext.gen);

        /*
         * Now let's test the behavior of dropping a transition.  We'll start
         * transitioning back to "Running".  Then, while we're still in
         * "Starting", will transition back to "Destroyed".  We should
         * immediately go to "Stopping", and completing the transition should
         * take us to "Destroyed".
         */
        assert!(rprev.run_state.is_stopped());
        let dropped = instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            })
            .unwrap();
        assert!(dropped.is_none());
        assert!(instance.requested_state.is_some());
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        assert!(!rnext.run_state.is_stopped());
        rprev = rnext;

        /*
         * Interrupt the async transition with a new one.
         */
        let dropped = instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Destroyed,
                reboot_wanted: false,
            })
            .unwrap();
        assert_eq!(dropped.unwrap().run_state, ApiInstanceState::Running);
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        rprev = rnext;

        /*
         * Finish the async transition.
         */
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert!(instance.requested_state.is_none());
        assert_eq!(rprev.run_state, ApiInstanceState::Stopping);
        assert_eq!(rnext.run_state, ApiInstanceState::Destroyed);
        rprev = rnext;
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rprev.gen, rnext.gen);

        logctx.cleanup_successful();
    }

    /*
     * Test reboot-related transitions.
     */
    #[tokio::test]
    async fn test_sim_instance_reboot() {
        let logctx = test_setup_log("test_sim_instance_reboot").await;

        /*
         * Get an initial instance up to "Running".
         */
        let (mut instance, _rx) =
            make_instance(&logctx, ApiInstanceState::Creating);
        let r1 = instance.current_state.clone();

        info!(logctx.log, "new instance"; "run_state" => ?r1.run_state);
        assert_eq!(r1.run_state, ApiInstanceState::Creating);
        assert_eq!(r1.gen, 1);
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            })
            .unwrap()
            .is_none());
        instance.transition_finish();
        let (rprev, rnext) = (r1, instance.current_state.clone());
        assert!(rnext.gen > rprev.gen);

        /*
         * Now, take it through a reboot sequence.
         */
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            })
            .unwrap()
            .is_none());
        let (rprev, rnext) = (rnext, instance.current_state.clone());
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated > rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        assert!(rnext.reboot_in_progress);
        assert!(instance.requested_state.is_some());
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.current_state.clone());
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated > rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        assert!(!rnext.reboot_in_progress);
        assert!(instance.requested_state.is_some());
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.current_state.clone());
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated > rprev.time_updated);
        assert_eq!(rnext.run_state, ApiInstanceState::Running);
        assert!(instance.requested_state.is_none());

        /*
         * Begin a reboot.  Then, while it's still "Stopping", begin another
         * reboot.  This should go through exactly one reboot sequence, as the
         * second reboot is totally superfluous.
         */
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            })
            .unwrap()
            .is_none());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            })
            .unwrap()
            .is_some());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Running);
        assert!(instance.requested_state.is_none());
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.current_state.clone());
        assert_eq!(rprev.gen, rnext.gen);

        /*
         * Begin a reboot.  Then, while it's "Starting" (on the way back up),
         * begin another reboot.  This should go through a second reboot
         * sequence.
         */
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            })
            .unwrap()
            .is_none());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            })
            .unwrap()
            .is_some());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Running);
        assert!(instance.requested_state.is_none());
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.current_state.clone());
        assert_eq!(rprev.gen, rnext.gen);

        /*
         * At this point, we've exercised what happens when a reboot is issued
         * from "Running", from "Starting" with a reboot in progress, from
         * "Stopping" with a reboot in progress.  All that's left is "Starting"
         * with no reboot in progress.  First, stop the instance.  Then start
         * it.  Then, while it's starting, begin a reboot sequence.
         */
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Stopped,
                reboot_wanted: false,
            })
            .unwrap()
            .is_none());
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Stopped);
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            })
            .unwrap()
            .is_none());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        assert!(instance
            .transition(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            })
            .unwrap()
            .is_some());
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Starting);
        instance.transition_finish();
        let rnext = instance.current_state.clone();
        assert_eq!(rnext.run_state, ApiInstanceState::Running);
        assert!(instance.requested_state.is_none());
        instance.transition_finish();
        let (rprev, rnext) = (rnext, instance.current_state.clone());
        assert_eq!(rprev.gen, rnext.gen);

        /*
         * Issuing a reboot from any other state is not defined, including from
         * "Stopping" while not in the process of a reboot and from any
         * "stopped" state.  instance_ensure() will prevent this, while
         * transition() will allow it.  We don't test the behavior of
         * transition() because it's subject to change.
         */

        logctx.cleanup_successful();
    }

    /**
     * Tests basic usage of `SimDisk`.  This is somewhat less exhaustive than
     * the analogous tests for `SimInstance` because much of that functionality
     * is implemented in `SimObject`, common to both.  So we don't bother
     * verifying dropped state, messages sent to the background task, or some
     * sanity checks around completion of async transitions when none is
     * pending.
     */
    #[tokio::test]
    async fn test_sim_disk() {
        let logctx = test_setup_log("test_sim_disk").await;
        let (mut disk, _rx) = make_disk(&logctx, ApiDiskState::Creating);
        let r1 = disk.current_state.clone();

        info!(logctx.log, "new disk"; "disk_state" => ?r1.disk_state);
        assert_eq!(r1.disk_state, ApiDiskState::Creating);
        assert_eq!(r1.gen, 1);

        /*
         * Try transitioning to every other detached state.
         */
        let detached_states = vec![
            (ApiDiskStateRequested::Detached, ApiDiskState::Detached),
            (ApiDiskStateRequested::Destroyed, ApiDiskState::Destroyed),
            (ApiDiskStateRequested::Faulted, ApiDiskState::Faulted),
        ];
        let mut rprev = r1;
        for (requested, next) in detached_states {
            assert!(!rprev.disk_state.is_attached());
            disk.transition(requested.clone()).unwrap();
            let rnext = disk.current_state.clone();
            assert!(rnext.gen > rprev.gen);
            assert!(rnext.time_updated >= rprev.time_updated);
            assert_eq!(rnext.disk_state, next);
            rprev = rnext;
        }

        /*
         * Now if we transition to "Attached", we should go through an async
         * transition.
         */
        let id = uuid::Uuid::new_v4();
        assert!(!rprev.disk_state.is_attached());
        assert!(disk
            .transition(ApiDiskStateRequested::Attached(id.clone()))
            .unwrap()
            .is_none());
        let rnext = disk.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        assert_eq!(rnext.disk_state, ApiDiskState::Attaching(id.clone()));
        assert!(rnext.disk_state.is_attached());
        assert_eq!(id, *rnext.disk_state.attached_instance_id().unwrap());
        let rprev = rnext;

        disk.transition_finish();
        let rnext = disk.current_state.clone();
        assert_eq!(rnext.disk_state, ApiDiskState::Attached(id.clone()));
        assert!(rnext.gen > rprev.gen);
        assert!(rnext.time_updated >= rprev.time_updated);
        let rprev = rnext;

        disk.transition_finish();
        let rnext = disk.current_state.clone();
        assert_eq!(rnext.gen, rprev.gen);
        assert_eq!(rnext.disk_state, ApiDiskState::Attached(id.clone()));
        assert!(rnext.disk_state.is_attached());
        let rprev = rnext;

        /* If we go straight to "Attached" again, there's nothing to do. */
        assert!(disk
            .transition(ApiDiskStateRequested::Attached(id.clone()))
            .unwrap()
            .is_none());
        let rnext = disk.current_state.clone();
        assert_eq!(rnext.gen, rprev.gen);
        let rprev = rnext;

        /*
         * It's illegal to go straight to attached to a different instance.
         */
        let id2 = uuid::Uuid::new_v4();
        assert_ne!(id, id2);
        let error = disk
            .transition(ApiDiskStateRequested::Attached(id2.clone()))
            .unwrap_err();
        if let ApiError::InvalidRequest {
            message,
        } = error
        {
            assert_eq!("disk is already attached", message);
        } else {
            panic!("unexpected error type");
        }
        let rnext = disk.current_state.clone();
        assert_eq!(rprev.gen, rnext.gen);
        let rprev = rnext;

        /*
         * If we go to a different detached state, we go through the async
         * transition again.
         */
        disk.transition(ApiDiskStateRequested::Detached).unwrap();
        let rnext = disk.current_state.clone();
        assert!(rnext.gen > rprev.gen);
        assert_eq!(rnext.disk_state, ApiDiskState::Detaching(id.clone()));
        assert!(rnext.disk_state.is_attached());
        let rprev = rnext;

        disk.transition_finish();
        let rnext = disk.current_state.clone();
        assert_eq!(rnext.disk_state, ApiDiskState::Detached);
        assert!(rnext.gen > rprev.gen);

        /*
         * Verify that it works fine to change directions in the middle of an
         * async transition.
         */
        disk.transition(ApiDiskStateRequested::Attached(id.clone())).unwrap();
        assert_eq!(
            disk.current_state.disk_state,
            ApiDiskState::Attaching(id.clone())
        );
        disk.transition(ApiDiskStateRequested::Destroyed).unwrap();
        assert_eq!(
            disk.current_state.disk_state,
            ApiDiskState::Detaching(id.clone())
        );
        disk.transition_finish();
        assert_eq!(disk.current_state.disk_state, ApiDiskState::Destroyed);

        disk.transition(ApiDiskStateRequested::Attached(id.clone())).unwrap();
        disk.transition_finish();
        assert_eq!(
            disk.current_state.disk_state,
            ApiDiskState::Attached(id.clone())
        );
        disk.transition(ApiDiskStateRequested::Faulted).unwrap();
        assert_eq!(
            disk.current_state.disk_state,
            ApiDiskState::Detaching(id.clone())
        );
        let error = disk
            .transition(ApiDiskStateRequested::Attached(id.clone()))
            .unwrap_err();
        if let ApiError::InvalidRequest {
            message,
        } = error
        {
            assert_eq!("cannot attach while detaching", message);
        } else {
            panic!("unexpected error type");
        }
        disk.transition_finish();
        assert_eq!(disk.current_state.disk_state, ApiDiskState::Faulted);
    }
}
