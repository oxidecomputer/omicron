/*!
 * Facilities for interacting with Server Controllers.  See RFD 48.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceRuntimeStateParams;
use crate::api_model::ApiInstanceState;
use crate::controller::ControllerScApi;
use chrono::Utc;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

/**
 * `ServerController` is a handle for the software service running on a compute
 * server that manages the control plane on that server.  The current
 * implementation simulates a server directly in this program.
 *
 * **It's important to be careful about the interface exposed by this struct.**
 * The intent is for it to eventually be implemented using requests to a remote
 * server.  The tighter the coupling that exists now, the harder this will be to
 * move later.
 */
pub struct ServerController {
    /** unique id for this server */
    pub id: Uuid,

    /** handle for the internal control plane API */
    ctlsc: ControllerScApi,
    /** debug log */
    log: Logger,
    /** collection of simulated instances, indexed by instance uuid */
    instances: Mutex<BTreeMap<Uuid, SimInstance>>,
}

/**
 * `SimInstance` simulates an Oxide Rack Instance (virtual machine), as created
 * by the public API.
 *
 * We only simulate the Instance from the perspective of an API consumer, which
 * means for example accepting a request to boot it, reporting the current state
 * as "starting", and then some time later reporting that the state is
 * "running".
 */
struct SimInstance {
    /** debug log */
    log: Logger,
    /** current runtime state of the instance */
    current_run_state: ApiInstanceRuntimeState,
    /** requested runtime state of the instance */
    requested_run_state: Option<ApiInstanceRuntimeStateParams>,

    /** background task that handles simulated transitions */
    task: JoinHandle<()>,
    /** channel for transmitting to the background task */
    channel_tx: Sender<()>,
}

/**
 * Buffer size for channel used to communicate with each SimInstance's
 * background task.  Messages sent on this channel trigger the task to simulate
 * an Instance state transition by sleeping for some interval and then updating
 * the Instance state.  Note that when the background task updates the Instance
 * state after sleeping, it always looks at the current state to decide what to
 * do.  As a result, we never need to queue up more than one transition.  In
 * turn, tha tmeans we don't need (or want) a channel buffer larger than 1.
 * If we were to queue up multiple messages in the buffer, the net effect would
 * be exactly the same as if just one message were queued.  (Because of what we
 * said above, as part of processing that message, the receiver will wind up
 * handling all state transitions requested up to the point where the first
 * message is read.  If another transition is requested after that point,
 * another message will be enqueued and the receiver will process that
 * transition then.  There's no need to queue more than one message.)  Even
 * stronger: we don't want a larger buffer because that would only cause extra
 * laps through the sleep cycle, which just wastes resources and increases the
 * latency for processing the next real transition request.
 */
const SIM_INSTANCE_CHANNEL_BUFFER_SIZE: usize = 0;

/**
 * TODO-coverage: unit tests for SimInstance could cover a lot of ground,
 * especially if we add an at_rest() function.
 */
impl SimInstance {
    /** Create a new `SimInstance`. */
    fn new(
        api_instance: &Arc<ApiInstance>,
        log: Logger,
        tx: Sender<()>,
        task: JoinHandle<()>,
    ) -> SimInstance {
        SimInstance {
            log: log,
            current_run_state: api_instance.runtime.clone(),
            requested_run_state: None,
            task: task,
            channel_tx: tx,
        }
    }

    fn transition(
        &mut self,
        target: &ApiInstanceRuntimeStateParams,
    ) -> Option<ApiInstanceRuntimeStateParams> {
        let state_before = &self.current_run_state.run_state;
        let dropped = self.requested_run_state.replace(target.clone());

        let state_after = match target.run_state {
            /* There's nothing to do if the target run state isn't changing. */
            None => return dropped,
            Some(ref target_run_state) if target_run_state == state_before => {
                return dropped
            }
            /*
             * Several states are basically illegal to request, but if they're
             * requested, we just try to do something reasonable.
             * TODO-cleanup use a different type?
             */
            Some(ApiInstanceState::Creating) => &ApiInstanceState::Running,
            Some(ApiInstanceState::Starting) => &ApiInstanceState::Running,
            Some(ApiInstanceState::Stopping) => &ApiInstanceState::Stopped,

            /* This is the most common case. */
            Some(ref target_run_state) => target_run_state,
        };

        let (immed_next_state, need_async) =
            if state_before.is_stopped() && !state_after.is_stopped() {
                (&ApiInstanceState::Starting, true)
            } else if !state_before.is_stopped() && state_after.is_stopped() {
                (&ApiInstanceState::Stopping, true)
            } else {
                (state_after, false)
            };

        self.current_run_state = ApiInstanceRuntimeState {
            run_state: immed_next_state.clone(),
            server_uuid: self.current_run_state.server_uuid,
            gen: self.current_run_state.gen + 1,
            time_updated: Utc::now(),
        };

        /*
         * If this is an asynchronous transition, notify the background task to
         * simulate it.
         */
        if need_async {
            let result = self.channel_tx.try_send(());
            if let Err(error) = result {
                /*
                 * There are a few possible error cases:
                 *
                 * (1) We failed to send the message because the channel's
                 *     buffer is full.  All we need to guarantee in the first
                 *     place is that the receiver will receive a message at
                 *     least once after this function is invoked.  If there's a
                 *     message in the buffer, we don't need to do anything else
                 *     here.
                 *
                 * (2) We failed to send the message because the channel is
                 *     disconnected.  This would be a programmer error -- the
                 *     contract between us and the receiver is that we shut down
                 *     the channel first.  As a result, we panic if we find this
                 *     case.
                 *
                 * (3) We failed to send the message for some other reason.
                 *     This appears impossible at the time of this writing.   It
                 *     would be nice if the returned error type were implemented
                 *     in a way that we could identify this case at compile time
                 *     (e.g., using an enum), but that's not currently the case.
                 */
                assert!(!error.is_disconnected());
                assert!(error.is_full());
            }
        }

        dropped
    }

    fn transition_finish(&mut self) {
        let requested_run_state = match self.requested_run_state.take() {
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
            None => return,
            Some(run_state) => run_state,
        };

        /*
         * The `self.transition()` function ensures a few invariants:
         *
         * (1) If the requested transition is synchronous (i.e., not simulated),
         *     then the change is immediately applied and
         *     `self.requested_run_state` is `None`.
         *
         * (2) Otherwise, the requested transition is asynchronous (i.e., will
         *     be simulated), in which case the current state is set to an
         *     either "Starting" (for boot) or "Stopping" (for halt).  By
         *     construction, the requested state must be "Running" or "Stopped",
         *     respectively.
         *
         * Since we checked above whether `self.requested_run_state` was
         * non-None, we know that we're in case (2) above and we can assert
         * these invariants.
         */
        let run_state_before = &self.current_run_state.run_state;
        let run_state_after = requested_run_state.run_state.unwrap();
        match run_state_before {
            ApiInstanceState::Starting => {
                assert_eq!(run_state_after, ApiInstanceState::Running)
            }
            ApiInstanceState::Stopping => {
                assert_eq!(run_state_after, ApiInstanceState::Stopped)
            }
            _ => panic!("async transition started for unexpected state"),
        };

        /*
         * Having verified all that, we can update the Instance's state.  The
         * generation number does not change because this update is not a
         * user-requested one.
         */
        let new_server_uuid = requested_run_state
            .server_uuid
            .unwrap_or(self.current_run_state.server_uuid);
        self.current_run_state = ApiInstanceRuntimeState {
            run_state: run_state_after.clone(),
            server_uuid: new_server_uuid,
            gen: self.current_run_state.gen,
            time_updated: Utc::now(),
        }
    }

    async fn cleanup(self) {
        /*
         * TODO-debug It would be nice to have visibility into instances that
         * are cleaning up in case we have to debug resource leaks here.
         */
        let task = self.task;
        let mut tx = self.channel_tx;
        tx.close_channel();
        task.await.unwrap();
    }
}

impl ServerController {
    /** Constructs a simulated ServerController with the given uuid. */
    pub fn new_simulated_with_id(
        id: &Uuid,
        log: Logger,
        ctlsc: ControllerScApi,
    ) -> ServerController {
        info!(log, "created server controller");

        ServerController {
            id: id.clone(),
            log: log,
            ctlsc: ctlsc,
            instances: Mutex::new(BTreeMap::new()),
        }
    }

    /**
     * Idempotently ensures that the given API Instance (described by
     * `api_instance`) exists on this server in the given runtime state
     * (described by `target`).
     * TODO-correctness What's the semantics of the generation number in the
     * "target" here?  It seems like if the API receives one of these, the
     * generation number ought to be a base to be compared against (i.e., a
     * precondition).  However, by the time we get here, this call is supposed
     * to be idempotent, so we shouldn't have a precondition.  Nor is it
     * meaningful to have this be propagated to the RuntimeState (!Params).
     * _That_ generation number should only be incremented when the backend (us)
     * actually changes the runtime state.
     */
    pub async fn instance_ensure(
        self: &Arc<Self>,
        api_instance: Arc<ApiInstance>,
        target: &ApiInstanceRuntimeStateParams,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        let mut instances = self.instances.lock().await;

        /*
         * A `SimInstance` simulates an actual Instance.  While the controller
         * can change the `ApiInstance`'s state instantly on a whim, a real
         * Instance cannot change states so quickly -- it takes time to boot,
         * shut down, etc.  That means we need to consider state transitions as
         * well as just the states.
         *
         * We define the semantics of this call that if no state transition is
         * in progress, then we will begin a new one from the current state to
         * the target state.  If a state transition is already in progress, then
         * we will wait for it to complete and then begin a new transition to
         * the new target.  Note that we will not enqueue more than one
         * transition -- if one is already enqueued and this call requests a new
         * one, then the queued transition is abandoned.
         */
        let id = api_instance.identity.id.clone();
        let maybe_current_instance = instances.remove(&id);
        let mut instance = {
            if let Some(current_instance) = maybe_current_instance {
                current_instance
            } else {
                /* Create a new Instance. */
                let (tx, rx) = futures::channel::mpsc::channel(
                    SIM_INSTANCE_CHANNEL_BUFFER_SIZE,
                );
                let idc = id.clone();
                let selfc = Arc::clone(&self);
                let task = tokio::spawn(async move {
                    selfc.instance_sim(idc, rx).await;
                });
                let log = self.log.new(o!("instance_id" => idc.to_string()));
                debug!(log, "instance_ensure (new instance)";
                    "initial_state" => ?api_instance);
                SimInstance::new(&api_instance, log, tx, task)
            }
        };

        let before = instance.current_run_state.clone();
        let dropped = instance.transition(target);
        let after = instance.current_run_state.clone();

        info!(instance.log, "instance_ensure";
            "initial" => ?before,
            "next_state" => ?after,
            "dropped_transition" => ?dropped.map(|d| d.run_state),
        );

        instances.insert(id.clone(), instance);
        Ok(after)
    }

    async fn instance_sim(&self, id: Uuid, mut rx: Receiver<()>) {
        while let Some(_) = rx.next().await {
            tokio::time::delay_for(Duration::from_millis(1500)).await;
            self.instance_poke(id).await;
        }
    }

    async fn instance_poke(&self, id: Uuid) {
        let (new_state, to_destroy) = {
            /* Do as little as possible with the lock held. */
            let mut instances = self.instances.lock().await;
            let mut instance = instances.remove(&id).unwrap();
            let before = instance.current_run_state.clone();
            instance.transition_finish();
            let after = instance.current_run_state.clone();
            info!(instance.log, "instance transitioning";
                "previously" => ?before,
                "now" => ?after,
            );

            if instance.requested_run_state.is_none()
                && after.run_state == ApiInstanceState::Destroyed
            {
                info!(instance.log, "instance came to rest destroyed");
                (after, Some(instance))
            } else {
                instances.insert(id.clone(), instance);
                (after, None)
            }
        };

        /*
         * Notify the controller that the instance state has changed.
         * TODO-correctness: how do we make sure that the latest state is
         * reflected in the database if two of these calls are processed out of
         * order? And that we don't clobber other database state changes?  Maybe
         * we say that the SC (us) are authoritative, but only for certain
         * values.  Then we can provide a generation number or even maybe a
         * timestamp with this request that OXCP can use to make sure the latest
         * state is reflected.
         * TODO-correctness: how will state be correctly updated if OXCP or the
         * data storage system are down right now?  Something will need to
         * resolve that asynchronously.
         */
        self.ctlsc.notify_instance_updated(&id, &new_state).await;

        /*
         * If the instance came to rest destroyed, complete any async cleanup
         * needed now.
         */
        if let Some(destroyed_instance) = to_destroy {
            destroyed_instance.cleanup().await;
        }
    }
}
