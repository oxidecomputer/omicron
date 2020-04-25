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
use std::fmt;
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
    /** description of the Instance, as we last saw it */
    api_instance: Arc<ApiInstance>,
    /** tracks the simulated transition of this Instance between run states */
    transition: SimInstanceTransitionState,
    /** background task that handles simulated transitions */
    task: JoinHandle<()>,
    /** channel for transmitting to the background task */
    channel_tx: Sender<()>,
}

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
            api_instance: Arc::clone(api_instance),
            transition: SimInstanceTransitionState::AtRest {
                current: api_instance.runtime.clone(),
            },
            channel_tx: tx,
            task: task,
        }
    }

    /**
     * Consuming the `SimInstance` and return a new `SimInstance` with the
     * requested `transition`.
     */
    fn now_transitioning(
        self,
        transition: &SimInstanceTransitionState,
    ) -> SimInstance {
        SimInstance {
            log: self.log,
            api_instance: Arc::clone(&self.api_instance),
            transition: transition.clone(),
            channel_tx: self.channel_tx,
            task: self.task,
        }
    }

    /**
     * Begins simulating the current transition by sending a message to the
     * background task.
     */
    fn begin_simulated_transition(&mut self) {
        let result = self.channel_tx.try_send(());
        if let Err(error) = result {
            /*
             * There are a few possible error cases:
             *
             * (1) We failed to send the message because the channel's buffer is
             *     full.  All we need to guarantee in the first place is that
             *     the receiver will receive a message at least once after this
             *     function is invoked.  If there's a message in the buffer, we
             *     don't need to do anything else here.
             * (2) We failed to send the message because the channel is
             *     disconnected.  This would be a programmer error -- the
             *     contract between us and the receiver is that we shut down the
             *     channel first.  As a result, we panic if we find this case.
             * (3) We failed to send the message for some other reason.  This
             *     appears impossible at the time of this writing.   It would be
             *     nice if the returned error type were implemented in a way
             *     that we could identify this case at compile time (e.g., using
             *     an enum), but that's not currently the case.
             */
            assert!(!error.is_disconnected());
            assert!(error.is_full());
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

#[derive(Clone)]
enum SimInstanceTransitionState {
    Transitioning {
        previous: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateParams,
    },
    TransitioningWithPending {
        previous: ApiInstanceRuntimeState,
        intermediate: ApiInstanceRuntimeStateParams,
        target: ApiInstanceRuntimeStateParams,
    },
    AtRest {
        current: ApiInstanceRuntimeState,
    },
}

impl fmt::Debug for SimInstanceTransitionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SimInstanceTransitionState::Transitioning {
                previous,
                target,
            } => write!(
                f,
                "transitioning from \"{}\" to \"{:?}\"",
                previous.run_state, target
            ),
            SimInstanceTransitionState::TransitioningWithPending {
                previous,
                intermediate,
                target,
            } => write!(
                f,
                "transitioning from \"{}\" to \"{:?}\", and then to \"{:?}\"",
                previous.run_state, intermediate, target
            ),
            SimInstanceTransitionState::AtRest {
                current,
            } => write!(f, "at rest in state \"{}\"", current.run_state),
        }
    }
}

impl SimInstanceTransitionState {
    fn transition_start(
        &self,
        target: ApiInstanceRuntimeStateParams,
    ) -> (SimInstanceTransitionState, Option<ApiInstanceRuntimeStateParams>)
    {
        let mut dropped: Option<ApiInstanceRuntimeStateParams> = None;

        let next_transition = match self {
            SimInstanceTransitionState::Transitioning {
                previous,
                target: old_target,
            } => SimInstanceTransitionState::TransitioningWithPending {
                previous: previous.clone(),
                intermediate: old_target.clone(),
                target,
            },

            SimInstanceTransitionState::TransitioningWithPending {
                previous,
                intermediate,
                target: old_target,
            } => {
                dropped = Some(old_target.clone());
                SimInstanceTransitionState::TransitioningWithPending {
                    previous: previous.clone(),
                    intermediate: intermediate.clone(),
                    target,
                }
            }

            SimInstanceTransitionState::AtRest {
                current,
            } => SimInstanceTransitionState::Transitioning {
                previous: current.clone(),
                target,
            },
        };

        (next_transition, dropped)
    }

    fn transition_finish(&self) -> SimInstanceTransitionState {
        match self {
            SimInstanceTransitionState::Transitioning {
                previous,
                target,
            } => SimInstanceTransitionState::AtRest {
                current: next_runtime_state(previous, target),
            },

            SimInstanceTransitionState::TransitioningWithPending {
                previous,
                intermediate,
                target,
            } => SimInstanceTransitionState::Transitioning {
                previous: next_runtime_state(previous, intermediate),
                target: target.clone(),
            },

            SimInstanceTransitionState::AtRest {
                ..
            } => panic!("no instance state transition in progress"),
        }
    }

    fn current_state(&self) -> ApiInstanceRuntimeState {
        let current_state = match self {
            SimInstanceTransitionState::Transitioning {
                previous, ..
            } => previous,

            SimInstanceTransitionState::TransitioningWithPending {
                previous,
                ..
            } => previous,

            SimInstanceTransitionState::AtRest {
                current,
            } => current,
        };

        current_state.clone()
    }

    fn transition_pending(&self) -> bool {
        match self {
            SimInstanceTransitionState::Transitioning {
                ..
            } => false,
            SimInstanceTransitionState::TransitioningWithPending {
                ..
            } => true,
            SimInstanceTransitionState::AtRest {
                ..
            } => false,
        }
    }

    fn at_rest_destroyed(&self) -> bool {
        if let SimInstanceTransitionState::AtRest {
            current,
        } = self
        {
            current.run_state == ApiInstanceState::Destroyed
        } else {
            false
        }
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
     * Idempotently ensures that the given instance exists on this host in the
     * given state.
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
        let mut next_instance = {
            let current_instance = {
                if let Some(current_instance) = maybe_current_instance {
                    current_instance
                } else {
                    /*
                     * Create a new Instance.
                     *
                     * The channel below is used to send a message to a
                     * background task to simulate an Instance state transition
                     * by sleeping for some interval and then updating the
                     * Instance state.  Note that when the background task
                     * updates the Instance state after sleeping, it invokes
                     * `instance_poke()`, which looks at the current state and
                     * the target next state.  These are computed (in
                     * `instance_ensure()`) in such a way as to avoid queueing
                     * up multiple sequential transitions.
                     *
                     * The net result of this is that we don't need (or want) a
                     * channel buffer larger than 1.  That's because if we were
                     * to queue up multiple messages in the buffer, the net
                     * effect would be exactly the same as if just one message
                     * were queued.  As part of processing that message, the
                     * receiver will wind up handling all state transitions
                     * requested up to the point where the first message is
                     * read.  If another transition is requested after that
                     * point, another message will be enqueued and the receiver
                     * will process that transition then.  There's no need to
                     * queue more than one message.  Even stronger: we don't
                     * want a larger buffer because that would only cause extra
                     * laps through the sleep cycle, which just wastes resources
                     * and increases the latency for processing the next real
                     * transition request.
                     */
                    let (tx, rx) = futures::channel::mpsc::channel(0);
                    let idc = id.clone();
                    let selfc = Arc::clone(&self);

                    let task = tokio::spawn(async move {
                        selfc.instance_sim(idc, rx).await;
                    });

                    let mut instance = SimInstance::new(
                        &api_instance,
                        self.log.new(o!("instance_id" => idc.to_string())),
                        tx,
                        task,
                    );

                    /*
                     * TODO we need to rethink this a little bit.  We need to
                     * consider the set of valid next states that we can be
                     * given (say: running, stopped, destroyed but NOT created,
                     * starting, or stopping).  Then we need to figure out what
                     * the appropriate next state is, given our current state.
                     * If we want to be Running and we're asked to be Running,
                     * we don't need to do anything.  If we want to be Running
                     * and we're currently Started, we need to go through
                     * Starting.  We may _also_ need to keep track of the
                     * desired next state (e.g., Starting -> Started), unless
                     * it's always unambiguous and we can just assume we know
                     * it (e.g., Starting is always followed by Started).
                     *
                     * We already have a bunch of logic to keep track of the
                     * next user-requested state (and queue only one of those
                     * requests).  But there are two levels of "next" state we
                     * need to consider: that one, and the intermediate next
                     * state described above.
                     *
                     * TODO an additional complexity here is the generation
                     * numbers.  The caller provided us one (do we really need
                     * it in the params?) but we need to use that one for an
                     * intermediate state and produce another one.  Maybe the
                     * caller should be providing us a base one to go on as a
                     * precondition, and we should be careful not to re-evaluate
                     * the precondition after we've completed our intermediate
                     * state change, lest we fail spuriously.
                     *
                     * For now, we hardcode the one case we care about
                     * simulating right now.
                     */
                    if api_instance.runtime.run_state
                        == ApiInstanceState::Creating
                        && target.run_state == Some(ApiInstanceState::Running)
                    {
                        /* Transition from "creating" to "starting". */
                        let (trans2, _) = instance.transition.transition_start(
                            ApiInstanceRuntimeStateParams {
                                run_state: Some(ApiInstanceState::Starting),
                                server_uuid: None,
                                gen: target.gen,
                            },
                        );
                        let inst2 = instance.now_transitioning(&trans2);
                        let trans3 = trans2.transition_finish();
                        let inst3 = inst2.now_transitioning(&trans3);

                        /* Begin transition from "starting" to "running". */
                        let (trans4, _) =
                            inst3.transition.transition_start(target.clone());
                        let inst4 = inst3.now_transitioning(&trans4);
                        instance = inst4;
                    }

                    debug!(instance.log, "instance_ensure (new instance)";
                        "initial_state" => ?api_instance);
                    instance
                }
            };

            let before = current_instance.transition.clone();
            let (after, dropped) = before.transition_start(target.clone());
            let next = current_instance.now_transitioning(&after);

            info!(next.log, "instance_ensure";
                "initial" => ?before,
                "next_state" => ?next.transition,
                "dropped_transition" => ?dropped.map(|d| d.run_state),
            );

            next
        };

        next_instance.begin_simulated_transition();
        let rv = next_instance.transition.current_state().clone();
        instances.insert(id.clone(), next_instance);
        Ok(rv)
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
            let current_instance = instances.remove(&id).unwrap();
            let orig_transition = &current_instance.transition;
            let next_transition = orig_transition.transition_finish();

            info!(current_instance.log, "instance transitioning";
                "previously" => ?orig_transition,
                "now" => ?next_transition,
            );

            let next_instance_state = next_transition.current_state();
            if next_transition.at_rest_destroyed() {
                info!(current_instance.log, "instance came to rest destroyed");
                (next_instance_state, Some(current_instance))
            } else {
                let mut next_instance =
                    current_instance.now_transitioning(&next_transition);
                if next_transition.transition_pending() {
                    next_instance.begin_simulated_transition();
                }
                instances.insert(id.clone(), next_instance);
                (next_instance_state, None)
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

fn next_runtime_state(
    start: &ApiInstanceRuntimeState,
    params: &ApiInstanceRuntimeStateParams,
) -> ApiInstanceRuntimeState {
    ApiInstanceRuntimeState {
        run_state: params
            .run_state
            .as_ref()
            .unwrap_or(&start.run_state)
            .clone(),
        server_uuid: params
            .server_uuid
            .as_ref()
            .unwrap_or(&start.server_uuid)
            .clone(),
        gen: start.gen + 1,
        time_updated: Utc::now(),
    }
}
