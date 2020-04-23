/*!
 * Facilities for interacting with Server Controllers.  See RFD 48.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceState;
use crate::controller::ControllerScApi;
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
 * `ServerController` is our handle for the software service running on a
 * compute server that manages the control plane on that server.  The current
 * implementation is simulated directly in Rust.  The intent is that this object
 * will be implemented using requests to a remote server and the simulation
 * would be moved to the other side of the network.
 */
pub struct ServerController {
    pub id: Uuid,

    ctlsc: ControllerScApi,
    log: Logger,
    instances: Mutex<BTreeMap<Uuid, SimInstance>>,
}

/*
 * TODO-debug We need visibility into the runtime state of all this.  e.g.,
 * SimInstances that are in async cleanup().
 */
struct SimInstance {
    log: Logger,
    transition_state: SimInstanceTransitionState,
    channel_tx: Sender<()>,
    task: JoinHandle<()>,
}

impl SimInstance {
    fn new(
        api_instance: &Arc<ApiInstance>,
        log: Logger,
        tx: Sender<()>,
        task: JoinHandle<()>,
    ) -> SimInstance {
        SimInstance {
            log: log,
            transition_state: SimInstanceTransitionState::Creating {
                target: Arc::clone(api_instance),
            },
            channel_tx: tx,
            task: task,
        }
    }

    fn with_state(self, new_state: &SimInstanceTransitionState) -> SimInstance {
        SimInstance {
            log: self.log,
            transition_state: new_state.clone(),
            channel_tx: self.channel_tx,
            task: self.task,
        }
    }

    fn notify(&mut self) {
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
        let task = self.task;
        let mut tx = self.channel_tx;
        tx.close_channel();
        task.await.unwrap();
    }
}

#[derive(Clone)]
enum SimInstanceTransitionState {
    Creating {
        target: Arc<ApiInstance>,
    },
    CreatingWithPending {
        intermediate: Arc<ApiInstance>,
        target: Arc<ApiInstance>,
    },
    Transitioning {
        previous: Arc<ApiInstance>,
        target: Arc<ApiInstance>,
    },
    TransitioningWithPending {
        previous: Arc<ApiInstance>,
        intermediate: Arc<ApiInstance>,
        target: Arc<ApiInstance>,
    },
    AtRest {
        current: Arc<ApiInstance>,
    },
}

impl fmt::Debug for SimInstanceTransitionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SimInstanceTransitionState::Creating {
                target,
            } => write!(f, "creating instance with state \"{}\"", target.state),
            SimInstanceTransitionState::CreatingWithPending {
                intermediate,
                target,
            } => write!(
                f,
                "creating instance with state \"{}\", then transitioning to \
                 \"{}\"",
                intermediate.state, target.state
            ),
            SimInstanceTransitionState::Transitioning {
                previous,
                target,
            } => write!(
                f,
                "transitioning from \"{}\" to \"{}\"",
                previous.state, target.state
            ),
            SimInstanceTransitionState::TransitioningWithPending {
                previous,
                intermediate,
                target,
            } => write!(
                f,
                "transitioning from \"{}\" to \"{}\", and then to \"{}\"",
                previous.state, intermediate.state, target.state
            ),
            SimInstanceTransitionState::AtRest {
                current,
            } => write!(f, "at rest in state \"{}\"", current.state),
        }
    }
}

impl SimInstanceTransitionState {
    fn transition_start(
        &self,
        target: Arc<ApiInstance>,
    ) -> (SimInstanceTransitionState, Option<ApiInstanceState>) {
        let mut dropped: Option<ApiInstanceState> = None;

        let next_transition_state = match self {
            SimInstanceTransitionState::Creating {
                target: old_target,
            } => SimInstanceTransitionState::CreatingWithPending {
                intermediate: Arc::clone(old_target),
                target: target,
            },

            SimInstanceTransitionState::CreatingWithPending {
                intermediate: old_intermediate,
                target: old_target,
            } => {
                dropped = Some(old_target.state.clone());
                SimInstanceTransitionState::CreatingWithPending {
                    intermediate: Arc::clone(old_intermediate),
                    target,
                }
            }

            SimInstanceTransitionState::Transitioning {
                previous,
                target: old_target,
            } => SimInstanceTransitionState::TransitioningWithPending {
                previous: Arc::clone(previous),
                intermediate: Arc::clone(old_target),
                target,
            },

            SimInstanceTransitionState::TransitioningWithPending {
                previous,
                intermediate,
                target: old_target,
            } => {
                dropped = Some(old_target.state.clone());
                SimInstanceTransitionState::TransitioningWithPending {
                    previous: Arc::clone(previous),
                    intermediate: Arc::clone(intermediate),
                    target,
                }
            }

            SimInstanceTransitionState::AtRest {
                current,
            } => SimInstanceTransitionState::Transitioning {
                previous: Arc::clone(current),
                target,
            },
        };

        (next_transition_state, dropped)
    }

    fn transition_finish(&self) -> SimInstanceTransitionState {
        match self {
            SimInstanceTransitionState::Creating {
                target,
            } => SimInstanceTransitionState::AtRest {
                current: Arc::clone(target),
            },

            SimInstanceTransitionState::CreatingWithPending {
                intermediate,
                target,
            } => SimInstanceTransitionState::Transitioning {
                previous: Arc::clone(intermediate),
                target: Arc::clone(target),
            },

            SimInstanceTransitionState::Transitioning {
                previous: _,
                target,
            } => SimInstanceTransitionState::AtRest {
                current: Arc::clone(target),
            },

            SimInstanceTransitionState::TransitioningWithPending {
                previous: _,
                intermediate,
                target,
            } => SimInstanceTransitionState::Transitioning {
                previous: Arc::clone(intermediate),
                target: Arc::clone(target),
            },

            SimInstanceTransitionState::AtRest {
                ..
            } => panic!("no instance state transition in progress"),
        }
    }

    fn current_instance(&self) -> Arc<ApiInstance> {
        let current_instance = match self {
            SimInstanceTransitionState::Creating {
                ..
            } => {
                panic!("no current instance while Creating");
            }

            SimInstanceTransitionState::CreatingWithPending {
                ..
            } => {
                panic!("no current instance while Creating");
            }

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

        Arc::clone(current_instance)
    }

    fn transition_pending(&self) -> bool {
        match self {
            SimInstanceTransitionState::Creating {
                ..
            } => false,
            SimInstanceTransitionState::CreatingWithPending {
                ..
            } => true,
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
            current.state == ApiInstanceState::Destroyed
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
    ) -> Result<(), ApiError> {
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
         *
         * To achieve this, we store:
         *
         *   * `current`: always the current runtime state.  This is never
         *     directly changed by this function.
         *
         *   * `target`: always the desired final state.  This is always
         *      replaced with every call to this function.
         *
         *   * `current_transition`: represents the ongoing transition from the
         *     current state to the next state.  The next state may not be the
         *     `target`, if a state transition was ongoing when this function is
         *     invoked.  As a simulation, we might remain transitioning either
         *     for a fixed period of time or until some internal API call
         *     indicates that the transition should complete.
         */
        let id = api_instance.identity.id.clone();
        let target = Arc::clone(&api_instance);
        let maybe_current_instance = instances.remove(&id);
        let mut next_instance = {
            if let Some(current_instance) = maybe_current_instance {
                let orig_state = current_instance.transition_state.clone();
                let (next_state, dropped) = orig_state.transition_start(target);
                let next = current_instance.with_state(&next_state);

                info!(next.log, "instance_ensure (existing)";
                    "initial" => ?orig_state,
                    "requested" => ?api_instance,
                    "next_state" => ?next.transition_state,
                    "dropped_transition" => ?dropped,
                );

                next
            } else {
                /*
                 * Create a new Instance.
                 *
                 * The channel below is used to send a message to a background
                 * task to simulate an Instance state transition by sleeping for
                 * some interval and then updating the Instance state.  Note
                 * that when the background task updates the Instance state
                 * after sleeping, it invokes `instance_poke()`, which looks at
                 * the current state and the target next state.  These are
                 * computed (in `instance_ensure()`) in such a way as to avoid
                 * queueing up multiple sequential transitions.
                 *
                 * The net result of this is that we don't need (or want) a
                 * channel buffer larger than 1.  That's because if we were to
                 * queue up multiple messages in the buffer, the net effect
                 * would be exactly the same as if just one message were queued.
                 * As part of processing that message, the receiver will wind up
                 * handling all state transitions requested up to the point
                 * where the first message is read.  If another transition is
                 * requested after that point, another message will be enqueued
                 * and the receiver will process that transition then.  There's
                 * no need to queue more than one message.  Even stronger: we
                 * don't want a larger buffer because that would only cause
                 * extra laps through the sleep cycle, which just wastes
                 * resources and increases the latency for processing the next
                 * real transition request.
                 */
                let (tx, rx) = futures::channel::mpsc::channel(0);
                let idc = id.clone();
                let selfc = Arc::clone(&self);

                let task = tokio::spawn(async move {
                    selfc.instance_sim(idc, rx).await;
                });

                let next = SimInstance::new(
                    &api_instance,
                    self.log.new(o!("instance_id" => idc.to_string())),
                    tx,
                    task,
                );
                info!(next.log, "instance_ensure (new)";
                    "requested" => ?api_instance,
                    "transition" => ?next.transition_state);
                next
            }
        };

        next_instance.notify();
        instances.insert(id.clone(), next_instance);
        Ok(())
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
            let orig_state = &current_instance.transition_state;
            let next_state = orig_state.transition_finish();

            info!(current_instance.log, "instance transitioning";
                "previously" => ?orig_state,
                "now" => ?next_state,
            );

            let next_instance_state =
                next_state.current_instance().state.clone();
            if next_state.at_rest_destroyed() {
                info!(current_instance.log, "instance came to rest destroyed");
                (next_instance_state, Some(current_instance))
            } else {
                let mut next_instance =
                    current_instance.with_state(&next_state);
                if next_state.transition_pending() {
                    next_instance.notify();
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
