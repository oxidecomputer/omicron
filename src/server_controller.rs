/*!
 * Facilities for interacting with Server Controllers.  See RFD 48.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiDiskStateRequested;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceRuntimeStateParams;
use crate::api_model::ApiInstanceState;
use crate::controller::ControllerScApi;
use async_trait::async_trait;
use chrono::Utc;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use futures::lock::MutexGuard;
use futures::stream::StreamExt;
use slog::Logger;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
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

    /** indicates how to simulate instance transitions */
    pub sim_mode: ServerControllerSimMode,

    /** handle for the internal control plane API */
    ctlsc: Arc<ControllerScApi>,
    /** debug log */
    log: Logger,

    /** collection of simulated instances, indexed by instance uuid */
    instances: Arc<SimCollection<SimInstance>>,
    /** collection of simulated disks, indexed by disk uuid */
    disks: Arc<SimCollection<SimDisk>>,
}

#[derive(Copy, Clone)]
pub enum ServerControllerSimMode {
    Auto,
    Api,
}

impl ServerController {
    /** Constructs a simulated ServerController with the given uuid. */
    pub fn new_simulated_with_id(
        id: &Uuid,
        sim_mode: ServerControllerSimMode,
        log: Logger,
        ctlsc: ControllerScApi,
    ) -> ServerController {
        info!(&log, "created server controller");

        let instance_log = log.new(o!("kind" => "instances"));
        let disk_log = log.new(o!("kind" => "disks"));
        let ctlsc = Arc::new(ctlsc);

        ServerController {
            id: id.clone(),
            sim_mode,
            log,
            ctlsc: Arc::clone(&ctlsc),
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
        api_instance: Arc<ApiInstance>,
        target: ApiInstanceRuntimeStateParams,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        self.instances
            .sim_ensure(
                &api_instance.identity.id,
                api_instance.runtime.clone(),
                target.clone(),
            )
            .await
    }

    /**
     * Idempotently ensures that the given API Disk (described by `api_disk`)
     * is attached (or not) as specified.  This simulates disk attach and
     * detach, similar to instance boot and halt.
     */
    pub async fn disk_ensure(
        self: &Arc<Self>,
        api_disk: Arc<ApiDisk>,
    ) -> Result<ApiDiskState, ApiError> {
        /*
         * XXX does it make sense that disks are different from instances in
         * this way?
         */
        let target = &api_disk.state_requested;
        self.disks
            .sim_ensure(
                &api_disk.identity.id,
                api_disk.state.clone(),
                target.clone(),
            )
            .await
    }

    //
    //    /**
    //     * Idempotently ensures that the given API Instance (described by
    //     * `api_instance`) exists on this server in the given runtime state
    //     * (described by `target`).
    //     */
    //    pub async fn instance_ensure(
    //        self: &Arc<Self>,
    //        api_instance: Arc<ApiInstance>,
    //        target: &ApiInstanceRuntimeStateParams,
    //    ) -> Result<ApiInstanceRuntimeState, ApiError> {
    //        if target.reboot_wanted && target.run_state != ApiInstanceState::Running
    //        {
    //            return Err(ApiError::InvalidRequest {
    //                message: String::from(
    //                    "cannot reboot to a state other than \"running\"",
    //                ),
    //            });
    //        }
    //
    //        let id = api_instance.identity.id.clone();
    //        let mut instances = self.instances.lock().await;
    //        let maybe_current_instance = instances.remove(&id);
    //
    //        let (mut instance, is_new) = {
    //            if let Some(current_instance) = maybe_current_instance {
    //                (current_instance, false)
    //            } else {
    //                /* Create a new Instance. */
    //                let idc = id.clone();
    //                let log = self.log.new(o!("instance_id" => idc.to_string()));
    //
    //                if let ServerControllerSimMode::Auto = self.sim_mode {
    //                    let (instance, rx) = SimInstance::new_simulated_auto(
    //                        &api_instance.runtime,
    //                        log,
    //                    );
    //                    let selfc = Arc::clone(&self);
    //                    tokio::spawn(async move {
    //                        selfc.instance_sim(idc, rx).await;
    //                    });
    //                    (instance, true)
    //                } else {
    //                    (
    //                        SimInstance::new_simulated_explicit(
    //                            &api_instance.runtime,
    //                            log,
    //                        ),
    //                        true,
    //                    )
    //                }
    //            }
    //        };
    //
    //        let current_state = &instance.current_run_state.run_state;
    //        let rv = if target.reboot_wanted
    //            && !is_new
    //            && *current_state != ApiInstanceState::Starting
    //            && *current_state != ApiInstanceState::Running
    //            && (*current_state != ApiInstanceState::Stopping
    //                || !instance.current_run_state.reboot_in_progress)
    //        {
    //            Err(ApiError::InvalidRequest {
    //                message: format!(
    //                    "cannot reboot instance in state \"{}\"",
    //                    current_state
    //                ),
    //            })
    //        } else {
    //            instance.transition(target);
    //            Ok(instance.current_run_state.clone())
    //        };
    //
    //        instances.insert(id.clone(), instance);
    //        rv
    //    }
    //
    //    /**
    //     * Body of the background task (one per `SimInstance`) that simulates
    //     * Instance booting and halting.  Each time we read a message from the
    //     * instance's channel, we sleep for a bit and then invoke `instance_poke()`
    //     * to complete whatever transition is currently outstanding.
    //     */
    //    async fn instance_sim(&self, id: Uuid, mut rx: Receiver<()>) {
    //        while let Some(_) = rx.next().await {
    //            tokio::time::delay_for(Duration::from_millis(1500)).await;
    //            self.instance_poke(id).await;
    //        }
    //    }
    //
    //    /**
    //     * Invoked as part of simulation to complete whatever asynchronous
    //     * transition is currently going on for instance `id`.
    //     */
    //    async fn instance_poke(&self, id: Uuid) {
    //        let (new_state, to_destroy) = {
    //            /* Do as little as possible with the lock held. */
    //            let mut instances = self.instances.lock().await;
    //            let mut instance = instances.remove(&id).unwrap();
    //            instance.transition_finish();
    //            let after = instance.current_run_state.clone();
    //            if instance.requested_run_state.is_none()
    //                && after.run_state == ApiInstanceState::Destroyed
    //            {
    //                info!(instance.log, "instance came to rest destroyed");
    //                (after, Some(instance))
    //            } else {
    //                instances.insert(id.clone(), instance);
    //                (after, None)
    //            }
    //        };
    //
    //        /*
    //         * Notify the controller that the instance state has changed.  The
    //         * server controller is authoritative for the runtime state, and we use
    //         * a generation number here so that calls processed out of order do not
    //         * settle on the wrong value.
    //         * TODO-robustness: If this fails, we need to put it on some list of
    //         * updates to retry later.
    //         */
    //        self.ctlsc.notify_instance_updated(&id, &new_state).await.unwrap();
    //
    //        /*
    //         * If the instance came to rest destroyed, complete any async cleanup
    //         * needed now.
    //         * TODO-debug It would be nice to have visibility into instances that
    //         * are cleaning up in case we have to debug resource leaks here.
    //         * TODO-correctness Is it a problem that nobody waits on the background
    //         * task?  If we did it here, we'd deadlock, since we're invoked from the
    //         * background task.
    //         */
    //        if let Some(destroyed_instance) = to_destroy {
    //            if let Some(mut tx) = destroyed_instance.channel_tx {
    //                tx.close_channel();
    //            }
    //        }
    //    }
    //
    //    /**
    //     * Idempotently ensures that the given API Disk (described by `api_disk`)
    //     * is attached (or not) as specified.  This simulates disk attach and
    //     * detach, similar to instance boot and halt.
    //     */
    //    pub async fn disk_ensure(
    //        self: &Arc<Self>,
    //        api_disk: Arc<ApiDisk>,
    //    ) -> Result<ApiDiskState, ApiError> {
    //        let id = api_disk.identity.id.clone();
    //        let mut disks = self.disks.lock().await;
    //        let maybe_current_disk = disks.remove(&id);
    //
    //        let (disk, is_new) = {
    //            if let Some(current_disk) = maybe_current_disk {
    //                (current_disk, false)
    //            } else {
    //                /* TODO commonize with SimInstance */
    //                let log = self.log.new(o!("disk_id" => id.to_string()));
    //                if let ServerControllerSimMode::Auto = self.sim_mode {
    //                    let (new_disk, rx) =
    //                        SimDisk::new_simulated_auto(&api_disk.state, log);
    //                    let selfc = Arc::clone(&self);
    //                    let idc = id.clone();
    //                    tokio::spawn(async move {
    //                        selfc.disk_sim(idc, rx).await;
    //                    });
    //                    (new_disk, true)
    //                } else {
    //                    let new_disk =
    //                        SimDisk::new_simulated_explicit(&api_disk.state, log);
    //                    (new_disk, true)
    //                }
    //            }
    //        };
    //
    //        disk.transition(&api_disk.state_requested);
    //        let rv = Ok(disk.current_state.clone());
    //        disks.insert(id.clone(), disk);
    //        rv
    //    }
    //
    //    async fn disk_sim(&self, id: Uuid, mut rx: Receiver<()>) {
    //        while let Some(_) = rx.next().await {
    //            tokio::time::delay_for(Duration::from_millis(1500)).await;
    //            self.disk_poke(id).await;
    //        }
    //    }
    //
    //    async fn disk_poke(&self, id: Uuid) {
    //        let (new_state, to_destroy) = {
    //            let mut disks = self.disks.lock().await;
    //            let mut disk = disks.remove(&id).unwrap();
    //            disk.transition_finish();
    //            let after = disk.current_state.clone();
    //            if disk.requested_state == ApiDiskStateRequested::NoChange
    //                && disk.current_state == ApiDiskState::Destroyed
    //            {
    //                info!(disk.log, "disk came to rest destroyed");
    //                (after, Some(disk))
    //            } else {
    //                disks.insert(id.clone(), disk);
    //                (after, None)
    //            }
    //        };
    //
    //        /* TODO-robutsness See instance_poke(). */
    //        self.ctlsc.notify_disk_updated(&id, &new_state).await.unwrap();
    //        /* TODO-debug TODO-correctness See instance_poke(). */
    //        if let Some(destroyed_disk) = to_destroy {
    //            if let Some(mut tx) = destroyed_disk.channel_tx {
    //                tx.close_channel();
    //            }
    //        }
    //    }
}

/**
 * Trait used to expose interfaces for use only by the test suite.
 */
#[async_trait]
pub trait ServerControllerTestInterfaces {
    async fn instance_finish_transition(&self, id: Uuid);
}

#[async_trait]
impl ServerControllerTestInterfaces for ServerController {
    async fn instance_finish_transition(&self, id: Uuid) {
        self.instances.sim_poke(id).await;
    }
}

// /**
//  * `SimInstance` simulates an Oxide Rack Instance (virtual machine), as created
//  * by the public API.
//  *
//  * We only simulate the Instance from the perspective of an API consumer, which
//  * means for example accepting a request to boot it, reporting the current state
//  * as "starting", and then some time later reporting that the state is
//  * "running".
//  */
// struct SimInstance {
//     /** Current runtime state of the instance */
//     current_run_state: ApiInstanceRuntimeState,
//     /**
//      * Requested runtime state of the instance.  This field is non-None if and
//      * only if we're currently simulating an asynchronous transition (e.g., boot
//      * or halt).
//      */
//     requested_run_state: Option<ApiInstanceRuntimeStateParams>,
//
//     /** Debug log */
//     log: Logger,
//     /** Channel for transmitting to the background task */
//     channel_tx: Option<Sender<()>>,
// }

/**
 * Buffer size for channel used to communicate with each SimInstance's
 * background task.  Messages sent on this channel trigger the task to simulate
 * an Instance state transition by sleeping for some interval and then updating
 * the Instance state.  When the background task updates the Instance state
 * after sleeping, it always looks at the current state to decide what to do.
 * As a result, we never need to queue up more than one transition.  In turn,
 * that means we don't need (or want) a channel buffer larger than 1.  If we
 * were to queue up multiple messages in the buffer, the net effect would be
 * exactly the same as if just one message were queued.  (Because of what we
 * said above, as part of processing that message, the receiver will wind up
 * handling all state transitions requested up to the point where the first
 * message is read.  If another transition is requested after that point,
 * another message will be enqueued and the receiver will process that
 * transition then.  There's no need to queue more than one message.)  Even
 * stronger: we don't want a larger buffer because that would only cause extra
 * laps through the sleep cycle, which just wastes resources and increases the
 * latency for processing the next real transition request.
 */
const SIM_CHANNEL_BUFFER_SIZE: usize = 0;

// impl SimInstance {
//     /**
//      * Create a new `SimInstance` with state transitions automatically
//      * simulated by a background task.  The caller is expected to provide the
//      * background task that reads from the channel and advances the simulation.
//      */
//     fn new_simulated_auto(
//         initial_runtime: &ApiInstanceRuntimeState,
//         log: Logger,
//     ) -> (SimInstance, Receiver<()>) {
//         debug!(log, "created simulated instance";
//             "initial_state" => ?initial_runtime);
//         let (tx, rx) = futures::channel::mpsc::channel(SIM_CHANNEL_BUFFER_SIZE);
//         (
//             SimInstance {
//                 current_run_state: initial_runtime.clone(),
//                 requested_run_state: None,
//                 log,
//                 channel_tx: Some(tx),
//             },
//             rx,
//         )
//     }
//
//     /**
//      * Create a new `SimInstance` with state transitions simulated by explicit
//      * calls.  The only difference from the perspective of this struct is that
//      * we won't have a channel to which we send notifications when asynchronous
//      * state transitions begin.
//      */
//     fn new_simulated_explicit(
//         initial_runtime: &ApiInstanceRuntimeState,
//         log: Logger,
//     ) -> SimInstance {
//         debug!(log, "created simulated instance";
//             "initial_state" => ?initial_runtime);
//         SimInstance {
//             current_run_state: initial_runtime.clone(),
//             requested_run_state: None,
//             log,
//             channel_tx: None,
//         }
//     }
//
//     /**
//      * Transition this Instance to state `given_target`.  In some cases, the
//      * transition may happen immediately (e.g., going from "Stopped" to
//      * "Destroyed").  In other cases, as when going from "Stopped" to "Running",
//      * we immediately transition to an intermediate state ("Starting", in this
//      * case), simulate the transition, and some time later update to the desired
//      * state.
//      *
//      * This function supports transitions that don't change the state at all
//      * (either because the requested state change was `None` -- maybe we were
//      * changing some other runtime state parameter -- or because we're already
//      * in the desired state).
//      */
//     fn transition(
//         &mut self,
//         given_target: &ApiInstanceRuntimeStateParams,
//     ) -> Option<ApiInstanceRuntimeStateParams> {
//         /*
//          * In all cases, set `requested_run_state` to the new target.  If there
//          * was already a requested run state, we will return this to the caller
//          * so that they can log a possible dropped transition.  This is only
//          * intended for debugging.
//          */
//         let dropped = self.requested_run_state.take();
//         let state_before = self.current_run_state.run_state.clone();
//         let mut state_after = match given_target.run_state {
//             /*
//              * For intermediate states (which don't really make sense to
//              * request), just try to do the closest reasonable thing.
//              * TODO-cleanup Use a different type here.
//              */
//             ApiInstanceState::Creating => &ApiInstanceState::Running,
//             ApiInstanceState::Starting => &ApiInstanceState::Running,
//             ApiInstanceState::Stopping => &ApiInstanceState::Stopped,
//
//             /* This is the most common interesting case. */
//             ref target_run_state => target_run_state,
//         };
//
//         /*
//          * There's nothing to do if the current and target states are the same
//          * AND either:
//          *
//          * - there's neither a reboot pending nor a reboot requested
//          * - there's both a reboot pending and a reboot requested and
//          *   the current reboot is still in the "Stopping" phase
//          *
//          * Otherwise, even if the states match, we may need to take action to
//          * begin or cancel a reboot.
//          *
//          * Reboot can only be requested with a target state of "Running".
//          * It doesn't make sense to reboot to any other state.
//          * TODO-debug log a warning in this case or make it impossible to
//          * represent?  Or validate it sooner?  TODO-cleanup if we create a
//          * separate ApiInstanceStateRequested as discussed elsewhere, the
//          * `Running` state could have a boolean indicating whether a reboot
//          * is requested first.
//          */
//         let reb_pending = self.current_run_state.reboot_in_progress;
//         let reb_wanted = *state_after == ApiInstanceState::Running
//             && given_target.reboot_wanted;
//         if *state_after == state_before
//             && ((!reb_pending && !reb_wanted)
//                 || (reb_pending
//                     && reb_wanted
//                     && state_before == ApiInstanceState::Stopping))
//         {
//             debug!(self.log, "noop transition"; "target" => ?given_target);
//             return dropped;
//         }
//
//         /*
//          * If we're doing a reboot, then we've already verified that the target
//          * run state is "Running", but for the rest of this function we'll treat
//          * it like a transition to "Stopped" (with an extra bit telling us later
//          * to transition again to Running).
//          */
//         if reb_wanted {
//             state_after = &ApiInstanceState::Stopped;
//         }
//
//         /*
//          * Depending on what state we're in and what state we're going to, we
//          * may need to transition to an intermediate state before we can get to
//          * the requested state.  In that case, we'll asynchronously simulate the
//          * transition.
//          */
//         let (immed_next_state, need_async) =
//             if state_before.is_stopped() && !state_after.is_stopped() {
//                 (&ApiInstanceState::Starting, true)
//             } else if !state_before.is_stopped() && state_after.is_stopped() {
//                 (&ApiInstanceState::Stopping, true)
//             } else {
//                 (state_after, false)
//             };
//
//         /*
//          * Update the current state to reflect what we've decided -- either
//          * going directly to the requested state or to an intermediate state.
//          */
//         self.current_run_state = ApiInstanceRuntimeState {
//             run_state: immed_next_state.clone(),
//             reboot_in_progress: reb_wanted,
//             server_uuid: self.current_run_state.server_uuid.clone(),
//             gen: self.current_run_state.gen + 1,
//             time_updated: Utc::now(),
//         };
//
//         debug!(self.log, "instance transition";
//             "state_before" => %state_before,
//             "state_after" => %state_after,
//             "immed_next_state" => %immed_next_state,
//             "dropped" => ?dropped,
//             "async" => %need_async,
//             "new_runtime" => ?self.current_run_state
//         );
//
//         /*
//          * If this is an asynchronous transition, notify the background task to
//          * simulate it.  There are a few possible error cases:
//          *
//          * (1) We fail to send the message because the channel's buffer is full.
//          *     All we need to guarantee in the first place is that the receiver
//          *     will receive a message at least once after this function is
//          *     invoked.  If there's already a message in the buffer, we don't
//          *     need to do anything else to achieve that.
//          *
//          * (2) We fail to send the message because the channel is disconnected.
//          *     This would be a programmer error -- the contract between us and
//          *     the receiver is that we shut down the channel first.  As a
//          *     result, we panic if we find this case.
//          *
//          * (3) We failed to send the message for some other reason.  This
//          *     appears impossible at the time of this writing.   It would be
//          *     nice if the returned error type were implemented in a way that we
//          *     could identify this case at compile time (e.g., using an enum),
//          *     but that's not currently the case.
//          */
//         if need_async {
//             self.requested_run_state = Some(ApiInstanceRuntimeStateParams {
//                 run_state: state_after.clone(),
//                 reboot_wanted: reb_wanted,
//             });
//             if let Some(ref mut tx) = self.channel_tx {
//                 let result = tx.try_send(());
//                 if let Err(error) = result {
//                     assert!(!error.is_disconnected());
//                     assert!(error.is_full());
//                 }
//             }
//         }
//
//         dropped
//     }
//
//     /**
//      * Finish simulating a "boot" or "halt" transition.
//      */
//     fn transition_finish(&mut self) {
//         let requested_run_state = match self.requested_run_state.take() {
//             /*
//              * Somebody must have requested a state change while we were
//              * simulating a previous asynchronous one.  By definition, the new
//              * one must also be asynchronous, and the first of the two calls to
//              * `transition_finish()` will complete the new transition.  The
//              * second one will find us here.
//              * TODO-cleanup We could probably eliminate this case by not
//              * sending a message to the background task if we were already in an
//              * async transition.
//              */
//             None => {
//                 debug!(self.log, "noop transition finish";
//                     "current_run_state" => %self.current_run_state.run_state);
//                 return;
//             }
//             Some(run_state) => run_state,
//         };
//
//         /*
//          * As documented above, `self.requested_run_state` is only non-None when
//          * there's an asynchronous (simulated) transition in progress, and the
//          * only such transitions start at "Starting" or "Stopping" and go to
//          * "Running" or one of several stopped states, respectively.  Since we
//          * checked `self.requested_run_state` above, we know we're in one of
//          * these two transitions and assert that here.
//          */
//         let run_state_before = self.current_run_state.run_state.clone();
//         let run_state_after = requested_run_state.run_state;
//         match run_state_before {
//             ApiInstanceState::Starting => {
//                 assert_eq!(run_state_after, ApiInstanceState::Running);
//                 assert!(!requested_run_state.reboot_wanted);
//             }
//             ApiInstanceState::Stopping => {
//                 assert!(run_state_after.is_stopped());
//                 assert_eq!(
//                     requested_run_state.reboot_wanted,
//                     self.current_run_state.reboot_in_progress
//                 );
//                 assert!(
//                     !requested_run_state.reboot_wanted
//                         || run_state_after == ApiInstanceState::Stopped
//                 );
//             }
//             _ => panic!("async transition started for unexpected state"),
//         };
//
//         /*
//          * Having verified all that, we can update the Instance's state.
//          */
//         self.current_run_state = ApiInstanceRuntimeState {
//             run_state: run_state_after.clone(),
//             reboot_in_progress: requested_run_state.reboot_wanted,
//             server_uuid: self.current_run_state.server_uuid.clone(),
//             gen: self.current_run_state.gen + 1,
//             time_updated: Utc::now(),
//         };
//
//         debug!(self.log, "simulated transition finish";
//             "state_before" => %run_state_before,
//             "state_after" => %run_state_after,
//             "new_runtime" => ?self.current_run_state
//         );
//
//         if self.current_run_state.reboot_in_progress {
//             assert_eq!(run_state_after, ApiInstanceState::Stopped);
//             self.transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: false,
//             });
//         }
//     }
// }
//
// struct SimDisk {
//     /** Current state of the disk */
//     current_state: ApiDiskState,
//     /** Requested change in state of the disk */
//     requested_state: ApiDiskStateRequested,
//     /** Debug log */
//     log: Logger,
//     /** Channel for transmitting to the background task */
//     channel_tx: Option<Sender<()>>,
// }
//
// impl SimDisk {
//     /**
//      * Create a new `SimDisk` with state transitions automatically simulated by
//      * a background task.  The caller is expected to provide the background task
//      * that reads from the channel and advances the simulation.
//      */
//     fn new_simulated_auto(
//         initial_state: &ApiDiskState,
//         log: Logger,
//     ) -> (SimDisk, Receiver<()>) {
//         debug!(log, "created simulated disk";
//             "initial_state" => ?initial_state);
//         let (tx, rx) = futures::channel::mpsc::channel(SIM_CHANNEL_BUFFER_SIZE);
//         (
//             SimDisk {
//                 current_state: initial_state.clone(),
//                 requested_state: ApiDiskStateRequested::NoChange,
//                 log,
//                 channel_tx: Some(tx),
//             },
//             rx,
//         )
//     }
//
//     /**
//      * Create a new `SimDisk` with state transitions simulated by explicit
//      * calls.  The only difference from the perspective of this struct is that
//      * we won't have a channel to which we send notifications when asynchronous
//      * state transitions begin.
//      */
//     fn new_simulated_explicit(
//         initial_state: &ApiDiskState,
//         log: Logger,
//     ) -> SimDisk {
//         debug!(log, "created simulated disk";
//             "initial_state" => ?initial_state);
//         SimDisk {
//             current_state: initial_state.clone(),
//             requested_state: ApiDiskStateRequested::NoChange,
//             log,
//             channel_tx: None,
//         }
//     }
//
//     /**
//      * Transition this Disk to the desired state.  In some cases, the transition
//      * may happen immediately (e.g., going from "Detached" to "Destroyed").  In
//      * other cases, as when going from "Attached" to "Detached", we immediately
//      * transition to an intermediate state ("Detaching", in this case), simulate
//      * the transition, and some time later update to the desired state.
//      *
//      * This function supports transitions that don't change the state at all
//      * (either because the requested state change was `None` -- maybe we were
//      * changing some other runtime state parameter -- or because we're already
//      * in the desired state).
//      */
//     fn transition(
//         &mut self,
//         requested_state: &ApiDiskStateRequested,
//     ) -> ApiDiskStateRequested {
//         /*
//          * In all cases, set `requested_state` to the new target.  If there was
//          * already a requested run state, we will return this to the caller so
//          * that they can log a possible dropped transition.  This is only
//          * intended for debugging.
//          */
//         let dropped = self.requested_state.clone();
//         let state_before = self.current_state.clone();
//         let state_after = requested_state;
//
//         let to_do = match (state_before, state_after) {
//             /*
//              * This is the primary way to indicate no change from the current
//              * state.
//              */
//             (_, ApiDiskStateRequested::NoChange) => None,
//
//             /*
//              * It's conceivable that we'd be asked to transition from a state to
//              * itself, in which case we also don't need to do anything.
//              */
//             (ApiDiskState::Attached, ApiDiskStateRequested::Attached(_)) => {
//                 None
//             }
//             (ApiDiskState::Detached, ApiDiskStateRequested::Detached) => None,
//             (ApiDiskState::Destroyed, ApiDiskStateRequested::Destroyed) => None,
//             (ApiDiskState::Faulted, ApiDiskStateRequested::Faulted) => None,
//
//             /*
//              * If we're going from any unattached state to "Attached" (the only
//              * requestable attached state), the appropriate next state is
//              * "Attaching", and it will be an asynchronous transition to
//              * "Attached".
//              */
//             (state, ApiDiskStateRequested::Attached(_))
//                 if !state.is_attached() =>
//             {
//                 Some((ApiDiskState::Attaching, true))
//             }
//
//             /*
//              * If we're going from any attached state to any detached state,
//              * then we'll go straight to "Detaching" en route to the new state.
//              */
//             (from_state, to_state)
//                 if from_state.is_attached() && !to_state.is_attached() =>
//             {
//                 Some((ApiDiskState::Detaching, true))
//             }
//
//             /*
//              * The only remaining options are transitioning from one detached
//              * state to a different one, in which case we can go straight there
//              * with no need for an asynchronous transition.
//              */
//             (from_state, ApiDiskStateRequested::Destroyed) => {
//                 assert!(!from_state.is_attached());
//                 Some((ApiDiskState::Destroyed, false))
//             }
//
//             (from_state, ApiDiskStateRequested::Detached) => {
//                 assert!(!from_state.is_attached());
//                 Some((ApiDiskState::Detached, false))
//             }
//
//             (from_state, ApiDiskStateRequested::Faulted) => {
//                 assert!(!from_state.is_attached());
//                 Some((ApiDiskState::Faulted, false))
//             }
//         };
//
//         if to_do.is_none() {
//             debug!(self.log, "noop transition";
//                 "requested_state" => ?requested_state);
//             return dropped;
//         }
//
//         let (immed_next_state, need_async) = to_do.unwrap();
//         self.current_state = immed_next_state;
//
//         debug!(self.log, "disk transition";
//             "state_before" => %state_before,
//             "state_after" => ?state_after,
//             "immed_next_state" => %immed_next_state,
//             "dropped" => ?dropped,
//             "async" => %need_async,
//             "current_state" => ?self.current_state
//         );
//
//         /*
//          * TODO-cleanup see SimInstance::transition().
//          */
//         if need_async {
//             self.requested_state = requested_state.clone();
//             if let Some(ref mut tx) = self.channel_tx {
//                 let result = tx.try_send(());
//                 if let Err(error) = result {
//                     assert!(!error.is_disconnected());
//                     assert!(error.is_full());
//                 }
//             }
//         } else {
//             self.requested_state = ApiDiskStateRequested::NoChange;
//         }
//
//         dropped
//     }
//
//     /**
//      * Finish simulating an "attach" or "detach" transition.
//      */
//     fn transition_finish(&mut self) {
//         let next_state = match self.requested_state {
//             /* TODO-cleanup see SimInstance::transition_finish(). */
//             ApiDiskStateRequested::NoChange => {
//                 debug!(self.log, "noop transition finish";
//                     "current_state" => %self.current_state);
//                 return;
//             }
//
//             ApiDiskStateRequested::Attached(_) => ApiDiskState::Attached,
//             ApiDiskStateRequested::Destroyed => ApiDiskState::Destroyed,
//             ApiDiskStateRequested::Faulted => ApiDiskState::Faulted,
//             ApiDiskStateRequested::Detached => ApiDiskState::Detached,
//         };
//
//         let state_before = self.current_state.clone();
//         self.current_state = next_state;
//
//         debug!(self.log, "simulated transition finish";
//             "state_before" => %state_before,
//             "state_after" => %self.current_state,
//         );
//     }
// }
//
// #[cfg(test)]
// mod test {
//     use super::SimInstance;
//     use crate::api_model::ApiInstanceRuntimeState;
//     use crate::api_model::ApiInstanceRuntimeStateParams;
//     use crate::api_model::ApiInstanceState;
//     use crate::test_util::test_setup_log;
//     use chrono::Utc;
//     use dropshot::test_util::LogContext;
//     use futures::channel::mpsc::Receiver;
//
//     fn make_instance(
//         logctx: &LogContext,
//         initial_state: ApiInstanceState,
//     ) -> (SimInstance, Receiver<()>) {
//         let now = Utc::now();
//         let initial_runtime = {
//             ApiInstanceRuntimeState {
//                 run_state: initial_state,
//                 reboot_in_progress: false,
//                 server_uuid: uuid::Uuid::new_v4(),
//                 gen: 1,
//                 time_updated: now,
//             }
//         };
//
//         SimInstance::new_simulated_auto(&initial_runtime, logctx.log.new(o!()))
//     }
//
//     /*
//      * Test non-reboot-related transitions.
//      */
//     #[tokio::test]
//     async fn test_sim_instance() {
//         let logctx = test_setup_log("test_sim_instance").await;
//         let (mut instance, mut rx) =
//             make_instance(&logctx, ApiInstanceState::Creating);
//         let r1 = instance.current_run_state.clone();
//
//         info!(logctx.log, "new instance"; "run_state" => ?r1.run_state);
//         assert_eq!(r1.run_state, ApiInstanceState::Creating);
//         assert_eq!(r1.gen, 1);
//
//         /*
//          * There's no asynchronous transition going on yet so a
//          * transition_finish() shouldn't change anything.
//          */
//         assert!(instance.requested_run_state.is_none());
//         instance.transition_finish();
//         assert!(instance.requested_run_state.is_none());
//         assert_eq!(&r1.time_updated, &instance.current_run_state.time_updated);
//         assert_eq!(&r1.run_state, &instance.current_run_state.run_state);
//         assert_eq!(r1.gen, instance.current_run_state.gen);
//         assert!(rx.try_next().is_err());
//
//         /*
//          * We should be able to transition immediately to any other stopped
//          * state.  We can't do this for "Creating" because transition() treats
//          * that as a transition to "Running".
//          */
//         let stopped_states = vec![
//             ApiInstanceState::Stopped,
//             ApiInstanceState::Repairing,
//             ApiInstanceState::Failed,
//             ApiInstanceState::Destroyed,
//         ];
//         let mut rprev = r1;
//         for state in stopped_states {
//             assert!(rprev.run_state.is_stopped());
//             let dropped = instance.transition(&ApiInstanceRuntimeStateParams {
//                 run_state: state.clone(),
//                 reboot_wanted: false,
//             });
//             assert!(dropped.is_none());
//             assert!(instance.requested_run_state.is_none());
//             let rnext = instance.current_run_state.clone();
//             if state != rprev.run_state {
//                 assert!(rnext.gen > rprev.gen);
//             }
//             assert!(rnext.time_updated >= rprev.time_updated);
//             assert_eq!(rnext.run_state, state);
//             assert!(rx.try_next().is_err());
//             rprev = rnext;
//         }
//
//         /*
//          * Now, if we transition to "Running", we must go through the async
//          * process.
//          */
//         assert!(rprev.run_state.is_stopped());
//         assert!(rx.try_next().is_err());
//         let dropped = instance.transition(&ApiInstanceRuntimeStateParams {
//             run_state: ApiInstanceState::Running,
//             reboot_wanted: false,
//         });
//         assert!(dropped.is_none());
//         assert!(instance.requested_run_state.is_some());
//         assert!(rx.try_next().is_ok());
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         assert!(!rnext.run_state.is_stopped());
//         rprev = rnext;
//
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert!(instance.requested_run_state.is_none());
//         assert!(rx.try_next().is_err());
//         assert_eq!(rprev.run_state, ApiInstanceState::Starting);
//         assert_eq!(rnext.run_state, ApiInstanceState::Running);
//         rprev = rnext;
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rprev.gen, rnext.gen);
//
//         /*
//          * If we transition again to "Running", the process should complete
//          * immediately.
//          */
//         assert!(!rprev.run_state.is_stopped());
//         let dropped = instance.transition(&ApiInstanceRuntimeStateParams {
//             run_state: ApiInstanceState::Running,
//             reboot_wanted: false,
//         });
//         assert!(dropped.is_none());
//         assert!(instance.requested_run_state.is_none());
//         assert!(rx.try_next().is_err());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.gen, rprev.gen);
//         assert_eq!(rnext.time_updated, rprev.time_updated);
//         assert_eq!(rnext.run_state, rprev.run_state);
//         rprev = rnext;
//
//         /*
//          * If we go back to any stopped state, we go through the async process
//          * again.
//          */
//         assert!(!rprev.run_state.is_stopped());
//         assert!(rx.try_next().is_err());
//         let dropped = instance.transition(&ApiInstanceRuntimeStateParams {
//             run_state: ApiInstanceState::Destroyed,
//             reboot_wanted: false,
//         });
//         assert!(dropped.is_none());
//         assert!(instance.requested_run_state.is_some());
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         assert!(!rnext.run_state.is_stopped());
//         rprev = rnext;
//
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert!(instance.requested_run_state.is_none());
//         assert_eq!(rprev.run_state, ApiInstanceState::Stopping);
//         assert_eq!(rnext.run_state, ApiInstanceState::Destroyed);
//         rprev = rnext;
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rprev.gen, rnext.gen);
//
//         /*
//          * Now let's test the behavior of dropping a transition.  We'll start
//          * transitioning back to "Running".  Then, while we're still in
//          * "Starting", will transition back to "Destroyed".  We should
//          * immediately go to "Stopping", and completing the transition should
//          * take us to "Destroyed".
//          */
//         assert!(rprev.run_state.is_stopped());
//         let dropped = instance.transition(&ApiInstanceRuntimeStateParams {
//             run_state: ApiInstanceState::Running,
//             reboot_wanted: false,
//         });
//         assert!(dropped.is_none());
//         assert!(instance.requested_run_state.is_some());
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         assert!(!rnext.run_state.is_stopped());
//         rprev = rnext;
//
//         /*
//          * Interrupt the async transition with a new one.
//          */
//         let dropped = instance.transition(&ApiInstanceRuntimeStateParams {
//             run_state: ApiInstanceState::Destroyed,
//             reboot_wanted: false,
//         });
//         assert_eq!(dropped.unwrap().run_state, ApiInstanceState::Running);
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         rprev = rnext;
//
//         /*
//          * Finish the async transition.
//          */
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated >= rprev.time_updated);
//         assert!(instance.requested_run_state.is_none());
//         assert_eq!(rprev.run_state, ApiInstanceState::Stopping);
//         assert_eq!(rnext.run_state, ApiInstanceState::Destroyed);
//         rprev = rnext;
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rprev.gen, rnext.gen);
//
//         logctx.cleanup_successful();
//     }
//
//     /*
//      * Test reboot-related transitions.
//      */
//     #[tokio::test]
//     async fn test_sim_instance_reboot() {
//         let logctx = test_setup_log("test_sim_instance_reboot").await;
//
//         /*
//          * Get an initial instance up to "Running".
//          */
//         let (mut instance, _rx) =
//             make_instance(&logctx, ApiInstanceState::Creating);
//         let r1 = instance.current_run_state.clone();
//
//         info!(logctx.log, "new instance"; "run_state" => ?r1.run_state);
//         assert_eq!(r1.run_state, ApiInstanceState::Creating);
//         assert_eq!(r1.gen, 1);
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: false,
//             })
//             .is_none());
//         instance.transition_finish();
//         let (rprev, rnext) = (r1, instance.current_run_state.clone());
//         assert!(rnext.gen > rprev.gen);
//
//         /*
//          * Now, take it through a reboot sequence.
//          */
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: true,
//             })
//             .is_none());
//         let (rprev, rnext) = (rnext, instance.current_run_state.clone());
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated > rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         assert!(rnext.reboot_in_progress);
//         assert!(instance.requested_run_state.is_some());
//         instance.transition_finish();
//         let (rprev, rnext) = (rnext, instance.current_run_state.clone());
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated > rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         assert!(!rnext.reboot_in_progress);
//         assert!(instance.requested_run_state.is_some());
//         instance.transition_finish();
//         let (rprev, rnext) = (rnext, instance.current_run_state.clone());
//         assert!(rnext.gen > rprev.gen);
//         assert!(rnext.time_updated > rprev.time_updated);
//         assert_eq!(rnext.run_state, ApiInstanceState::Running);
//         assert!(instance.requested_run_state.is_none());
//
//         /*
//          * Begin a reboot.  Then, while it's still "Stopping", begin another
//          * reboot.  This should go through exactly one reboot sequence, as the
//          * second reboot is totally superfluous.
//          */
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: true,
//             })
//             .is_none());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: true,
//             })
//             .is_some());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Running);
//         assert!(instance.requested_run_state.is_none());
//         instance.transition_finish();
//         let (rprev, rnext) = (rnext, instance.current_run_state.clone());
//         assert_eq!(rprev.gen, rnext.gen);
//
//         /*
//          * Begin a reboot.  Then, while it's "Starting" (on the way back up),
//          * begin another reboot.  This should go through a second reboot
//          * sequence.
//          */
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: true,
//             })
//             .is_none());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: true,
//             })
//             .is_some());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Running);
//         assert!(instance.requested_run_state.is_none());
//         instance.transition_finish();
//         let (rprev, rnext) = (rnext, instance.current_run_state.clone());
//         assert_eq!(rprev.gen, rnext.gen);
//
//         /*
//          * At this point, we've exercised what happens when a reboot is issued
//          * from "Running", from "Starting" with a reboot in progress, from
//          * "Stopping" with a reboot in progress.  All that's left is "Starting"
//          * with no reboot in progress.  First, stop the instance.  Then start
//          * it.  Then, while it's starting, begin a reboot sequence.
//          */
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Stopped,
//                 reboot_wanted: false,
//             })
//             .is_none());
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopped);
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: false,
//             })
//             .is_none());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         assert!(instance
//             .transition(&ApiInstanceRuntimeStateParams {
//                 run_state: ApiInstanceState::Running,
//                 reboot_wanted: true,
//             })
//             .is_some());
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Stopping);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Starting);
//         instance.transition_finish();
//         let rnext = instance.current_run_state.clone();
//         assert_eq!(rnext.run_state, ApiInstanceState::Running);
//         assert!(instance.requested_run_state.is_none());
//         instance.transition_finish();
//         let (rprev, rnext) = (rnext, instance.current_run_state.clone());
//         assert_eq!(rprev.gen, rnext.gen);
//
//         /*
//          * Issuing a reboot from any other state is not defined, including from
//          * "Stopping" while not in the process of a reboot and from any
//          * "stopped" state.  instance_ensure() will prevent this, while
//          * transition() will allow it.  We don't test the behavior of
//          * transition() because it's subject to change.
//          */
//
//         logctx.cleanup_successful();
//     }
// }

/*
 * XXX docs, etc.
 */
#[async_trait]
trait Simulatable: fmt::Debug {
    type CurrentState: Send + Clone + fmt::Debug;
    type RequestedState: Send + Clone + fmt::Debug;

    /*
     * This function returns the immediate next state and whether an async
     * transition has been started.
     */
    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>;

    /*
     * This function returns the next "current state" when applying an
     * asynchronous requested state.
     */
    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        next: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>);

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool;

    fn ready_to_destroy(
        current: &Self::CurrentState,
        next: &Option<Self::RequestedState>,
    ) -> bool;

    /*
     * This function notifies the controller about a state change.
     */
    async fn notify(
        csc: &Arc<ControllerScApi>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError>;
}

#[derive(Debug)]
struct SimObject<S: Simulatable> {
    current_state: S::CurrentState,
    requested_state: Option<S::RequestedState>,
    log: Logger,
    channel_tx: Option<Sender<()>>,
}

impl<S: Simulatable> SimObject<S> {
    fn new_simulated_auto(
        initial_state: &S::CurrentState,
        log: Logger,
    ) -> (SimObject<S>, Receiver<()>) {
        debug!(log, "created"; "initial_state" => ?initial_state);
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

    fn new_simulated_explicit(
        initial_state: &S::CurrentState,
        log: Logger,
    ) -> SimObject<S> {
        debug!(log, "created"; "initial_state" => ?initial_state);
        SimObject {
            current_state: initial_state.clone(),
            requested_state: None,
            log,
            channel_tx: None,
        }
    }

    /* This is the more direct analog to SimInstance::transition() */
    fn transition(
        &mut self,
        target: S::RequestedState,
    ) -> Result<Option<S::RequestedState>, ApiError> {
        let dropped = self.requested_state.clone();
        let state_before = self.current_state.clone();

        let (state_after, requested_state) =
            S::next_state_for_new_target(&state_before, &dropped, &target)?;

        if S::state_unchanged(&state_before, &state_after) {
            debug!(self.log, "noop transition"; "target" => ?target);
            return Ok(None);
        }

        debug!(self.log, "transition";
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
        if let Some(new_state) = &requested_state {
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
            debug!(self.log, "noop transition finish"; "current" => ?self);
            return;
        }

        let requested_state = self.requested_state.take().unwrap();
        let (next_state, next_async) =
            S::next_state_for_async_transition_finish(
                &self.current_state,
                &requested_state,
            );
        debug!(self.log, "simulated transition finish";
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

struct SimCollection<S: Simulatable> {
    ctlsc: Arc<ControllerScApi>,
    log: Logger,
    sim_mode: ServerControllerSimMode,
    objects: Mutex<BTreeMap<Uuid, SimObject<S>>>,
}

impl<S: Simulatable + 'static> SimCollection<S> {
    fn new(
        ctlsc: Arc<ControllerScApi>,
        log: Logger,
        sim_mode: ServerControllerSimMode,
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
     */
    async fn sim_step(&self, id: Uuid, mut rx: Receiver<()>) {
        while let Some(_) = rx.next().await {
            tokio::time::delay_for(Duration::from_millis(1500)).await;
            self.sim_poke(id).await;
        }
    }

    async fn sim_poke(&self, id: Uuid) {
        let (new_state, to_destroy) = {
            let mut objects = self.objects.lock().await;
            let mut object = objects.remove(&id).unwrap();
            object.transition_finish();
            let after = object.current_state.clone();
            if S::ready_to_destroy(
                &object.current_state,
                &object.requested_state,
            ) {
                (after, Some(object))
            } else {
                objects.insert(id.clone(), object);
                (after, None)
            }
        };

        /*
         * TODO-robustness: If this fails, we need to put it on some list of
         * updates to retry later.
         */
        S::notify(&self.ctlsc, &id, new_state).await.unwrap();

        if let Some(destroyed_object) = to_destroy {
            if let Some(mut tx) = destroyed_object.channel_tx {
                tx.close_channel();
            }
        }
    }

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

                if let ServerControllerSimMode::Auto = self.sim_mode {
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

#[derive(Debug)]
struct SimInstance {}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = ApiInstanceRuntimeState;
    type RequestedState = ApiInstanceRuntimeStateParams;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        target: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        if target.reboot_wanted && target.run_state != ApiInstanceState::Running
        {
            return Err(ApiError::InvalidRequest {
                message: String::from(
                    "cannot reboot to a state other than \"running\"",
                ),
            });
        }

        let state_before = current.run_state.clone();

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
            server_uuid: current.server_uuid.clone(),
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };

        let next_async = if need_async {
            Some(ApiInstanceRuntimeStateParams {
                run_state: state_after.clone(),
                reboot_wanted: reb_wanted,
            })
        } else {
            None
        };

        Ok((next_state, next_async))
    }

    /*
     * This function returns the next "current state" when applying an
     * asynchronous requested state.
     */
    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        /*
         * As documented above, `self.requested_run_state` is only non-None when
         * there's an asynchronous (simulated) transition in progress, and the
         * only such transitions start at "Starting" or "Stopping" and go to
         * "Running" or one of several stopped states, respectively.  Since we
         * checked `self.requested_run_state` above, we know we're in one of
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
            server_uuid: current.server_uuid.clone(),
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };

        let next_async = if next_state.reboot_in_progress {
            assert_eq!(*run_state_after, ApiInstanceState::Stopped);
            Some(ApiInstanceRuntimeStateParams {
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

    fn ready_to_destroy(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
    ) -> bool {
        pending.is_none() && current.run_state == ApiInstanceState::Destroyed
    }

    async fn notify(
        csc: &Arc<ControllerScApi>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        /*
         * Notify the controller that the instance state has changed.  The
         * server controller is authoritative for the runtime state, and we use
         * a generation number here so that calls processed out of order do not
         * settle on the wrong value.
         */
        csc.notify_instance_updated(id, &current).await
    }
}

#[derive(Debug)]
struct SimDisk {}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = ApiDiskState;
    type RequestedState = ApiDiskStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        todo!();
    }

    /*
     * This function returns the next "current state" when applying an
     * asynchronous requested state.
     */
    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        next: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        todo!();
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        todo!();
    }

    fn ready_to_destroy(
        current: &Self::CurrentState,
        next: &Option<Self::RequestedState>,
    ) -> bool {
        todo!();
    }

    async fn notify(
        csc: &Arc<ControllerScApi>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        todo!();
    }
}
