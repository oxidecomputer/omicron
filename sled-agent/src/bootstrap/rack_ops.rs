// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal API for rack-level bootstrap agent operations.

use crate::bootstrap::RssAccessError::MultirackJoinNotYetStarted;
use crate::bootstrap::rss_handle::run_rss;
use bootstrap_agent_lockstep_types::MultirackJoinRequest;
use bootstrap_agent_lockstep_types::MultirackJoinServiceState;
use bootstrap_agent_lockstep_types::RackOperationStatus;
use bootstrap_agent_lockstep_types::RssStep;
use futures::FutureExt;
use futures::future::Shared;
use omicron_uuid_kinds::MultirackJoinUuid;
use omicron_uuid_kinds::RackInitUuid;
use sled_agent_bootstrap_common::RssContext;
use sled_agent_multirack_join::MultirackJoinInput;
use sled_agent_multirack_join::MultirackJoinServiceError;
use sled_agent_multirack_join::MultirackJoinServiceHandle;
use sled_agent_rack_setup::RackInitializeRequestParams;
use sled_agent_rack_setup::SetupServiceError;
use sled_agent_types::multirack::MultirackJoinServiceStatus;
use slog_error_chain::InlineErrorChain;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, thiserror::Error)]
pub enum RssAccessError {
    #[error("RSS is still initializing and cannot run concurrently")]
    StillInitializing,
    #[error("RSS failed to initialize: {message}")]
    InitializationFailed { message: String },
    #[error("RSS panicked while initializing")]
    InitializationPanicked,
    #[error("RSS is already initialized")]
    AlreadyInitialized,
    #[error("Multirack join in progress")]
    MultirackJoinInProgress,
    #[error("Multirack join failed: {message}")]
    MultirackJoinFailed { message: String },
    #[error("MultirackJoin panicked")]
    MultirackJoinPanicked,
    #[error("Already part of a multirack cluster")]
    MultirackJoinCompleted,
    #[error("Membership cannot be changed anymore")]
    MembershipChangeNoLongerAllowed,
    #[error("Rack subnet cannot be changed after initial configuration")]
    RackSubnetChangeNotAllowed,
    #[error("Multirack join has not yet started")]
    MultirackJoinNotYetStarted,
    #[error("Multirack join service status is not yet available")]
    MultirackJoinStatusNotYetAvailable,
    #[error("Multirack join task exited unexpectedly")]
    MultirackJoinTaskExitedUnexpectedly,
    #[error("Multirack join bootstore generation is not available")]
    MultirackJoinGenerationUnavailable,
    #[error(
        "Multirack join network configuration is not yet written to the \
        bootstore"
    )]
    MultirackJoinPendingConfigUpdate,
}

impl RssAccessError {
    /// Return a stable error code for this error.
    pub fn error_code(&self) -> &'static str {
        // Don't change these even if enum variants change.
        match self {
            RssAccessError::StillInitializing => "StillInitializing",
            RssAccessError::InitializationFailed { .. } => {
                "InitializationFailed"
            }
            RssAccessError::InitializationPanicked => "InitializationPanicked",
            RssAccessError::AlreadyInitialized => "AlreadyInitialized",
            RssAccessError::MultirackJoinInProgress => {
                "MultirackJoinInProgress"
            }
            RssAccessError::MultirackJoinFailed { .. } => "MultirackJoinFailed",
            RssAccessError::MultirackJoinPanicked => "MultirackJoinPanicked",
            RssAccessError::MultirackJoinCompleted => "MultirackJoinCompleted",
            RssAccessError::MembershipChangeNoLongerAllowed => {
                "MembershipChangeNoLongerAllowed"
            }
            RssAccessError::RackSubnetChangeNotAllowed => {
                "RackSubnetChangeNotAllowed"
            }
            RssAccessError::MultirackJoinNotYetStarted => {
                "MultirackJoinNotYetStarted"
            }
            RssAccessError::MultirackJoinStatusNotYetAvailable => {
                "MultirackJoinStatusNotYetAvailable"
            }
            RssAccessError::MultirackJoinTaskExitedUnexpectedly => {
                "MultirackJoinTaskExitedUnexpectedly"
            }
            RssAccessError::MultirackJoinGenerationUnavailable => {
                "MultirackJoinGenerationUnavailable"
            }
            RssAccessError::MultirackJoinPendingConfigUpdate => {
                "MultirackJoinPendingConfigUpdate"
            }
        }
    }
}

/// A mechanism for sled-agent to access its local multirack join service task.
#[derive(Clone)]
pub(crate) struct MultirackJoinAccessor(RssAccess);

impl MultirackJoinAccessor {
    pub fn new(rss_access: RssAccess) -> MultirackJoinAccessor {
        Self(rss_access)
    }

    /// Complete the multirack join and wait for the multirack join task to
    /// exit.
    ///
    /// This is triggered on a request from Nexus to sled-agent.
    pub async fn complete_multirack_join(&self) -> Result<u64, RssAccessError> {
        self.0.complete_multirack_join().await
    }

    /// Return the current status of the multirack join service if it is
    /// running.
    pub fn multirack_join_service_status(
        &self,
    ) -> Result<MultirackJoinServiceStatus, RssAccessError> {
        self.0.multirack_join_service_status()
    }
}

/// A mechanism for accessing rack setup related functionality. We're
/// using `RSS` as a catch all here. This type allows access to both rack
/// initialization and mulitrack regional cluster join services.
#[derive(Clone)]
pub(crate) struct RssAccess {
    // Note: The `Mutex` here is a std mutex, not a tokio mutex, and thus not
    // subject to async cancellation issues (and also cannot be held across an
    // `.await` point). We only keep it held long enough to perform quick
    // operations: fill it in or read its current value.
    status: Arc<Mutex<RssStatus>>,
}

impl RssAccess {
    pub(super) fn new(initialized: bool) -> Self {
        let status = if initialized {
            RssStatus::Initialized { id: None }
        } else {
            RssStatus::Uninitialized
        };
        Self { status: Arc::new(Mutex::new(status)) }
    }

    pub(super) fn operation_status(&self) -> RackOperationStatus {
        let mut status = self.status.lock().unwrap();

        match &mut *status {
            RssStatus::Uninitialized => RackOperationStatus::Uninitialized,
            RssStatus::Initializing { id, completion, step_rx } => {
                let id = *id;
                // This is our only chance to notice the initialization task has
                // panicked: if it dropped the sending half of `completion`
                // without reporting in.
                match completion.try_recv() {
                    Ok(()) => {
                        // This should be unreachable, I think? But it is
                        // harmless to report the initialized state.
                        RackOperationStatus::Initialized { id: Some(id) }
                    }
                    Err(TryRecvError::Empty) => {
                        // Initialization task is still running
                        // Update the step we are on.
                        RackOperationStatus::Initializing {
                            id,
                            step: *step_rx.borrow(),
                        }
                    }
                    Err(TryRecvError::Closed) => {
                        // Initialization task has panicked!
                        *status = RssStatus::InitializationPanicked { id };
                        RackOperationStatus::InitializationPanicked { id }
                    }
                }
            }
            RssStatus::Initialized { id } => {
                RackOperationStatus::Initialized { id: *id }
            }
            RssStatus::InitializationFailed { id, err } => {
                RackOperationStatus::InitializationFailed {
                    id: *id,
                    message: InlineErrorChain::new(err).to_string(),
                }
            }
            RssStatus::InitializationPanicked { id } => {
                RackOperationStatus::InitializationPanicked { id: *id }
            }
            RssStatus::MultirackJoinInProgress { id, .. } => {
                RackOperationStatus::MultirackJoinInProgress { id: *id }
            }
            RssStatus::MultirackJoinCompleted { id, .. } => {
                RackOperationStatus::MultirackJoinCompleted { id: Some(*id) }
            }
            RssStatus::MultirackJoinFailed { id, err } => {
                RackOperationStatus::MultirackJoinFailed {
                    id: *id,
                    message: InlineErrorChain::new(err).to_string(),
                }
            }
            RssStatus::MultirackJoinPanicked { id } => {
                RackOperationStatus::MultirackJoinPanicked { id: *id }
            }
        }
    }

    pub(crate) fn start_initializing(
        &self,
        ctx: RssContext,
        request: RackInitializeRequestParams,
    ) -> Result<RackInitUuid, RssAccessError> {
        let mut status = self.status.lock().unwrap();

        match &*status {
            RssStatus::Uninitialized => {
                let (completion_tx, completion) = oneshot::channel();
                let id = RackInitUuid::new_v4();
                let (step_tx, step_rx) = watch::channel(RssStep::Requested);
                *status = RssStatus::Initializing { id, completion, step_rx };
                mem::drop(status);
                let status = Arc::clone(&self.status);
                tokio::spawn(async move {
                    let result = run_rss(ctx, request, step_tx).await;
                    let new_status = match result {
                        Ok(()) => RssStatus::Initialized { id: Some(id) },
                        Err(err) => RssStatus::InitializationFailed { id, err },
                    };

                    // Order here is critical: store the new status in the
                    // shared mutex _before_ signaling on the channel that
                    // initialization has completed; otherwise, callers waiting
                    // on the channel could see an incomplete status.
                    *status.lock().unwrap() = new_status;
                    _ = completion_tx.send(());
                });
                Ok(id)
            }

            RssStatus::Initializing { .. } => {
                Err(RssAccessError::StillInitializing)
            }
            RssStatus::Initialized { .. } => {
                Err(RssAccessError::AlreadyInitialized)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(RssAccessError::InitializationFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }

            RssStatus::MultirackJoinInProgress { .. } => {
                Err(RssAccessError::MultirackJoinInProgress)
            }
            RssStatus::MultirackJoinCompleted { .. } => {
                Err(RssAccessError::MultirackJoinCompleted)
            }
            RssStatus::MultirackJoinFailed { err, .. } => {
                Err(RssAccessError::MultirackJoinFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::MultirackJoinPanicked { .. } => {
                Err(RssAccessError::MultirackJoinPanicked)
            }
        }
    }

    /// This should only occur once this sled is successfully on the underlay
    /// network and discoverable by Nexus.
    ///
    /// Waits for the multirack join task to exit and returns the final
    /// bootstore generation it wrote. If the join has already completed
    /// successfully we treat this as idempotent.
    pub(crate) async fn complete_multirack_join(
        &self,
    ) -> Result<u64, RssAccessError> {
        let result_rx = match &mut *self.status.lock().unwrap() {
            RssStatus::Uninitialized => {
                Err(RssAccessError::MultirackJoinNotYetStarted)
            }
            RssStatus::Initializing { .. } => {
                Err(RssAccessError::StillInitializing)
            }
            RssStatus::Initialized { .. } => {
                // After cold boot Nexus may still be trying to get a
                // response for whether multirack join has completed. If the
                // `StartSledAgentRequest` was recorded to the ledger, the sled
                // agent itself will start with this status. The final
                // bootstore generation was only kept in memory, so we can't
                // return it.
                //
                // The same applies in the case that Nexus initialized a rack
                // via RSS, which would only occur if Nexus sent the completion
                // request to an RSS'd sled-agent, which it really should not
                // do.
                //
                // See https://github.com/oxidecomputer/omicron/issues/11477
                Err(RssAccessError::MultirackJoinGenerationUnavailable)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(RssAccessError::InitializationFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }
            RssStatus::MultirackJoinInProgress {
                input_tx,
                membership_change_still_possible,
                written_bootstore_generation_rx,
                rack_join_successful_tx,
                result_rx,
                ..
            } => {
                // We can't successfully complete the join if we haven't
                // finished setting up trust quorum.
                if membership_change_still_possible.load(Ordering::Relaxed) {
                    return Err(RssAccessError::MultirackJoinInProgress);
                }

                // Only complete the join once the multirack join task has
                // written the latest accepted input to the bootstore.
                if *written_bootstore_generation_rx.borrow()
                    != input_tx.borrow().bootstore_generation
                {
                    return Err(
                        RssAccessError::MultirackJoinPendingConfigUpdate,
                    );
                }

                // We haven't sent the completion signal yet.
                if let Some(tx) = rack_join_successful_tx.take() {
                    // If the task already exited, this fails and we return
                    // its result below.
                    _ = tx.send(());
                }

                // We've either sent the completion signal this time, or we did
                // previously. This can happen due to a lost reply to Nexus and
                // a retry. Either way, we wait for the task's result.
                Ok(result_rx.clone())
            }
            RssStatus::MultirackJoinCompleted {
                bootstore_generation, ..
            } => return Ok(*bootstore_generation),
            RssStatus::MultirackJoinFailed { err, .. } => {
                Err(RssAccessError::MultirackJoinFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::MultirackJoinPanicked { .. } => {
                Err(RssAccessError::MultirackJoinPanicked)
            }
        }?;

        wait_for_multirack_join_result(result_rx).await
    }

    pub(crate) fn start_multirack_join(
        &self,
        ctx: RssContext,
        request: MultirackJoinRequest,
    ) -> Result<MultirackJoinUuid, RssAccessError> {
        let mut status = self.status.lock().unwrap();

        match &mut *status {
            RssStatus::Uninitialized => {
                let id = MultirackJoinUuid::new_v4();
                let MultirackJoinServiceHandle {
                    join_handle,
                    input_tx,
                    output_rx,
                    membership_change_still_possible,
                    written_bootstore_generation_rx,
                    rack_join_successful_tx,
                } = MultirackJoinServiceHandle::spawn(ctx, request);
                let (result_tx, result_rx) = oneshot::channel();
                *status = RssStatus::MultirackJoinInProgress {
                    id,
                    input_tx,
                    output_rx,
                    membership_change_still_possible,
                    written_bootstore_generation_rx,
                    rack_join_successful_tx: Some(rack_join_successful_tx),
                    result_rx: result_rx.shared(),
                };
                mem::drop(status);

                self.spawn_multirack_join_supervisor(
                    id,
                    join_handle,
                    result_tx,
                );

                Ok(id)
            }

            RssStatus::Initializing { .. } => {
                Err(RssAccessError::StillInitializing)
            }
            RssStatus::Initialized { .. } => {
                Err(RssAccessError::AlreadyInitialized)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(RssAccessError::InitializationFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }

            RssStatus::MultirackJoinInProgress {
                id,
                input_tx,
                membership_change_still_possible,
                rack_join_successful_tx,
                ..
            } => {
                // We allow updating the input so that we don't always require a
                // clean slate like in RSS.
                let mut invalid_membership_change = false;
                let mut invalid_rack_subnet_change = false;
                let mut multirack_join_completed = false;
                input_tx.send_if_modified(|saved| {
                    if request != saved.request {
                        // After trust quorum initializes we no longer allow
                        // membership changes. Check for that here so we can
                        // error out early.
                        //
                        // At that point, we also no longer allow rack network
                        // changes, so also error out if necessary.
                        if !membership_change_still_possible
                            .load(Ordering::Relaxed)
                        {
                            if saved.request.trust_quorum_peers
                                != request.trust_quorum_peers
                            {
                                invalid_membership_change = true;
                                return false;
                            }
                            if saved.request.rack_network_config.rack_subnet
                                != request.rack_network_config.rack_subnet
                            {
                                invalid_rack_subnet_change = true;
                                return false;
                            }
                        }
                        if rack_join_successful_tx.is_none() {
                            // `complete_multirack_join` has already been
                            // called, but we haven't witnessed the multirack
                            // join task exit yet. It's racy to accept changes
                            // after this point and so we reject them.
                            // `complete_multirack_join` relies on this, since
                            // the task writes nothing more once it gets the
                            // signal.
                            multirack_join_completed = true;
                            return false;
                        }
                        saved.update(request);
                        true
                    } else {
                        false
                    }
                });
                if invalid_membership_change {
                    Err(RssAccessError::MembershipChangeNoLongerAllowed)
                } else if invalid_rack_subnet_change {
                    Err(RssAccessError::RackSubnetChangeNotAllowed)
                } else if multirack_join_completed {
                    Err(RssAccessError::MultirackJoinCompleted)
                } else {
                    Ok(*id)
                }
            }
            RssStatus::MultirackJoinCompleted { .. } => {
                Err(RssAccessError::MultirackJoinCompleted)
            }
            RssStatus::MultirackJoinFailed { err, .. } => {
                Err(RssAccessError::MultirackJoinFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::MultirackJoinPanicked { .. } => {
                Err(RssAccessError::MultirackJoinPanicked)
            }
        }
    }

    /// Spawn a task that waits for the multirack join service task to exit,
    /// then records its result in the status and publishes it on `result_tx`
    fn spawn_multirack_join_supervisor(
        &self,
        id: MultirackJoinUuid,
        join_handle: JoinHandle<Result<u64, MultirackJoinServiceError>>,
        result_tx: oneshot::Sender<Result<u64, RssAccessError>>,
    ) {
        let status = Arc::clone(&self.status);
        tokio::spawn(async move {
            let (new_status, result) = match join_handle.await {
                Ok(Ok(bootstore_generation)) => (
                    RssStatus::MultirackJoinCompleted {
                        id,
                        bootstore_generation,
                    },
                    Ok(bootstore_generation),
                ),
                Ok(Err(err)) => {
                    let message = InlineErrorChain::new(&err).to_string();
                    (
                        RssStatus::MultirackJoinFailed { id, err },
                        Err(RssAccessError::MultirackJoinFailed { message }),
                    )
                }
                Err(_) => (
                    RssStatus::MultirackJoinPanicked { id },
                    Err(RssAccessError::MultirackJoinPanicked),
                ),
            };

            // Update status before sending the result, so that callers see the
            // latest.
            *status.lock().unwrap() = new_status;

            // There may be no receivers left so we ignore the `send` result.
            let _ = result_tx.send(result);
        });
    }

    pub(crate) fn multirack_join_service_status(
        &self,
    ) -> Result<MultirackJoinServiceStatus, RssAccessError> {
        match &*self.status.lock().unwrap() {
            RssStatus::MultirackJoinInProgress { output_rx, .. } => {
                match output_rx.borrow().clone() {
                    MultirackJoinServiceState::StartSledAgents(status) => {
                        Ok(status.into())
                    }
                    MultirackJoinServiceState::ConfigureNetworking {
                        start_sled_agent_status: Some(status),
                    } => Ok(status.into()),
                    MultirackJoinServiceState::WaitForCompletion(status) => {
                        Ok(status.into())
                    }
                    MultirackJoinServiceState::ConfigureNetworking {
                        start_sled_agent_status: None,
                    }
                    | MultirackJoinServiceState::Requested
                    | MultirackJoinServiceState::Starting
                    | MultirackJoinServiceState::TrustQuorumReconfigure(_)
                    | MultirackJoinServiceState::TrustQuorumPreparing(_)
                    | MultirackJoinServiceState::TrustQuorumCommitting(_) => {
                        Err(RssAccessError::MultirackJoinStatusNotYetAvailable)
                    }
                    MultirackJoinServiceState::Uninitialized => {
                        Err(RssAccessError::MultirackJoinNotYetStarted)
                    }
                    MultirackJoinServiceState::Completed => {
                        Err(RssAccessError::MultirackJoinCompleted)
                    }
                    MultirackJoinServiceState::Failed { message } => {
                        Err(RssAccessError::MultirackJoinFailed { message })
                    }
                    MultirackJoinServiceState::InvalidMembershipSize {
                        message,
                    } => Err(RssAccessError::MultirackJoinFailed { message }),
                    MultirackJoinServiceState::TaskPanicked => {
                        Err(RssAccessError::MultirackJoinPanicked)
                    }
                }
            }
            RssStatus::Uninitialized => Err(MultirackJoinNotYetStarted),
            RssStatus::Initialized { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }
            RssStatus::Initializing { .. } => {
                Err(RssAccessError::StillInitializing)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(RssAccessError::InitializationFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }
            RssStatus::MultirackJoinCompleted { .. } => {
                Err(RssAccessError::MultirackJoinCompleted)
            }
            RssStatus::MultirackJoinFailed { err, .. } => {
                Err(RssAccessError::MultirackJoinFailed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::MultirackJoinPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }
        }
    }

    pub(crate) fn get_multirack_join_state(
        &self,
    ) -> Result<MultirackJoinServiceState, RssConflictError> {
        let mut status = self.status.lock().unwrap();

        match &mut *status {
            RssStatus::Uninitialized => {
                Ok(MultirackJoinServiceState::Uninitialized)
            }

            RssStatus::Initializing { .. } => Err(RssConflictError),
            RssStatus::Initialized { .. } => Err(RssConflictError),
            RssStatus::InitializationFailed { .. } => Err(RssConflictError),
            RssStatus::InitializationPanicked { .. } => Err(RssConflictError),

            RssStatus::MultirackJoinInProgress { output_rx, .. } => {
                let state = output_rx.borrow().clone();
                Ok(state)
            }
            RssStatus::MultirackJoinCompleted { .. } => {
                Ok(MultirackJoinServiceState::Completed)
            }
            RssStatus::MultirackJoinFailed { err, .. } => {
                Ok(MultirackJoinServiceState::Failed {
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::MultirackJoinPanicked { .. } => {
                Ok(MultirackJoinServiceState::TaskPanicked)
            }
        }
    }
}

// RSS was run, and not the multirack join service
pub struct RssConflictError;

// The result of the multirack join task, published by its supervisor once the
// task exits.
type MultirackJoinResultRx =
    Shared<oneshot::Receiver<Result<u64, RssAccessError>>>;

/// Wait for the multirack join result published by the supervisor
async fn wait_for_multirack_join_result(
    result_rx: MultirackJoinResultRx,
) -> Result<u64, RssAccessError> {
    result_rx
        .await
        // The supervisor exited without publishing a result
        .unwrap_or(Err(RssAccessError::MultirackJoinTaskExitedUnexpectedly))
}

enum RssStatus {
    // Our two main primary states.
    Uninitialized,
    Initialized {
        // We can either be initialized on startup (in which case `id`
        // is None) or because initialization has completed (in which case `id`
        // is Some).
        id: Option<RackInitUuid>,
    },

    // Transitory states (which we may be in for a long time, even on human time
    // scales, but should eventually leave).
    Initializing {
        id: RackInitUuid,
        completion: oneshot::Receiver<()>,
        // Used by the RSS task to update us with what step it is on.
        // This holds the current RSS step.
        step_rx: watch::Receiver<RssStep>,
    },

    // Terminal failure states; these require support intervention.
    InitializationFailed {
        id: RackInitUuid,
        err: SetupServiceError,
    },
    InitializationPanicked {
        id: RackInitUuid,
    },

    // Tranistory states (which we may be in for a long time, even on human time
    // scales, but should eventually leave).
    MultirackJoinInProgress {
        id: MultirackJoinUuid,
        input_tx: watch::Sender<MultirackJoinInput>,
        output_rx: watch::Receiver<MultirackJoinServiceState>,
        membership_change_still_possible: Arc<AtomicBool>,
        written_bootstore_generation_rx: watch::Receiver<u64>,
        rack_join_successful_tx: Option<oneshot::Sender<()>>,
        // Holds the result of the multirack join task once it exits and the
        // status has been updated.
        result_rx: MultirackJoinResultRx,
    },

    MultirackJoinCompleted {
        id: MultirackJoinUuid,
        // The final bootstore generation written by the multirack join task.
        // This is only kept in memory, so it's unavailable after sled-agent
        // restarts.
        bootstore_generation: u64,
    },

    // Terminal failure states; these require support intervention.
    MultirackJoinFailed {
        id: MultirackJoinUuid,
        err: MultirackJoinServiceError,
    },
    MultirackJoinPanicked {
        id: MultirackJoinUuid,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use dropshot::ErrorStatusCode;
    use dropshot::HttpError;
    use sled_agent_types::early_networking::{
        LinkSpeed, PortConfig, RackNetworkConfig, SwitchSlot, UplinkPorts,
    };
    use std::collections::BTreeSet;
    use std::time::Duration;

    const TEST_TIMEOUT: Duration = Duration::from_secs(60);

    async fn with_timeout<F: Future>(fut: F) -> F::Output {
        tokio::time::timeout(TEST_TIMEOUT, fut).await.expect("timed out")
    }

    fn request() -> MultirackJoinRequest {
        // RackNetworkConfig's ports must be nonempty.
        let ports = vec![PortConfig {
            routes: Vec::new(),
            addresses: Vec::new(),
            switch: SwitchSlot::Switch1,
            port: "qsfp0".to_owned(),
            uplink_port_speed: LinkSpeed::Speed100G,
            uplink_port_fec: None,
            bgp_peers: Vec::new(),
            autoneg: false,
            lldp: None,
            tx_eq: None,
            allow_ddm_traffic: false,
        }];
        MultirackJoinRequest {
            trust_quorum_peers: BTreeSet::new(),
            rack_network_config: RackNetworkConfig {
                rack_subnet: "fd00:abcd:ffff::/56".parse().unwrap(),
                infra_ip_first: "10.0.0.1".parse().unwrap(),
                infra_ip_last: "10.0.0.100".parse().unwrap(),
                ports: UplinkPorts::new(ports).unwrap(),
                bgp: Vec::new(),
                bfd: Vec::new(),
            },
        }
    }

    #[derive(Debug)]
    enum FakeTaskExit {
        Success(u64),
        Failure,
    }

    /// The test's side of a fake multirack join task
    struct FakeTask {
        rack_join_successful_rx: oneshot::Receiver<()>,
        written_bootstore_generation_tx: watch::Sender<u64>,
        exit_tx: oneshot::Sender<FakeTaskExit>,
        _input_rx: watch::Receiver<MultirackJoinInput>,
        _output_tx: watch::Sender<MultirackJoinServiceState>,
    }

    impl FakeTask {
        fn exit(self, exit: FakeTaskExit) {
            self.exit_tx.send(exit).unwrap();
        }
    }

    /// Put `rss` into the `MultirackJoinInProgress` state, after trust quorum
    /// has committed, with a supervised task that exits when the test says.
    fn start_fake_multirack_join(rss: &RssAccess) -> FakeTask {
        // One change to the request has been accepted
        let (input_tx, input_rx) = watch::channel(MultirackJoinInput {
            request: request(),
            bootstore_generation: 2,
        });
        let (output_tx, output_rx) =
            watch::channel(MultirackJoinServiceState::Requested);
        let (rack_join_successful_tx, rack_join_successful_rx) =
            oneshot::channel();
        // The task has written it
        let (written_bootstore_generation_tx, written_bootstore_generation_rx) =
            watch::channel(2);
        let (exit_tx, exit_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            match exit_rx.await.unwrap() {
                FakeTaskExit::Success(generation) => Ok(generation),
                FakeTaskExit::Failure => {
                    Err(MultirackJoinServiceError::TqBadCoordinator)
                }
            }
        });

        let id = MultirackJoinUuid::new_v4();
        let (result_tx, result_rx) = oneshot::channel();
        *rss.status.lock().unwrap() = RssStatus::MultirackJoinInProgress {
            id,
            input_tx,
            output_rx,
            membership_change_still_possible: Arc::new(AtomicBool::new(false)),
            written_bootstore_generation_rx,
            rack_join_successful_tx: Some(rack_join_successful_tx),
            result_rx: result_rx.shared(),
        };
        rss.spawn_multirack_join_supervisor(id, join_handle, result_tx);

        FakeTask {
            rack_join_successful_rx,
            written_bootstore_generation_tx,
            exit_tx,
            _input_rx: input_rx,
            _output_tx: output_tx,
        }
    }

    #[tokio::test]
    async fn completion_rejected_until_latest_input_written() {
        let rss = RssAccess::new(false);
        let mut task = start_fake_multirack_join(&rss);

        // The task has only written an earlier generation. Its request is
        // the same, as when an update is reverted.
        task.written_bootstore_generation_tx.send_replace(1);
        assert_matches!(
            rss.complete_multirack_join().now_or_never(),
            Some(Err(RssAccessError::MultirackJoinPendingConfigUpdate))
        );
        assert_matches!(
            task.rack_join_successful_rx.try_recv(),
            Err(TryRecvError::Empty)
        );

        // The task hasn't written anything yet
        task.written_bootstore_generation_tx.send_replace(0);
        assert_matches!(
            rss.complete_multirack_join().now_or_never(),
            Some(Err(RssAccessError::MultirackJoinPendingConfigUpdate))
        );
        assert_matches!(
            task.rack_join_successful_rx.try_recv(),
            Err(TryRecvError::Empty)
        );

        // Once the latest generation is written, the join can complete
        task.written_bootstore_generation_tx.send_replace(2);
        let mut caller = Box::pin(rss.complete_multirack_join());
        assert!(caller.as_mut().now_or_never().is_none());
        task.rack_join_successful_rx.try_recv().unwrap();

        task.exit(FakeTaskExit::Success(2));
        assert_eq!(with_timeout(caller).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn completion_is_idempotent() {
        let rss = RssAccess::new(false);
        let mut task = start_fake_multirack_join(&rss);

        // Concurrent callers all wait for the task
        let mut callers: Vec<_> =
            (0..3).map(|_| Box::pin(rss.complete_multirack_join())).collect();
        for caller in &mut callers {
            assert!(caller.as_mut().now_or_never().is_none());
        }
        task.rack_join_successful_rx.try_recv().unwrap();

        task.exit(FakeTaskExit::Success(7));
        for caller in callers {
            assert_eq!(with_timeout(caller).await.unwrap(), 7);
        }

        // Later callers get the same generation right away
        assert_matches!(
            rss.complete_multirack_join().now_or_never(),
            Some(Ok(7))
        );
    }

    #[tokio::test]
    async fn failure_wakes_waiters() {
        let rss = RssAccess::new(false);
        let task = start_fake_multirack_join(&rss);

        let mut caller = Box::pin(rss.complete_multirack_join());
        assert!(caller.as_mut().now_or_never().is_none());

        task.exit(FakeTaskExit::Failure);
        let message = assert_matches!(
            with_timeout(caller).await,
            Err(RssAccessError::MultirackJoinFailed { message }) => message
        );

        // Later callers read the status and get the same message
        assert_matches!(
            rss.complete_multirack_join().now_or_never(),
            Some(Err(RssAccessError::MultirackJoinFailed { message: later }))
                if later == message
        );
    }

    #[tokio::test]
    async fn generation_unavailable_when_already_initialized() {
        let rss = RssAccess::new(true);

        assert_matches!(
            rss.complete_multirack_join().now_or_never(),
            Some(Err(RssAccessError::MultirackJoinGenerationUnavailable))
        );

        let err = HttpError::from(crate::sled_agent::Error::from(
            RssAccessError::MultirackJoinGenerationUnavailable,
        ));
        assert_eq!(err.status_code, ErrorStatusCode::CONFLICT);
        assert_eq!(
            err.error_code.as_deref(),
            Some("MultirackJoinGenerationUnavailable")
        );
    }
}
