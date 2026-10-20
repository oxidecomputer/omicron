// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal API for rack-level bootstrap agent operations.

use crate::bootstrap::rss_handle::run_rss;
use bootstrap_agent_lockstep_types::MultirackJoinRequest;
use bootstrap_agent_lockstep_types::MultirackJoinServiceState;
use bootstrap_agent_lockstep_types::RackOperationStatus;
use bootstrap_agent_lockstep_types::RssStep;
use dropshot::HttpError;
use omicron_uuid_kinds::MultirackJoinUuid;
use omicron_uuid_kinds::RackInitUuid;
use sled_agent_bootstrap_common::RssContext;
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
        }
    }
}

impl From<RssAccessError> for HttpError {
    fn from(err: RssAccessError) -> Self {
        // Most variants of RssAccessError report states that conflict with the
        // requested rack operation in some fashion:
        //
        // * `StillInitializing`/`MultirackJoinInProgress` mean that an
        //   operation is in progress.
        // * `AlreadyInitialized`/`MultirackJoinCompleted` mean that we're
        //   already at the requested end state. (Note that we don't try and
        //   be idempotent here -- to do so we'd have to ensure the actual RSS
        //   config is the same, which is tricky. Instead, clients should look
        //   at the error code to determine what to do.)
        // * `MultirackJoinNotYetStarted` means it can't complete when signalled.
        // * The other states are terminal states that need operator intervention.
        //
        // We map all of these states to 409 Conflict errors, and (since this is
        // an internal API) expose the error text to the client.
        match err {
            RssAccessError::MultirackJoinTaskExitedUnexpectedly => {
                HttpError::for_internal_error(err.to_string())
            }
            RssAccessError::StillInitializing
            | RssAccessError::InitializationFailed { .. }
            | RssAccessError::InitializationPanicked
            | RssAccessError::AlreadyInitialized
            | RssAccessError::MultirackJoinInProgress
            | RssAccessError::MultirackJoinFailed { .. }
            | RssAccessError::MultirackJoinPanicked
            | RssAccessError::MultirackJoinCompleted
            | RssAccessError::MembershipChangeNoLongerAllowed
            | RssAccessError::RackSubnetChangeNotAllowed
            | RssAccessError::MultirackJoinNotYetStarted
            | RssAccessError::MultirackJoinStatusNotYetAvailable => {
                HttpError::for_client_error(
                    Some(err.error_code().to_string()),
                    dropshot::ClientErrorStatusCode::CONFLICT,
                    err.to_string(),
                )
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

    /// Complete the multirack join.
    ///
    /// This is triggered on a request from Nexus to sled-agent.
    pub fn complete_multirack_join(&self) -> Result<(), RssAccessError> {
        self.0.complete_multirack_join()
    }

    /// Return the current status of the multirack join service if it is
    /// running.
    pub fn multirack_join_service_status(
        &self,
    ) -> Result<Option<MultirackJoinServiceStatus>, RssAccessError> {
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
            RssStatus::MultirackJoinCompleted { id } => {
                RackOperationStatus::MultirackJoinCompleted { id: *id }
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
    /// If the join has already completed succcessfully we treat this as idempotent.
    pub(crate) fn complete_multirack_join(&self) -> Result<(), RssAccessError> {
        let mut status = self.status.lock().unwrap();
        match &mut *status {
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
                // agent itself will start with this status.
                //
                // Since Nexus should only be sending the completion signals
                // to sled-agents it knows were joining, we figure it's correct
                // to idempotently return `Ok(())` here rather than an error.
                // This ensures Nexus gets a valid response.
                //
                // The whole point of sending this signal from Nexus is to
                // ensure that the sled-agent is on the underlay network before
                // we conclude the join. Therefore if we are already up and
                // running and receive messages from Nexus we know we are on the
                // underlay network, and may as well reply in the affirmative
                // to Nexus.
                //
                // We'll also return `OK(())` in the case that Nexus initialized
                // a rack via RSS, but that seems harmless, and would only occur
                // if Nexus sent the completion request to an RSS'd sled-agent,
                // which it really should not do.
                //
                // See https://github.com/oxidecomputer/omicron/issues/11477
                Ok(())
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
                membership_change_still_possible,
                rack_join_successful_tx,
                ..
            } => {
                // We can't successfully complete the join if we haven't
                // finished setting up trust quorum.
                if membership_change_still_possible.load(Ordering::Relaxed) {
                    return Err(RssAccessError::MultirackJoinInProgress);
                }

                // We haven't sent the completion signal yet.
                if let Some(tx) = rack_join_successful_tx.take() {
                    tx.send(()).map_err(|_| {
                        RssAccessError::MultirackJoinTaskExitedUnexpectedly
                    })?;
                }

                // We've either sent the completion signal this time, or we did
                // previously. This can happen due to a lost reply to Nexus and
                // a retry. We treat both as success, even though it's highly
                // unlikely given the timespan differences between task receipt
                // and HTTP retry and resend.
                Ok(())
            }
            RssStatus::MultirackJoinCompleted { .. } => Ok(()),
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
                    status_rx,
                    membership_change_still_possible,
                    rack_join_successful_tx,
                } = MultirackJoinServiceHandle::spawn(ctx, request);
                *status = RssStatus::MultirackJoinInProgress {
                    id,
                    input_tx,
                    output_rx,
                    status_rx,
                    membership_change_still_possible,
                    rack_join_successful_tx: Some(rack_join_successful_tx),
                };
                mem::drop(status);
                let status = Arc::clone(&self.status);

                // Spawn a task that waits for the multirack join service task to exit
                tokio::spawn(async move {
                    let new_status = match join_handle.await {
                        Ok(Ok(())) => {
                            RssStatus::MultirackJoinCompleted { id: Some(id) }
                        }
                        Ok(Err(err)) => {
                            RssStatus::MultirackJoinFailed { id, err }
                        }
                        Err(_) => RssStatus::MultirackJoinPanicked { id },
                    };
                    *status.lock().unwrap() = new_status;
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
                    if &request != saved {
                        // After trust quorum initializes we no longer allow
                        // membership changes. Check for that here so we can
                        // error out early.
                        //
                        // At that point, we also no longer allow rack network
                        // changes, so also error out if necessary.
                        if !membership_change_still_possible
                            .load(Ordering::Relaxed)
                        {
                            if saved.trust_quorum_peers
                                != request.trust_quorum_peers
                            {
                                invalid_membership_change = true;
                                return false;
                            }
                            if saved.rack_network_config.rack_subnet
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
                            multirack_join_completed = true;
                            return false;
                        }
                        *saved = request;
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

    pub(crate) fn multirack_join_service_status(
        &self,
    ) -> Result<Option<MultirackJoinServiceStatus>, RssAccessError> {
        match &*self.status.lock().unwrap() {
            RssStatus::MultirackJoinInProgress { status_rx, .. } => {
                match status_rx.borrow().clone() {
                    Some(status) => Ok(Some(status)),
                    None => {
                        Err(RssAccessError::MultirackJoinStatusNotYetAvailable)
                    }
                }
            }
            RssStatus::Uninitialized
            | RssStatus::Initialized { .. }
            | RssStatus::Initializing { .. }
            | RssStatus::InitializationFailed { .. }
            | RssStatus::InitializationPanicked { .. }
            | RssStatus::MultirackJoinCompleted { .. }
            | RssStatus::MultirackJoinFailed { .. }
            | RssStatus::MultirackJoinPanicked { .. } => Ok(None),
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
        input_tx: watch::Sender<MultirackJoinRequest>,
        output_rx: watch::Receiver<MultirackJoinServiceState>,
        status_rx: watch::Receiver<Option<MultirackJoinServiceStatus>>,
        membership_change_still_possible: Arc<AtomicBool>,
        rack_join_successful_tx: Option<oneshot::Sender<()>>,
    },

    MultirackJoinCompleted {
        // We can either be joined on startup (in which case `id` is None) or
        // because join has completed (in which case `id` is Some).
        id: Option<MultirackJoinUuid>,
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

    #[test]
    fn rss_access_error_to_http_error() {
        let cases = [
            RssAccessError::StillInitializing,
            RssAccessError::InitializationFailed {
                message: "boom".to_string(),
            },
            RssAccessError::InitializationPanicked,
            RssAccessError::AlreadyInitialized,
        ];
        for err in cases {
            let error_code = err.error_code();
            let display = err.to_string();
            let http_error = HttpError::from(err);
            assert_eq!(
                http_error.status_code.as_u16(),
                409,
                "{error_code}: expected 409 Conflict"
            );
            assert_eq!(
                http_error.error_code.as_deref(),
                Some(error_code),
                "{error_code}: error_code should be the variant name"
            );
            assert_eq!(
                http_error.external_message, display,
                "{error_code}: full error text should be exposed to the \
                 client"
            );
            assert_eq!(
                http_error.internal_message, display,
                "{error_code}: internal message should match the error \
                 text"
            );
        }
    }
}
