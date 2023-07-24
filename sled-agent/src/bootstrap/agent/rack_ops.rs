// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal API for rack-level bootstrap agent operations.

use super::Agent;
use crate::bootstrap::http_entrypoints::RackOperationStatus;
use crate::bootstrap::params::RackInitializeRequest;
use crate::bootstrap::rss_handle::RssHandle;
use crate::rack_setup::service::SetupServiceError;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use uuid::Uuid;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct RackInitId(pub Uuid);

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct RackResetId(pub Uuid);

#[derive(Debug, Clone, thiserror::Error)]
pub enum RssAccessError {
    #[error("RSS is still initializating and cannot run concurrently")]
    StillInitializing,
    #[error("RSS failed to initialize: {message}")]
    InitializationFailed { message: String },
    #[error("RSS panicked while initializing")]
    InitializationPanicked,
    #[error("RSS is already initialized")]
    AlreadyInitialized,
    #[error("RSS is still resetting and cannot run concurrently")]
    StillResetting,
    #[error("RSS failed to reset: {message}")]
    ResetFailed { message: String },
    #[error("RSS panicked while resetting")]
    ResetPanicked,
    #[error("RSS is already reset")]
    AlreadyReset,
}

pub(super) struct RssAccess {
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
            RssStatus::Uninitialized { reset_id: None }
        };
        Self { status: Arc::new(Mutex::new(status)) }
    }

    pub(super) fn operation_status(&self) -> RackOperationStatus {
        let mut status = self.status.lock().unwrap();

        match &mut *status {
            RssStatus::Initializing { id, completion } => {
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
                        RackOperationStatus::Initializing { id }
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
                    message: format!("{err:#}"),
                }
            }
            RssStatus::InitializationPanicked { id } => {
                RackOperationStatus::InitializationPanicked { id: *id }
            }
            RssStatus::Uninitialized { reset_id } => {
                RackOperationStatus::Uninitialized { reset_id: *reset_id }
            }
            RssStatus::Resetting { id, completion } => {
                let id = *id;
                // This is our only chance to notice the initialization task has
                // panicked: if it dropped the sending half of `completion`
                // without reporting in.
                match completion.try_recv() {
                    Ok(()) => {
                        // This should be unreachable, I think? But it is
                        // harmless to report the reset state.
                        RackOperationStatus::Uninitialized {
                            reset_id: Some(id),
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        // Initialization task is still running
                        RackOperationStatus::Resetting { id }
                    }
                    Err(TryRecvError::Closed) => {
                        // Initialization task has panicked!
                        *status = RssStatus::ResetPanicked { id };
                        RackOperationStatus::ResetPanicked { id }
                    }
                }
            }
            RssStatus::ResetFailed { id, err } => {
                RackOperationStatus::ResetFailed {
                    id: *id,
                    message: format!("{err:#}"),
                }
            }
            RssStatus::ResetPanicked { id } => {
                RackOperationStatus::ResetPanicked { id: *id }
            }
        }
    }

    pub(super) fn start_initializing(
        &self,
        agent: &Arc<Agent>,
        request: RackInitializeRequest,
    ) -> Result<RackInitId, RssAccessError> {
        let mut status = self.status.lock().unwrap();

        match &*status {
            RssStatus::Initializing { .. } => {
                Err(RssAccessError::StillInitializing)
            }
            RssStatus::Initialized { .. } => {
                Err(RssAccessError::AlreadyInitialized)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(RssAccessError::InitializationFailed {
                    message: err.to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }

            RssStatus::Resetting { .. } => Err(RssAccessError::StillResetting),
            RssStatus::ResetFailed { err, .. } => {
                Err(RssAccessError::ResetFailed { message: err.to_string() })
            }
            RssStatus::ResetPanicked { .. } => {
                Err(RssAccessError::ResetPanicked)
            }
            RssStatus::Uninitialized { .. } => {
                let (completion_tx, completion) = oneshot::channel();
                let id = RackInitId(Uuid::new_v4());
                *status = RssStatus::Initializing { id, completion };
                mem::drop(status);

                let agent = Arc::clone(agent);
                let status = Arc::clone(&self.status);
                tokio::spawn(async move {
                    let result = rack_initialize(&agent, request).await;
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
        }
    }

    pub(super) fn start_reset(
        &self,
        agent: &Arc<Agent>,
    ) -> Result<RackResetId, RssAccessError> {
        let mut status = self.status.lock().unwrap();

        match &*status {
            RssStatus::Initializing { .. } => {
                Err(RssAccessError::StillInitializing)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(RssAccessError::InitializationFailed {
                    message: err.to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }
            RssStatus::Resetting { .. } => Err(RssAccessError::StillResetting),
            RssStatus::ResetFailed { err, .. } => {
                Err(RssAccessError::ResetFailed { message: err.to_string() })
            }
            RssStatus::ResetPanicked { .. } => {
                Err(RssAccessError::ResetPanicked)
            }
            RssStatus::Uninitialized { .. } => {
                Err(RssAccessError::AlreadyReset)
            }
            RssStatus::Initialized { .. } => {
                let (completion_tx, completion) = oneshot::channel();
                let id = RackResetId(Uuid::new_v4());
                *status = RssStatus::Resetting { id, completion };
                mem::drop(status);

                let agent = Arc::clone(agent);
                let status = Arc::clone(&self.status);
                tokio::spawn(async move {
                    let result = rack_reset(&agent).await;
                    let new_status = match result {
                        Ok(()) => {
                            RssStatus::Uninitialized { reset_id: Some(id) }
                        }
                        Err(err) => RssStatus::ResetFailed { id, err },
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
        }
    }
}

enum RssStatus {
    // Our two main primary states.
    Uninitialized {
        // We can either be uninitialized on startup (in which case `reset_id`
        // is None) or because a reset has completed (in which case `reset_id`
        // is Some).
        reset_id: Option<RackResetId>,
    },
    Initialized {
        // We can either be initialized on startup (in which case `id`
        // is None) or because initialization has completed (in which case `id`
        // is Some).
        id: Option<RackInitId>,
    },

    // Tranistory states (which we may be in for a long time, even on human time
    // scales, but should eventually leave).
    Initializing {
        id: RackInitId,
        completion: oneshot::Receiver<()>,
    },
    Resetting {
        id: RackResetId,
        completion: oneshot::Receiver<()>,
    },

    // Terminal failure states; these require support intervention.
    InitializationFailed {
        id: RackInitId,
        err: SetupServiceError,
    },
    InitializationPanicked {
        id: RackInitId,
    },
    ResetFailed {
        id: RackResetId,
        err: SetupServiceError,
    },
    ResetPanicked {
        id: RackResetId,
    },
}

async fn rack_initialize(
    agent: &Agent,
    request: RackInitializeRequest,
) -> Result<(), SetupServiceError> {
    RssHandle::run_rss(
        &agent.parent_log,
        request,
        agent.ip,
        agent.storage_resources.clone(),
        agent.get_bootstore_node_handle(),
    )
    .await
}

async fn rack_reset(agent: &Agent) -> Result<(), SetupServiceError> {
    RssHandle::run_rss_reset(&agent.parent_log, agent.ip).await
}
