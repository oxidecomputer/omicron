// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal API for rack-level bootstrap agent operations.

use super::Agent;
use crate::bootstrap::http_entrypoints::RackOperationStatus;
use crate::bootstrap::params::RackInitializeRequest;
use crate::bootstrap::rss_handle::RssHandle;
use crate::config::SidecarRevision;
use crate::rack_setup::service::SetupServiceError;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RackInitId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RackResetId(pub Uuid);

#[derive(Debug, Clone, thiserror::Error)]
pub enum ConcurrentRssAccess {
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
            RssStatus::InitializedOnStartup
        } else {
            RssStatus::ResetOnStartup
        };
        Self { status: Arc::new(Mutex::new(status)) }
    }

    pub(super) fn operation_status(&self) -> RackOperationStatus {
        let mut status = self.status.lock().unwrap();

        match &mut *status {
            RssStatus::InitializedOnStartup => {
                RackOperationStatus::Initialized { id: None }
            }
            RssStatus::Initializing { id, completion } => {
                // This is our only chance to notice the initialization task has
                // panicked: if it dropped the sending half of `completion`
                // without reporting in.
                match completion.try_recv() {
                    Ok(()) => {
                        // This should be unreachable, I think? But it is
                        // harmless to report the initialized state.
                        RackOperationStatus::Initialized { id: Some(id.0) }
                    }
                    Err(TryRecvError::Empty) => {
                        // Initialization task is still running
                        RackOperationStatus::Initializing { id: id.0 }
                    }
                    Err(TryRecvError::Closed) => {
                        // Initialization task has panicked!
                        let id = *id;
                        *status = RssStatus::InitializationPanicked { id };
                        RackOperationStatus::InitializationPanicked { id: id.0 }
                    }
                }
            }
            RssStatus::Initialized { id } => {
                RackOperationStatus::Initialized { id: Some(id.0) }
            }
            RssStatus::InitializationFailed { id, err } => {
                RackOperationStatus::InitializationFailed {
                    id: id.0,
                    message: format!("{err:#}"),
                }
            }
            RssStatus::InitializationPanicked { id } => {
                RackOperationStatus::InitializationPanicked { id: id.0 }
            }
            RssStatus::ResetOnStartup => {
                RackOperationStatus::Reset { id: None }
            }
            RssStatus::Resetting { id, completion } => {
                // This is our only chance to notice the initialization task has
                // panicked: if it dropped the sending half of `completion`
                // without reporting in.
                match completion.try_recv() {
                    Ok(()) => {
                        // This should be unreachable, I think? But it is
                        // harmless to report the reset state.
                        RackOperationStatus::Reset { id: Some(id.0) }
                    }
                    Err(TryRecvError::Empty) => {
                        // Initialization task is still running
                        RackOperationStatus::Resetting { id: id.0 }
                    }
                    Err(TryRecvError::Closed) => {
                        // Initialization task has panicked!
                        let id = *id;
                        *status = RssStatus::ResetPanicked { id };
                        RackOperationStatus::ResetPanicked { id: id.0 }
                    }
                }
            }
            RssStatus::Reset { id } => {
                RackOperationStatus::Reset { id: Some(id.0) }
            }
            RssStatus::ResetFailed { id, err } => {
                RackOperationStatus::ResetFailed {
                    id: id.0,
                    message: format!("{err:#}"),
                }
            }
            RssStatus::ResetPanicked { id } => {
                RackOperationStatus::ResetPanicked { id: id.0 }
            }
        }
    }

    pub(super) fn start_initializing(
        &self,
        agent: &Arc<Agent>,
        request: RackInitializeRequest,
    ) -> Result<RackInitId, ConcurrentRssAccess> {
        let mut status = self.status.lock().unwrap();

        match &*status {
            RssStatus::Initializing { .. } => {
                Err(ConcurrentRssAccess::StillInitializing)
            }
            RssStatus::InitializedOnStartup | RssStatus::Initialized { .. } => {
                Err(ConcurrentRssAccess::AlreadyInitialized)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(ConcurrentRssAccess::InitializationFailed {
                    message: err.to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(ConcurrentRssAccess::InitializationPanicked)
            }

            RssStatus::Resetting { .. } => {
                Err(ConcurrentRssAccess::StillResetting)
            }
            RssStatus::ResetFailed { err, .. } => {
                Err(ConcurrentRssAccess::ResetFailed {
                    message: err.to_string(),
                })
            }
            RssStatus::ResetPanicked { .. } => {
                Err(ConcurrentRssAccess::ResetPanicked)
            }
            RssStatus::ResetOnStartup | RssStatus::Reset { .. } => {
                let (completion_tx, completion) = oneshot::channel();
                let id = RackInitId(Uuid::new_v4());
                *status = RssStatus::Initializing { id, completion };
                mem::drop(status);

                let agent = Arc::clone(agent);
                let status = Arc::clone(&self.status);
                tokio::spawn(async move {
                    let result = rack_initialize(&agent, request).await;
                    let new_status = match result {
                        Ok(()) => RssStatus::Initialized { id },
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
    ) -> Result<RackResetId, ConcurrentRssAccess> {
        let mut status = self.status.lock().unwrap();

        match &*status {
            RssStatus::Initializing { .. } => {
                Err(ConcurrentRssAccess::StillInitializing)
            }
            RssStatus::InitializationFailed { err, .. } => {
                Err(ConcurrentRssAccess::InitializationFailed {
                    message: err.to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(ConcurrentRssAccess::InitializationPanicked)
            }
            RssStatus::Resetting { .. } => {
                Err(ConcurrentRssAccess::StillResetting)
            }
            RssStatus::ResetFailed { err, .. } => {
                Err(ConcurrentRssAccess::ResetFailed {
                    message: err.to_string(),
                })
            }
            RssStatus::ResetPanicked { .. } => {
                Err(ConcurrentRssAccess::ResetPanicked)
            }
            RssStatus::ResetOnStartup | RssStatus::Reset { .. } => {
                Err(ConcurrentRssAccess::AlreadyReset)
            }
            RssStatus::InitializedOnStartup | RssStatus::Initialized { .. } => {
                let (completion_tx, completion) = oneshot::channel();
                let id = RackResetId(Uuid::new_v4());
                *status = RssStatus::Resetting { id, completion };
                mem::drop(status);

                let agent = Arc::clone(agent);
                let status = Arc::clone(&self.status);
                tokio::spawn(async move {
                    let result = rack_reset(&agent).await;
                    let new_status = match result {
                        Ok(()) => RssStatus::Reset { id },
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
    InitializedOnStartup,
    Initializing { id: RackInitId, completion: oneshot::Receiver<()> },
    Initialized { id: RackInitId },
    InitializationFailed { id: RackInitId, err: SetupServiceError },
    InitializationPanicked { id: RackInitId },
    ResetOnStartup,
    Resetting { id: RackResetId, completion: oneshot::Receiver<()> },
    Reset { id: RackResetId },
    ResetFailed { id: RackResetId, err: SetupServiceError },
    ResetPanicked { id: RackResetId },
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
        match &agent.sled_config.sidecar_revision {
            SidecarRevision::Physical(_) => {
                super::SIDECAR_REV_A_B_N_QSFP28_PORTS
            }
            SidecarRevision::Soft(config) => config.front_port_count,
        },
    )
    .await
}

async fn rack_reset(agent: &Agent) -> Result<(), SetupServiceError> {
    RssHandle::run_rss_reset(&agent.parent_log, agent.ip).await
}
