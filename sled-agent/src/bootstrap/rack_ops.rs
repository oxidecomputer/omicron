// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal API for rack-level bootstrap agent operations.

use crate::bootstrap::rss_handle::RssHandle;
use bootstore::schemes::v0 as bootstore;
use bootstrap_agent_lockstep_types::RackOperationStatus;
use bootstrap_agent_lockstep_types::RssStep;
use omicron_uuid_kinds::RackInitUuid;
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_agent_measurements::MeasurementsHandle;
use sled_agent_rack_setup::RackInitializeRequestParams;
use sled_agent_rack_setup::SetupServiceError;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use sprockets_tls::keys::SprocketsConfig;
use std::mem;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::watch;

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
}

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
            RssStatus::Uninitialized => RackOperationStatus::Uninitialized,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_initializing(
        &self,
        parent_log: &Logger,
        sprockets: SprocketsConfig,
        global_zone_bootstrap_ip: Ipv6Addr,
        internal_disks_rx: &InternalDisksReceiver,
        measurements: Arc<MeasurementsHandle>,
        bootstore_node_handle: &bootstore::NodeHandle,
        trust_quorum_handle: &trust_quorum::NodeTaskHandle,
        request: RackInitializeRequestParams,
    ) -> Result<RackInitUuid, RssAccessError> {
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
                    message: InlineErrorChain::new(err).to_string(),
                })
            }
            RssStatus::InitializationPanicked { .. } => {
                Err(RssAccessError::InitializationPanicked)
            }

            RssStatus::Uninitialized => {
                let (completion_tx, completion) = oneshot::channel();
                let id = RackInitUuid::new_v4();
                let (step_tx, step_rx) = watch::channel(RssStep::Requested);
                *status = RssStatus::Initializing { id, completion, step_rx };
                mem::drop(status);
                let parent_log = parent_log.clone();
                let internal_disks_rx = internal_disks_rx.clone();
                let bootstore_node_handle = bootstore_node_handle.clone();
                let status = Arc::clone(&self.status);
                let trust_quorum_handle = trust_quorum_handle.clone();
                tokio::spawn(async move {
                    let result = rack_initialize(
                        &parent_log,
                        sprockets,
                        global_zone_bootstrap_ip,
                        internal_disks_rx,
                        measurements,
                        bootstore_node_handle,
                        trust_quorum_handle,
                        request,
                        step_tx,
                    )
                    .await;
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

    // Tranistory states (which we may be in for a long time, even on human time
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
}

#[allow(clippy::too_many_arguments)]
async fn rack_initialize(
    parent_log: &Logger,
    sprockets: SprocketsConfig,
    global_zone_bootstrap_ip: Ipv6Addr,
    internal_disks_rx: InternalDisksReceiver,
    measurements: Arc<MeasurementsHandle>,
    bootstore_node_handle: bootstore::NodeHandle,
    trust_quorum_handle: trust_quorum::NodeTaskHandle,
    request: RackInitializeRequestParams,
    step_tx: watch::Sender<RssStep>,
) -> Result<(), SetupServiceError> {
    RssHandle::run_rss(
        parent_log,
        sprockets,
        request,
        global_zone_bootstrap_ip,
        internal_disks_rx,
        measurements,
        bootstore_node_handle,
        trust_quorum_handle,
        step_tx,
    )
    .await
}
