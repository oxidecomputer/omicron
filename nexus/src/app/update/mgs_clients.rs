// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module providing support for handling failover between multiple MGS clients

use futures::Future;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::Client;
use slog::Logger;
use std::collections::VecDeque;
use std::sync::Arc;
use uuid::Uuid;

pub(super) type GatewayClientError =
    gateway_client::Error<gateway_client::types::Error>;

pub(super) enum PollUpdateStatus {
    Preparing { progress: Option<f64> },
    InProgress { progress: Option<f64> },
    Complete,
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateStatusError {
    #[error("different update is now preparing ({0})")]
    DifferentUpdatePreparing(Uuid),
    #[error("different update is now in progress ({0})")]
    DifferentUpdateInProgress(Uuid),
    #[error("different update is now complete ({0})")]
    DifferentUpdateComplete(Uuid),
    #[error("different update is now aborted ({0})")]
    DifferentUpdateAborted(Uuid),
    #[error("different update failed ({0})")]
    DifferentUpdateFailed(Uuid),
    #[error("update status lost (did the SP reset?)")]
    UpdateStatusLost,
    #[error("update was aborted")]
    UpdateAborted,
    #[error("update failed (error code {0})")]
    UpdateFailedWithCode(u32),
    #[error("update failed (error message {0})")]
    UpdateFailedWithMessage(String),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PollUpdateStatusError {
    #[error(transparent)]
    StatusError(#[from] UpdateStatusError),
    #[error(transparent)]
    ClientError(#[from] GatewayClientError),
}

#[derive(Debug, Clone)]
pub struct MgsClients {
    clients: VecDeque<Arc<Client>>,
}

impl MgsClients {
    /// Create a new `MgsClients` with the given `clients`.
    ///
    /// # Panics
    ///
    /// Panics if `clients` is empty.
    pub fn new<T: Into<VecDeque<Arc<Client>>>>(clients: T) -> Self {
        let clients = clients.into();
        assert!(!clients.is_empty());
        Self { clients }
    }

    /// Create a new `MgsClients` with the given `clients`.
    ///
    /// # Panics
    ///
    /// Panics if `clients` is empty.
    pub fn from_clients<I: IntoIterator<Item = Client>>(clients: I) -> Self {
        let clients = clients
            .into_iter()
            .map(Arc::new)
            .collect::<VecDeque<Arc<Client>>>();
        Self::new(clients)
    }

    /// Run `op` against all clients, returning either the first success, the
    /// first non-communication error, or the communication error (if all
    /// clients failed with communication errors).
    ///
    /// On a successful return, the internal client list will be reordered so
    /// any future accesses will attempt the most-recently-successful client.
    pub(super) async fn try_all<T, F, Fut>(
        &mut self,
        log: &Logger,
        op: F,
    ) -> Result<T, GatewayClientError>
    where
        // Seems like it would be nicer to take `&Client` here instead of
        // needing to clone each `Arc`, but there's currently no decent way of
        // doing that without boxing the returned future:
        // https://users.rust-lang.org/t/how-to-express-that-the-future-returned-by-a-closure-lives-only-as-long-as-its-argument/90039/10
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T, GatewayClientError>>,
    {
        let mut last_err = None;
        for (i, client) in self.clients.iter().enumerate() {
            match op(Arc::clone(client)).await {
                Ok(value) => {
                    self.clients.rotate_left(i);
                    return Ok(value);
                }
                Err(GatewayClientError::CommunicationError(err)) => {
                    if i < self.clients.len() {
                        warn!(
                            log, "communication error with MGS; \
                                  will try next client";
                            "mgs_addr" => client.baseurl(),
                            "err" => %err,
                        );
                    }
                    last_err = Some(err);
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        // The only way to get here is if all clients failed with communication
        // errors. Return the error from the last MGS we tried.
        Err(GatewayClientError::CommunicationError(last_err.unwrap()))
    }

    /// Poll for the status of an expected-to-be-in-progress update.
    pub(super) async fn poll_update_status(
        &mut self,
        sp_type: SpType,
        sp_slot: u32,
        component: &'static str,
        update_id: Uuid,
        log: &Logger,
    ) -> Result<PollUpdateStatus, PollUpdateStatusError> {
        let update_status = self
            .try_all(log, |client| async move {
                let update_status = client
                    .sp_component_update_status(sp_type, sp_slot, component)
                    .await?;

                debug!(
                    log, "got update status";
                    "mgs_addr" => client.baseurl(),
                    "status" => ?update_status,
                );

                Ok(update_status)
            })
            .await?
            .into_inner();

        match update_status {
            // For `Preparing` and `InProgress`, we could check the progress
            // information returned by these steps and try to check that
            // we're still _making_ progress, but every Nexus instance needs
            // to do that anyway in case we (or the MGS instance delivering
            // the update) crash, so we'll omit that check here. Instead, we
            // just sleep and we'll poll again shortly.
            SpUpdateStatus::Preparing { id, progress } => {
                if id == update_id {
                    let progress = progress.and_then(|progress| {
                        if progress.current > progress.total {
                            warn!(
                                log, "nonsense preparing progress";
                                "current" => progress.current,
                                "total" => progress.total,
                            );
                            None
                        } else if progress.total == 0 {
                            None
                        } else {
                            Some(
                                f64::from(progress.current)
                                    / f64::from(progress.total),
                            )
                        }
                    });
                    Ok(PollUpdateStatus::Preparing { progress })
                } else {
                    Err(UpdateStatusError::DifferentUpdatePreparing(id).into())
                }
            }
            SpUpdateStatus::InProgress { id, bytes_received, total_bytes } => {
                if id == update_id {
                    let progress = if bytes_received > total_bytes {
                        warn!(
                            log, "nonsense update progress";
                            "bytes_received" => bytes_received,
                            "total_bytes" => total_bytes,
                        );
                        None
                    } else if total_bytes == 0 {
                        None
                    } else {
                        Some(f64::from(bytes_received) / f64::from(total_bytes))
                    };
                    Ok(PollUpdateStatus::InProgress { progress })
                } else {
                    Err(UpdateStatusError::DifferentUpdateInProgress(id).into())
                }
            }
            SpUpdateStatus::Complete { id } => {
                if id == update_id {
                    Ok(PollUpdateStatus::Complete)
                } else {
                    Err(UpdateStatusError::DifferentUpdateComplete(id).into())
                }
            }
            SpUpdateStatus::None => {
                Err(UpdateStatusError::UpdateStatusLost.into())
            }
            SpUpdateStatus::Aborted { id } => {
                if id == update_id {
                    Err(UpdateStatusError::UpdateAborted.into())
                } else {
                    Err(UpdateStatusError::DifferentUpdateAborted(id).into())
                }
            }
            SpUpdateStatus::Failed { code, id } => {
                if id == update_id {
                    Err(UpdateStatusError::UpdateFailedWithCode(code).into())
                } else {
                    Err(UpdateStatusError::DifferentUpdateFailed(id).into())
                }
            }
            SpUpdateStatus::RotError { id, message } => {
                if id == update_id {
                    Err(UpdateStatusError::UpdateFailedWithMessage(format!(
                        "rot error: {message}"
                    ))
                    .into())
                } else {
                    Err(UpdateStatusError::DifferentUpdateFailed(id).into())
                }
            }
        }
    }
}
