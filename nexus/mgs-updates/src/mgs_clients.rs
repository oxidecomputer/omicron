// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module providing support for handling failover between multiple MGS clients

use futures::Future;
use gateway_client::Client;
use gateway_client::HostPhase1HashError;
use gateway_client::types::SpComponentResetError;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::VecDeque;
use std::sync::Arc;

pub(super) type GatewayClientError =
    gateway_client::Error<gateway_client::types::Error>;
pub(super) type GatewaySpComponentResetError =
    gateway_client::Error<SpComponentResetError>;

/// Helper trait for error types we can get from various gateway_client API
/// calls.
///
/// For each error type, we need to know whether we should retry the request (by
/// sending it to the next MGS instance).
pub(super) trait RetryableMgsError: std::error::Error {
    fn should_try_next_mgs(&self) -> bool;
}

impl RetryableMgsError for GatewayClientError {
    fn should_try_next_mgs(&self) -> bool {
        matches!(self, GatewayClientError::CommunicationError(_))
    }
}

impl RetryableMgsError for GatewaySpComponentResetError {
    fn should_try_next_mgs(&self) -> bool {
        match self {
            gateway_client::Error::CommunicationError(_) => true,
            gateway_client::Error::ErrorResponse(resp) => match resp.as_ref() {
                SpComponentResetError::ResetSpOfLocalSled => true,
                SpComponentResetError::Other { .. } => false,
            },
            _ => false,
        }
    }
}

impl RetryableMgsError for HostPhase1HashError {
    fn should_try_next_mgs(&self) -> bool {
        match self {
            // `Timeout` here means we were successfully communicating with the
            // SP via MGS, but the SP failed to compute the hash in a reasonable
            // amount of time. We have no reason to believe retrying via the
            // other MGS will change this result. (The only time we've seen this
            // error in practice was due to a bug on the SP where the hashing
            // engine got stuck.)
            HostPhase1HashError::Timeout(_) => false,
            HostPhase1HashError::ContentsModifiedWhileHashing => false,
            HostPhase1HashError::RequestError { err, .. } => {
                err.should_try_next_mgs()
            }
        }
    }
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

    /// Run `op` against all clients in sequence until either one succeeds (in
    /// which case the success value is returned), one fails with a
    /// non-retryable error (in which case that error is returned), or all of
    /// them fail with retryable errors (in which case the error from the
    /// last-attempted client is returned).
    ///
    /// On a successful return, the internal client list will be reordered so
    /// any future accesses will attempt the most-recently-successful client.
    pub(super) async fn try_all_serially<T, E, F, Fut>(
        &mut self,
        log: &Logger,
        op: F,
    ) -> Result<T, E>
    where
        // Seems like it would be nicer to take `&Client` here instead of
        // needing to clone each `Arc`, but there's currently no decent way of
        // doing that without boxing the returned future:
        // https://users.rust-lang.org/t/how-to-express-that-the-future-returned-by-a-closure-lives-only-as-long-as-its-argument/90039/10
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: RetryableMgsError,
    {
        let mut last_err = None;
        for (i, client) in self.clients.iter().enumerate() {
            match op(Arc::clone(client)).await {
                Ok(value) => {
                    self.clients.rotate_left(i);
                    return Ok(value);
                }
                Err(err) => {
                    if err.should_try_next_mgs() {
                        if i < self.clients.len() {
                            warn!(
                                log, "retryable error with MGS; \
                                      will try next client";
                                "mgs_addr" => client.baseurl(),
                                InlineErrorChain::new(&err),
                            );
                        }
                        last_err = Some(err);
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        // The only way to get here is if all clients failed with retryable
        // errors. Return the error from the last MGS we tried.
        Err(last_err.unwrap())
    }
}
