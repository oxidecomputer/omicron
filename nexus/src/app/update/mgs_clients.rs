// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module providing support for handling failover between multiple MGS clients

use futures::Future;
use gateway_client::Client;
use slog::Logger;
use std::collections::VecDeque;
use std::sync::Arc;

pub(super) type GatewayClientError =
    gateway_client::Error<gateway_client::types::Error>;

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
    /// non-communication error (in which case that error is returned), or all
    /// of them fail with communication errors (in which case the communication
    /// error from the last-attempted client is returned).
    ///
    /// On a successful return, the internal client list will be reordered so
    /// any future accesses will attempt the most-recently-successful client.
    pub(super) async fn try_all_serially<T, F, Fut>(
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
}
