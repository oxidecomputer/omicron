// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for proxying API requests from one trust quorum node to another
//! over sprockets.
//!
//! This is necessary in the case when there is no sled-agent with which Nexus
//! can directly communicate on the underlay network. Since Nexus is not on the
//! bootstrap network it cannot talk directly to trust quorum nodes.
//!
//! This proxy mechanism is also useful during RSS and for general debugging
//! purposes.

use crate::{CommitStatus, task::NodeApiRequest};
use derive_more::From;
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use slog_error_chain::SlogInlineError;
use tokio::sync::{mpsc, oneshot};
use trust_quorum_protocol::{
    BaseboardId, CommitError, Configuration, Epoch, PrepareAndCommitError,
};
use uuid::Uuid;

/// Requests that can be proxied to another node. Proxied requests should not be
/// proxied again once receieved. The receiving node should instead immediately
/// respond to the request to limit the time spent in the event loop, like a
/// normal NodeApiRequest.
pub struct WireRequest {
    request_id: Uuid,
    op: WireOp,
}

pub enum WireOp {
    Commit { rack_id: RackUuid, epoch: Epoch },
    PrepareAndCommit { config: Configuration },
}

pub struct WireResponse {
    request_id: Uuid,
    result: Result<WireValue, WireError>,
}

/// A trackable in-flight proxy request, owned by the `Tracker`
pub struct TrackableRequest {
    destination: BaseboardId,
    request_id: Uuid,
    tx: oneshot::Sender<Result<WireValue, WireOp>>,
}

/// A mechanism for proxying requests
pub struct Proxy {
    // A mechanism to send a `WireRequest` to our local task for proxying.
    tx: mpsc::Sender<NodeApiRequest>,
}

impl Proxy {
    pub async fn commit(
        &self,
        destination: BaseboardId,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> oneshot::Receiver<Result<CommitStatus, ProxyError<CommitError>>> {
        let op = WireOp::Commit { rack_id, epoch };
        let destructure_fn = move |res| match res {
            Ok(val) => {
                let WireValue::Commit(cs) = val;
                Ok(cs)
            }
            Err(err) => match err {
                WireError::Commit(e) => Err(e.into()),
                WireError::PrepareAndCommit(e) => {
                    Err(ProxyError::InvalidResponse(e.to_string()))
                }
            },
        };

        self.send(destination, op, destructure_fn).await
    }

    pub async fn prepare_and_commit(
        &self,
        destination: BaseboardId,
        config: Configuration,
    ) -> oneshot::Receiver<
        Result<CommitStatus, ProxyError<PrepareAndCommitError>>,
    > {
        let op = WireOp::PrepareAndCommit { config };
        let destructure_fn = move |res| match res {
            Ok(val) => {
                let WireValue::Commit(cs) = val;
                Ok(cs)
            }
            Err(err) => match err {
                WireError::Commit(e) => {
                    Err(ProxyError::InvalidResponse(e.to_string()))
                }
                WireError::PrepareAndCommit(e) => Err(e.into()),
            },
        };
        self.send(destination, op, destructure_fn).await
    }

    /// Send a `WireRequest` to a task and destructure its response to the
    /// appropriate type
    async fn send<F, V, E>(
        &self,
        destination: BaseboardId,
        op: WireOp,
        f: F,
    ) -> oneshot::Receiver<Result<V, ProxyError<E>>>
    where
        E: std::error::Error,
        F: FnOnce(Result<WireValue, WireError>) -> Result<V, ProxyError<E>>,
    {
        let request_id = Uuid::new_v4();
        let wire_request = WireRequest { request_id, op };

        // A wrapper for responses from the task
        let (task_tx, task_rx) = oneshot::channel();

        // The message to send to the task
        let api_request =
            NodeApiRequest::Proxy { destination, wire_request, tx: task_tx };

        // The channel used to communicate with the user
        let (tx, rx) = oneshot::channel();

        if let Err(e) = self.tx.try_send(api_request) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    tx.send(Err(ProxyError::Busy));
                }
                mpsc::error::TrySendError::Closed(_) => {
                    tx.send(Err(ProxyError::RecvError));
                }
            }
            return rx;
        }

        let Ok(res) = task_rx.await else {
            tx.send(Err(ProxyError::RecvError));
            return rx;
        };

        let user_response = f(res);
        tx.send(user_response);
        rx
    }
}

/// The successful variant of a [`WireResponse`]
pub enum WireValue {
    /// The successful response value for a `commit` or `prepare_and_commit` operation.
    Commit(CommitStatus),
}

/// The error variant of a [`WireResponse`]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, From)]
pub enum WireError {
    Commit(ProxyError<CommitError>),
    PrepareAndCommit(ProxyError<PrepareAndCommitError>),
}

/// The error result of a proxy request
#[derive(
    Debug,
    Clone,
    thiserror::Error,
    PartialEq,
    Eq,
    SlogInlineError,
    Serialize,
    Deserialize,
)]
pub enum ProxyError<T: std::error::Error> {
    #[error(transparent)]
    Inner(#[from] T),
    #[error("disconnected")]
    Disconnected,
    #[error("response for different type received: {0}")]
    InvalidResponse(String),
    #[error("task sender dropped")]
    RecvError,
    #[error("task is busy: channel full")]
    Busy,
}

pub struct TrackedProxyRequest {
    destination: BaseboardId,
    tx: oneshot::Sender<Result<WireValue, WireError>>,
}
/// A mechanism to keep track of proxied requests and wait for their replies
pub struct Tracker {
    // In flight operations
    ops: iddqd::BiHashMap<TrackableRequest>,
}

impl Tracker {}
