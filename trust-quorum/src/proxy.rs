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

use crate::{
    CommitStatus,
    task::{NodeApiRequest, NodeStatus},
};
use debug_ignore::DebugIgnore;
use derive_more::From;
use iddqd::{IdHashItem, IdHashMap, id_upcast};
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use tokio::sync::{mpsc, oneshot};
use trust_quorum_protocol::{
    BaseboardId, CommitError, Configuration, Epoch, PrepareAndCommitError,
};
use uuid::Uuid;

/// Requests that can be proxied to another node. Proxied requests should not be
/// proxied again once receieved. The receiving node should instead immediately
/// respond to the request to limit the time spent in the event loop, like a
/// normal NodeApiRequest.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireRequest {
    pub request_id: Uuid,
    pub op: WireOp,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WireOp {
    Commit { rack_id: RackUuid, epoch: Epoch },
    PrepareAndCommit { config: Configuration },
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WireResponse {
    pub request_id: Uuid,
    pub result: Result<WireValue, WireError>,
}

/// The successful variant of a [`WireResponse`]
#[derive(Debug, Serialize, Deserialize, From)]
pub enum WireValue {
    /// The successful response value for a `commit` or `prepare_and_commit`
    /// operation
    Commit(CommitStatus),
    /// The successful response value for a `status` operation
    Status(NodeStatus),
}

/// The error variant of a [`WireResponse`]
#[derive(
    Debug, PartialEq, Eq, Serialize, Deserialize, From, thiserror::Error,
)]
pub enum WireError {
    #[error(transparent)]
    Commit(CommitError),
    #[error(transparent)]
    PrepareAndCommit(PrepareAndCommitError),
    #[error(transparent)]
    NoInnerError(NoInnerError),
}

/// Define an Error for cases where the remote task doesn't return an error
///
/// This is roughly analagous to `Infallible`, but derives `Error`, `Serialize`,
/// and `Deserialize`.
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
#[error("inner error when none expected")]
pub struct NoInnerError;

/// A mechanism for proxying API requests to another node
pub struct Proxy {
    // A mechanism to send a `WireRequest` to our local task for proxying.
    tx: mpsc::Sender<NodeApiRequest>,
}

impl Proxy {
    pub fn new(tx: mpsc::Sender<NodeApiRequest>) -> Proxy {
        Proxy { tx }
    }

    pub async fn commit(
        &self,
        destination: BaseboardId,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<CommitStatus, ProxyError<CommitError>> {
        let op = WireOp::Commit { rack_id, epoch };
        let destructure_fn = move |res| match res {
            Ok(val) => match val {
                WireValue::Commit(cs) => Ok(cs),
                other => {
                    Err(ProxyError::InvalidResponse(format!("{other:#?}")))
                }
            },
            Err(err) => match err {
                WireError::Commit(e) => Err(e.into()),
                other => Err(ProxyError::InvalidResponse(
                    InlineErrorChain::new(&other).to_string(),
                )),
            },
        };

        self.send(destination, op, destructure_fn).await
    }

    pub async fn prepare_and_commit(
        &self,
        destination: BaseboardId,
        config: Configuration,
    ) -> Result<CommitStatus, ProxyError<PrepareAndCommitError>> {
        let op = WireOp::PrepareAndCommit { config };
        let destructure_fn = move |res| match res {
            Ok(val) => match val {
                WireValue::Commit(cs) => Ok(cs),
                other => {
                    Err(ProxyError::InvalidResponse(format!("{other:#?}")))
                }
            },
            Err(err) => match err {
                WireError::PrepareAndCommit(e) => Err(e.into()),
                other => Err(ProxyError::InvalidResponse(
                    InlineErrorChain::new(&other).to_string(),
                )),
            },
        };
        self.send(destination, op, destructure_fn).await
    }

    pub async fn status(
        &self,
        destination: BaseboardId,
    ) -> Result<NodeStatus, ProxyError<NoInnerError>> {
        let op = WireOp::Status;
        let destructure_fn = move |res| match res {
            Ok(val) => match val {
                WireValue::Status(status) => Ok(status),
                other => {
                    Err(ProxyError::InvalidResponse(format!("{other:#?}")))
                }
            },
            Err(err) => match err {
                WireError::NoInnerError(e) => Err(e.into()),
                other => Err(ProxyError::InvalidResponse(
                    InlineErrorChain::new(&other).to_string(),
                )),
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
    ) -> Result<V, ProxyError<E>>
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

        if let Err(e) = self.tx.try_send(api_request) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    return Err(ProxyError::Busy);
                }
                mpsc::error::TrySendError::Closed(_) => {
                    return Err(ProxyError::RecvError);
                }
            }
        }

        let res = task_rx.await.map_err(|_| ProxyError::RecvError)?;

        let res = match res {
            Err(TrackerError::Disconnected) => {
                return Err(ProxyError::Disconnected);
            }
            Err(TrackerError::Wire(err)) => Err(err),
            Ok(val) => Ok(val),
        };

        f(res)
    }
}

/// The error result of a proxy request
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq, SlogInlineError)]
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

/// An error returned from a [`Tracker`].
///
/// This error wraps a `WireError` so that it can also indicate when no response
/// was received, such as when the sprockets channel was disconnected.
pub enum TrackerError {
    Wire(WireError),
    Disconnected,
}

/// A trackable in-flight proxy request, owned by the `Tracker`
#[derive(Debug)]
pub struct TrackableRequest {
    request_id: Uuid,
    destination: BaseboardId,
    // The option exists so we can take the sender out in `on_disconnect`, when
    // the request is borrowed, but about to be discarded.
    tx: DebugIgnore<Option<oneshot::Sender<Result<WireValue, TrackerError>>>>,
}

impl TrackableRequest {
    pub fn new(
        destination: BaseboardId,
        request_id: Uuid,
        tx: oneshot::Sender<Result<WireValue, TrackerError>>,
    ) -> TrackableRequest {
        TrackableRequest { request_id, destination, tx: DebugIgnore(Some(tx)) }
    }
}

impl IdHashItem for TrackableRequest {
    type Key<'a> = &'a Uuid;

    fn key(&self) -> Self::Key<'_> {
        &self.request_id
    }

    id_upcast!();
}

/// A mechanism to keep track of proxied requests and wait for their replies
pub struct Tracker {
    // In flight operations
    ops: IdHashMap<TrackableRequest>,
}

impl Tracker {
    pub fn new() -> Tracker {
        Tracker { ops: IdHashMap::new() }
    }

    /// The number of proxied requests outstanding
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn insert(&mut self, req: TrackableRequest) {
        self.ops.insert_unique(req).expect("no duplicate request IDs");
    }

    /// Handle a proxied response from a peer
    pub fn on_response(
        &mut self,
        request_id: Uuid,
        result: Result<WireValue, WireError>,
    ) {
        if let Some(mut req) = self.ops.remove(&request_id) {
            let res = result.map_err(TrackerError::Wire);
            let _ = req.tx.take().unwrap().send(res);
        }
    }

    /// A remote peer has disconnected
    pub fn on_disconnect(&mut self, from: &BaseboardId) {
        self.ops.retain(|mut req| {
            if &req.destination == from {
                let tx = req.tx.take().unwrap();
                let _ = tx.send(Err(TrackerError::Disconnected));
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::spawn;

    /// Recv a message from the proxy and insert it into the tracker.
    async fn recv_and_insert(
        rx: &mut mpsc::Receiver<NodeApiRequest>,
        tracker: &mut Tracker,
    ) {
        let Some(NodeApiRequest::Proxy { destination, wire_request, tx }) =
            rx.recv().await
        else {
            panic!("Invalid NodeApiRequest")
        };

        let req =
            TrackableRequest::new(destination, wire_request.request_id, tx);
        tracker.insert(req);
    }

    #[tokio::test]
    async fn proxy_roundtrip_concurrent() {
        let destination = BaseboardId {
            part_number: "test".to_string(),
            serial_number: "test".to_string(),
        };
        let rack_id = RackUuid::new_v4();

        // Test channel where the sender is usually cloned from the [`crate::NodeTaskHandle`],
        // and the receiver is owned by the local [`crate::NodeTask`]
        let (tx, mut rx) = mpsc::channel(5);
        let proxy = Proxy::new(tx.clone());
        let mut tracker = Tracker::new();

        let requests_completed = Arc::new(AtomicUsize::new(0));

        // This is the first "user" task that will issue proxy operations
        let count = requests_completed.clone();
        let dest = destination.clone();
        let _ = spawn(async move {
            let s = proxy.commit(dest, rack_id, Epoch(1)).await.unwrap();

            // The first attempt should succeed
            assert_eq!(s, CommitStatus::Committed);
            let _ = count.fetch_add(1, Ordering::Relaxed);
        });

        // No requests have been received yet
        assert_eq!(tracker.len(), 0);

        // Simulate receiving a request by the [`NodeTask`]
        recv_and_insert(&mut rx, &mut tracker).await;

        // We now have a request in the tracker
        assert_eq!(tracker.len(), 1);

        // We haven't actually completed our operation yet
        assert_eq!(requests_completed.load(Ordering::Relaxed), 0);

        // Get the first request_id. It's internal to the system, but we need to
        // know it here to fake a response.
        let request_id_1 = tracker.ops.iter().next().unwrap().request_id;

        // Now spawn a concurrent "user" task that proxies a `Status` request
        // to the same node.
        let proxy = Proxy::new(tx);
        let count = requests_completed.clone();
        let _ = spawn(async move {
            let s = proxy.status(destination.clone()).await.unwrap();
            assert_matches!(s, NodeStatus { .. });
            let _ = count.fetch_add(1, Ordering::Relaxed);
        });

        // Simulate receiving a request by the [`NodeTask`]
        recv_and_insert(&mut rx, &mut tracker).await;
        assert_eq!(tracker.len(), 2);

        // We still haven't actually completed any operations yet
        assert_eq!(requests_completed.load(Ordering::Relaxed), 0);
        let request_id_2 = tracker
            .ops
            .iter()
            .filter(|&r| r.request_id != request_id_1)
            .next()
            .unwrap()
            .request_id;

        // Now simulate completion of both requests, in reverse order.
        tracker.on_response(
            request_id_2,
            Ok(WireValue::Status(NodeStatus::default())),
        );
        tracker.on_response(
            request_id_1,
            Ok(WireValue::Commit(CommitStatus::Committed)),
        );

        // Now wait for both responses to be processed
        wait_for_condition(
            async || {
                if requests_completed.load(Ordering::Relaxed) == 2 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(10),
            &Duration::from_secs(10),
        )
        .await
        .unwrap();

        assert_eq!(tracker.len(), 0);
    }

    #[tokio::test]
    async fn proxy_roundtrip_invalid_response() {
        let destination = BaseboardId {
            part_number: "test".to_string(),
            serial_number: "test".to_string(),
        };
        let rack_id = RackUuid::new_v4();

        // Test channel where the sender is usually cloned from the [`crate::NodeTaskHandle`],
        // and the receiver is owned by the local [`crate::NodeTask`]
        let (tx, mut rx) = mpsc::channel(5);
        let proxy = Proxy::new(tx.clone());
        let mut tracker = Tracker::new();

        let requests_completed = Arc::new(AtomicUsize::new(0));

        // This is the first "user" task that will issue proxy operations
        let count = requests_completed.clone();
        let dest = destination.clone();
        let _ = spawn(async move {
            let s = proxy.commit(dest, rack_id, Epoch(1)).await.unwrap_err();

            // The first attempt should succeed
            assert_matches!(s, ProxyError::InvalidResponse(_));
            let _ = count.fetch_add(1, Ordering::Relaxed);
        });

        // No requests have been received yet
        assert_eq!(tracker.len(), 0);

        // Simulate receiving a request by the [`NodeTask`]
        recv_and_insert(&mut rx, &mut tracker).await;

        // We now have a request in the tracker
        assert_eq!(tracker.len(), 1);

        // We haven't actually completed our operation yet
        assert_eq!(requests_completed.load(Ordering::Relaxed), 0);

        // Get the request_id. It's internal to the system, but we need to know
        // it here to fake a response.
        let request_id = tracker.ops.iter().next().unwrap().request_id;

        // Now return a successful response, but of the wrong type.
        tracker.on_response(
            request_id,
            Ok(WireValue::Status(NodeStatus::default())),
        );

        // Now wait for the error responses to be processed
        wait_for_condition(
            async || {
                if requests_completed.load(Ordering::Relaxed) == 1 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(10),
            &Duration::from_secs(10),
        )
        .await
        .unwrap();

        assert_eq!(tracker.len(), 0);
    }

    #[tokio::test]
    async fn proxy_roundtrip_disconnected() {
        let destination = BaseboardId {
            part_number: "test".to_string(),
            serial_number: "test".to_string(),
        };
        let rack_id = RackUuid::new_v4();

        // Test channel where the sender is usually cloned from the [`crate::NodeTaskHandle`],
        // and the receiver is owned by the local [`crate::NodeTask`]
        let (tx, mut rx) = mpsc::channel(5);
        let proxy = Proxy::new(tx.clone());
        let mut tracker = Tracker::new();

        let requests_completed = Arc::new(AtomicUsize::new(0));

        // This is the first "user" task that will issue proxy operations
        let count = requests_completed.clone();
        let dest = destination.clone();
        let _ = spawn(async move {
            let s = proxy.commit(dest, rack_id, Epoch(1)).await.unwrap_err();

            // The first attempt should succeed
            assert_eq!(s, ProxyError::Disconnected);
            let _ = count.fetch_add(1, Ordering::Relaxed);
        });

        // No requests have been received yet
        assert_eq!(tracker.len(), 0);

        // Simulate receiving a request by the [`NodeTask`]
        recv_and_insert(&mut rx, &mut tracker).await;

        // We now have a request in the tracker
        assert_eq!(tracker.len(), 1);

        // We haven't actually completed our operation yet
        assert_eq!(requests_completed.load(Ordering::Relaxed), 0);

        // Now spawn a concurrent "user" task that proxies a `Status` request
        // to the same node.
        let proxy = Proxy::new(tx);
        let count = requests_completed.clone();
        let dest = destination.clone();
        let _ = spawn(async move {
            let s = proxy.status(dest.clone()).await.unwrap_err();
            assert_eq!(s, ProxyError::Disconnected);
            let _ = count.fetch_add(1, Ordering::Relaxed);
        });

        // Simulate receiving a request by the [`NodeTask`]
        recv_and_insert(&mut rx, &mut tracker).await;
        assert_eq!(tracker.len(), 2);

        // We still haven't actually completed any operations yet
        assert_eq!(requests_completed.load(Ordering::Relaxed), 0);

        // Now simulate a disconnection to the proxy destination
        tracker.on_disconnect(&destination);

        // Now wait for both responses to be processed
        wait_for_condition(
            async || {
                if requests_completed.load(Ordering::Relaxed) == 2 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(10),
            &Duration::from_secs(10),
        )
        .await
        .unwrap();

        assert_eq!(tracker.len(), 0);
    }
}
