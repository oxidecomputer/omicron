// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for proxying API requests from one trust quorum node to another
//! over sprockets.
//!
//! This is necessary in the case when there is no sled-agent with which Nexus
//! can directly communicate on the underlay network. Since nexus is not on the
//! bootstrap network it cannot talk directly to trust quorum nodes.
//!
//! This proxy mechanism is also useful during RSS and for general debugging
//! purposes.

use futures::channel::oneshot;
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use slog_error_chain::SlogInlineError;
use trust_quorum_protocol::{
    BaseboardId, CommitError, Configuration, Epoch, PrepareAndCommitError,
};
use uuid::Uuid;

/// Requests that can be proxied to another node. Proxied requests should not be
/// proxied again once receieved. The receiving node should instead immediately
/// respond to the request to limit the time spent in the event loop, like a
/// normal NodeApiRequest.
pub struct ProxyRequest {
    request_id: Uuid,
    op: ProxyOp,
}

pub enum ProxyOp {
    Commit { rack_id: RackUuid, epoch: Epoch },
    PrepareAndCommit { config: Configuration },
}

pub struct ProxyResponse {
    request_id: Uuid,
    result: Result<ProxyValue, ProxyError>,
}

/// The successful result of a proxy request
pub enum ProxyValue {}

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
pub enum ProxyError {
    #[error(transparent)]
    Commit(#[from] CommitError),
    #[error(transparent)]
    PrepareAndCommit(PrepareAndCommitError),
    #[error("disconnected")]
    Disconnected,
    #[error("timeout")]
    Timeout(BaseboardId),
}

pub struct TrackedProxyRequest {
    destination: BaseboardId,
    tx: oneshot::Sender<Result<ProxyValue, ProxyError>>,
}
/// A mechanism to keep track of proxied requests and wait for their replies
pub struct Proxy {}

impl Proxy {
    pub async fn send(
        op: ProxyOp,
        tx: oneshot::Sender<Result<ProxyValue, ProxyError>>,
    ) {
        // TODO: Track the op by uuid and destination, set a timer, and return
        // the request to be handed off to the connection manager.
        todo!()
    }
}
