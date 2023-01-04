// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::SocketAddrV6, time::Duration};

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ArtifactFetchError {
    #[error("peer {peer} returned an HTTP error")]
    HttpError {
        peer: SocketAddrV6,
        #[source]
        error: installinator_artifact_client::Error,
    },

    #[error("peer {peer} timed out ({timeout:?}) after returning {bytes_fetched} bytes")]
    Timeout { peer: SocketAddrV6, timeout: Duration, bytes_fetched: usize },
}

#[derive(Debug, Error)]
pub(crate) enum DiscoverPeersError {
    #[error("failed to discover peers (will retry)")]
    #[allow(unused)]
    Retry(#[source] anyhow::Error),

    #[error("failed to discover peers (no more retries left, will abort)")]
    #[allow(unused)]
    Abort(#[source] anyhow::Error),
}
