// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::SocketAddr, time::Duration};

use installinator_client::ClientError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ArtifactFetchError {
    #[error("peer {peer} returned an HTTP error")]
    HttpError {
        peer: SocketAddr,
        #[source]
        error: HttpError,
    },

    #[error(
        "peer {peer} timed out ({timeout:?}) after returning {bytes_fetched} bytes"
    )]
    Timeout { peer: SocketAddr, timeout: Duration, bytes_fetched: usize },

    #[error(
        "artifact size in Content-Length header ({artifact_size}) did not match downloaded size ({downloaded_bytes})"
    )]
    SizeMismatch { artifact_size: u64, downloaded_bytes: u64 },
}

#[derive(Debug, Error)]
pub(crate) enum DiscoverPeersError {
    #[error("failed to discover peers (will retry)")]
    #[allow(unused)]
    Retry(#[source] anyhow::Error),

    /// Abort further discovery.
    ///
    /// The installinator must keep retrying until it has completed, which is why
    /// there's no abort case here in the not cfg(test) case. However, we test
    /// some abort-related functionality in tests.
    #[cfg(test)]
    #[error("failed to discover peers (will abort)")]
    Abort(#[source] anyhow::Error),
}

#[derive(Debug, Error)]
pub(crate) enum HttpError {
    #[error("HTTP client error")]
    Client(#[from] ClientError),

    #[error("missing Content-Length header")]
    MissingContentLength,

    #[error("Content-Length header could not be parsed into an integer")]
    InvalidContentLength,
}
