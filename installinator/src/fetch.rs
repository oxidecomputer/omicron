// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Logic to fetch artifacts from a peer.

use std::{fmt, future::Future, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use buf_list::BufList;
use bytes::Bytes;
use display_error_chain::DisplayErrorChain;
use installinator_client::ClientError;
use installinator_common::{
    InstallinatorProgressMetadata, StepContext, StepProgress,
};
use tokio::{sync::mpsc, time::Instant};
use tufaceous_artifact::ArtifactHashId;
use update_engine::events::ProgressUnits;

use crate::{
    artifact::ArtifactClient,
    errors::{ArtifactFetchError, DiscoverPeersError, HttpError},
    peers::{PeerAddress, PeerAddresses},
};

/// A fetched artifact.
pub(crate) struct FetchedArtifact {
    pub(crate) attempt: usize,
    pub(crate) peer: PeerAddress,
    pub(crate) artifact: BufList,
}

impl FetchedArtifact {
    /// In a loop, discover peers, and fetch from them.
    ///
    /// If `discover_fn` returns [`DiscoverPeersError::Retry`], this function will retry. If it
    /// returns `DiscoverPeersError::Abort`, this function will exit with the underlying error.
    pub(crate) async fn loop_fetch_from_peers<F, Fut>(
        cx: &StepContext,
        log: &slog::Logger,
        mut discover_fn: F,
        artifact_hash_id: &ArtifactHashId,
    ) -> Result<Self>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<FetchArtifactBackend, DiscoverPeersError>>,
    {
        // How long to sleep between retries if we fail to find a peer or fail
        // to fetch an artifact from a found peer.
        const RETRY_DELAY: Duration = Duration::from_secs(5);

        let mut attempt = 0;
        loop {
            attempt += 1;
            let fetch_backend = match discover_fn().await {
                Ok(peers) => peers,
                Err(DiscoverPeersError::Retry(error)) => {
                    slog::warn!(
                        log,
                        "(attempt {attempt}) failed to discover peers, retrying: {}",
                        DisplayErrorChain::new(
                            AsRef::<dyn std::error::Error>::as_ref(&error)
                        ),
                    );
                    cx.send_progress(StepProgress::retry(format!(
                        "failed to discover peers: {error}"
                    )))
                    .await;
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
                #[cfg(test)]
                Err(DiscoverPeersError::Abort(error)) => {
                    return Err(error);
                }
            };

            let peers = fetch_backend.peers();
            slog::info!(
                log,
                "discovered {} peers: [{}]",
                peers.len(),
                peers.display(),
            );
            match fetch_backend.fetch_artifact(&cx, artifact_hash_id).await {
                Some((peer, artifact)) => {
                    return Ok(Self { attempt, peer, artifact });
                }
                None => {
                    slog::warn!(
                        log,
                        "unable to fetch artifact from peers, retrying discovery",
                    );
                    cx.send_progress(StepProgress::retry(format!(
                        "unable to fetch artifact from any of {} peers, retrying",
                        peers.len(),
                    )))
                    .await;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }
}

impl fmt::Debug for FetchedArtifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchedArtifact")
            .field("attempt", &self.attempt)
            .field("peer", &self.peer)
            .field(
                "artifact",
                &format!(
                    "({} bytes in {} chunks)",
                    self.artifact.num_bytes(),
                    self.artifact.num_chunks()
                ),
            )
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct FetchArtifactBackend {
    log: slog::Logger,
    imp: Box<dyn FetchArtifactImpl>,
    timeout: Duration,
}

impl FetchArtifactBackend {
    pub(crate) fn new(
        log: &slog::Logger,
        imp: Box<dyn FetchArtifactImpl>,
        timeout: Duration,
    ) -> Self {
        let log = log.new(slog::o!("component" => "Peers"));
        Self { log, imp, timeout }
    }

    pub(crate) async fn fetch_artifact(
        &self,
        cx: &StepContext,
        artifact_hash_id: &ArtifactHashId,
    ) -> Option<(PeerAddress, BufList)> {
        // TODO: do we want a check phase that happens before the download?
        let peers = self.peers();
        let mut remaining_peers = peers.len();

        let log = self.log.new(
            slog::o!("artifact_hash_id" => format!("{artifact_hash_id:?}")),
        );

        slog::debug!(log, "start fetch from peers"; "remaining_peers" => remaining_peers);

        for &peer in peers.peers() {
            remaining_peers -= 1;

            slog::debug!(
                log,
                "start fetch from peer {peer:?}"; "remaining_peers" => remaining_peers,
            );

            // Attempt to download data from this peer.
            let start = Instant::now();
            match self.fetch_from_peer(cx, peer, artifact_hash_id).await {
                Ok(artifact_bytes) => {
                    let elapsed = start.elapsed();
                    slog::info!(
                        log,
                        "fetched artifact from peer {peer} in {elapsed:?}"
                    );
                    return Some((peer, artifact_bytes));
                }
                Err(error) => {
                    let elapsed = start.elapsed();
                    slog::warn!(
                        log,
                        "error after {elapsed:?}: {}",
                        DisplayErrorChain::new(&error);
                        "remaining_peers" => remaining_peers,
                    );
                }
            }
        }

        None
    }

    pub(crate) fn peers(&self) -> &PeerAddresses {
        self.imp.peers()
    }

    async fn fetch_from_peer(
        &self,
        cx: &StepContext,
        peer: PeerAddress,
        artifact_hash_id: &ArtifactHashId,
    ) -> Result<BufList, ArtifactFetchError> {
        let log = self.log.new(slog::o!("peer" => peer.to_string()));

        let (total_bytes, mut receiver) = match self
            .imp
            .fetch_from_peer_impl(peer, artifact_hash_id.clone())
            .await
        {
            Ok(x) => x,
            Err(error) => {
                cx.send_progress(StepProgress::Reset {
                    metadata: InstallinatorProgressMetadata::Download {
                        peer: peer.address(),
                    },
                    message: error.to_string().into(),
                })
                .await;
                return Err(ArtifactFetchError::HttpError {
                    peer: peer.address(),
                    error,
                });
            }
        };

        let mut artifact_bytes = BufList::new();
        let mut downloaded_bytes = 0u64;
        let metadata =
            InstallinatorProgressMetadata::Download { peer: peer.address() };

        loop {
            match tokio::time::timeout(self.timeout, receiver.recv()).await {
                Ok(Some(Ok(bytes))) => {
                    slog::debug!(
                        &log,
                        "received chunk of {} bytes from peer",
                        bytes.len()
                    );
                    downloaded_bytes += bytes.len() as u64;
                    artifact_bytes.push_chunk(bytes);
                    cx.send_progress(StepProgress::with_current_and_total(
                        downloaded_bytes,
                        total_bytes,
                        ProgressUnits::BYTES,
                        metadata.clone(),
                    ))
                    .await;
                }
                Ok(Some(Err(error))) => {
                    slog::debug!(
                        &log,
                        "received error from peer, sending cancellation: {}",
                        DisplayErrorChain::new(&error),
                    );
                    cx.send_progress(StepProgress::Reset {
                        metadata: metadata.clone(),
                        message: error.to_string().into(),
                    })
                    .await;
                    return Err(ArtifactFetchError::HttpError {
                        peer: peer.address(),
                        error: error.into(),
                    });
                }
                Ok(None) => {
                    // The entire artifact has been downloaded.
                    break;
                }
                Err(_) => {
                    // The operation timed out.
                    cx.send_progress(StepProgress::Reset {
                        metadata,
                        message: format!(
                            "operation timed out ({:?})",
                            self.timeout
                        )
                        .into(),
                    })
                    .await;
                    return Err(ArtifactFetchError::Timeout {
                        peer: peer.address(),
                        timeout: self.timeout,
                        bytes_fetched: artifact_bytes.num_bytes(),
                    });
                }
            }
        }

        // Check that the artifact size matches the returned size.
        if total_bytes != artifact_bytes.num_bytes() as u64 {
            let error = ArtifactFetchError::SizeMismatch {
                artifact_size: total_bytes,
                downloaded_bytes,
            };
            cx.send_progress(StepProgress::reset(metadata, error.to_string()))
                .await;
            return Err(error);
        }

        Ok(artifact_bytes)
    }
}

/// Backend implementation for fetching artifacts.
///
/// Note: While [`crate::reporter::ReportProgressImpl`] is a persistent
/// structure, a new `FetchArtifactImpl` is generated separately each time
/// discovery occurs. We should align this with `ReportProgressImpl` in the
/// future, though we'd need some way of looking up delay information in
/// `mock_peers`.
#[async_trait]
pub(crate) trait FetchArtifactImpl: fmt::Debug + Send + Sync {
    fn peers(&self) -> &PeerAddresses;

    /// Returns (size, receiver) on success, and an error on failure.
    async fn fetch_from_peer_impl(
        &self,
        peer: PeerAddress,
        artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError>;
}

/// The send side of the channel over which data is sent.
pub(crate) type FetchReceiver = mpsc::Receiver<Result<Bytes, ClientError>>;

/// A [`FetchArtifactImpl`] that uses HTTP to fetch artifacts from peers.
///
/// This is the real implementation.
#[derive(Clone, Debug)]
pub(crate) struct HttpFetchBackend {
    log: slog::Logger,
    peers: PeerAddresses,
}

impl HttpFetchBackend {
    pub(crate) fn new(log: &slog::Logger, peers: PeerAddresses) -> Self {
        let log = log.new(slog::o!("component" => "HttpPeers"));
        Self { log, peers }
    }
}

#[async_trait]
impl FetchArtifactImpl for HttpFetchBackend {
    fn peers(&self) -> &PeerAddresses {
        &self.peers
    }

    async fn fetch_from_peer_impl(
        &self,
        peer: PeerAddress,
        artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError> {
        // TODO: be able to fetch from sled-agent clients as well
        let artifact_client = ArtifactClient::new(peer.address(), &self.log);
        artifact_client.fetch(artifact_hash_id).await
    }
}
