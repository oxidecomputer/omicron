// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    fmt,
    future::Future,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use buf_list::BufList;
use bytes::Bytes;
use display_error_chain::DisplayErrorChain;
use futures::{Stream, StreamExt};
use installinator_client::ClientError;
use installinator_common::{
    EventReport, InstallinatorProgressMetadata, StepContext, StepProgress,
};
use itertools::Itertools;
use omicron_common::address::BOOTSTRAP_ARTIFACT_PORT;
use omicron_common::update::ArtifactHashId;
use omicron_ddm_admin_client::Client as DdmAdminClient;
use reqwest::StatusCode;
use sled_hardware_types::underlay::BootstrapInterface;
use tokio::{sync::mpsc, time::Instant};
use update_engine::events::ProgressUnits;
use uuid::Uuid;

use crate::{
    artifact::ArtifactClient,
    errors::{ArtifactFetchError, DiscoverPeersError, HttpError},
};

/// A chosen discovery mechanism for peers, passed in over the command line.
#[derive(Clone, Debug)]
pub(crate) enum DiscoveryMechanism {
    /// The default discovery mechanism: hit the bootstrap network.
    Bootstrap,

    /// A list of peers is manually specified.
    List(Vec<SocketAddr>),
}

impl DiscoveryMechanism {
    /// Discover peers.
    pub(crate) async fn discover_peers(
        &self,
        log: &slog::Logger,
    ) -> Result<Box<dyn PeersImpl>, DiscoverPeersError> {
        let peers = match self {
            Self::Bootstrap => {
                // XXX: consider adding aborts to this after a certain number of tries.

                let ddm_admin_client =
                    DdmAdminClient::localhost(log).map_err(|err| {
                        DiscoverPeersError::Retry(anyhow::anyhow!(err))
                    })?;
                // We want to find both sled-agent (global zone) and wicketd
                // (switch zone) peers.
                let addrs = ddm_admin_client
                    .derive_bootstrap_addrs_from_prefixes(&[
                        BootstrapInterface::GlobalZone,
                        BootstrapInterface::SwitchZone,
                    ])
                    .await
                    .map_err(|err| {
                        DiscoverPeersError::Retry(anyhow::anyhow!(err))
                    })?;
                addrs
                    .map(|addr| {
                        SocketAddr::new(
                            IpAddr::V6(addr),
                            BOOTSTRAP_ARTIFACT_PORT,
                        )
                    })
                    .collect()
            }
            Self::List(peers) => peers.clone(),
        };

        Ok(Box::new(HttpPeers::new(log, peers)))
    }
}

impl fmt::Display for DiscoveryMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap => write!(f, "bootstrap"),
            Self::List(peers) => {
                write!(f, "list:{}", peers.iter().join(","))
            }
        }
    }
}

impl FromStr for DiscoveryMechanism {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "bootstrap" {
            Ok(Self::Bootstrap)
        } else if let Some(peers) = s.strip_prefix("list:") {
            let peers = peers
                .split(',')
                .map(|s| s.parse())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Self::List(peers))
        } else {
            bail!("invalid discovery mechanism (expected \"bootstrap\" or \"list:[::1]:8000\"): {}", s);
        }
    }
}

/// A fetched artifact.
pub(crate) struct FetchedArtifact {
    pub(crate) attempt: usize,
    pub(crate) addr: SocketAddr,
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
        Fut: Future<Output = Result<Peers, DiscoverPeersError>>,
    {
        // How long to sleep between retries if we fail to find a peer or fail
        // to fetch an artifact from a found peer.
        const RETRY_DELAY: Duration = Duration::from_secs(5);

        let mut attempt = 0;
        loop {
            attempt += 1;
            let peers = match discover_fn().await {
                Ok(peers) => peers,
                Err(DiscoverPeersError::Retry(error)) => {
                    slog::warn!(
                        log,
                        "(attempt {attempt}) failed to discover peers, retrying: {}",
                        DisplayErrorChain::new(AsRef::<dyn std::error::Error>::as_ref(&error)),
                    );
                    cx.send_progress(StepProgress::retry(format!(
                        "failed to discover peers: {error}"
                    )))
                    .await;
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
                Err(DiscoverPeersError::Abort(error)) => {
                    return Err(error);
                }
            };

            slog::info!(
                log,
                "discovered {} peers: [{}]",
                peers.peer_count(),
                peers.display(),
            );
            match peers.fetch_artifact(&cx, artifact_hash_id).await {
                Some((addr, artifact)) => {
                    return Ok(Self { attempt, addr, artifact })
                }
                None => {
                    slog::warn!(
                        log,
                        "unable to fetch artifact from peers, retrying discovery",
                    );
                    cx.send_progress(StepProgress::retry(format!(
                        "unable to fetch artifact from any of {} peers, retrying",
                        peers.peer_count(),
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
            .field("addr", &self.addr)
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
pub(crate) struct Peers {
    log: slog::Logger,
    imp: Box<dyn PeersImpl>,
    timeout: Duration,
}

impl Peers {
    pub(crate) fn new(
        log: &slog::Logger,
        imp: Box<dyn PeersImpl>,
        timeout: Duration,
    ) -> Self {
        let log = log.new(slog::o!("component" => "Peers"));
        Self { log, imp, timeout }
    }

    pub(crate) async fn fetch_artifact(
        &self,
        cx: &StepContext,
        artifact_hash_id: &ArtifactHashId,
    ) -> Option<(SocketAddr, BufList)> {
        // TODO: do we want a check phase that happens before the download?
        let peers = self.peers();
        let mut remaining_peers = self.peer_count();

        let log = self.log.new(
            slog::o!("artifact_hash_id" => format!("{artifact_hash_id:?}")),
        );

        slog::debug!(log, "start fetch from peers"; "remaining_peers" => remaining_peers);

        for peer in peers {
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

    pub(crate) fn peers(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.imp.peers()
    }

    pub(crate) fn peer_count(&self) -> usize {
        self.imp.peer_count()
    }

    pub(crate) fn display(&self) -> impl fmt::Display {
        self.peers().join(", ")
    }

    async fn fetch_from_peer(
        &self,
        cx: &StepContext,
        peer: SocketAddr,
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
                    metadata: InstallinatorProgressMetadata::Download { peer },
                    message: error.to_string().into(),
                })
                .await;
                return Err(ArtifactFetchError::HttpError { peer, error });
            }
        };

        let mut artifact_bytes = BufList::new();
        let mut downloaded_bytes = 0u64;
        let metadata = InstallinatorProgressMetadata::Download { peer };

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
                        peer,
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
                        peer,
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

    pub(crate) fn broadcast_report(
        &self,
        update_id: Uuid,
        report: EventReport,
    ) -> impl Stream<Item = Result<(), ClientError>> + Send + '_ {
        futures::stream::iter(self.peers())
            .map(move |peer| {
                let report = report.clone();
                self.send_report_to_peer(peer, update_id, report)
            })
            .buffer_unordered(8)
    }

    async fn send_report_to_peer(
        &self,
        peer: SocketAddr,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        let log = self.log.new(slog::o!("peer" => peer.to_string()));
        // For each peer, report it to the network.
        match self.imp.report_progress_impl(peer, update_id, report).await {
            Ok(()) => Ok(()),
            Err(err) => {
                // Error 422 means that the server didn't accept the update ID.
                if err.status() == Some(StatusCode::UNPROCESSABLE_ENTITY) {
                    slog::debug!(
                        log,
                        "received HTTP 422 Unprocessable Entity \
                         for update ID {update_id} (update ID unrecognized)",
                    );
                } else if err.status() == Some(StatusCode::GONE) {
                    // XXX If we establish a 1:1 relationship
                    // between a particular instance of wicketd and
                    // installinator, 410 Gone can be used to abort
                    // the update. But we don't have that kind of
                    // relationship at the moment.
                    slog::warn!(
                        log,
                        "received HTTP 410 Gone for update ID {update_id} \
                         (receiver closed)",
                    );
                } else {
                    slog::warn!(
                        log,
                        "received HTTP error code {:?} for update ID {update_id}",
                        err.status()
                    );
                }
                Err(err)
            }
        }
    }
}

#[async_trait]
pub(crate) trait PeersImpl: fmt::Debug + Send + Sync {
    fn peers(&self) -> Box<dyn Iterator<Item = SocketAddr> + Send + '_>;
    fn peer_count(&self) -> usize;

    /// Returns (size, receiver) on success, and an error on failure.
    async fn fetch_from_peer_impl(
        &self,
        peer: SocketAddr,
        artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError>;

    async fn report_progress_impl(
        &self,
        peer: SocketAddr,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError>;
}

/// The send side of the channel over which data is sent.
pub(crate) type FetchReceiver = mpsc::Receiver<Result<Bytes, ClientError>>;

/// A [`PeersImpl`] that uses HTTP to fetch artifacts from peers. This is the real implementation.
#[derive(Clone, Debug)]
pub(crate) struct HttpPeers {
    log: slog::Logger,
    peers: Vec<SocketAddr>,
}

impl HttpPeers {
    pub(crate) fn new(log: &slog::Logger, peers: Vec<SocketAddr>) -> Self {
        let log = log.new(slog::o!("component" => "HttpPeers"));
        Self { log, peers }
    }
}

#[async_trait]
impl PeersImpl for HttpPeers {
    fn peers(&self) -> Box<dyn Iterator<Item = SocketAddr> + Send + '_> {
        Box::new(self.peers.iter().copied())
    }

    fn peer_count(&self) -> usize {
        self.peers.len()
    }

    async fn fetch_from_peer_impl(
        &self,
        peer: SocketAddr,
        artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError> {
        // TODO: be able to fetch from sled-agent clients as well
        let artifact_client = ArtifactClient::new(peer, &self.log);
        artifact_client.fetch(artifact_hash_id).await
    }

    async fn report_progress_impl(
        &self,
        peer: SocketAddr,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        let artifact_client = ArtifactClient::new(peer, &self.log);
        artifact_client.report_progress(update_id, report).await
    }
}
