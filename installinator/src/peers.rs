// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, future::Future, net::Ipv6Addr, str::FromStr, time::Duration};

use anyhow::{bail, Result};
use buf_list::BufList;
use bytes::{Bytes, BytesMut};
use display_error_chain::DisplayErrorChain;
use itertools::Itertools;
use progenitor_client::ResponseValue;
use reqwest::StatusCode;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

use crate::errors::{ArtifactFetchError, DiscoverPeersError};

/// A fetched artifact
pub(crate) struct FetchedArtifact {
    pub(crate) attempt: usize,
    pub(crate) addr: Ipv6Addr,
    pub(crate) artifact: BufList,
}

impl FetchedArtifact {
    /// In a loop, discover peers, and fetch from them.
    ///
    /// This only produces an error if the discover function errors out. In normal use, it is expected
    /// to never error out.
    pub(crate) async fn loop_fetch_from_peers<F, Fut>(
        log: &slog::Logger,
        mut discover_fn: F,
        artifact_id: &ArtifactId,
    ) -> Result<Self>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Peers, DiscoverPeersError>>,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let peers = match discover_fn().await {
                Ok(peers) => peers,
                Err(DiscoverPeersError::Retry(error)) => {
                    slog::debug!(
                        log,
                        "(attempt {attempt}) failed to discover peers, retrying: {}",
                        DisplayErrorChain::new(AsRef::<dyn std::error::Error>::as_ref(&error)),
                    );
                    // XXX: add a delay here?
                    continue;
                }
                Err(DiscoverPeersError::Abort(error)) => {
                    return Err(error);
                }
            };

            slog::debug!(
                log,
                "discovered {} peers: [{}]",
                peers.peer_count(),
                peers.display(),
            );
            match peers.fetch_artifact(artifact_id).await {
                Some((addr, artifact)) => {
                    return Ok(Self { attempt, addr, artifact })
                }
                None => {
                    slog::debug!(
                        log,
                        "unable to fetch artifact from peers, retrying",
                    );
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

    // TODO: replace this with MockPeersUniverse
    pub(crate) async fn mock_discover(log: &slog::Logger) -> Result<Peers> {
        Ok(Self::new(
            log,
            Box::new(MockPeers::new(log)?),
            Duration::from_secs(10),
        ))
    }

    pub(crate) async fn fetch_artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Option<(Ipv6Addr, BufList)> {
        // TODO: do we want a check phase that happens before the download?
        let peers = self.peers();
        let mut remaining_peers = self.peer_count();

        let log =
            self.log.new(slog::o!("artifact_id" => artifact_id.to_string()));

        slog::debug!(log, "start fetch from peers"; "remaining_peers" => remaining_peers);

        for peer in peers {
            remaining_peers -= 1;

            slog::debug!(
                log,
                "start fetch from peer {peer:?}"; "remaining_peers" => remaining_peers,
            );

            // Attempt to download data from this peer.
            let start = Instant::now();
            match self.fetch_from_peer(peer, artifact_id).await {
                Ok(artifact_bytes) => {
                    let elapsed = start.elapsed();
                    slog::debug!(
                        log,
                        "fetched artifact from peer {peer} in {elapsed:?}"
                    );
                    return Some((peer, artifact_bytes));
                }
                Err(error) => {
                    let elapsed = start.elapsed();
                    slog::debug!(
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

    pub(crate) fn peers(&self) -> impl Iterator<Item = Ipv6Addr> + '_ {
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
        peer: Ipv6Addr,
        artifact_id: &ArtifactId,
    ) -> Result<BufList, ArtifactFetchError> {
        let (sender, mut receiver) = mpsc::channel(8);
        let (cancel_sender, cancel_receiver) = oneshot::channel();

        self.imp.start_fetch_artifact(
            peer,
            artifact_id,
            sender,
            cancel_receiver,
        );

        let mut artifact_bytes = BufList::new();

        loop {
            match tokio::time::timeout(self.timeout, receiver.recv()).await {
                Ok(Some(Ok(bytes))) => {
                    artifact_bytes.push_chunk(bytes);
                }
                Ok(Some(Err(error))) => {
                    _ = cancel_sender.send(());
                    return Err(ArtifactFetchError::HttpError { peer, error });
                }
                Ok(None) => {
                    // The entire artifact has been downloaded.
                    return Ok(artifact_bytes);
                }
                Err(_) => {
                    // The operation timed out.
                    _ = cancel_sender.send(());
                    return Err(ArtifactFetchError::Timeout {
                        peer,
                        timeout: self.timeout,
                        bytes_fetched: artifact_bytes.num_bytes(),
                    });
                }
            }
        }
    }
}

pub(crate) trait PeersImpl: fmt::Debug + Send + Sync {
    fn peers(&self) -> Box<dyn Iterator<Item = Ipv6Addr> + '_>;
    fn peer_count(&self) -> usize;

    fn start_fetch_artifact(
        &self,
        peer: Ipv6Addr,
        artifact_id: &ArtifactId,
        sender: mpsc::Sender<Result<Bytes, progenitor_client::Error>>,
        cancel_receiver: oneshot::Receiver<()>,
    );
}

#[derive(Clone, Debug)]
struct MockPeers {
    peers: Vec<Ipv6Addr>,
}

impl MockPeers {
    fn new(log: &slog::Logger) -> Result<Self> {
        let log = log.new(slog::o!("component" => "MockPeers"));

        // TODO: reach out to the bootstrap network
        slog::debug!(
            log,
            "returning Ipv6Addr::LOCALHOST and UNSPECIFIED as mock peer addresses"
        );
        let peers = vec![Ipv6Addr::LOCALHOST, Ipv6Addr::UNSPECIFIED];
        Ok(Self { peers })
    }
}

impl PeersImpl for MockPeers {
    fn peers(&self) -> Box<dyn Iterator<Item = Ipv6Addr> + '_> {
        Box::new(self.peers.iter().copied())
    }

    fn peer_count(&self) -> usize {
        self.peers.len()
    }

    fn start_fetch_artifact(
        &self,
        peer: Ipv6Addr,
        artifact_id: &ArtifactId,
        sender: mpsc::Sender<Result<Bytes, progenitor_client::Error>>,
        // MockPeers doesn't need cancel_receiver (yet)
        _cancel_receiver: oneshot::Receiver<()>,
    ) {
        let artifact_id = artifact_id.clone();
        tokio::spawn(async move {
            let res = if peer == Ipv6Addr::LOCALHOST {
                let mut bytes = BytesMut::new();
                bytes.extend_from_slice(b"mock");
                bytes.extend_from_slice(artifact_id.to_string().as_bytes());
                Ok(bytes.freeze())
            } else if peer == Ipv6Addr::UNSPECIFIED {
                Err(progenitor_client::Error::ErrorResponse(
                    ResponseValue::new(
                        (),
                        StatusCode::NOT_FOUND,
                        Default::default(),
                    ),
                ))
            } else {
                panic!("invalid peer, unknown to MockPeers: {peer}")
            };

            // Ignore errors in case the channel is dropped.
            _ = sender.send(res).await;
        });
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ArtifactId {
    name: String,
    version: String,
}

impl ArtifactId {
    #[cfg(test)]
    pub(crate) fn dummy() -> Self {
        Self { name: "dummy".to_owned(), version: "0.1.0".to_owned() }
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.version)
    }
}

impl FromStr for ArtifactId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.rsplit_once(':') {
            Some((name, version)) => {
                Ok(Self { name: name.to_owned(), version: version.to_owned() })
            }
            None => {
                bail!("input `{s}` did not contain `:`");
            }
        }
    }
}
