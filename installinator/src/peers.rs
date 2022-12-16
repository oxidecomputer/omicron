// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    fmt, future::Future, net::SocketAddrV6, pin::Pin, str::FromStr,
    time::Duration,
};

use anyhow::{bail, Result};
use buf_list::BufList;
use bytes::Bytes;
use display_error_chain::DisplayErrorChain;
use futures::StreamExt;
use itertools::Itertools;
use tokio::{sync::mpsc, time::Instant};

use crate::{
    ddm_admin_client::DdmAdminClient,
    errors::{ArtifactFetchError, DiscoverPeersError},
};

/// A chosen discovery mechanism for peers, passed in over the command line.
#[derive(Clone, Debug)]
pub(crate) enum DiscoveryMechanism {
    /// The default discovery mechanism: hit the bootstrap network.
    Bootstrap,

    /// A list of peers is manually specified.
    List(Vec<SocketAddrV6>),
}

// TODO: This currently hardcodes this wicketd port, will probably want to sync up on this.
const WICKETD_PORT: u16 = 14000;

impl DiscoveryMechanism {
    /// Discover peers.
    pub(crate) async fn discover_peers(
        &self,
        log: &slog::Logger,
    ) -> Result<Box<dyn PeersImpl>, DiscoverPeersError> {
        let peers = match self {
            Self::Bootstrap => {
                // TODO: add aborts to this after a certain number of tries?

                let ddm_admin_client =
                    DdmAdminClient::new(log).map_err(|err| {
                        DiscoverPeersError::Retry(anyhow::anyhow!(err))
                    })?;
                let addrs =
                    ddm_admin_client.peer_addrs().await.map_err(|err| {
                        DiscoverPeersError::Retry(anyhow::anyhow!(err))
                    })?;
                addrs
                    .map(|addr| {
                        // TODO: this currently hardcodes the wicketd port.
                        SocketAddrV6::new(addr, WICKETD_PORT, 0, 0)
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
    pub(crate) addr: SocketAddrV6,
    pub(crate) artifact: BufList,
}

impl FetchedArtifact {
    /// In a loop, discover peers, and fetch from them.
    ///
    /// If `discover_fn` returns [`DiscoverPeersError::Retry`], this function will retry. If it
    /// returns `DiscoverPeersError::Abort`, this function will exit with the underlying error.
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
                    slog::warn!(
                        log,
                        "(attempt {attempt}) failed to discover peers, retrying: {}",
                        DisplayErrorChain::new(AsRef::<dyn std::error::Error>::as_ref(&error)),
                    );
                    // Add a small delay here to avoid slamming the CPU.
                    tokio::time::sleep(Duration::from_millis(10)).await;
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
            match peers.fetch_artifact(artifact_id).await {
                Some((addr, artifact)) => {
                    return Ok(Self { attempt, addr, artifact })
                }
                None => {
                    slog::warn!(
                        log,
                        "unable to fetch artifact from peers, retrying discovery",
                    );
                    // Add a small delay here to avoid slamming the CPU.
                    tokio::time::sleep(Duration::from_millis(10)).await;
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
        artifact_id: &ArtifactId,
    ) -> Option<(SocketAddrV6, BufList)> {
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

    pub(crate) fn peers(&self) -> impl Iterator<Item = SocketAddrV6> + '_ {
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
        peer: SocketAddrV6,
        artifact_id: &ArtifactId,
    ) -> Result<BufList, ArtifactFetchError> {
        let log = self.log.new(slog::o!("peer" => peer.to_string()));
        let (sender, mut receiver) = mpsc::channel(8);

        let fetch =
            self.imp.fetch_from_peer_impl(peer, artifact_id.clone(), sender);

        tokio::spawn(fetch);

        let mut artifact_bytes = BufList::new();

        loop {
            match tokio::time::timeout(self.timeout, receiver.recv()).await {
                Ok(Some(Ok(bytes))) => {
                    slog::debug!(
                        &log,
                        "received chunk of {} bytes from peer",
                        bytes.len()
                    );
                    artifact_bytes.push_chunk(bytes);
                }
                Ok(Some(Err(error))) => {
                    slog::debug!(
                        &log,
                        "received error from peer, sending cancellation: {}",
                        DisplayErrorChain::new(&error),
                    );
                    return Err(ArtifactFetchError::HttpError { peer, error });
                }
                Ok(None) => {
                    // The entire artifact has been downloaded.
                    return Ok(artifact_bytes);
                }
                Err(_) => {
                    // The operation timed out.
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
    fn peers(&self) -> Box<dyn Iterator<Item = SocketAddrV6> + '_>;
    fn peer_count(&self) -> usize;

    fn fetch_from_peer_impl(
        &self,
        peer: SocketAddrV6,
        artifact_id: ArtifactId,
        sender: FetchSender,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// The send side of the channel over which data is sent.
pub(crate) type FetchSender =
    mpsc::Sender<Result<Bytes, progenitor_client::Error>>;

/// A [`PeersImpl`] that uses HTTP to fetch artifacts from peers. This is the real implementation.
#[derive(Clone, Debug)]
pub(crate) struct HttpPeers {
    log: slog::Logger,
    peers: Vec<SocketAddrV6>,
}

impl HttpPeers {
    pub(crate) fn new(log: &slog::Logger, peers: Vec<SocketAddrV6>) -> Self {
        let log = log.new(slog::o!("component" => "HttpPeers"));
        Self { log, peers }
    }
}

impl PeersImpl for HttpPeers {
    fn peers(&self) -> Box<dyn Iterator<Item = SocketAddrV6> + '_> {
        Box::new(self.peers.iter().copied())
    }

    fn peer_count(&self) -> usize {
        self.peers.len()
    }

    fn fetch_from_peer_impl(
        &self,
        peer: SocketAddrV6,
        artifact_id: ArtifactId,
        sender: FetchSender,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        // TODO: be able to fetch from sled-agent clients as well
        let artifact_client = ArtifactClient::new(peer, &self.log);
        Box::pin(
            async move { artifact_client.fetch(artifact_id, sender).await },
        )
    }
}

#[derive(Debug)]
struct ArtifactClient {
    log: slog::Logger,
    client: wicketd_client::Client,
}

impl ArtifactClient {
    fn new(addr: SocketAddrV6, log: &slog::Logger) -> Self {
        let endpoint = format!("http://[{}]:{}", addr.ip(), addr.port());
        let log = log.new(
            slog::o!("component" => "ArtifactClient", "peer" => addr.to_string()),
        );
        let client = wicketd_client::Client::new(&endpoint, log.clone());
        Self { log, client }
    }

    async fn fetch(&self, artifact_id: ArtifactId, sender: FetchSender) {
        let artifact_bytes = match self
            .client
            .get_artifact(&artifact_id.name, &artifact_id.version)
            .await
        {
            Ok(artifact_bytes) => artifact_bytes,
            Err(error) => {
                // TODO: does this lose too much info (wicketd_client::types::Error)?
                _ = sender.send(Err(error.into_untyped())).await;
                return;
            }
        };

        slog::debug!(
            &self.log,
            "preparing to receive {:?} bytes from artifact",
            artifact_bytes.content_length(),
        );

        let mut bytes = artifact_bytes.into_inner_stream();
        while let Some(item) = bytes.next().await {
            if let Err(_) = sender.send(item.map_err(Into::into)).await {
                // The sender was dropped, which indicates that the job was cancelled.
                return;
            }
        }
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
