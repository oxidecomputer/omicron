// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, net::Ipv6Addr, str::FromStr};

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use tokio::sync::{broadcast, mpsc};

#[derive(Debug)]
pub(crate) struct Peers {
    log: slog::Logger,
    imp: Box<dyn PeersImpl>,
}

impl Peers {
    pub(crate) async fn mock_discover(log: &slog::Logger) -> Result<Peers> {
        let log = log.new(slog::o!("component" => "Peers"));
        let imp = Box::new(MockPeers::new(&log)?);
        Ok(Self { log, imp })
    }

    pub(crate) async fn fetch_artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<(Ipv6Addr, Bytes)> {
        // TODO: do we want a check phase that happens before the download?

        let (sender, mut receiver) = mpsc::channel(8);
        let (cancel_sender, cancel_receiver) = broadcast::channel(1);
        for peer in self.imp.peers().iter() {
            self.imp.start_fetch_artifact(
                *peer,
                artifact_id,
                sender.clone(),
                cancel_sender.subscribe(),
            );
        }

        std::mem::drop((sender, cancel_receiver));

        loop {
            match receiver.recv().await {
                Some((peer, Ok(bytes))) => {
                    slog::debug!(
                        self.log,
                        "for artifact {artifact_id}, peer {peer} \
                             returned an artifact with {} bytes",
                        bytes.len(),
                    );

                    // TODO: maybe perform checksumming and other signature validation here

                    // A result was received -- cancel remaining jobs.
                    let _ = cancel_sender.send(());
                    break Ok((peer, bytes));
                }
                Some((peer, Err(err))) => {
                    slog::debug!(
                        self.log,
                        "for artifact {artifact_id}, peer {peer} returned {err:?}",
                    );
                }
                None => {
                    // No more peers are left.
                    bail!(
                        "for artifact {artifact_id}, no peers \
                             returned a successful result out of [{}]",
                        self.display()
                    )
                }
            }
        }
    }

    pub(crate) fn display(&self) -> String {
        self.imp.peers().iter().join(", ")
    }
}

trait PeersImpl: fmt::Debug + Send + Sync {
    fn peers(&self) -> &[Ipv6Addr];

    fn start_fetch_artifact(
        &self,
        peer: Ipv6Addr,
        artifact_id: &ArtifactId,
        sender: mpsc::Sender<(Ipv6Addr, Result<Bytes>)>,
        cancel_receiver: broadcast::Receiver<()>,
    );
}

/// Return a list of nodes discovered on the bootstrap network.
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
    fn peers(&self) -> &[Ipv6Addr] {
        &self.peers
    }

    fn start_fetch_artifact(
        &self,
        peer: Ipv6Addr,
        artifact_id: &ArtifactId,
        sender: mpsc::Sender<(Ipv6Addr, Result<Bytes>)>,
        // MockPeers doesn't need cancel_receiver (yet)
        _cancel_receiver: broadcast::Receiver<()>,
    ) {
        let artifact_id = artifact_id.clone();
        tokio::spawn(async move {
            let res = if peer == Ipv6Addr::LOCALHOST {
                let mut bytes = BytesMut::new();
                bytes.extend_from_slice(b"mock");
                bytes.extend_from_slice(artifact_id.to_string().as_bytes());
                Ok(bytes.freeze())
            } else if peer == Ipv6Addr::UNSPECIFIED {
                // TODO: return a real HTTP error
                Err(anyhow!("404 not found"))
            } else {
                panic!("invalid peer, unknown to MockPeers: {peer}")
            };

            // Ignore errors in case the channel is dropped.
            let _ = sender.send((peer, res)).await;
        });
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ArtifactId {
    name: String,
    version: String,
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
