// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, net::Ipv6Addr, str::FromStr};

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};
use futures::{prelude::*, stream::FuturesUnordered};
use itertools::Itertools;

/// Return a list of nodes discovered on the bootstrap network.
#[derive(Clone, Debug)]
pub(crate) struct Peers {
    log: slog::Logger,
    // TODO: implement
    peers: Vec<Ipv6Addr>,
}

impl Peers {
    pub(crate) async fn discover(log: &slog::Logger) -> Result<Self> {
        let log = log.new(slog::o!("component" => "Peers"));

        // TODO: reach out to the bootstrap network
        slog::debug!(
            log,
            "returning Ipv6Addr::LOCALHOST and UNSPECIFIED as mock peer addresses"
        );
        let peers = vec![Ipv6Addr::LOCALHOST, Ipv6Addr::UNSPECIFIED];
        Ok(Self { log, peers })
    }

    pub(crate) fn display(&self) -> impl fmt::Display {
        self.peers.iter().join(", ")
    }

    pub(crate) async fn fetch_artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<(Ipv6Addr, Bytes)> {
        // TODO: do we want a check phase that happens before the download?

        let download_futs: FuturesUnordered<_> = self
            .peers
            .iter()
            .map(|peer| async {
                let result = self.fetch_impl(peer, artifact_id).await;
                (*peer, result)
            })
            .collect();
        let filtered = download_futs.filter_map(|(peer, res)| match res {
            Ok(bytes) => {
                slog::debug!(
                    self.log,
                    "for artifact {artifact_id}, peer {peer} \
                         returned an artifact with {} bytes",
                    bytes.len(),
                );

                // TODO: maybe perform checksumming here
                future::ready(Some((peer, bytes)))
            }
            Err(err) => {
                slog::debug!(
                    self.log,
                    "for artifact {artifact_id}, peer {peer} returned {err:?}",
                );
                future::ready(None)
            }
        });
        tokio::pin!(filtered);

        // Did we get any successful values?
        filtered.next().await.ok_or_else(|| {
            anyhow!(
                "for artifact {artifact_id}, no peers \
                     returned a successful result out of [{}]",
                self.display()
            )
        })
    }

    // ---

    async fn fetch_impl(
        &self,
        peer: &Ipv6Addr,
        artifact_id: &ArtifactId,
    ) -> Result<Bytes> {
        // TODO: implement this
        if peer == &Ipv6Addr::LOCALHOST {
            let mut bytes = BytesMut::new();
            bytes.extend_from_slice(b"mock");
            bytes.extend_from_slice(artifact_id.to_string().as_bytes());
            Ok(bytes.freeze())
        } else if peer == &Ipv6Addr::UNSPECIFIED {
            // TODO: return a real HTTP error
            bail!("404 not found")
        } else {
            bail!("invalid peer")
        }
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
