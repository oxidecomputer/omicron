// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeMap, fmt, net::Ipv6Addr, time::Duration};

use bytes::Bytes;
use progenitor_client::ResponseValue;
use reqwest::StatusCode;
use tokio::sync::mpsc;

use crate::peers::{ArtifactId, PeersImpl};

struct MockPeersUniverse {
    artifact: Bytes,
    peers: BTreeMap<Ipv6Addr, MockPeer>,
}

impl fmt::Debug for MockPeersUniverse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockPeersUniverse")
            .field("artifact", &format!("({} bytes)", self.artifact.len()))
            .field("peers", &self.peers)
            .finish()
    }
}

impl MockPeersUniverse {
    /// Returns the peer that can return the entire dataset within the timeout.
    #[allow(dead_code)]
    fn successful_peer(&self, timeout: Duration) -> Option<Ipv6Addr> {
        self.peers
            .iter()
            .filter_map(|(addr, peer)| {
                if peer.artifact != self.artifact {
                    // We don't handle the case where the peer returns the wrong artifact yet.
                    panic!("peer artifact not the same as self.artifact -- can't happen in normal use");
                }

                match &peer.response {
                    MockResponse::Response(actions) => {
                        let mut total_count = 0;
                        for action in actions {
                            // Each action must finish under the timeout. Note that within Tokio,
                            // timers of the same duration should fire in the order that they were
                            // created, because that's the order they'll be added to the linked list
                            // for that timer wheel slot. While this is not yet guaranteed in
                            // Tokio's documentation, it is the only reasonable implementation so we
                            // rely on it here.
                            //
                            // Since Peers creates the timeout BEFORE MockPeersUniverse sets its
                            // delay, action.after must be less than timeout.
                            if action.after >= timeout {
                                return None;
                            }

                            total_count += action.count;
                            if total_count >= peer.artifact.len() {
                                return Some(*addr);
                            }
                        }
                        None
                    }
                    MockResponse::Forbidden { .. } | MockResponse::NotFound { .. } => None,
                }
            })
            .next()
    }
}

impl PeersImpl for MockPeersUniverse {
    fn peers(&self) -> Box<dyn Iterator<Item = Ipv6Addr> + '_> {
        Box::new(self.peers.keys().copied())
    }

    fn peer_count(&self) -> usize {
        self.peers.len()
    }

    fn start_fetch_artifact(
        &self,
        peer: Ipv6Addr,
        // For now, we ignore the artifact ID -- we assume that there's only one ID under test.
        _artifact_id: &ArtifactId,
        sender: mpsc::Sender<Result<Bytes, progenitor_client::Error>>,
        cancel_receiver: tokio::sync::oneshot::Receiver<()>,
    ) {
        let peer_data = self
            .peers
            .get(&peer)
            .unwrap_or_else(|| panic!("peer {peer} not found"))
            .clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = peer_data.send_response(sender) => {}
                _ = cancel_receiver => {}
            }
        });
    }
}

#[derive(Clone)]
struct MockPeer {
    artifact: Bytes,
    response: MockResponse,
}

impl fmt::Debug for MockPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockPeer")
            .field("artifact", &format!("({} bytes)", self.artifact.len()))
            .field("response", &self.response)
            .finish()
    }
}

impl MockPeer {
    async fn send_response(
        self,
        sender: mpsc::Sender<Result<Bytes, progenitor_client::Error>>,
    ) {
        let mut artifact = self.artifact;
        match self.response {
            MockResponse::Response(actions) => {
                for action in actions {
                    if !action.after.is_zero() {
                        tokio::time::sleep(action.after).await;
                    }
                    let at = action.count.min(artifact.len());
                    let value = artifact.split_to(at);
                    _ = sender.send(Ok(value)).await;
                    if artifact.is_empty() {
                        // If there's no more data left, we're done.
                        return;
                    }
                }

                // If we got here, we didn't manage to return all the data we needed. Sleep forever.
                tokio::time::sleep(far_future()).await;
            }
            MockResponse::NotFound { after } => {
                tokio::time::sleep(after).await;
                _ = sender
                    .send(Err(progenitor_client::Error::ErrorResponse(
                        ResponseValue::new(
                            (),
                            StatusCode::NOT_FOUND,
                            Default::default(),
                        ),
                    )))
                    .await;
            }
            MockResponse::Forbidden { after } => {
                tokio::time::sleep(after).await;
                _ = sender
                    .send(Err(progenitor_client::Error::ErrorResponse(
                        ResponseValue::new(
                            (),
                            StatusCode::FORBIDDEN,
                            Default::default(),
                        ),
                    )))
                    .await;
            }
        }
    }
}

// (This trivial function is copied from tokio's source.)
pub(crate) fn far_future() -> Duration {
    // Roughly 30 years from now.
    // API does not provide a way to obtain max `Instant`
    // or convert specific date in the future to instant.
    // 1000 years overflows on macOS, 100 years overflows on FreeBSD.
    Duration::from_secs(86400 * 365 * 30)
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum MockResponse {
    // Once the actions run out, this times out.
    Response(Vec<ResponseAction>),
    NotFound { after: Duration },
    Forbidden { after: Duration },
}

#[derive(Clone, Debug)]
struct ResponseAction {
    after: Duration,
    count: usize,
}

#[cfg(test)]
mod proptest_helpers {
    use proptest::prelude::*;
    use test_strategy::Arbitrary;

    use super::*;

    impl MockPeersUniverse {
        pub(crate) fn strategy(
            max_peer_count: usize,
        ) -> impl Strategy<Value = Self> {
            let artifact_strategy = prop_oneof![
                // Don't try shrinking the bytes inside the artifact -- their individual values don't
                // matter.
                99 => prop::collection::vec(any::<u8>().no_shrink(), 0..16384),
                // Make it not very unlikely that the artifact is empty.
                1 => Just(Vec::new()),
            ];

            // We can assume without loss of generality that content is fetched from peers in
            // ascending IPv6 order. In other words, the addresses themselves aren't relevant beyond
            // being unique identifiers. This means that this code can use a BTreeMap rather than a
            // fancier structure like an IndexMap.
            let peers_strategy = prop::collection::btree_map(
                any::<Ipv6Addr>(),
                any::<MockResponse_>(),
                0..max_peer_count,
            );

            (artifact_strategy, peers_strategy).prop_map(
                |(artifact, peers): (
                    Vec<u8>,
                    BTreeMap<Ipv6Addr, MockResponse_>,
                )| {
                    let artifact = Bytes::from(artifact);
                    let peers = peers
                        .into_iter()
                        .map(|(peer, response)| {
                            let response = response.into_actual(&artifact);
                            (
                                peer,
                                MockPeer {
                                    artifact: artifact.clone(),
                                    response,
                                },
                            )
                        })
                        .collect();
                    Self { artifact, peers }
                },
            )
        }
    }

    #[derive(Debug, Arbitrary)]
    enum MockResponse_ {
        #[weight(8)]
        Response(
            #[strategy(prop::collection::vec(any::<ResponseAction_>(), 0..32))]
            Vec<ResponseAction_>,
        ),
        #[weight(1)]
        NotFound {
            #[strategy((0..1000u64).prop_map(Duration::from_millis))]
            after: Duration,
        },
        #[weight(1)]
        Forbidden {
            #[strategy((0..1000u64).prop_map(Duration::from_millis))]
            after: Duration,
        },
    }

    impl MockResponse_ {
        fn into_actual(self, data: &[u8]) -> MockResponse {
            // This means that the data will be broken up into at least 2 packets, and more likely
            // 4-8.
            let limit = data.len() / 2;
            match self {
                Self::Response(actions) => {
                    let actions = actions
                        .into_iter()
                        .map(|action| action.into_actual(limit))
                        .collect();
                    MockResponse::Response(actions)
                }
                Self::NotFound { after } => MockResponse::NotFound { after },
                Self::Forbidden { after } => MockResponse::Forbidden { after },
            }
        }
    }

    #[derive(Debug, Arbitrary)]
    struct ResponseAction_ {
        #[strategy((0..1000u64).prop_map(Duration::from_millis))]
        after: Duration,
        count: prop::sample::Index,
    }

    impl ResponseAction_ {
        // The return value is the actual action + the number of bytes to advance data by.
        fn into_actual(self, limit: usize) -> ResponseAction {
            let limit = if limit == 0 {
                // limit = 0 if it's an empty artifact. We can try and record that we're returning
                // some bytes anyway.
                8
            } else {
                limit
            };
            let count = self.count.index(limit);
            ResponseAction { after: self.after, count }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::peers::Peers;

    use super::*;
    use bytes::Buf;
    use proptest::prelude::*;
    use slog::Drain;
    use test_strategy::proptest;

    use std::future::Future;

    // The #[proptest] macro doesn't currently with with #[tokio::test] sadly.
    #[proptest]
    fn proptest_fetch_artifact(
        #[strategy(MockPeersUniverse::strategy(32))]
        universe: MockPeersUniverse,
        #[strategy((0..2000u64).prop_map(Duration::from_millis))]
        timeout: Duration,
    ) {
        with_test_runtime(move || async move {
            let log = test_logger();
            let successful_peer = universe.successful_peer(timeout);
            let expected_artifact = universe.artifact.clone();

            let peers = Peers::new(&log, Box::new(universe), timeout);

            let res = peers.fetch_artifact(&ArtifactId::dummy()).await;

            match (successful_peer, res) {
                (Some(expected_addr), Some((addr, mut artifact))) => {
                    assert_eq!(
                        expected_addr, addr,
                        "expected successful peer is the same as actual peer"
                    );
                    let artifact = artifact.copy_to_bytes(artifact.num_bytes());
                    assert_eq!(
                        expected_artifact, artifact,
                        "correct artifact fetched from peer {}",
                        addr,
                    );
                }
                (None, None) => {}
                (None, Some((addr, _))) => {
                    panic!("expected failure to fetch but found success with peer `{addr}`");
                }
                (Some(addr), None) => {
                    panic!("expected success from `{addr}` but found failure");
                }
            }
        })
    }

    fn with_test_runtime<F, Fut, T>(f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("tokio Runtime built successfully");
        runtime.block_on(f())
    }

    fn test_logger() -> slog::Logger {
        // To control logging, use RUST_LOG.
        let decorator = slog_term::PlainDecorator::new(std::io::stderr());
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let mut builder = slog_envlogger::LogBuilder::new(drain);
        if let Ok(s) = std::env::var("RUST_TEST_LOG") {
            builder = builder.parse(&s);
        }
        let drain = builder.build();

        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }
}
