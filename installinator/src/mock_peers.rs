// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO: Remove when https://github.com/frozenlib/test-strategy/commit/c0ca38711757d2b51f74b28e80ef4c78275c284f
// is pulled into a new published version of "test_strategy" that we can use.
#![allow(clippy::arc_with_non_send_sync)]

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Mutex,
    time::Duration,
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use installinator_client::{ClientError, ResponseValue};
use installinator_common::EventReport;
use proptest::{collection::vec_deque, prelude::*};
use reqwest::StatusCode;
use test_strategy::Arbitrary;
use tokio::sync::mpsc;
use tufaceous_artifact::ArtifactHashId;
use update_engine::events::StepEventIsTerminal;
use uuid::Uuid;

use crate::{
    errors::{DiscoverPeersError, HttpError},
    fetch::{FetchArtifactImpl, FetchReceiver},
    peers::{PeerAddress, PeerAddresses},
    reporter::ReportProgressImpl,
};

struct MockPeersUniverse {
    artifact: Bytes,
    peers: BTreeMap<PeerAddress, MockPeer>,
    attempt_bitmaps: Vec<AttemptBitmap>,
}

impl fmt::Debug for MockPeersUniverse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockPeersUniverse")
            .field("artifact", &format!("({} bytes)", self.artifact.len()))
            .field("peers", &self.peers)
            .field("attempt_bitmaps", &self.attempt_bitmaps)
            .finish()
    }
}

impl MockPeersUniverse {
    fn new(
        artifact: Bytes,
        peers: BTreeMap<PeerAddress, MockPeer>,
        attempt_bitmaps: Vec<AttemptBitmap>,
    ) -> Self {
        assert!(peers.len() <= 32, "this test only supports up to 32 peers");
        Self { artifact, peers, attempt_bitmaps }
    }

    fn strategy(max_peer_count: usize) -> impl Strategy<Value = Self> {
        let artifact_strategy = prop_oneof![
            // Don't try shrinking the bytes inside the artifact -- their individual values don't
            // matter.
            99 => prop::collection::vec(any::<u8>().no_shrink(), 0..4096),
            // Make it not very unlikely that the artifact is empty.
            1 => Just(Vec::new()),
        ];

        // We can assume without loss of generality that content is fetched from peers in
        // ascending IPv6 order. In other words, the addresses themselves aren't relevant beyond
        // being unique identifiers. This means that this code can use a BTreeMap rather than a
        // fancier structure like an IndexMap.
        let peers_strategy = prop::collection::btree_map(
            any::<PeerAddress>(),
            any::<MockResponse_>(),
            0..max_peer_count,
        );

        // Any u32 is a valid bitmap (see the documentation for AttemptBitmap).
        let attempt_bitmaps_strategy =
            prop::collection::vec(any::<AttemptBitmap>(), 1..16);

        (artifact_strategy, peers_strategy, attempt_bitmaps_strategy).prop_map(
            |(artifact, peers, attempt_bitmaps): (
                Vec<u8>,
                BTreeMap<PeerAddress, MockResponse_>,
                Vec<AttemptBitmap>,
            )| {
                let artifact = Bytes::from(artifact);
                let peers = peers
                    .into_iter()
                    .map(|(peer, response)| {
                        let response = response.into_actual(&artifact);
                        (
                            peer,
                            MockPeer { artifact: artifact.clone(), response },
                        )
                    })
                    .collect();
                Self::new(artifact, peers, attempt_bitmaps)
            },
        )
    }

    /// On success this returns (successful attempt, peer).
    ///
    /// On failure this returns the number of attempts that failed.
    fn expected_result(
        &self,
        timeout: Duration,
    ) -> Result<(usize, PeerAddress), usize> {
        self.attempts()
            .enumerate()
            .filter_map(|(attempt, peers)| {
                peers
                    .ok()?
                    .successful_peer(timeout)
                    // attempt is zero-indexed here, but the attempt returned by FetchedArtifact is
                    // 1-indexed.
                    .map(|addr| (attempt + 1, addr))
            })
            .next()
            .ok_or_else(|| {
                // We're going to try one last time after the attempt bitmaps
                // run out, then abort. Hence + 1.
                self.attempt_bitmaps.len() + 1
            })
    }

    fn attempts(&self) -> impl Iterator<Item = Result<MockFetchBackend>> + '_ {
        self.attempt_bitmaps.iter().enumerate().map(
            move |(i, &attempt_bitmap)| {
                match attempt_bitmap {
                    AttemptBitmap::Success(bitmap) => {
                        let selected_peers = self
                            .peers
                            .iter()
                            .enumerate()
                            .filter_map(move |(i, (addr, peer))| {
                                // Check that the ith bit is set in attempt_bitmap
                                (bitmap & (1 << i) != 0)
                                    .then(|| (*addr, peer.clone()))
                            })
                            .collect();
                        Ok(MockFetchBackend::new(
                            self.artifact.clone(),
                            selected_peers,
                        ))
                    }
                    AttemptBitmap::Failure => {
                        bail!(
                            "[MockPeersUniverse] attempt {i} failed, retrying"
                        );
                    }
                }
            },
        )
    }
}

#[derive(Copy, Clone, Debug, Arbitrary)]
enum AttemptBitmap {
    /// Any u32 is a valid bitmap. If there are fewer peers than bits in the bitmap, the
    /// higher-order bits will be ignored.
    ///
    /// (We check in MockPeersUniverse::new that there are at most 32 peers.)
    #[weight(9)]
    Success(u32),

    #[weight(1)]
    Failure,
}

#[derive(Debug)]
struct MockFetchBackend {
    artifact: Bytes,
    // Peers within the universe that have been selected
    selected_peers: BTreeMap<PeerAddress, MockPeer>,
    // selected_peers keys stored in a suitable form for the
    // FetchArtifactImpl trait
    peer_addresses: PeerAddresses,
}

impl MockFetchBackend {
    fn new(
        artifact: Bytes,
        selected_peers: BTreeMap<PeerAddress, MockPeer>,
    ) -> Self {
        let peer_addresses = selected_peers.keys().copied().collect();
        Self { artifact, selected_peers, peer_addresses }
    }

    fn get(&self, peer: PeerAddress) -> Option<&MockPeer> {
        self.selected_peers.get(&peer)
    }

    /// Returns the peer that can return the entire dataset within the timeout.
    fn successful_peer(&self, timeout: Duration) -> Option<PeerAddress> {
        self.selected_peers.iter()
            .filter_map(|(addr, peer)| {
                if peer.artifact != self.artifact {
                    // We don't handle the case where the peer returns the wrong artifact yet.
                    panic!("peer artifact not the same as self.artifact -- can't happen in normal use");
                }

                match &peer.response {
                    MockResponse::Response(actions) => {
                        let mut total_count = 0;
                        for action in actions {
                            match action {
                                ResponseAction::Response { after, count } => {
                                    // Each action must finish under the timeout. Note that within Tokio,
                                    // timers of the same duration should fire in the order that they were
                                    // created, because that's the order they'll be added to the linked list
                                    // for that timer wheel slot. While this is not yet guaranteed in
                                    // Tokio's documentation, it is the only reasonable implementation so we
                                    // rely on it here.
                                    //
                                    // Since Peers creates the timeout BEFORE MockPeersUniverse sets its
                                    // delay, action.after must be less than timeout.
                                    if *after >= timeout {
                                        return None;
                                    }

                                    total_count += count;
                                    if total_count >= peer.artifact.len() {
                                        return Some(*addr);
                                    }
                                }
                                ResponseAction::Error => return None,
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

#[async_trait]
impl FetchArtifactImpl for MockFetchBackend {
    fn peers(&self) -> &PeerAddresses {
        &self.peer_addresses
    }

    async fn fetch_from_peer_impl(
        &self,
        peer: PeerAddress,
        // We don't (yet) use the artifact ID in MockPeers
        _artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError> {
        let peer_data = self
            .get(peer)
            .unwrap_or_else(|| panic!("peer {peer} not found in selection"))
            .clone();
        let artifact_size = peer_data.artifact.len() as u64;

        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move { peer_data.send_response(sender).await });
        // TODO: add tests to ensure an invalid artifact size is correctly detected
        Ok((artifact_size, receiver))
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
        sender: mpsc::Sender<Result<Bytes, ClientError>>,
    ) {
        let mut artifact = self.artifact;
        match self.response {
            MockResponse::Response(actions) => {
                for action in actions {
                    match action {
                        ResponseAction::Response { after, count } => {
                            if !after.is_zero() {
                                tokio::time::sleep(after).await;
                            }
                            let at = count.min(artifact.len());
                            let value = artifact.split_to(at);
                            if let Err(_) = sender.send(Ok(value)).await {
                                // The receiver has been dropped, which indicates that this task
                                // should be cancelled.
                                return;
                            }
                            if artifact.is_empty() {
                                // If there's no more data left, we're done.
                                return;
                            }
                        }
                        ResponseAction::Error => {
                            // The real implementation generates a reqwest::Error, which can't be
                            // created outside of the reqwest library. Generate a different error.
                            _ = sender
                                .send(Err(ClientError::InvalidRequest(
                                    "sending error".to_owned(),
                                )))
                                .await;
                            return;
                        }
                    }
                }

                // (This trivial function is copied from tokio's source.)
                fn far_future() -> Duration {
                    // Roughly 30 years from now.
                    // API does not provide a way to obtain max `Instant`
                    // or convert specific date in the future to instant.
                    // 1000 years overflows on macOS, 100 years overflows on FreeBSD.
                    Duration::from_secs(86400 * 365 * 30)
                }

                // If we got here, we didn't manage to return all the data we needed. Sleep forever.
                tokio::time::sleep(far_future()).await;
            }
            MockResponse::NotFound { after } => {
                tokio::time::sleep(after).await;
                _ = sender
                    .send(Err(ClientError::ErrorResponse(ResponseValue::new(
                        installinator_client::types::Error {
                            error_code: None,
                            message: format!("not-found error after {after:?}"),
                            request_id: "mock-request-id".to_owned(),
                        },
                        StatusCode::NOT_FOUND,
                        Default::default(),
                    ))))
                    .await;
            }
            MockResponse::Forbidden { after } => {
                tokio::time::sleep(after).await;
                _ = sender
                    .send(Err(ClientError::ErrorResponse(ResponseValue::new(
                        installinator_client::types::Error {
                            error_code: None,
                            message: format!("forbidden error after {after:?}"),
                            request_id: "mock-request-id".to_owned(),
                        },
                        StatusCode::FORBIDDEN,
                        Default::default(),
                    ))))
                    .await;
            }
        }
    }
}

#[derive(Clone, Debug)]
enum MockResponse {
    // Once the actions run out, this times out. We don't need a separate "timeout" variant here
    // because we don't lose any generality by relying on the length of the actions list.
    Response(Vec<ResponseAction>),
    NotFound { after: Duration },
    Forbidden { after: Duration },
}

#[derive(Clone, Debug)]
enum ResponseAction {
    Response { after: Duration, count: usize },
    Error,
}

/// This is an enum that has the same shape as `MockResponse`, except it uses `prop::sample::Index`
/// (within `ResponseAction_`) to represent an unresolved index into the artifact.
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

/// This is an enum that has the same shape as `ResponseAction_`, except it uses `prop::sample::Index`
/// (within `ResponseAction_`) to represent an unresolved index into the artifact.
#[derive(Debug, Arbitrary)]
enum ResponseAction_ {
    #[weight(19)]
    Response {
        #[strategy((0..1000u64).prop_map(Duration::from_millis))]
        after: Duration,
        count: prop::sample::Index,
    },
    #[weight(1)]
    Error,
}

impl ResponseAction_ {
    fn into_actual(self, limit: usize) -> ResponseAction {
        match self {
            Self::Response { after, count } => {
                let limit = if limit == 0 {
                    // limit = 0 if it's an empty artifact. We can try and record that we're returning
                    // some bytes anyway.
                    8
                } else {
                    limit
                };
                let count = count.index(limit);
                ResponseAction::Response { after, count }
            }
            Self::Error => ResponseAction::Error,
        }
    }
}

/// A `PeersImpl` for reporting values.
///
/// In the future, this will be combined with `MockPeers` so we can model.
#[derive(Debug)]
struct MockProgressBackend {
    update_id: Uuid,
    // Use an unbounded sender to avoid async code in handle_valid_peer_event.
    report_sender: mpsc::UnboundedSender<EventReport>,
    behaviors: Mutex<ReportBehaviors>,
}

impl MockProgressBackend {
    const VALID_PEER: PeerAddress = PeerAddress::new(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
        2000,
    ));

    const INVALID_PEER: PeerAddress = PeerAddress::new(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2)),
        2000,
    ));

    const UNRESPONSIVE_PEER: PeerAddress = PeerAddress::new(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 3)),
        2000,
    ));

    fn new(
        update_id: Uuid,
        report_sender: mpsc::UnboundedSender<EventReport>,
        behaviors: ReportBehaviors,
    ) -> Self {
        Self { update_id, report_sender, behaviors: Mutex::new(behaviors) }
    }

    fn handle_valid_peer_event(
        &self,
        report: EventReport,
    ) -> Result<(), ClientError> {
        let is_terminal = matches!(
            report.step_events.last().map(|e| e.kind.is_terminal()),
            Some(StepEventIsTerminal::Terminal { .. })
        );

        let mut lock = self.behaviors.lock().unwrap();
        let next_behavior = lock.next_valid_peer_behavior(is_terminal);

        match next_behavior {
            ValidPeerBehavior::Accept => {
                if lock.terminal_accepted {
                    // Return Gone to indicate that the peer has accepted the
                    // report in the past.
                    Err(ClientError::ErrorResponse(ResponseValue::new(
                        installinator_client::types::Error {
                            error_code: None,
                            message: "terminal message received => Gone"
                                .to_owned(),
                            request_id: "mock-request-id".to_owned(),
                        },
                        StatusCode::GONE,
                        Default::default(),
                    )))
                } else {
                    // Accept the report.
                    _ = self.report_sender.send(report);
                    if is_terminal {
                        lock.terminal_accepted = true;
                    }
                    Ok(())
                }
            }
            ValidPeerBehavior::AcceptError => {
                // The real implementation generates a reqwest::Error, which can't be
                // created outside of the reqwest library. Generate a different error.
                Err(ClientError::InvalidRequest(
                    "peer could not receive response".to_owned(),
                ))
            }
            ValidPeerBehavior::ResponseError => {
                // Accept the report but return an error.
                if !lock.terminal_accepted {
                    _ = self.report_sender.send(report);
                    if is_terminal {
                        lock.terminal_accepted = true;
                    }
                }

                Err(ClientError::InvalidRequest(
                    "peer received response, but failed to transmit \
                     that back to installinator"
                        .to_owned(),
                ))
            }
        }
    }
}

#[async_trait]
impl ReportProgressImpl for MockProgressBackend {
    async fn discover_peers(
        &self,
    ) -> Result<PeerAddresses, DiscoverPeersError> {
        let mut lock = self.behaviors.lock().unwrap();
        match lock.next_discovery_behavior() {
            ReportDiscoveryBehavior::Retry => Err(DiscoverPeersError::Retry(
                anyhow::anyhow!("simulated retry error"),
            )),
            // TODO: it would be nice to simulate some peers disappearing here.
            ReportDiscoveryBehavior::Success => Ok([
                Self::VALID_PEER,
                Self::INVALID_PEER,
                Self::UNRESPONSIVE_PEER,
            ]
            .into_iter()
            .collect()),
        }
    }

    async fn report_progress_impl(
        &self,
        peer: PeerAddress,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        assert_eq!(update_id, self.update_id, "update ID matches");
        if peer == Self::VALID_PEER {
            self.handle_valid_peer_event(report)
        } else if peer == Self::INVALID_PEER {
            Err(ClientError::ErrorResponse(ResponseValue::new(
                installinator_client::types::Error {
                    error_code: None,
                    message: "invalid peer => HTTP 422".to_owned(),
                    request_id: "mock-request-id".to_owned(),
                },
                StatusCode::UNPROCESSABLE_ENTITY,
                Default::default(),
            )))
        } else if peer == Self::UNRESPONSIVE_PEER {
            // The real implementation generates a reqwest::Error, which can't be
            // created outside of the reqwest library. Generate a different error.
            Err(ClientError::InvalidRequest("unresponsive peer".to_owned()))
        } else {
            panic!("unrecognized peer: {peer}")
        }
    }
}

#[derive(Clone, Copy, Debug, Arbitrary)]
enum ReportDiscoveryBehavior {
    /// Return all peers successfully.
    #[weight(4)]
    Success,

    /// Simulate a retry error.
    #[weight(1)]
    Retry,
}

/// For reporting results, controls how discovery and the valid peer should
/// behave.
///
/// Used to simulate network flakiness while discovering peers and reporting
/// results.
#[derive(Clone, Debug)]
struct ReportBehaviors {
    discovery: VecDeque<ReportDiscoveryBehavior>,
    progress: VecDeque<ValidPeerBehavior>,
    terminal: VecDeque<ValidPeerBehavior>,
    terminal_accepted: bool,
}

impl ReportBehaviors {
    fn next_discovery_behavior(&mut self) -> ReportDiscoveryBehavior {
        // Once the queue of behaviors is exhausted, always return success.
        self.discovery.pop_front().unwrap_or(ReportDiscoveryBehavior::Success)
    }

    fn next_valid_peer_behavior(
        &mut self,
        is_terminal: bool,
    ) -> ValidPeerBehavior {
        // Once the queues of behaviors are exhausted, always accept.
        if is_terminal {
            self.terminal.pop_front().unwrap_or(ValidPeerBehavior::Accept)
        } else {
            self.progress.pop_front().unwrap_or(ValidPeerBehavior::Accept)
        }
    }
}

impl Arbitrary for ReportBehaviors {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (
            vec_deque(any::<ReportDiscoveryBehavior>(), 0..128),
            vec_deque(any::<ValidPeerBehavior>(), 0..128),
            vec_deque(any::<ValidPeerBehavior>(), 0..128),
        )
            .prop_map(|(discovery, progress, terminal)| ReportBehaviors {
                discovery,
                progress,
                terminal,
                terminal_accepted: false,
            })
            .boxed()
    }
}

/// Model situations in which the peer that accepts the update misbehaves or
/// has flakiness.
///
/// The AcceptError and ResponseError variants are low-probability ones in
/// reality, but we set them to be higher probability here (1/3 each) to get
/// better coverage for error conditions.
#[derive(Clone, Copy, Debug, Arbitrary)]
enum ValidPeerBehavior {
    /// Accept the update and return Ok(()).
    Accept,

    /// Fail to accept the update, simulating situations where the server fails
    /// to receive the report.
    AcceptError,

    /// Accept the update but return an error, simulating situations where the
    /// server receives the report but is unable to transmit this fact back to
    /// the client.
    ResponseError,
}

mod tests {
    use super::*;
    use crate::{
        errors::DiscoverPeersError,
        fetch::{FetchArtifactBackend, FetchedArtifact},
        reporter::{ProgressReporter, ReportProgressBackend},
        test_helpers::{dummy_artifact_hash_id, with_test_runtime},
    };

    use bytes::Buf;
    use futures::{StreamExt, future};
    use installinator_common::{
        InstallinatorCompletionMetadata, InstallinatorComponent,
        InstallinatorProgressMetadata, InstallinatorStepId, StepContext,
        StepEvent, StepEventKind, StepOutcome, StepSuccess, UpdateEngine,
    };
    use omicron_test_utils::dev::test_setup_log;
    use test_strategy::proptest;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tufaceous_artifact::KnownArtifactKind;

    // The #[proptest] macro doesn't currently with with #[tokio::test] sadly.
    #[proptest]
    fn proptest_fetch_artifact(
        #[strategy(MockPeersUniverse::strategy(32))]
        universe: MockPeersUniverse,
        #[strategy((0..2000u64).prop_map(Duration::from_millis))]
        timeout: Duration,
        #[strategy(any::<[u8; 16]>().prop_map(Uuid::from_bytes))]
        update_id: Uuid,
        valid_peer_behaviors: ReportBehaviors,
    ) {
        with_test_runtime(async move {
            let logctx = test_setup_log("proptest_fetch_artifact");
            let expected_result = universe.expected_result(timeout);
            let expected_artifact = universe.artifact.clone();

            let attempts = universe.attempts();

            let (report_sender, report_receiver) = mpsc::unbounded_channel();

            let receiver_handle = tokio::spawn(async move {
                UnboundedReceiverStream::new(report_receiver)
                    .collect::<Vec<_>>()
                    .await
            });

            let (progress_reporter, event_sender) = ProgressReporter::new(
                &logctx.log,
                update_id,
                ReportProgressBackend::new(
                    &logctx.log,
                    MockProgressBackend::new(
                        update_id,
                        report_sender,
                        valid_peer_behaviors,
                    ),
                ),
            );
            let progress_handle = progress_reporter.start();

            let engine = UpdateEngine::new(&logctx.log, event_sender);
            let log = logctx.log.clone();
            let artifact_handle = engine
                .new_step(
                    InstallinatorComponent::HostPhase2,
                    InstallinatorStepId::Download,
                    "Downloading artifact",
                    async move |cx| {
                        let artifact =
                            fetch_artifact(&cx, &log, attempts, timeout)
                                .await?;
                        let peer = artifact.peer;
                        StepSuccess::new(artifact)
                            .with_metadata(
                                InstallinatorCompletionMetadata::Download {
                                    address: peer.address(),
                                },
                            )
                            .into()
                    },
                )
                .register();

            let fetched_artifact = match engine.execute().await {
                Ok(completion_cx) => {
                    Ok(artifact_handle.into_value(completion_cx.token()).await)
                }
                Err(error) => Err(error),
            };

            progress_handle
                .await
                .expect("progress reporter task exited successfully");

            let reports = receiver_handle
                .await
                .expect("progress report receiver task exited successfully");

            match (expected_result, fetched_artifact) {
                (
                    Ok((expected_attempt, expected_addr)),
                    Ok(FetchedArtifact { attempt, peer, mut artifact }),
                ) => {
                    assert_eq!(
                        expected_attempt, attempt,
                        "expected successful attempt is the same as actual attempt"
                    );
                    assert_eq!(
                        expected_addr, peer,
                        "expected successful peer is the same as actual peer"
                    );
                    let artifact = artifact.copy_to_bytes(artifact.num_bytes());
                    assert_eq!(
                        expected_artifact, artifact,
                        "correct artifact fetched from peer {}",
                        peer,
                    );
                }
                (Err(_), Err(_)) => {}
                (Err(_), Ok(fetched_artifact)) => {
                    panic!(
                        "expected failure to fetch but found success: {fetched_artifact:?}"
                    );
                }
                (Ok((attempt, addr)), Err(err)) => {
                    panic!(
                        "expected success at attempt `{attempt}` from `{addr}`, but found failure: {err}"
                    );
                }
            }

            assert_reports(&reports, expected_result);

            logctx.cleanup_successful();
        });
    }

    async fn fetch_artifact(
        cx: &StepContext,
        log: &slog::Logger,
        attempts: impl IntoIterator<Item = Result<MockFetchBackend>>,
        timeout: Duration,
    ) -> Result<FetchedArtifact> {
        let mut attempts = attempts.into_iter();
        FetchedArtifact::loop_fetch_from_peers(
            cx,
            log,
            || match attempts.next() {
                Some(Ok(peers)) => future::ok(FetchArtifactBackend::new(
                    &log,
                    Box::new(peers),
                    timeout,
                )),
                Some(Err(error)) => {
                    future::err(DiscoverPeersError::Retry(error))
                }
                None => future::err(DiscoverPeersError::Abort(
                    anyhow::anyhow!("ran out of attempts"),
                )),
            },
            &dummy_artifact_hash_id(KnownArtifactKind::ControlPlane),
        )
        .await
    }

    fn assert_reports(
        reports: &[EventReport],
        expected_result: Result<(usize, PeerAddress), usize>,
    ) {
        let all_step_events: Vec<_> =
            reports.iter().flat_map(|report| &report.step_events).collect();

        // Assert that we received failure events for all prior attempts and
        // a success event for the current attempt.
        match expected_result {
            Ok((expected_attempt, expected_addr)) => {
                assert_success_events(
                    all_step_events,
                    expected_attempt,
                    expected_addr,
                );
            }
            Err(expected_total_attempts) => {
                assert_failure_events(all_step_events, expected_total_attempts);
            }
        }
    }

    fn assert_success_events(
        all_step_events: Vec<&StepEvent>,
        expected_attempt: usize,
        expected_peer: PeerAddress,
    ) {
        let mut saw_success = false;

        for event in all_step_events {
            match &event.kind {
                StepEventKind::ProgressReset { attempt, metadata, .. } => {
                    if *attempt == expected_attempt {
                        match metadata {
                            InstallinatorProgressMetadata::Download {
                                peer,
                            } => {
                                assert_ne!(
                                    *peer,
                                    expected_peer.address(),
                                    "peer cannot match since this is the last attempt"
                                );
                            }
                            other => {
                                panic!(
                                    "expected download metadata, found {other:?}"
                                );
                            }
                        };
                    }
                }
                StepEventKind::AttemptRetry { next_attempt, .. } => {
                    // It's hard to say anything about failing attempts for now
                    // because it's possible we didn't have any peers. In the
                    // future we can look at the MockPeersUniverse to ensure
                    // that we receive failing events from every peer that
                    // should have failed.

                    assert!(
                        *next_attempt <= expected_attempt,
                        "next attempt {next_attempt} is <= {expected_attempt} + 1"
                    );
                }
                StepEventKind::ExecutionCompleted {
                    last_attempt,
                    last_outcome,
                    ..
                } => {
                    assert_eq!(
                        *last_attempt, expected_attempt,
                        "last attempt matches expected"
                    );
                    match last_outcome {
                        StepOutcome::Success {
                            metadata:
                                Some(InstallinatorCompletionMetadata::Download {
                                    address,
                                }),
                            ..
                        } => {
                            assert_eq!(
                                *address,
                                expected_peer.address(),
                                "address matches expected"
                            );
                        }
                        other => {
                            panic!("expected success, found {other:?}");
                        }
                    }
                    saw_success = true;
                }
                StepEventKind::ExecutionFailed { .. } => {
                    panic!("received unexpected failed event {:?}", event.kind)
                }
                _ => {}
            }
        }

        assert!(saw_success, "successful event must have been produced");
    }

    fn assert_failure_events(
        all_step_events: Vec<&StepEvent>,
        expected_total_attempts: usize,
    ) {
        let mut saw_failure = false;
        for event in all_step_events {
            match &event.kind {
                StepEventKind::AttemptRetry { next_attempt, .. } => {
                    assert!(
                        *next_attempt <= expected_total_attempts,
                        "next attempt {next_attempt} \
                             is less than {expected_total_attempts}"
                    );
                }
                StepEventKind::ExecutionFailed { total_attempts, .. } => {
                    assert_eq!(
                        *total_attempts, expected_total_attempts,
                        "total attempts matches expected"
                    );
                    saw_failure = true;
                }
                StepEventKind::ExecutionCompleted { .. } => {
                    panic!(
                        "received unexpected success event {:?}",
                        event.kind
                    );
                }
                _ => {}
            }
        }

        assert!(saw_failure, "failure event must have been produced");
    }
}
