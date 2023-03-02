// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeMap,
    fmt,
    net::{Ipv6Addr, SocketAddrV6},
    time::Duration,
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use installinator_artifact_client::ClientError;
use installinator_common::ProgressReport;
use omicron_common::update::ArtifactHashId;
use progenitor_client::ResponseValue;
use proptest::prelude::*;
use reqwest::StatusCode;
use test_strategy::Arbitrary;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    errors::HttpError,
    peers::{FetchReceiver, PeersImpl},
};

struct MockPeersUniverse {
    artifact: Bytes,
    peers: BTreeMap<SocketAddrV6, MockPeer>,
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
        peers: BTreeMap<SocketAddrV6, MockPeer>,
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
            any::<SocketAddrV6>(),
            any::<MockResponse_>(),
            0..max_peer_count,
        );

        // Any u32 is a valid bitmap (see the documentation for AttemptBitmap).
        let attempt_bitmaps_strategy =
            prop::collection::vec(any::<AttemptBitmap>(), 1..16);

        (artifact_strategy, peers_strategy, attempt_bitmaps_strategy).prop_map(
            |(artifact, peers, attempt_bitmaps): (
                Vec<u8>,
                BTreeMap<SocketAddrV6, MockResponse_>,
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

    fn expected_success(
        &self,
        timeout: Duration,
    ) -> Option<(usize, SocketAddrV6)> {
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
    }

    fn attempts(&self) -> impl Iterator<Item = Result<MockPeers>> + '_ {
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
                        Ok(MockPeers {
                            artifact: self.artifact.clone(),
                            selected_peers,
                        })
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
struct MockPeers {
    artifact: Bytes,
    // Peers within the universe that have been selected
    selected_peers: BTreeMap<SocketAddrV6, MockPeer>,
}

impl MockPeers {
    fn get(&self, addr: SocketAddrV6) -> Option<&MockPeer> {
        self.selected_peers.get(&addr)
    }

    fn peers(&self) -> impl Iterator<Item = (&SocketAddrV6, &MockPeer)> + '_ {
        self.selected_peers.iter()
    }

    /// Returns the peer that can return the entire dataset within the timeout.
    fn successful_peer(&self, timeout: Duration) -> Option<SocketAddrV6> {
        self.peers()
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
impl PeersImpl for MockPeers {
    fn peers(&self) -> Box<dyn Iterator<Item = SocketAddrV6> + Send + '_> {
        Box::new(self.selected_peers.keys().copied())
    }

    fn peer_count(&self) -> usize {
        self.selected_peers.len()
    }

    async fn fetch_from_peer_impl(
        &self,
        peer: SocketAddrV6,
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

    async fn report_progress_impl(
        &self,
        _peer: SocketAddrV6,
        _update_id: Uuid,
        _report: ProgressReport,
    ) -> Result<(), ClientError> {
        panic!(
            "this is currently unused -- at some point we'll want to \
             unify this with MockReportPeers"
        )
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
                        installinator_artifact_client::types::Error {
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
                        installinator_artifact_client::types::Error {
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
struct MockReportPeers {
    update_id: Uuid,
    report_sender: mpsc::Sender<ProgressReport>,
}

impl MockReportPeers {
    // SocketAddrV6::new is not a const fn in stable Rust as of this writing
    fn valid_peer() -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), 2000, 0, 0)
    }

    fn invalid_peer() -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2), 2000, 0, 0)
    }

    fn unresponsive_peer() -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 3), 2000, 0, 0)
    }

    fn new(
        update_id: Uuid,
        report_sender: mpsc::Sender<ProgressReport>,
    ) -> Self {
        Self { update_id, report_sender }
    }
}

#[async_trait]
impl PeersImpl for MockReportPeers {
    fn peers(&self) -> Box<dyn Iterator<Item = SocketAddrV6> + Send + '_> {
        Box::new(
            [
                Self::valid_peer(),
                Self::invalid_peer(),
                Self::unresponsive_peer(),
            ]
            .into_iter(),
        )
    }

    fn peer_count(&self) -> usize {
        3
    }

    async fn fetch_from_peer_impl(
        &self,
        _peer: SocketAddrV6,
        _artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError> {
        unimplemented!(
            "this should never be called -- \
            eventually we'll want to unify this with MockPeers",
        )
    }

    async fn report_progress_impl(
        &self,
        peer: SocketAddrV6,
        update_id: Uuid,
        report: ProgressReport,
    ) -> Result<(), ClientError> {
        assert_eq!(update_id, self.update_id, "update ID matches");
        if peer == Self::valid_peer() {
            _ = self.report_sender.send(report).await;
            Ok(())
        } else if peer == Self::invalid_peer() {
            Err(ClientError::ErrorResponse(ResponseValue::new(
                installinator_artifact_client::types::Error {
                    error_code: None,
                    message: "invalid peer => HTTP 422".to_owned(),
                    request_id: "mock-request-id".to_owned(),
                },
                StatusCode::UNPROCESSABLE_ENTITY,
                Default::default(),
            )))
        } else if peer == Self::unresponsive_peer() {
            // The real implementation generates a reqwest::Error, which can't be
            // created outside of the reqwest library. Generate a different error.
            Err(ClientError::InvalidRequest("unresponsive peer".to_owned()))
        } else {
            panic!("unrecognized peer: {peer}")
        }
    }
}

mod tests {
    use super::*;
    use crate::{
        errors::DiscoverPeersError,
        peers::{FetchedArtifact, Peers},
        reporter::ProgressReporter,
        test_helpers::{dummy_artifact_hash_id, with_test_runtime},
    };

    use bytes::Buf;
    use futures::{future, StreamExt};
    use omicron_test_utils::dev::test_setup_log;
    use test_strategy::proptest;
    use tokio_stream::wrappers::ReceiverStream;

    // The #[proptest] macro doesn't currently with with #[tokio::test] sadly.
    #[proptest]
    fn proptest_fetch_artifact(
        #[strategy(MockPeersUniverse::strategy(32))]
        universe: MockPeersUniverse,
        #[strategy((0..2000u64).prop_map(Duration::from_millis))]
        timeout: Duration,
        #[strategy(any::<[u8; 16]>().prop_map(Uuid::from_bytes))]
        update_id: Uuid,
    ) {
        with_test_runtime(move || async move {
            let logctx = test_setup_log("proptest_fetch_artifact");
            let expected_success = universe.expected_success(timeout);
            let expected_artifact = universe.artifact.clone();

            let mut attempts = universe.attempts();

            let (report_sender, report_receiver) = mpsc::channel(512);

            let receiver_handle = tokio::spawn(async move {
                ReceiverStream::new(report_receiver).collect::<Vec<_>>().await
            });

            let reporter_log = logctx.log.clone();

            let (progress_reporter, event_sender) =
                ProgressReporter::new(&logctx.log, update_id, move || {
                    let reporter_log = reporter_log.clone();
                    let report_sender = report_sender.clone();

                    async move {
                        Ok(Peers::new(
                            &reporter_log,
                            Box::new(MockReportPeers::new(
                                update_id,
                                report_sender,
                            )),
                            // The timeout is currently unused by broadcast_report.
                            Duration::from_secs(10),
                        ))
                    }
                });
            let progress_handle = progress_reporter.start();

            let fetched_artifact = FetchedArtifact::loop_fetch_from_peers(
                &logctx.log,
                || match attempts.next() {
                    Some(Ok(peers)) => future::ok(Peers::new(
                        &logctx.log,
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
                &dummy_artifact_hash_id(),
                &event_sender,
            )
            .await;

            std::mem::drop(event_sender);

            progress_handle
                .await
                .expect("progress reporter task exited successfully");

            let reports = receiver_handle
                .await
                .expect("progress report receiver task exited successfully");

            println!("finished receiving reports");

            match (expected_success, fetched_artifact) {
                (
                    Some((expected_attempt, expected_addr)),
                    Ok(FetchedArtifact { attempt, addr, mut artifact }),
                ) => {
                    assert_eq!(
                        expected_attempt, attempt,
                        "expected successful attempt is the same as actual attempt"
                    );
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
                (None, Err(_)) => {}
                (None, Ok(fetched_artifact)) => {
                    panic!("expected failure to fetch but found success: {fetched_artifact:?}");
                }
                (Some((attempt, addr)), Err(err)) => {
                    panic!("expected success at attempt `{attempt}` from `{addr}`, but found failure: {err}");
                }
            }

            assert_progress_reports(&reports, expected_success);

            logctx.cleanup_successful();
        });
    }

    fn assert_progress_reports(
        reports: &[ProgressReport],
        expected_success: Option<(usize, SocketAddrV6)>,
    ) {
        for report in reports {
            // Assert that completion event timestamps are before report timestamps.
            for event in &report.completion_events {
                assert!(
                    event.total_elapsed <= report.total_elapsed,
                    "event elapsed {:?} is before report elapsed {:?}",
                    event.total_elapsed,
                    report.total_elapsed
                );
            }
            // Assert that completion events are correctly ordered.
            for events in report.completion_events.windows(2) {
                assert!(
                    events[0].total_elapsed <= events[1].total_elapsed,
                    "preceding event elapsed {:?} is less than event elapsed {:?}",
                    events[0].total_elapsed,
                    events[1].total_elapsed
                );
                assert!(
                    events[0].kind.attempt() <= events[1].kind.attempt(),
                    "preceding event attempt {:?} is less than event attempt {:?}",
                    events[0].kind.attempt(),
                    events[1].kind.attempt(),
                );
            }
            // For now we only send a single progress event at most.
            assert!(
                report.progress_events.len() <= 1,
                "at most one progress event"
            );
            // Assert that progress event timestamps are before report timestamps.
            for event in &report.progress_events {
                assert!(
                    event.total_elapsed <= report.total_elapsed,
                    "event elapsed {:?} is before report elapsed {:?}",
                    event.total_elapsed,
                    report.total_elapsed
                )
            }
        }

        let all_completion_events: Vec<_> = reports
            .iter()
            .flat_map(|report| &report.completion_events)
            .collect();

        // Assert that we received failure events for all prior attempts and
        // a success event for the current attempt.
        if let Some((attempt, expected_addr)) = expected_success {
            let mut saw_success = false;

            for event in all_completion_events {
                match (event.kind.is_success(), event.kind.peer()) {
                    (true, Some(peer)) => {
                        saw_success = true;
                        assert_eq!(
                            peer, expected_addr,
                            "successful peer should match address"
                        );
                        assert_eq!(
                            event.kind.attempt(),
                            Some(attempt),
                            "successful attempt should match"
                        );
                    }
                    (false, Some(peer)) => {
                        // If the peer is the expected one, ensure that the
                        // attempt is lower.
                        if peer == expected_addr {
                            assert!(
                                event.kind.attempt().unwrap() < attempt,
                                "for expected peer, failed attempt {:?} < {attempt}",
                                event.kind.attempt()
                            );
                        } else {
                            assert!(
                                event.kind.attempt().unwrap() <= attempt,
                                "for different peer than expected, failed attempt {:?} <= {attempt}",
                                event.kind.attempt()
                            );
                        }
                    }
                    (_, None) => {
                        // This is a non-download event; ignore it.
                    }
                }
            }

            assert!(saw_success, "successful event must have been produced");
            // It's hard to say anything about failing events for now because
            // it's possible we didn't have any peers. In the future we can look
            // at the MockPeersUniverse to ensure that we receive failing
            // events from every peer that should have failed.
        } else {
            for event in all_completion_events {
                assert!(
                    !event.kind.is_success(),
                    "for failed attempts, all events must be failures"
                );
            }
        }
    }
}
