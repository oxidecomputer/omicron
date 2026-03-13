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
use futures::FutureExt;
use futures::future::BoxFuture;
use installinator_client::{ClientError, ResponseValue};
use installinator_common::EventReport;
use omicron_uuid_kinds::MupdateUuid;
use proptest::{collection::vec_deque, prelude::*};
use reqwest::StatusCode;
use test_strategy::Arbitrary;
use tokio::sync::mpsc;
use tufaceous_artifact::ArtifactHashId;
use update_engine::events::StepEventIsTerminal;

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
            // Don't try shrinking the bytes inside the artifact -- their
            // individual values don't matter.
            99 => prop::collection::vec(any::<u8>().no_shrink(), 0..4096),
            // Make it not very unlikely that the artifact is empty.
            1 => Just(Vec::new()),
        ];

        // We can assume without loss of generality that content is fetched from
        // peers in ascending IPv6 order. In other words, the addresses
        // themselves aren't relevant beyond being unique identifiers. This
        // means that this code can use a BTreeMap rather than a fancier
        // structure like an IndexMap.
        let peers_strategy = prop::collection::btree_map(
            any::<PeerAddress>(),
            any::<MockPeer_>(),
            0..max_peer_count,
        );

        // Any u32 is a valid bitmap (see the documentation for AttemptBitmap).
        let attempt_bitmaps_strategy =
            prop::collection::vec(any::<AttemptBitmap>(), 1..16);

        (artifact_strategy, peers_strategy, attempt_bitmaps_strategy).prop_map(
            |(artifact, peers, attempt_bitmaps)| {
                let artifact = Bytes::from(artifact);
                let peers = peers
                    .into_iter()
                    .map(|(addr, mock_peer)| {
                        let response =
                            mock_peer.response.into_actual(&artifact);
                        (
                            addr,
                            MockPeer {
                                artifact: artifact.clone(),
                                response,
                                connect_delay: mock_peer.connect_delay,
                            },
                        )
                    })
                    .collect();
                Self::new(artifact, peers, attempt_bitmaps)
            },
        )
    }

    /// On success this returns (successful attempt, winning peer).
    ///
    /// On failure this returns the number of attempts that failed.
    fn expected_result(
        &self,
        timeout: Duration,
    ) -> Result<(usize, PeerAddress), usize> {
        self.attempts()
            .enumerate()
            .filter_map(|(attempt, peers)| {
                let winner = peers.ok()?.simulate_concurrent_fetch(timeout)?;
                // attempt is zero-indexed here, but the attempt returned by
                // FetchedArtifact is 1-indexed.
                Some((attempt + 1, winner))
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
                        Ok(MockFetchBackend::new(selected_peers))
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
    /// Any u32 is a valid bitmap. If there are fewer peers than bits in the
    /// bitmap, the higher-order bits will be ignored.
    ///
    /// (We check in MockPeersUniverse::new that there are at most 32 peers.)
    #[weight(9)]
    Success(u32),

    #[weight(1)]
    Failure,
}

#[derive(Debug)]
struct MockFetchBackend {
    // Peers within the universe that have been selected.
    selected_peers: BTreeMap<PeerAddress, MockPeer>,
    // selected_peers keys stored in a suitable form for the
    // FetchArtifactImpl trait.
    peer_addresses: PeerAddresses,
}

impl MockFetchBackend {
    fn new(selected_peers: BTreeMap<PeerAddress, MockPeer>) -> Self {
        let peer_addresses = selected_peers.keys().copied().collect();
        Self { selected_peers, peer_addresses }
    }

    fn get(&self, peer: PeerAddress) -> Option<&MockPeer> {
        self.selected_peers.get(&peer)
    }

    /// Simulate the concurrent fetch for this attempt.
    ///
    /// All sender tasks start at t=0. Each peer's `fetch_from_peer_impl`
    /// completes at `t = connect_delay`, so `JoinSet` returns peers in
    /// ascending `connect_delay` order (where ties are broken by BTreeMap
    /// insertion order). The simulation processes peers in that order, tracking
    /// wall-clock time so that each peer's sender benefits from pre-progress
    /// accumulated while earlier peers were being processed.
    fn simulate_concurrent_fetch(
        &self,
        read_timeout: Duration,
    ) -> Option<PeerAddress> {
        // Build processing order: sort by (connect_delay, BTreeMap key order).
        // BTreeMap iteration is already in key order, so a stable sort by
        // connect_delay preserves key order for ties. This matches the
        // real `ParallelTaskSet`/`JoinSet` behavior: tasks spawned in
        // BTreeMap key order that complete at the same simulated instant
        // are returned in spawn (so key) order.
        let mut peers_ordered: Vec<_> = self
            .selected_peers
            .iter()
            .map(|(&addr, peer)| (addr, peer))
            .collect();
        peers_ordered.sort_by_key(|&(_, peer)| peer.connect_delay);

        let mut wall_clock = Duration::ZERO;

        for (addr, peer) in peers_ordered {
            // Advance wall clock to at least this peer's connect_delay
            // (the `JoinSet` won't return this peer's result before then).
            wall_clock = wall_clock.max(peer.connect_delay);

            let result =
                simulate_read_from_peer(peer, wall_clock, read_timeout);
            wall_clock = result.wall_clock;
            if result.success {
                return Some(addr);
            }
        }

        None
    }
}

/// Channel capacity used by the mock (matches `mpsc::channel(8)` in
/// `fetch_from_peer_impl`).
const CHANNEL_CAPACITY: usize = 8;

/// An item buffered in the simulated channel.
#[derive(Clone, Debug)]
enum ChannelItem {
    Data(usize),
    Error,
}

/// Tracks the state of a sender task for the simulation.
struct SenderSim {
    actions: Vec<ResponseAction>,
    action_idx: usize,
    /// The sender's local clock (time at which it last completed work).
    sender_clock: Duration,
    remaining_bytes: usize,
    buffer: VecDeque<ChannelItem>,
    /// The sender is blocked waiting for channel capacity.
    blocked_on_full: bool,
    /// The sender has finished (dropped the channel sender).
    channel_closed: bool,
    /// For `NotFound`/`Forbidden` responses: the absolute time at which the
    /// error will be produced. `None` for normal responses.
    pending_error_time: Option<Duration>,
}

impl SenderSim {
    fn new(peer: &MockPeer) -> Self {
        let (actions, remaining_bytes, pending_error_time) = match &peer
            .response
        {
            MockResponse::Response(actions) => {
                (actions.clone(), peer.artifact.len(), None)
            }
            // For NotFound/Forbidden, the sender sleeps `after` then
            // sends an error through the channel. We model the delay
            // with `pending_error_time` so that `advance()` produces
            // the error at the correct wall-clock time.
            MockResponse::NotFound { after }
            | MockResponse::Forbidden { after } => (vec![], 0, Some(*after)),
        };

        SenderSim {
            actions,
            action_idx: 0,
            sender_clock: Duration::ZERO,
            remaining_bytes,
            buffer: VecDeque::new(),
            blocked_on_full: false,
            channel_closed: false,
            pending_error_time,
        }
    }

    /// Advance the sender as far as possible up to `up_to` time.
    fn advance(&mut self, up_to: Duration) {
        if self.channel_closed || self.blocked_on_full {
            return;
        }

        // Handle pending error (NotFound/Forbidden responses).
        if let Some(error_time) = self.pending_error_time {
            if up_to >= error_time {
                if self.buffer.len() < CHANNEL_CAPACITY {
                    self.buffer.push_back(ChannelItem::Error);
                    self.pending_error_time = None;
                    self.channel_closed = true;
                } else {
                    self.blocked_on_full = true;
                }
            }
            return;
        }

        while self.action_idx < self.actions.len() {
            let action = &self.actions[self.action_idx];
            match action {
                ResponseAction::Response { after, count } => {
                    let wake_time = self.sender_clock + *after;
                    if wake_time > up_to {
                        // Timer hasn't fired yet.
                        return;
                    }

                    self.sender_clock = wake_time;

                    if self.buffer.len() >= CHANNEL_CAPACITY {
                        self.blocked_on_full = true;
                        return;
                    }

                    let actual = (*count).min(self.remaining_bytes);
                    self.buffer.push_back(ChannelItem::Data(actual));
                    self.remaining_bytes -= actual;
                    self.action_idx += 1;

                    if self.remaining_bytes == 0 {
                        // All data sent; sender drops.
                        self.channel_closed = true;
                        return;
                    }
                }
                ResponseAction::Error => {
                    if self.buffer.len() >= CHANNEL_CAPACITY {
                        self.blocked_on_full = true;
                        return;
                    }
                    self.buffer.push_back(ChannelItem::Error);
                    self.action_idx += 1;
                    self.channel_closed = true;
                    return;
                }
            }
        }

        // Actions exhausted without delivering all data. The sender
        // sleeps forever (far_future). It will never produce more
        // items, but it doesn't close the channel either.
    }

    /// Returns the time at which the sender will next produce an item,
    /// or `None` if it can't (blocked, closed, or sleeping forever).
    fn next_production_time(&self) -> Option<Duration> {
        if self.channel_closed || self.blocked_on_full {
            return None;
        }
        if let Some(error_time) = self.pending_error_time {
            return Some(error_time);
        }
        if self.action_idx >= self.actions.len() {
            // Actions exhausted; sleeping forever.
            return None;
        }
        match &self.actions[self.action_idx] {
            ResponseAction::Response { after, .. } => {
                Some(self.sender_clock + *after)
            }
            ResponseAction::Error => {
                // Error is sent immediately (no sleep).
                Some(self.sender_clock)
            }
        }
    }

    /// Unblock the sender after the reader consumed an item, freeing
    /// channel capacity.
    fn unblock_after_consume(&mut self) {
        if self.blocked_on_full {
            self.blocked_on_full = false;

            // Check if we were blocked on a pending error.
            if self.pending_error_time.is_some() {
                self.buffer.push_back(ChannelItem::Error);
                self.pending_error_time = None;
                self.channel_closed = true;
                return;
            }

            // The sender was blocked trying to produce an item.
            // Now there's space: produce it immediately (at the
            // sender's current clock — the sleep already elapsed).
            if self.action_idx < self.actions.len() {
                match &self.actions[self.action_idx] {
                    ResponseAction::Response { count, .. } => {
                        let actual = (*count).min(self.remaining_bytes);
                        self.buffer.push_back(ChannelItem::Data(actual));
                        self.remaining_bytes -= actual;
                        self.action_idx += 1;
                        if self.remaining_bytes == 0 {
                            self.channel_closed = true;
                        }
                    }
                    ResponseAction::Error => {
                        self.buffer.push_back(ChannelItem::Error);
                        self.action_idx += 1;
                        self.channel_closed = true;
                    }
                }
            }
        }
    }
}

/// Result of simulating a read from a single peer.
struct SimulatedRead {
    /// Whether the peer successfully delivered the full artifact.
    success: bool,
    /// Wall-clock time after processing this peer. Subsequent peers'
    /// senders have been running since t=0, so this determines how much
    /// pre-progress they've accumulated.
    wall_clock: Duration,
}

/// Simulate `fetch_full_artifact_from_peer` for a single peer.
fn simulate_read_from_peer(
    peer: &MockPeer,
    wall_clock: Duration,
    read_timeout: Duration,
) -> SimulatedRead {
    let artifact_len = peer.artifact.len();
    let mut sender = SenderSim::new(peer);
    let mut total_read: usize = 0;
    let mut wall_clock = wall_clock;

    // Advance the sender up to the current wall clock (it has been
    // running since t=0 while earlier peers were being processed).
    sender.advance(wall_clock);

    loop {
        // Try to consume a buffered item.
        if let Some(item) = sender.buffer.pop_front() {
            sender.unblock_after_consume();
            // Let the sender make further progress now that there's
            // channel space.
            sender.advance(wall_clock);

            match item {
                ChannelItem::Data(n) => {
                    total_read += n;
                    continue;
                }
                ChannelItem::Error => {
                    return SimulatedRead { success: false, wall_clock };
                }
            }
        }

        // Buffer is empty.
        if sender.channel_closed {
            // recv() returns None (channel closed). Check size.
            let success = total_read == artifact_len;
            return SimulatedRead { success, wall_clock };
        }

        // Race: sender's next production vs. read timeout.
        let timeout_deadline = wall_clock + read_timeout;

        match sender.next_production_time() {
            Some(t) if t < timeout_deadline => {
                // Sender produces strictly before the timeout.
                wall_clock = t;
                sender.advance(wall_clock);
                // Loop back to consume the newly-buffered item.
            }
            _ => {
                // Timeout fires first, or sender produces at exactly the
                // same instant. On ties, the timeout wins: tokio's timer
                // wheel uses LIFO ordering within a slot, and the
                // reader's timeout timer is registered after the
                // sender's sleep timer, so the timeout fires first.
                wall_clock = timeout_deadline;
                return SimulatedRead { success: false, wall_clock };
            }
        }
    }
}

impl FetchArtifactImpl for MockFetchBackend {
    fn peers(&self) -> &PeerAddresses {
        &self.peer_addresses
    }

    fn fetch_from_peer_impl(
        &self,
        peer: PeerAddress,
        // We don't (yet) use the artifact ID in MockPeers
        _artifact_hash_id: ArtifactHashId,
    ) -> BoxFuture<'static, Result<(u64, FetchReceiver), HttpError>> {
        let peer_data = self
            .get(peer)
            .unwrap_or_else(|| panic!("peer {peer} not found in selection"))
            .clone();
        let artifact_size = peer_data.artifact.len() as u64;
        let connect_delay = peer_data.connect_delay;

        let (sender, receiver) = mpsc::channel(8);
        // The sender starts immediately, accumulating pre-progress
        // while the connect delay elapses.
        tokio::spawn(async move { peer_data.send_response(sender).await });
        async move {
            if !connect_delay.is_zero() {
                tokio::time::sleep(connect_delay).await;
            }
            Ok((artifact_size, receiver))
        }
        .boxed()
    }
}

#[derive(Clone)]
struct MockPeer {
    artifact: Bytes,
    response: MockResponse,
    /// Simulates the time for the initial HTTP connection to succeed.
    /// The sender task starts immediately (at t=0), but the
    /// `fetch_from_peer_impl` future doesn't complete until this delay
    /// elapses. This gives the sender a head start ("pre-progress"),
    /// and makes `JoinSet` ordering deterministic (shortest delay first).
    connect_delay: Duration,
}

impl fmt::Debug for MockPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockPeer")
            .field("artifact", &format!("({} bytes)", self.artifact.len()))
            .field("response", &self.response)
            .field("connect_delay", &self.connect_delay)
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

/// Proptest-level representation of a `MockPeer`, before the artifact is
/// known. Resolved into a `MockPeer` by `MockPeersUniverse::strategy`.
#[derive(Debug, Arbitrary)]
struct MockPeer_ {
    response: MockResponse_,
    #[strategy((0..1000u64).prop_map(Duration::from_millis))]
    connect_delay: Duration,
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
    update_id: MupdateUuid,
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
        update_id: MupdateUuid,
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
        update_id: MupdateUuid,
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
        StepEventKind, StepOutcome, StepSuccess, UpdateEngine,
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
        read_timeout: Duration,
        #[strategy(any::<[u8; 16]>().prop_map(MupdateUuid::from_bytes))]
        update_id: MupdateUuid,
        valid_peer_behaviors: ReportBehaviors,
    ) {
        with_test_runtime(async move {
            let logctx = test_setup_log("proptest_fetch_artifact");
            let expected_result = universe.expected_result(read_timeout);
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
                            fetch_artifact(&cx, &log, attempts, read_timeout)
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
                        "expected successful attempt matches actual"
                    );
                    assert_eq!(
                        expected_addr, peer,
                        "expected successful peer matches actual"
                    );
                    let artifact_bytes =
                        artifact.copy_to_bytes(artifact.num_bytes());
                    assert_eq!(
                        expected_artifact, artifact_bytes,
                        "correct artifact fetched from peer {peer}",
                    );
                    assert_success_reports(&reports, attempt, peer);
                }
                (Err(expected_total_attempts), Err(_)) => {
                    assert_failure_reports(&reports, expected_total_attempts);
                }
                (Err(_), Ok(fetched_artifact)) => {
                    panic!(
                        "expected failure but found success: \
                         {fetched_artifact:?}"
                    );
                }
                (Ok((attempt, addr)), Err(err)) => {
                    panic!(
                        "expected success at attempt `{attempt}` from \
                         `{addr}`, but found failure: {err}"
                    );
                }
            }

            logctx.cleanup_successful();
        });
    }

    async fn fetch_artifact(
        cx: &StepContext,
        log: &slog::Logger,
        attempts: impl IntoIterator<Item = Result<MockFetchBackend>>,
        read_timeout: Duration,
    ) -> Result<FetchedArtifact> {
        let mut attempts = attempts.into_iter();
        FetchedArtifact::loop_fetch_from_peers(
            cx,
            log,
            || match attempts.next() {
                Some(Ok(peers)) => future::ok(FetchArtifactBackend::new(
                    &log,
                    Box::new(peers),
                    read_timeout,
                    None,
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

    /// Verify reports are self-consistent with an actual successful outcome.
    fn assert_success_reports(
        reports: &[EventReport],
        actual_attempt: usize,
        actual_peer: PeerAddress,
    ) {
        let all_step_events: Vec<_> =
            reports.iter().flat_map(|report| &report.step_events).collect();

        let mut saw_success = false;

        for event in all_step_events {
            match &event.kind {
                StepEventKind::ProgressReset { attempt, metadata, .. } => {
                    if *attempt == actual_attempt {
                        match metadata {
                            InstallinatorProgressMetadata::Download {
                                peer,
                            } => {
                                // The winning peer succeeded, so it
                                // should not have a reset on the winning
                                // attempt. Other peers failing is fine.
                                assert_ne!(
                                    *peer,
                                    actual_peer.address(),
                                    "winning peer should not have a \
                                     ProgressReset on the winning attempt"
                                );
                            }
                            other => {
                                panic!(
                                    "expected download metadata, found \
                                     {other:?}"
                                );
                            }
                        };
                    }
                }
                StepEventKind::AttemptRetry { next_attempt, .. } => {
                    assert!(
                        *next_attempt <= actual_attempt,
                        "retry next_attempt {next_attempt} should be <= \
                         actual successful attempt {actual_attempt}"
                    );
                }
                StepEventKind::ExecutionCompleted {
                    last_attempt,
                    last_outcome,
                    ..
                } => {
                    assert_eq!(
                        *last_attempt, actual_attempt,
                        "last attempt in report matches actual"
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
                                actual_peer.address(),
                                "report address matches actual peer"
                            );
                        }
                        other => {
                            panic!("expected success, found {other:?}");
                        }
                    }
                    saw_success = true;
                }
                StepEventKind::ExecutionFailed { .. } => {
                    panic!("received unexpected failed event {:?}", event.kind,)
                }
                _ => {}
            }
        }

        assert!(saw_success, "successful event must have been produced");
    }

    fn assert_failure_reports(
        reports: &[EventReport],
        expected_total_attempts: usize,
    ) {
        let all_step_events: Vec<_> =
            reports.iter().flat_map(|report| &report.step_events).collect();

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
