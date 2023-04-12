// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation details for polling MGS for rack inventory details.

use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpComponentInfo;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use gateway_client::types::SpState;
use gateway_messages::SpComponent;
use slog::warn;
use slog::Logger;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task;
use tokio::time::interval;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

use crate::inventory::RotInventory;

// Frequency at which we poll our local ignition controller (via our local
// sidecar SP) for the ignition state of all ignition targets in the rack.
const POLL_FREQ_IGNITION: Duration = Duration::from_secs(5);

// Frequency at which we poll SPs we believe to be present based on ignition
// results.
const POLL_FREQ_PRESENT_SP: Duration = Duration::from_secs(10);

// Frequency at which we poll SPs we believe are _not_ present based on ignition
// results. We still poll these SPs (albeit less frequently) to account for
// problems with ignition (either incorrect results, which should be extremely
// rare, or problems getting the state, which should also be rare).
const POLL_FREQ_MISSING_SP: Duration = Duration::from_secs(30);

pub(super) struct PollIgnition {
    pub(super) sps: BTreeMap<SpIdentifier, SpIgnition>,
    pub(super) mgs_received: Instant,
}

// Handle to the tokio task responsible for polling MGS for ignition state. When
// dropped, the polling task is cancelled.
pub(super) struct IgnitionPoller {
    task: task::JoinHandle<()>,
    rx: mpsc::Receiver<PollIgnition>,
    poll_now_tx: mpsc::Sender<()>,
}

impl Drop for IgnitionPoller {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl IgnitionPoller {
    /// Spawn the ignition polling task.
    pub(super) fn spawn(
        mgs_client: gateway_client::Client,
        log: Logger,
    ) -> Self {
        // We only want one outstanding ignition request at a time; if our
        // consumer is behind, we don't need to poll MGS until they can handle
        // our results.
        let (tx, rx) = mpsc::channel(1);

        // "Poll immediately" also only needs a channel depth of 1: if there is
        // already a message in this channel, we're already trying to poll ASAP.
        let (poll_now_tx, poll_now_rx) = mpsc::channel(1);

        let task = tokio::spawn(ignition_poller_task(
            tx,
            poll_now_rx,
            mgs_client,
            log,
        ));

        Self { task, rx, poll_now_tx }
    }

    /// Receive the next result from the ignition polling task.
    pub(super) async fn recv(&mut self) -> PollIgnition {
        // The task we spawned holds `tx` either until `rx` is dropped (which it
        // obviously is not here, since we're using it!) or it panics, so we can
        // unwrap here. The only way we panic is if our inner task already did.
        self.rx.recv().await.expect("ignition polling task panicked")
    }

    pub(super) fn poll_now(&self) {
        match self.poll_now_tx.try_send(()) {
            // If we succeeded or there's already a "poll now" request sitting
            // in the channel, we're done.
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => (),
            // If the channel is closed, that means our task has panicked -
            // propogate that panic.
            Err(mpsc::error::TrySendError::Closed(())) => {
                panic!("ignition polling task panicked")
            }
        }
    }
}

async fn ignition_poller_task(
    tx: mpsc::Sender<PollIgnition>,
    mut poll_now_rx: mpsc::Receiver<()>,
    mgs_client: gateway_client::Client,
    log: Logger,
) {
    let mut ticker = interval(POLL_FREQ_IGNITION);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => (),
            _ = poll_now_rx.recv() => {
                ticker.reset();
            }
        }

        let results = match mgs_client.ignition_list().await {
            Ok(response) => response.into_inner(),
            Err(err) => {
                warn!(
                    log, "Failed to poll MGS for ignition";
                    "err" => %err,
                );
                continue;
            }
        };

        let mut sps = BTreeMap::new();
        let mgs_received = Instant::now();
        for result in results {
            sps.insert(result.id, result.details);
        }

        let emit = PollIgnition { sps, mgs_received };

        // If our receiver is gone, we'll exit - there's no one left for
        // us to send results to!
        if tx.send(emit).await.is_err() {
            warn!(log, "Receiver for ignition polling task is gone");
            break;
        }
    }
}

pub(super) struct PollSp {
    pub(super) id: SpIdentifier,
    pub(super) state: SpState,
    pub(super) components: Option<Vec<SpComponentInfo>>,
    pub(super) caboose: Option<SpComponentCaboose>,
    pub(super) rot: RotInventory,
    pub(super) mgs_received: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IgnitionState {
    Present,
    Absent,
}

impl IgnitionState {
    fn poll_frequency(self) -> Duration {
        match self {
            IgnitionState::Present => POLL_FREQ_PRESENT_SP,
            IgnitionState::Absent => POLL_FREQ_MISSING_SP,
        }
    }
}

pub(super) struct SpPoller {
    task: task::JoinHandle<()>,
    ignition_state_tx: watch::Sender<Option<IgnitionState>>,
    poll_now_tx: mpsc::Sender<()>,
}

impl Drop for SpPoller {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SpPoller {
    /// Spawn a polling task for a single SP.
    ///
    /// Returns a handle for interacting with the task and a stream that emits
    /// polling results.
    pub(super) fn spawn(
        id: SpIdentifier,
        mgs_client: gateway_client::Client,
        log: Logger,
    ) -> (Self, ReceiverStream<PollSp>) {
        // We only want one outstanding request at a time; if our consumer is
        // behind, we don't need to poll MGS until they can handle our results.
        let (poll_tx, poll_rx) = mpsc::channel(1);

        // "Poll immediately" also only needs a channel depth of 1: if there is
        // already a message in this channel, we're already trying to poll ASAP.
        let (poll_now_tx, poll_now_rx) = mpsc::channel(1);

        let (ignition_state_tx, ignition_state_rx) = watch::channel(None);

        let task = tokio::spawn(sp_poller_task(
            id,
            poll_tx,
            poll_now_rx,
            ignition_state_rx,
            mgs_client,
            log,
        ));

        (
            Self { task, ignition_state_tx, poll_now_tx },
            ReceiverStream::new(poll_rx),
        )
    }

    pub(super) fn set_ignition_state(&self, state: IgnitionState) {
        // `tokio::watch::Sender` doesn't check for equality: only send an
        // update if this state is actually different.
        if *self.ignition_state_tx.borrow() != Some(state) {
            match self.ignition_state_tx.send(Some(state)) {
                Ok(()) => (),
                Err(_) => panic!("SP polling task panicked"),
            }
        }
    }

    pub(super) fn poll_now(&self) {
        match self.poll_now_tx.try_send(()) {
            // If we succeeded or there's already a "poll now" request sitting
            // in the channel, we're done.
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => (),
            // If the channel is closed, that means our task has panicked -
            // propogate that panic.
            Err(mpsc::error::TrySendError::Closed(())) => {
                panic!("SP polling task panicked")
            }
        }
    }
}

async fn sp_poller_task(
    id: SpIdentifier,
    tx: mpsc::Sender<PollSp>,
    mut poll_now: mpsc::Receiver<()>,
    mut ignition_state: watch::Receiver<Option<IgnitionState>>,
    mgs_client: gateway_client::Client,
    log: Logger,
) {
    let mut ticker = interval(
        ignition_state
            .borrow()
            .map_or(POLL_FREQ_PRESENT_SP, IgnitionState::poll_frequency),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut prev_state = None;
    let mut components = None;
    let mut caboose = None;
    let mut rot = RotInventory { caboose: None };

    loop {
        tokio::select! {
            _ = ticker.tick() => (),
            _ = poll_now.recv() => {
                ticker.reset();
            }
            _ = ignition_state.changed() => {
                // When our ignition state changes, clear out all cached data
                // and recreate `ticker`.
                prev_state = None;
                components = None;
                caboose = None;
                rot = RotInventory { caboose: None };

                ticker = interval(
                    ignition_state
                        .borrow()
                        .map_or(
                            POLL_FREQ_PRESENT_SP,
                            IgnitionState::poll_frequency,
                        ),
                );
                ticker.set_missed_tick_behavior(
                    tokio::time::MissedTickBehavior::Delay);
                ticker.reset();
            }
        }

        let state = match mgs_client.sp_get(id.type_, id.slot).await {
            // TODO Can we remove the ignition info from MGS's sp_get?
            Ok(response) => response.into_inner().details,
            Err(err) => {
                warn!(
                    log, "Failed to get state for SP";
                    "sp" => ?id,
                    "err" => %err,
                );
                continue;
            }
        };
        let mut mgs_received = Instant::now();

        // For each of our cached items that require additional MGS requests (SP
        // components, SP caboose, RoT caboose), only fetch them if either our
        // state has changed or we previously failed to fetch them after such a
        // state change.

        if prev_state.as_ref() != Some(&state) || components.is_none() {
            components =
                match mgs_client.sp_component_list(id.type_, id.slot).await {
                    Ok(response) => {
                        mgs_received = Instant::now();
                        Some(response.into_inner().components)
                    }
                    Err(err) => {
                        warn!(
                            log, "Failed to get component list for sp";
                            "sp" => ?id,
                            "err" => %err,
                        );
                        None
                    }
                };
        }

        if prev_state.as_ref() != Some(&state) || caboose.is_none() {
            caboose = match mgs_client
                .sp_component_caboose_get(
                    id.type_,
                    id.slot,
                    SpComponent::SP_ITSELF.const_as_str(),
                )
                .await
            {
                Ok(response) => {
                    mgs_received = Instant::now();
                    Some(response.into_inner())
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get caboose for sp";
                        "sp" => ?id,
                        "err" => %err,
                    );
                    None
                }
            };
        }

        if prev_state.as_ref() != Some(&state) || rot.caboose.is_none() {
            rot.caboose = match mgs_client
                .sp_component_caboose_get(
                    id.type_,
                    id.slot,
                    SpComponent::ROT.const_as_str(),
                )
                .await
            {
                Ok(response) => {
                    mgs_received = Instant::now();
                    Some(response.into_inner())
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get RoT caboose for sp";
                        "sp" => ?id,
                        "err" => %err,
                    );
                    None
                }
            };
        }

        let emit = PollSp {
            id,
            state,
            components: components.clone(),
            caboose: caboose.clone(),
            rot: rot.clone(),
            mgs_received,
        };

        // If our receiver is gone, we'll exit - there's no one left for
        // us to send results to!
        if tx.send(emit).await.is_err() {
            warn!(
                log, "Receiver for ignition polling task is gone";
                "sp" => ?id,
            );
            break;
        }
    }
}
