// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation details for polling MGS for rack inventory details.

use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpComponentInfo;
use gateway_client::types::SpIgnition;
use gateway_client::types::SpState;
use gateway_messages::SpComponent;
use slog::Logger;
use slog::warn;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::interval;
use tokio_stream::wrappers::ReceiverStream;
use wicket_common::inventory::RotInventory;
use wicket_common::inventory::SpIdentifier;

// Frequency at which we fetch state from our local ignition controller (via our
// local sidecar SP) for the ignition state of all ignition targets in the rack.
const FETCH_FREQ_IGNITION: Duration = Duration::from_secs(5);

// Frequency at which we fetch state from SPs we believe to be present based on
// ignition results.
const FETCH_FREQ_PRESENT_SP: Duration = Duration::from_secs(10);

// Frequency at which we fetch state from SPs we believe are _not_ present based
// on ignition results. We still attempt to fetch state from these SPs (albeit
// less frequently) to account for problems with ignition (either incorrect
// results, which should be extremely rare, or problems getting the state, which
// should also be rare).
const FETCH_FREQ_MISSING_SP: Duration = Duration::from_secs(30);

pub(super) struct FetchedIgnitionState {
    pub(super) sps: BTreeMap<SpIdentifier, SpIgnition>,
    pub(super) mgs_received: Instant,
}

// Handle to the tokio task responsible for fetching ignition state from MGS.
// When dropped, the task created by is `IgnitionStateFetcher::spawn()` is
// cancelled.
pub(super) struct IgnitionStateFetcher {
    task: task::JoinHandle<()>,
    rx: mpsc::Receiver<FetchedIgnitionState>,
    fetch_now_tx: mpsc::Sender<()>,
}

impl Drop for IgnitionStateFetcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl IgnitionStateFetcher {
    /// Spawn the ignition state-fetching task.
    pub(super) fn spawn(
        mgs_client: gateway_client::Client,
        log: Logger,
    ) -> Self {
        // We only want one outstanding ignition request at a time; if our
        // consumer is behind, we don't need to hit MGS until they can handle
        // our results.
        let (tx, rx) = mpsc::channel(1);

        // "Fetch immediately" also only needs a channel depth of 1: if there is
        // already a message in this channel, we're already trying to fetch
        // ASAP.
        let (fetch_now_tx, fetch_now_rx) = mpsc::channel(1);

        let task = tokio::spawn(ignition_fetching_task(
            tx,
            fetch_now_rx,
            mgs_client,
            log,
        ));

        Self { task, rx, fetch_now_tx }
    }

    /// Receive the next result from the ignition state-fetching task.
    pub(super) async fn recv(&mut self) -> FetchedIgnitionState {
        // The task we spawned holds `tx` either until `rx` is dropped (which it
        // obviously is not here, since we're using it!) or it panics, so we can
        // unwrap here. The only way we panic is if our inner task already did.
        self.rx.recv().await.expect("ignition state-fetching task panicked")
    }

    pub(super) fn fetch_now(&self) {
        match self.fetch_now_tx.try_send(()) {
            // If we succeeded or there's already a "fetch now" request sitting
            // in the channel, we're done.
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => (),
            // If the channel is closed, that means our task has panicked -
            // propogate that panic.
            Err(mpsc::error::TrySendError::Closed(())) => {
                panic!("ignition state-fetching task panicked")
            }
        }
    }
}

async fn ignition_fetching_task(
    tx: mpsc::Sender<FetchedIgnitionState>,
    mut fetch_now_rx: mpsc::Receiver<()>,
    mgs_client: gateway_client::Client,
    log: Logger,
) {
    let mut ticker = interval(FETCH_FREQ_IGNITION);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => (),
            _ = fetch_now_rx.recv() => {
                ticker.reset();
            }
        }

        let results = match mgs_client.ignition_list().await {
            Ok(response) => response.into_inner(),
            Err(err) => {
                warn!(
                    log, "Failed to get ignition state from MGS";
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

        let emit = FetchedIgnitionState { sps, mgs_received };

        // If our receiver is gone, we'll exit - there's no one left for
        // us to send results to!
        if tx.send(emit).await.is_err() {
            warn!(log, "Receiver for ignition state-fetching task is gone");
            break;
        }
    }
}

pub(super) struct FetchedSpData {
    pub(super) id: SpIdentifier,
    pub(super) state: SpState,
    pub(super) components: Option<Vec<SpComponentInfo>>,
    pub(super) caboose_active: Option<SpComponentCaboose>,
    pub(super) caboose_inactive: Option<SpComponentCaboose>,
    pub(super) rot: Option<RotInventory>,
    pub(super) mgs_received: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IgnitionPresence {
    Present,
    Absent,
}

impl IgnitionPresence {
    fn fetch_frequency(self) -> Duration {
        match self {
            IgnitionPresence::Present => FETCH_FREQ_PRESENT_SP,
            IgnitionPresence::Absent => FETCH_FREQ_MISSING_SP,
        }
    }
}

pub(super) struct SpStateFetcher {
    task: task::JoinHandle<()>,
    ignition_presence_tx: watch::Sender<Option<IgnitionPresence>>,
    fetch_now_tx: mpsc::Sender<()>,
}

impl Drop for SpStateFetcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SpStateFetcher {
    /// Spawn a task responsible for fetching state for a single SP from MGS.
    ///
    /// Returns a handle for interacting with the task and a stream that emits
    /// fetched data.
    pub(super) fn spawn(
        id: SpIdentifier,
        mgs_client: gateway_client::Client,
        log: Logger,
    ) -> (Self, ReceiverStream<FetchedSpData>) {
        // We only want one outstanding request at a time; if our consumer is
        // behind, we don't need to request new state MGS until they can handle
        // our results.
        let (data_tx, data_rx) = mpsc::channel(1);

        // "Fetch immediately" also only needs a channel depth of 1: if there is
        // already a message in this channel, we're already trying to fetch
        // ASAP.
        let (fetch_now_tx, fetch_now_rx) = mpsc::channel(1);

        let (ignition_presence_tx, ignition_presence_rx) = watch::channel(None);

        let task = tokio::spawn(sp_fetching_task(
            id,
            data_tx,
            fetch_now_rx,
            ignition_presence_rx,
            mgs_client,
            log,
        ));

        (
            Self { task, ignition_presence_tx, fetch_now_tx },
            ReceiverStream::new(data_rx),
        )
    }

    pub(super) fn set_ignition_presence(&self, presence: IgnitionPresence) {
        // `tokio::watch::Sender` doesn't check for equality: only send an
        // update if this presence is actually different.
        if *self.ignition_presence_tx.borrow() != Some(presence) {
            match self.ignition_presence_tx.send(Some(presence)) {
                Ok(()) => (),
                Err(_) => panic!("SP state-fetching task panicked"),
            }
        }
    }

    pub(super) fn fetch_now(&self) {
        match self.fetch_now_tx.try_send(()) {
            // If we succeeded or there's already a "fetch now" request sitting
            // in the channel, we're done.
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => (),
            // If the channel is closed, that means our task has panicked -
            // propogate that panic.
            Err(mpsc::error::TrySendError::Closed(())) => {
                panic!("SP state-fetching task panicked")
            }
        }
    }
}

async fn sp_fetching_task(
    id: SpIdentifier,
    tx: mpsc::Sender<FetchedSpData>,
    mut fetch_now: mpsc::Receiver<()>,
    mut ignition_presence: watch::Receiver<Option<IgnitionPresence>>,
    mgs_client: gateway_client::Client,
    log: Logger,
) {
    let mut ticker = interval(
        ignition_presence
            .borrow()
            .map_or(FETCH_FREQ_PRESENT_SP, IgnitionPresence::fetch_frequency),
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut prev_state = None;
    let mut components = None;
    let mut caboose_active = None;
    let mut caboose_inactive = None;
    let mut rot = None;

    loop {
        tokio::select! {
            _ = ticker.tick() => (),
            _ = fetch_now.recv() => {
                ticker.reset();
            }
            _ = ignition_presence.changed() => {
                // When our ignition state changes, clear out all cached data
                // and recreate `ticker`.
                prev_state = None;
                components = None;
                caboose_active = None;
                caboose_inactive = None;
                rot = None;

                ticker = interval(
                    ignition_presence
                        .borrow()
                        .map_or(
                            FETCH_FREQ_PRESENT_SP,
                            IgnitionPresence::fetch_frequency,
                        ),
                );
                ticker.set_missed_tick_behavior(
                    tokio::time::MissedTickBehavior::Delay);
                ticker.reset();
            }
        }

        let state = match mgs_client.sp_get(&id.type_, id.slot).await {
            Ok(response) => response.into_inner(),
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

        if rot.is_none() || prev_state.as_ref() != Some(&state) {
            match &state.rot {
                RotState::V2 { active, .. } => {
                    rot = Some(RotInventory {
                        active: *active,
                        caboose_a: None,
                        caboose_b: None,
                        caboose_stage0: None,
                        caboose_stage0next: None,
                    });
                }
                RotState::V3 { active, .. } => {
                    rot = Some(RotInventory {
                        active: *active,
                        caboose_a: None,
                        caboose_b: None,
                        caboose_stage0: Some(None),
                        caboose_stage0next: Some(None),
                    });
                }
                RotState::CommunicationFailed { message } => {
                    warn!(
                        log, "Failed to get RoT state from SP";
                        "message" => message,
                    );
                    rot = None;
                }
            }
        }

        // For each of our cached items that require additional MGS requests (SP
        // components, SP caboose, RoT caboose), only fetch them if either our
        // state has changed or we previously failed to fetch them after such a
        // state change.

        if prev_state.as_ref() != Some(&state) || components.is_none() {
            components =
                match mgs_client.sp_component_list(&id.type_, id.slot).await {
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

        if prev_state.as_ref() != Some(&state) || caboose_active.is_none() {
            caboose_active = match mgs_client
                .sp_component_caboose_get(
                    &id.type_,
                    id.slot,
                    SpComponent::SP_ITSELF.const_as_str(),
                    0,
                )
                .await
            {
                Ok(response) => {
                    mgs_received = Instant::now();
                    Some(response.into_inner())
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get caboose for sp (active slot)";
                        "sp" => ?id,
                        "err" => %err,
                    );
                    None
                }
            };
        }

        if prev_state.as_ref() != Some(&state) || caboose_inactive.is_none() {
            caboose_inactive = match mgs_client
                .sp_component_caboose_get(
                    &id.type_,
                    id.slot,
                    SpComponent::SP_ITSELF.const_as_str(),
                    1,
                )
                .await
            {
                Ok(response) => {
                    mgs_received = Instant::now();
                    Some(response.into_inner())
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get caboose for sp (inactive slot)";
                        "sp" => ?id,
                        "err" => %err,
                    );
                    None
                }
            };
        }

        if let Some(rot) = rot.as_mut() {
            if prev_state.as_ref() != Some(&state) || rot.caboose_a.is_none() {
                rot.caboose_a = match mgs_client
                    .sp_component_caboose_get(
                        &id.type_,
                        id.slot,
                        SpComponent::ROT.const_as_str(),
                        0,
                    )
                    .await
                {
                    Ok(response) => {
                        mgs_received = Instant::now();
                        Some(response.into_inner())
                    }
                    Err(err) => {
                        warn!(
                            log, "Failed to get RoT caboose (slot A) for sp";
                            "sp" => ?id,
                            "err" => %err,
                        );
                        None
                    }
                };
            }

            if prev_state.as_ref() != Some(&state) || rot.caboose_b.is_none() {
                rot.caboose_b = match mgs_client
                    .sp_component_caboose_get(
                        &id.type_,
                        id.slot,
                        SpComponent::ROT.const_as_str(),
                        1,
                    )
                    .await
                {
                    Ok(response) => {
                        mgs_received = Instant::now();
                        Some(response.into_inner())
                    }
                    Err(err) => {
                        warn!(
                            log, "Failed to get RoT caboose (slot B) for sp";
                            "sp" => ?id,
                            "err" => %err,
                        );
                        None
                    }
                };
            }

            if let Some(v) = &rot.caboose_stage0 {
                if prev_state.as_ref() != Some(&state) || v.is_none() {
                    rot.caboose_stage0 = match mgs_client
                        .sp_component_caboose_get(
                            &id.type_,
                            id.slot,
                            SpComponent::STAGE0.const_as_str(),
                            0,
                        )
                        .await
                    {
                        Ok(response) => {
                            mgs_received = Instant::now();
                            Some(Some(response.into_inner()))
                        }
                        Err(err) => {
                            warn!(
                                log, "Failed to get RoT caboose (stage0) for sp";
                                "sp" => ?id,
                                "err" => %err,
                            );
                            Some(None)
                        }
                    };
                }
            }

            if let Some(v) = &rot.caboose_stage0next {
                if prev_state.as_ref() != Some(&state) || v.is_none() {
                    rot.caboose_stage0next = match mgs_client
                        .sp_component_caboose_get(
                            &id.type_,
                            id.slot,
                            SpComponent::STAGE0.const_as_str(),
                            1,
                        )
                        .await
                    {
                        Ok(response) => {
                            mgs_received = Instant::now();
                            Some(Some(response.into_inner()))
                        }
                        Err(err) => {
                            warn!(
                                log, "Failed to get RoT caboose (stage0next) for sp";
                                "sp" => ?id,
                                "err" => %err,
                            );
                            Some(None)
                        }
                    };
                }
            }
        }

        let emit = FetchedSpData {
            id,
            state,
            components: components.clone(),
            caboose_active: caboose_active.clone(),
            caboose_inactive: caboose_inactive.clone(),
            rot: rot.clone(),
            mgs_received,
        };

        // If our receiver is gone, we'll exit - there's no one left for
        // us to send results to!
        if tx.send(emit).await.is_err() {
            warn!(
                log, "Receiver for SP state-fetching task is gone";
                "sp" => ?id,
            );
            break;
        }
    }
}
