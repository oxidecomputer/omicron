// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::{RackV1Inventory, SpInventory};
use futures::StreamExt;
use gateway_client::types::{SpIdentifier, SpIgnition};
use schemars::JsonSchema;
use serde::Serialize;
use slog::{info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tokio_stream::StreamMap;

use self::inventory::{
    FetchedIgnitionState, FetchedSpData, IgnitionPresence,
    IgnitionStateFetcher, SpStateFetcher,
};

mod inventory;

// This timeout feels long, but needs to be long for the case where we update
// our sidecar's SP: we won't get a respose until the SP brings back the
// management network, which often takes 15+ seconds.
const MGS_TIMEOUT: Duration = Duration::from_secs(30);

// We support:
//   * One outstanding query request from wicket
//   * One outstanding update/interaction request from wicket
//   * Room for some timeouts and re-requests from wicket.
const CHANNEL_CAPACITY: usize = 8;

#[derive(Debug)]
enum MgsRequest {
    GetInventory {
        #[allow(dead_code)]
        etag: Option<String>,
        reply_tx:
            oneshot::Sender<Result<GetInventoryResponse, GetInventoryError>>,
        force_refresh: Vec<SpIdentifier>,
    },
}

/// A mechanism for interacting with the  MgsManager
#[derive(Debug, Clone)]
pub struct MgsHandle {
    tx: tokio::sync::mpsc::Sender<MgsRequest>,
}

/// The response to a `get_inventory` call: the inventory known to wicketd, or a
/// notification that data is unavailable.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum GetInventoryResponse {
    Response { inventory: RackV1Inventory, mgs_last_seen: Duration },
    Unavailable,
}

/// Channel errors result only from system shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownInProgress;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetInventoryError {
    /// Channel errors result only from system shutdown.
    ShutdownInProgress,

    /// The client specified an invalid SP identifier in a `force_refresh`
    /// request.
    InvalidSpIdentifier,
}

impl MgsHandle {
    pub async fn get_cached_inventory(
        &self,
    ) -> Result<GetInventoryResponse, ShutdownInProgress> {
        match self.get_inventory_refreshing_sps(Vec::new()).await {
            Ok(response) => Ok(response),
            Err(GetInventoryError::ShutdownInProgress) => {
                Err(ShutdownInProgress)
            }
            Err(GetInventoryError::InvalidSpIdentifier) => {
                // We pass no SP identifiers to refresh, so it's not possible
                // for one of them to be invalid.
                unreachable!("empty SP list cannot contain an invalid ID");
            }
        }
    }

    pub async fn get_inventory_refreshing_sps(
        &self,
        force_refresh: Vec<SpIdentifier>,
    ) -> Result<GetInventoryResponse, GetInventoryError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let etag = None;
        self.tx
            .send(MgsRequest::GetInventory { etag, reply_tx, force_refresh })
            .await
            .map_err(|_| GetInventoryError::ShutdownInProgress)?;
        reply_rx.await.map_err(|_| GetInventoryError::ShutdownInProgress)?
    }
}

pub fn make_mgs_client(
    log: Logger,
    mgs_addr: SocketAddrV6,
) -> gateway_client::Client {
    // TODO-correctness Do all users of this client (including both direct API
    // calls by `UpdatePlanner` and polling by `MgsManager`) want the same
    // timeout?
    let endpoint = format!("http://[{}]:{}", mgs_addr.ip(), mgs_addr.port());
    info!(log, "MGS Endpoint: {}", endpoint);
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(MGS_TIMEOUT)
        .timeout(MGS_TIMEOUT)
        .build()
        .unwrap();

    gateway_client::Client::new_with_client(&endpoint, client, log)
}

/// The entity responsible for interacting with MGS
///
/// `MgsManager` will poll MGS periodically to update its local information,
/// and wicket will poll MGS periodically for updates. Inventory/status
/// requests (e.g. HTTP GET requests to wicketd) will be served from the
/// MgsManager and not result in a new underlying request to MGS. This keeps
/// interaction to a constant amount of work, and limits bursts to any updates
/// that require further pulling from MGS.
///
/// Update interacions from Wicket (e.g. HTTP POST/PUT requests to wicketd)
/// will be forwarded one at a time to MGS. We only allow one outstanding
/// update at a time. If the update request is to actually update software,
/// and there is a long running update in progress, the request will not
/// be forwarded and a relevant response will be returned to wicket.
pub struct MgsManager {
    log: Logger,
    tx: mpsc::Sender<MgsRequest>,
    rx: mpsc::Receiver<MgsRequest>,
    mgs_client: gateway_client::Client,
    inventory: BTreeMap<SpIdentifier, SpInventory>,

    // Our clients can request an immediate refresh for a particular set of SPs.
    // When they do, we don't want to reply to them until we get updates for
    // those SPs, so we hold the set of reply channels along with the list of
    // SPs they're waiting for in this vector. We expect this vec to be small
    // (almost always empty); whenever we get an update for an SP, we scan this
    // vec and check if any reply channels can now be satisfied.
    waiting_for_update: Vec<WaitingForRefresh>,
}

impl MgsManager {
    pub fn new(log: &Logger, mgs_addr: SocketAddrV6) -> MgsManager {
        let log = log.new(o!("component" => "wicketd MgsManager"));
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let mgs_client = make_mgs_client(log.clone(), mgs_addr);
        MgsManager {
            log,
            tx,
            rx,
            mgs_client,
            inventory: BTreeMap::new(),
            waiting_for_update: Vec::new(),
        }
    }

    pub fn get_handle(&self) -> MgsHandle {
        MgsHandle { tx: self.tx.clone() }
    }

    pub async fn run(mut self) {
        // First, wait until we get a list of SP identifiers that MGS knows how
        // to talk to. We use this to know how many (and which) SP data-fetching
        // tasks to create. This does not induce any management network traffic,
        // and if we can't get a response from this endpoint we're not going to
        // get responses from any other endpoint anyway, so it's fine to wait
        // until this succeeds.
        let all_sp_ids = loop {
            match self.mgs_client.sp_all_ids().await {
                Ok(response) => break response.into_inner(),
                Err(err) => {
                    const RETRY_INTERVAL: Duration = Duration::from_secs(1);
                    warn!(
                        self.log,
                        "Failed to get SP IDs from MGS (will retry after {:?})",
                        RETRY_INTERVAL;
                        "err" => %err,
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        };

        // We just fetched all SP IDs from MGS. We'll update this timestamp
        // below as we get results from the tasks we're about to spawn.
        let mut last_successful_mgs_response = Instant::now();

        let mut ignition_task_handle = IgnitionStateFetcher::spawn(
            self.mgs_client.clone(),
            self.log.clone(),
        );

        // We build two maps, both with the same keys (i.e., all SP IDs MGS
        // knows about): one map to the task handles, and the other is a
        // `StreamMap` that allows us to poll the merged receiver streams
        // concurrently.
        let mut sp_task_handles = BTreeMap::new();
        let mut sp_data_streams = StreamMap::with_capacity(all_sp_ids.len());
        for id in all_sp_ids {
            let (handle, stream) = SpStateFetcher::spawn(
                id,
                self.mgs_client.clone(),
                self.log.clone(),
            );
            sp_task_handles.insert(id, handle);
            sp_data_streams.insert(id, stream);
        }

        loop {
            tokio::select! {
                ignition = ignition_task_handle.recv() => {
                    last_successful_mgs_response =
                        last_successful_mgs_response.max(ignition.mgs_received);
                    self.update_inventory_with_ignition(
                        ignition,
                        &sp_task_handles,
                        last_successful_mgs_response,
                    );
                }

                Some((_, sp)) = sp_data_streams.next() => {
                    last_successful_mgs_response =
                        last_successful_mgs_response.max(sp.mgs_received);
                    self.update_inventory_with_sp(
                        sp,
                        last_successful_mgs_response,
                    );
                }

                Some(request) = self.rx.recv() => {
                    match request {
                        MgsRequest::GetInventory { reply_tx, force_refresh, .. } => {
                            self.handle_get_inventory_request(
                                &ignition_task_handle,
                                &sp_task_handles,
                                last_successful_mgs_response,
                                reply_tx,
                                force_refresh,
                            );
                        }
                    }
                }
            }
        }
    }

    fn current_inventory(
        &self,
        mgs_last_seen: Instant,
    ) -> GetInventoryResponse {
        if self.inventory.is_empty() {
            GetInventoryResponse::Unavailable
        } else {
            let inventory = RackV1Inventory {
                sps: self.inventory.values().cloned().collect(),
            };

            GetInventoryResponse::Response {
                inventory,
                mgs_last_seen: mgs_last_seen.elapsed(),
            }
        }
    }

    fn check_completed_waiters(&mut self, mgs_last_seen: Instant) {
        // This really wants `Vec::drain_filter()`, but it's unstable; instead,
        // use its sample code (but use `swap_remove()` instead of `remove()`
        // because we don't care about order).
        let mut i = 0;
        while i < self.waiting_for_update.len() {
            if self.waiting_for_update[i].sps_to_refresh.is_empty()
                && !self.waiting_for_update[i].need_ignition_refresh
            {
                let waiter = self.waiting_for_update.swap_remove(i);
                _ = waiter
                    .reply_tx
                    .send(Ok(self.current_inventory(mgs_last_seen)));
            } else {
                i += 1;
            }
        }
    }

    fn handle_get_inventory_request(
        &mut self,
        ignition_handle: &IgnitionStateFetcher,
        sp_handles: &BTreeMap<SpIdentifier, SpStateFetcher>,
        mgs_last_seen: Instant,
        reply_tx: oneshot::Sender<
            Result<GetInventoryResponse, GetInventoryError>,
        >,
        force_refresh: Vec<SpIdentifier>,
    ) {
        if force_refresh.is_empty() {
            // No force refresh: just return our latest cached inventory.
            _ = reply_tx.send(Ok(self.current_inventory(mgs_last_seen)));
            return;
        }

        // Trigger immediate refreshes for all SPs listed in `force_refresh`.
        for &id in &force_refresh {
            let Some(handle) = sp_handles.get(&id) else {
                _ = reply_tx.send(Err(GetInventoryError::InvalidSpIdentifier));
                return;
            };

            handle.fetch_now();
        }

        // Also fetch new data from ignition for any force refresh request; this
        // is only called once and covers all SPs simultaneously.
        ignition_handle.fetch_now();

        // We don't want to respond on `reply_tx` until we get a response to the
        // requests we just triggered, so push `reply_tx` onto our queue of
        // waiters. We'll respond as soon as we get updates for all SPs listen
        // in `force_refresh` (which should come soon since we just told their
        // tasks to refresh ASAP).
        self.waiting_for_update.push(WaitingForRefresh {
            reply_tx,
            sps_to_refresh: force_refresh.into_iter().collect(),
            need_ignition_refresh: true,
        });
    }

    fn update_inventory_with_ignition(
        &mut self,
        ignition: FetchedIgnitionState,
        sp_handles: &BTreeMap<SpIdentifier, SpStateFetcher>,
        mgs_last_seen: Instant,
    ) {
        for (id, ignition) in ignition.sps {
            let entry = self
                .inventory
                .entry(id)
                .or_insert_with(|| SpInventory::new(id));

            // Update our handle with the current ignition state so it can
            // (potentially) adjust its polling frequency.
            if let Some(sp_handle) = sp_handles.get(&id) {
                match &ignition {
                    SpIgnition::No => {
                        sp_handle
                            .set_ignition_presence(IgnitionPresence::Absent);
                    }
                    SpIgnition::Yes { .. } => {
                        sp_handle
                            .set_ignition_presence(IgnitionPresence::Present);
                    }
                }
            }

            entry.ignition = Some(ignition);
        }

        // Scan any pending waiters and clear their "waiting for ignition" bit;
        // if that was the last thing they needed, `check_completed_waiters()`
        // will send them a response.
        for waiting in &mut self.waiting_for_update {
            waiting.need_ignition_refresh = false;
        }
        self.check_completed_waiters(mgs_last_seen);
    }

    fn update_inventory_with_sp(
        &mut self,
        sp: FetchedSpData,
        mgs_last_seen: Instant,
    ) {
        let entry = self
            .inventory
            .entry(sp.id)
            .or_insert_with(|| SpInventory::new(sp.id));
        entry.state = Some(sp.state);
        entry.components = sp.components;
        entry.caboose_active = sp.caboose_active;
        entry.caboose_inactive = sp.caboose_inactive;
        entry.rot = sp.rot;

        // Scan any pending waiters and remove this SP from their list; if that
        // was the last thing they needed, `check_completed_waiters()` will send
        // them a response.
        for waiting in &mut self.waiting_for_update {
            waiting.sps_to_refresh.remove(&sp.id);
        }
        self.check_completed_waiters(mgs_last_seen);
    }
}

struct WaitingForRefresh {
    reply_tx: oneshot::Sender<Result<GetInventoryResponse, GetInventoryError>>,
    sps_to_refresh: BTreeSet<SpIdentifier>,
    need_ignition_refresh: bool,
}
