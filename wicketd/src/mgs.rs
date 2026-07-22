// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use dropshot::HttpError;
use futures::StreamExt;
use gateway_types::ignition::SpIgnition;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use slog::{Logger, info, o, warn};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tokio_stream::StreamMap;
use wicket_common::inventory::{MgsV1Inventory, SpIdentifier, SpInventory};

use crate::helpers::SpIdentifierDisplay;
use crate::http_helpers::http_error_with_message;
use crate::http_helpers::shutdown_to_http;

pub(crate) use self::inventory::{FetchedSpData, MgsFetchError};
// Will be used by the commissioning API.
#[cfg_attr(not(test), expect(unused_imports))]
pub(crate) use self::inventory::{Fetched, RotData, RotFetch, Stage0Fetch};
use self::inventory::{
    FetchedIgnitionState, IgnitionPresence, IgnitionStateFetcher,
    SpFetchResult, SpStateFetcher,
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

/// Response to a request for MGS-specific inventory information.
#[derive(Debug)]
pub enum GetInventoryResponse {
    Response {
        /// Every per-SP record the manager holds.
        ///
        /// Consumers that speak the (currently frozen) unstable wicketd API
        /// project this down to `MgsV1Inventory`.
        sps: IdOrdMap<SpRecord>,
        mgs_last_seen: Duration,
    },
    Unavailable,
}

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

/// Channel errors result only from system shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownInProgress;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetInventoryError {
    /// Channel errors result only from system shutdown.
    ShutdownInProgress,

    /// The client specified an invalid SP identifier in a `force_refresh`
    /// request.
    InvalidSpIdentifier {
        /// The invalid SP identifier.
        id: SpIdentifier,
    },
}

impl GetInventoryError {
    pub(crate) fn to_http_error(&self) -> HttpError {
        match self {
            GetInventoryError::ShutdownInProgress => {
                shutdown_to_http(ShutdownInProgress)
            }
            GetInventoryError::InvalidSpIdentifier { id } => {
                http_error_with_message(
                    dropshot::ErrorStatusCode::BAD_REQUEST,
                    None,
                    format!(
                        "invalid SP identifier in force_refresh request: {}",
                        SpIdentifierDisplay(*id)
                    ),
                )
            }
        }
    }
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
            Err(GetInventoryError::InvalidSpIdentifier { id }) => {
                // We pass no SP identifiers to refresh, so it's not possible
                // for one of them to be invalid.
                unreachable!(
                    "empty SP list cannot contain an invalid ID, but got {id:?}"
                );
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
    records: IdOrdMap<SpRecord>,

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
            records: IdOrdMap::new(),
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

                Some((id, result)) = sp_data_streams.next() => {
                    match result {
                        SpFetchResult::Data(data) => {
                            last_successful_mgs_response =
                                last_successful_mgs_response
                                    .max(data.mgs_received);
                            self.update_inventory_with_sp(
                                id,
                                *data,
                                last_successful_mgs_response,
                            );
                        }
                        SpFetchResult::StateFetchFailed(error) => {
                            // Do not satisfy any waiters on receiving this
                            // error. Completing a waiter on an error would hand
                            // back stale or absent state after what may be a
                            // transient failure, so we record the error and
                            // leave the waiter queued. (This retains behavioral
                            // compatibility with prior versions of this code
                            // path, in which the fetching task logged errors and
                            // never forwarded them here; we may choose to be
                            // smarter about this in the future.)
                            self.record_sp_state_fetch_error(id, error);
                        }
                    }
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
        // Inventory is available only once at least one record is populated
        // (see `SpRecord::is_populated`): a map holding nothing but error-only
        // records still reads as unavailable.
        if !self.records.iter().any(SpRecord::is_populated) {
            GetInventoryResponse::Unavailable
        } else {
            GetInventoryResponse::Response {
                sps: self.records.clone(),
                mgs_last_seen: mgs_last_seen.elapsed(),
            }
        }
    }

    /// Drop waiters whose receiver is closed (e.g., the HTTP request timed out)
    /// so they don't pile up in case a wedged SP never refreshes.
    fn prune_dead_waiters(&mut self) {
        self.waiting_for_update.retain(|waiter| !waiter.reply_tx.is_closed());
    }

    fn check_completed_waiters(&mut self, mgs_last_seen: Instant) {
        self.prune_dead_waiters();

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
        self.prune_dead_waiters();

        if force_refresh.is_empty() {
            // No force refresh: just return our latest cached inventory.
            _ = reply_tx.send(Ok(self.current_inventory(mgs_last_seen)));
            return;
        }

        // Trigger immediate refreshes for all SPs listed in `force_refresh`.
        for &id in &force_refresh {
            let Some(handle) = sp_handles.get(&id) else {
                _ = reply_tx
                    .send(Err(GetInventoryError::InvalidSpIdentifier { id }));
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
            let mut entry =
                self.records.entry(id).or_insert_with(|| SpRecord::new(id));

            // Update our handle with the current ignition state so it can
            // (potentially) adjust its polling frequency.
            if let Some(sp_handle) = sp_handles.get(&id) {
                match &ignition {
                    SpIgnition::Absent => {
                        sp_handle
                            .set_ignition_presence(IgnitionPresence::Absent);
                    }
                    SpIgnition::Present { .. } => {
                        sp_handle
                            .set_ignition_presence(IgnitionPresence::Present);
                    }
                }
            }

            // An ignition update populates `ignition` but deliberately leaves
            // `last_state_fetch_error` untouched: that field is managed by the
            // state-fetch path, which sets it on a failed state fetch and clears
            // it on a successful one.
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
        sp: SpIdentifier,
        data: FetchedSpData,
        mgs_last_seen: Instant,
    ) {
        // Scope the `record` RefMut so it is dropped before we access `&mut
        // self` again below.
        {
            let mut record =
                self.records.entry(sp).or_insert_with(|| SpRecord::new(sp));
            record.data = Some(data);

            // A successful state fetch supersedes any previously recorded
            // fetch error for this SP.
            record.last_state_fetch_error = None;
        }

        // Scan any pending waiters and remove this SP from their list; if that
        // was the last thing they needed, `check_completed_waiters()` will send
        // them a response.
        for waiting in &mut self.waiting_for_update {
            waiting.sps_to_refresh.remove(&sp);
        }
        self.check_completed_waiters(mgs_last_seen);
    }

    /// Record the latest state-fetch error for an SP.
    fn record_sp_state_fetch_error(
        &mut self,
        sp: SpIdentifier,
        error: MgsFetchError,
    ) {
        let mut record =
            self.records.entry(sp).or_insert_with(|| SpRecord::new(sp));
        // We do not reset `data` back to `None` here -- it represents the
        // last successful fetch.
        record.last_state_fetch_error = Some(error);
    }
}

/// A single SP's record in the manager's inventory map.
#[derive(Clone, Debug)]
pub struct SpRecord {
    pub id: SpIdentifier,
    pub ignition: Option<SpIgnition>,

    /// Data from the most recent successful fetch.
    pub(crate) data: Option<FetchedSpData>,

    /// The most recent state-fetch error for this SP, or `None` if the last
    /// state fetch succeeded (or fetching hasn't failed yet).
    last_state_fetch_error: Option<MgsFetchError>,
}

impl SpRecord {
    fn new(id: SpIdentifier) -> Self {
        Self { id, ignition: None, data: None, last_state_fetch_error: None }
    }

    /// Returns true if any fetched information (ignition or SP state data) has
    /// been recorded for this SP.
    fn is_populated(&self) -> bool {
        self.ignition.is_some() || self.data.is_some()
    }

    /// Project this record into the frozen `SpInventory` wire type.
    fn to_sp_inventory(&self) -> SpInventory {
        let (state, components, caboose_active, caboose_inactive, rot) =
            match &self.data {
                Some(FetchedSpData {
                    state,
                    components,
                    caboose_active,
                    caboose_inactive,
                    rot,
                    mgs_received: _,
                }) => (
                    Some(state.clone()),
                    components.to_option(),
                    caboose_active.to_option(),
                    caboose_inactive.to_option(),
                    rot.to_rot_inventory(),
                ),
                None => (None, None, None, None, None),
            };
        SpInventory {
            id: self.id,
            ignition: self.ignition.clone(),
            state,
            components,
            caboose_active,
            caboose_inactive,
            rot,
        }
    }
}

impl IdOrdItem for SpRecord {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

/// Project the per-SP records into the frozen `MgsV1Inventory` wire type.
pub(crate) fn records_to_mgs_inventory(
    records: &IdOrdMap<SpRecord>,
) -> MgsV1Inventory {
    let sps = records
        .iter()
        .filter(|record| record.is_populated())
        .map(SpRecord::to_sp_inventory)
        .collect();
    MgsV1Inventory { sps }
}

struct WaitingForRefresh {
    reply_tx: oneshot::Sender<Result<GetInventoryResponse, GetInventoryError>>,
    sps_to_refresh: BTreeSet<SpIdentifier>,
    need_ignition_refresh: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway_types::component::{PowerState, SpState};
    use gateway_types::rot::RotState;
    use std::net::Ipv6Addr;
    use wicket_common::inventory::{
        RotSlot, SpComponentCaboose, SpComponentInfo, SpType,
    };

    fn dummy_manager() -> MgsManager {
        let log = Logger::root(slog::Discard, o!());
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
        MgsManager::new(&log, addr)
    }

    fn sled(slot: u16) -> SpIdentifier {
        SpIdentifier { typ: SpType::Sled, slot }
    }

    fn sp_state(slot: u16) -> SpState {
        SpState {
            serial_number: format!("serial-{slot}"),
            model: "model".to_string(),
            revision: 0,
            hubris_archive_id: "archive".to_string(),
            base_mac_address: [0; 6],
            power_state: PowerState::A0,
            rot: RotState::CommunicationFailed {
                message: "rot is unhappy".to_string(),
            },
        }
    }

    fn caboose(version: &str) -> SpComponentCaboose {
        SpComponentCaboose {
            git_commit: "commit".to_string(),
            board: "board".to_string(),
            name: "name".to_string(),
            version: version.to_string(),
            sign: None,
            epoch: None,
        }
    }

    fn fetched_sp_data(id: SpIdentifier) -> FetchedSpData {
        FetchedSpData {
            state: sp_state(id.slot),
            components: Fetched::NotRead,
            caboose_active: Fetched::NotRead,
            caboose_inactive: Fetched::NotRead,
            rot: RotFetch::CommunicationFailed {
                message: "rot is unhappy".to_string(),
            },
            mgs_received: Instant::now(),
        }
    }

    fn record_with_data(
        id: SpIdentifier,
        components: Fetched<Vec<SpComponentInfo>>,
        rot: RotFetch,
    ) -> SpRecord {
        SpRecord {
            id,
            ignition: None,
            data: Some(FetchedSpData {
                state: sp_state(id.slot),
                components,
                caboose_active: Fetched::NotRead,
                caboose_inactive: Fetched::NotRead,
                rot,
                mgs_received: Instant::now(),
            }),
            last_state_fetch_error: None,
        }
    }

    fn mgs_fetch_error(message: &str) -> MgsFetchError {
        MgsFetchError {
            message: message.to_string(),
            observed_at: Instant::now(),
        }
    }

    /// Fetch the current inventory and unwrap it as available, panicking if the
    /// manager still reports `Unavailable`.
    fn expect_inventory(
        manager: &MgsManager,
        now: Instant,
    ) -> IdOrdMap<SpRecord> {
        let GetInventoryResponse::Response { sps, .. } =
            manager.current_inventory(now)
        else {
            panic!("expected inventory to be available");
        };
        sps
    }

    /// Return the recorded state-fetch error message for `id`, or `None` if the
    /// record carries no error.
    ///
    /// Panics if no record exists for `id` at all (it should always be
    /// populated).
    fn sp_fetch_error_message(
        sps: &IdOrdMap<SpRecord>,
        id: SpIdentifier,
    ) -> Option<&str> {
        sps.get(&id)
            .expect("record exists for the SP")
            .last_state_fetch_error
            .as_ref()
            .map(|error| error.message.as_str())
    }

    #[test]
    fn sp_state_fetch_error_lifecycle() {
        let mut manager = dummy_manager();
        let now = Instant::now();

        // (a) A failed fetch is recorded via the same method `run()` dispatches
        // to. It creates a record for the SP but does not by itself make
        // inventory available.
        manager.record_sp_state_fetch_error(
            sled(0),
            mgs_fetch_error("first failure"),
        );
        let record = manager
            .records
            .get(&sled(0))
            .expect("recording an error creates a record for the SP");
        assert!(
            !record.is_populated(),
            "an error-only record carries no fetched data: {record:?}",
        );
        match manager.current_inventory(now) {
            GetInventoryResponse::Unavailable => {}
            other => panic!("expected Unavailable, got {other:?}"),
        }

        // (b) A second error for the same SP overwrites the first; the overwrite
        // is asserted at step (c), once inventory is available.
        manager.record_sp_state_fetch_error(
            sled(0),
            mgs_fetch_error("second failure"),
        );

        // (c) Once any SP reports state, inventory becomes available. The
        // error-only record for sled 0 is excluded from the wire-visible
        // projection while sled 1's data appears; sled 0's error reflects the
        // latest failure, and the successfully-read sled 1 has no error.
        manager.update_inventory_with_sp(
            sled(1),
            fetched_sp_data(sled(1)),
            now,
        );
        let sps = expect_inventory(&manager, now);
        let inventory = records_to_mgs_inventory(&sps);
        assert!(
            inventory.sps.get(&sled(0)).is_none(),
            "the error-only record for sled 0 is excluded from inventory",
        );
        assert!(
            inventory.sps.get(&sled(1)).is_some(),
            "sled 1 has data, so it appears in inventory",
        );
        assert_eq!(
            sp_fetch_error_message(&sps, sled(0)),
            Some("second failure"),
            "the most recent error overwrites the earlier one",
        );
        assert_eq!(
            sp_fetch_error_message(&sps, sled(1)),
            None,
            "sled 1 was read successfully, so it has no error",
        );

        // (d) A later success for sled 0 clears its error.
        manager.update_inventory_with_sp(
            sled(0),
            fetched_sp_data(sled(0)),
            now,
        );
        let sps = expect_inventory(&manager, now);
        assert_eq!(
            sp_fetch_error_message(&sps, sled(0)),
            None,
            "a successful read clears the recorded error",
        );

        // (e) A fresh error after that success does not discard the data the
        // success captured.
        manager.record_sp_state_fetch_error(
            sled(0),
            mgs_fetch_error("third failure"),
        );
        let sps = expect_inventory(&manager, now);
        let sled0_record = sps.get(&sled(0)).expect("sled 0's record exists");
        assert!(
            sled0_record.data.is_some(),
            "the earlier successful data survives the later error: \
             {sled0_record:?}",
        );
        assert_eq!(
            sp_fetch_error_message(&sps, sled(0)),
            Some("third failure"),
            "the fresh error is recorded alongside the retained data",
        );
        let inventory = records_to_mgs_inventory(&sps);
        let sled0 = inventory
            .sps
            .get(&sled(0))
            .expect("sled 0's data is projected into inventory");
        let state = sled0
            .state
            .as_ref()
            .expect("sled 0's projected inventory carries SP state");
        assert_eq!(
            state.serial_number, "serial-0",
            "the projected state is the one from the successful fetch",
        );
    }

    #[test]
    fn sp_state_fetch_errors_do_not_satisfy_waiters() {
        let mut manager = dummy_manager();
        let now = Instant::now();

        // A waiter blocked on refreshes for both sled 0 and sled 1.
        let (reply_tx, mut reply_rx) = oneshot::channel();
        manager.waiting_for_update.push(WaitingForRefresh {
            reply_tx,
            sps_to_refresh: [sled(0), sled(1)].into_iter().collect(),
            need_ignition_refresh: false,
        });

        // An error for sled 0 followed by a success for sled 1 must not satisfy
        // the waiter.
        manager.record_sp_state_fetch_error(
            sled(0),
            mgs_fetch_error("mgs is unhappy"),
        );
        manager.update_inventory_with_sp(
            sled(1),
            fetched_sp_data(sled(1)),
            now,
        );
        assert_eq!(
            manager.waiting_for_update.len(),
            1,
            "the waiter stays queued while sled 0 is still outstanding",
        );
        match reply_rx.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            other => {
                panic!("waiter should have no response yet, got {other:?}")
            }
        }

        // A successful fetch for sled 0 satisfies the waiter and reports the
        // error as cleared.
        manager.update_inventory_with_sp(
            sled(0),
            fetched_sp_data(sled(0)),
            now,
        );
        assert!(
            manager.waiting_for_update.is_empty(),
            "the waiter fires once every SP has refreshed",
        );
        let GetInventoryResponse::Response { sps, .. } = reply_rx
            .try_recv()
            .expect("the waiter received a response")
            .expect("the response is not an error")
        else {
            panic!("expected inventory to be available");
        };
        assert_eq!(
            sp_fetch_error_message(&sps, sled(0)),
            None,
            "the successful fetch cleared sled 0's error in the reply",
        );
    }

    #[test]
    fn ignition_update_does_not_clear_errors() {
        let mut manager = dummy_manager();
        let now = Instant::now();

        manager.record_sp_state_fetch_error(
            sled(0),
            mgs_fetch_error("mgs is unhappy"),
        );

        // An ignition update populates data for sled 0 (so inventory becomes
        // available) but must not clear the recorded state-fetch error.
        let mut sps = BTreeMap::new();
        sps.insert(sled(0), SpIgnition::Absent);
        let ignition = FetchedIgnitionState { sps, mgs_received: now };
        manager.update_inventory_with_ignition(ignition, &BTreeMap::new(), now);

        let sps = expect_inventory(&manager, now);
        let inventory = records_to_mgs_inventory(&sps);
        assert!(
            inventory.sps.get(&sled(0)).is_some(),
            "the ignition update gave sled 0 data, so it appears in inventory",
        );
        assert_eq!(
            sp_fetch_error_message(&sps, sled(0)),
            Some("mgs is unhappy"),
            "sled 0's error survives the ignition update",
        );
    }

    #[test]
    fn sub_fetch_error_projects_to_none() {
        // A sub-fetch failure (here, the SP component list) does not have a
        // representation in the unstable API, so it is projected to `None`.
        // (But it is still tracked internally.)
        let error = MgsFetchError {
            message: "component list failed".to_string(),
            observed_at: Instant::now(),
        };
        let record = record_with_data(
            sled(0),
            Fetched::Error(error),
            RotFetch::CommunicationFailed { message: "rot".to_string() },
        );

        let inventory = record.to_sp_inventory();
        assert!(
            inventory.components.is_none(),
            "a failed component fetch collapses to None on the wire",
        );

        let Some(FetchedSpData { components: Fetched::Error(err), .. }) =
            &record.data
        else {
            panic!("the record still carries the component fetch error");
        };
        assert_eq!(err.message, "component list failed");
    }

    #[test]
    fn stage0_fetch_projection_table() {
        // * Unsupported -> None
        // * Supported(NotRead) -> Some(None)
        // * Supported(Error) -> Some(None)
        // * Supported(Read(c)) -> Some(Some(c))
        let cases: [(Stage0Fetch, Option<Option<SpComponentCaboose>>); 4] = [
            (Stage0Fetch::Unsupported, None),
            (Stage0Fetch::Supported(Fetched::NotRead), Some(None)),
            (
                Stage0Fetch::Supported(Fetched::Error(MgsFetchError {
                    message: "stage0 failed".to_string(),
                    observed_at: Instant::now(),
                })),
                Some(None),
            ),
            (
                Stage0Fetch::Supported(Fetched::Read(caboose("stage0"))),
                Some(Some(caboose("stage0"))),
            ),
        ];

        for (stage0, expected) in cases {
            let rot = RotFetch::Read(Box::new(RotData {
                active: RotSlot::A,
                caboose_a: Fetched::NotRead,
                caboose_b: Fetched::NotRead,
                stage0: stage0.clone(),
                stage0next: Stage0Fetch::Unsupported,
            }));
            let record = record_with_data(sled(0), Fetched::NotRead, rot);
            let rot_inventory = record
                .to_sp_inventory()
                .rot
                .expect("RoT data projects to Some");
            assert_eq!(
                rot_inventory.caboose_stage0, expected,
                "stage0 {stage0:?} should project to {expected:?}",
            );
        }
    }

    #[test]
    fn rot_communication_failure_projects_to_none() {
        let record = record_with_data(
            sled(0),
            Fetched::NotRead,
            RotFetch::CommunicationFailed {
                message: "rot is unreachable".to_string(),
            },
        );
        assert!(
            record.to_sp_inventory().rot.is_none(),
            "an RoT communication failure projects to rot: None",
        );
    }

    #[test]
    fn prune_dead_waiters_drops_only_closed_receivers() {
        let mut manager = dummy_manager();

        // Simulate a dead waiter with a closed receiver and a non-empty
        // sps_to_refresh.
        let (dead_tx, dead_rx) = oneshot::channel();
        drop(dead_rx);
        manager.waiting_for_update.push(WaitingForRefresh {
            reply_tx: dead_tx,
            sps_to_refresh: std::iter::once(sled(0)).collect(),
            need_ignition_refresh: true,
        });

        // Similarly, simulate a live waiter.
        let (live_tx, _live_rx) = oneshot::channel();
        manager.waiting_for_update.push(WaitingForRefresh {
            reply_tx: live_tx,
            sps_to_refresh: std::iter::once(sled(1)).collect(),
            need_ignition_refresh: true,
        });

        manager.prune_dead_waiters();

        assert_eq!(
            manager.waiting_for_update.len(),
            1,
            "only the live waiter remains after pruning",
        );
        let remaining = &manager.waiting_for_update[0];
        assert!(
            !remaining.reply_tx.is_closed(),
            "the surviving waiter still has a live receiver",
        );
        assert_eq!(
            remaining.sps_to_refresh,
            std::iter::once(sled(1)).collect::<BTreeSet<_>>(),
            "the surviving waiter is the one waiting on sled 1",
        );
    }
}
