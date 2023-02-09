// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::http_entrypoints::{
    ComponentUpdateRunningState, ComponentUpdateRunningStatus,
    ComponentUpdateTerminalState, ComponentUpdateTerminalStatus,
    SpComponentIdentifier, UpdateStatusAll,
};
use crate::{RackV1Inventory, SpInventory};
use buf_list::BufList;
use dropshot::HttpError;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use gateway_client::types::{
    SpComponentList, SpIdentifier, SpInfo, SpType, SpUpdateStatus, UpdateBody,
};
use gateway_messages::SpComponent;
use omicron_common::api::internal::nexus::{
    UpdateArtifactId, UpdateArtifactKind,
};
use schemars::JsonSchema;
use serde::Serialize;
use slog::{info, o, warn, Logger};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

const MGS_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MGS_TIMEOUT: Duration = Duration::from_secs(10);

// We support:
//   * One outstanding query request from wicket
//   * One outstanding update/interaction request from wicket
//   * Room for some timeouts and re-requests from wicket.
const CHANNEL_CAPACITY: usize = 8;

/// Channel errors result only from system shutdown. We have to report them to
/// satisfy function calls, so we define an error type here.
#[derive(Debug, PartialEq, Eq)]
pub struct ShutdownInProgress;

impl From<ShutdownInProgress> for HttpError {
    fn from(_: ShutdownInProgress) -> Self {
        HttpError::for_unavail(None, "Server is shutting down".into())
    }
}

/// Possible errors from attempting to start a component update.
#[derive(Debug, Error)]
pub enum StartComponentUpdateError {
    #[error("server is shutting down")]
    ShutdownInProgress,
    #[error("cannot apply updates of kind {0:?}")]
    CannotApplyUpdatesOfKind(UpdateArtifactKind),
    #[error("artifact kind {kind:?} requires target of type {required:?}, but target is of type {target:?}")]
    CannotApplyUpdateToTargetType {
        kind: UpdateArtifactKind,
        required: SpType,
        target: SpType,
    },
    #[error("the target SP is busy applying an update")]
    TargetSpBusy,
}

impl From<StartComponentUpdateError> for HttpError {
    fn from(err: StartComponentUpdateError) -> Self {
        match err {
            StartComponentUpdateError::ShutdownInProgress
            | StartComponentUpdateError::TargetSpBusy => {
                Self::for_unavail(None, err.to_string())
            }
            StartComponentUpdateError::CannotApplyUpdatesOfKind(_)
            | StartComponentUpdateError::CannotApplyUpdateToTargetType {
                ..
            } => Self::for_bad_request(None, err.to_string()),
        }
    }
}

fn map_mgs_client_error(
    err: gateway_client::Error<gateway_client::types::Error>,
) -> HttpError {
    use gateway_client::Error;

    match err {
        Error::InvalidRequest(message) => {
            HttpError::for_bad_request(None, message)
        }
        Error::CommunicationError(err) | Error::InvalidResponsePayload(err) => {
            HttpError::for_internal_error(err.to_string())
        }
        Error::UnexpectedResponse(response) => HttpError::for_internal_error(
            format!("unexpected response from MGS: {:?}", response.status()),
        ),
        // Proxy MGS's response to our caller.
        Error::ErrorResponse(response) => {
            let status_code = response.status();
            let response = response.into_inner();
            HttpError {
                status_code,
                error_code: response.error_code,
                external_message: response.message,
                internal_message: format!(
                    "error response from MGS (request_id = {})",
                    response.request_id
                ),
            }
        }
    }
}

#[derive(Debug)]
enum MgsRequest {
    GetInventory {
        etag: Option<String>,
        reply_tx: oneshot::Sender<GetInventoryResponse>,
    },
    StartComponentUpdate {
        sp: SpIdentifier,
        artifact: UpdateArtifactId,
        update_slot: u16,
        data: BufList,
        reply_tx: oneshot::Sender<Result<(), StartComponentUpdateError>>,
        completion_tx: oneshot::Sender<ComponentUpdateTerminalState>,
    },
    UpdateStatusAll {
        reply_tx: oneshot::Sender<UpdateStatusAll>,
    },
}

/// A mechanism for interacting with the  MgsManager
#[derive(Debug, Clone)]
pub struct MgsHandle {
    tx: tokio::sync::mpsc::Sender<MgsRequest>,
    mgs_client: gateway_client::Client,
}

/// The response to a `get_inventory` call: the inventory known to wicketd, or a
/// notification that data is unavailable.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum GetInventoryResponse {
    Response { inventory: RackV1Inventory, received_ago: Duration },
    Unavailable,
}

impl GetInventoryResponse {
    fn new(inventory: Option<(RackV1Inventory, Instant)>) -> Self {
        match inventory {
            Some((inventory, received_at)) => GetInventoryResponse::Response {
                inventory,
                received_ago: received_at.elapsed(),
            },
            None => GetInventoryResponse::Unavailable,
        }
    }
}

impl MgsHandle {
    pub async fn get_inventory(
        &self,
    ) -> Result<GetInventoryResponse, ShutdownInProgress> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let etag = None;
        self.tx
            .send(MgsRequest::GetInventory { etag, reply_tx })
            .await
            .map_err(|_| ShutdownInProgress)?;
        reply_rx.await.map_err(|_| ShutdownInProgress)
    }

    pub(crate) async fn start_component_update(
        &self,
        target: SpIdentifier,
        update_slot: u16,
        artifact: UpdateArtifactId,
        data: BufList,
    ) -> Result<
        oneshot::Receiver<ComponentUpdateTerminalState>,
        StartComponentUpdateError,
    > {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        self.tx
            .send(MgsRequest::StartComponentUpdate {
                sp: target,
                artifact,
                update_slot,
                data,
                reply_tx,
                completion_tx,
            })
            .await
            .map_err(|_| StartComponentUpdateError::ShutdownInProgress)?;
        reply_rx
            .await
            .map_err(|_| StartComponentUpdateError::ShutdownInProgress)
            .and_then(|res| res)?;
        Ok(completion_rx)
    }

    pub async fn update_status_all(
        &self,
    ) -> Result<UpdateStatusAll, ShutdownInProgress> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MgsRequest::UpdateStatusAll { reply_tx })
            .await
            .map_err(|_| ShutdownInProgress)?;
        reply_rx.await.map_err(|_| ShutdownInProgress)
    }

    pub async fn get_component_update_status(
        &self,
        target: SpComponentIdentifier,
    ) -> Result<SpUpdateStatus, HttpError> {
        self.mgs_client
            .sp_component_update_status(
                target.type_,
                target.slot,
                &target.component,
            )
            .await
            .map(|resp| resp.into_inner())
            .map_err(map_mgs_client_error)
    }

    pub async fn component_update_abort(
        &self,
        target: SpComponentIdentifier,
        update_id: Uuid,
    ) -> Result<(), HttpError> {
        self.mgs_client
            .sp_component_update_abort(
                target.type_,
                target.slot,
                &target.component,
                &gateway_client::types::UpdateAbortBody { id: update_id },
            )
            .await
            .map(|resp| resp.into_inner())
            .map_err(map_mgs_client_error)
    }

    pub async fn sp_reset(
        &self,
        target: SpIdentifier,
    ) -> Result<(), HttpError> {
        self.mgs_client
            .sp_reset(target.type_, target.slot)
            .await
            .map(|resp| resp.into_inner())
            .map_err(map_mgs_client_error)
    }
}

struct InFlightUpdate {
    status: ComponentUpdateStatus,
    completion_tx: oneshot::Sender<ComponentUpdateTerminalState>,
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
    // The Instant indicates the moment at which the inventory was received.
    inventory: Option<(RackV1Inventory, Instant)>,
    // Any given SP can be in the process of applying at most one update
    // concurrently; this hash map records that one update (if it exists).
    inflight_updates: BTreeMap<SpIdentifier, InFlightUpdate>,
    // TODO-correctness finish commenting why this thing is bad
    complete_updates: BTreeMap<SpIdentifier, Vec<ComponentUpdateStatus>>,
}

impl MgsManager {
    pub fn new(log: &Logger, mgs_addr: SocketAddrV6) -> MgsManager {
        let log = log.new(o!("component" => "wicketd MgsManager"));
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let endpoint =
            format!("http://[{}]:{}", mgs_addr.ip(), mgs_addr.port());
        info!(log, "MGS Endpoint: {}", endpoint);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(MGS_TIMEOUT)
            .timeout(MGS_TIMEOUT)
            .build()
            .unwrap();

        let mgs_client = gateway_client::Client::new_with_client(
            &endpoint,
            client,
            log.clone(),
        );
        MgsManager {
            log,
            tx,
            rx,
            mgs_client,
            inventory: None,
            inflight_updates: BTreeMap::new(),
            complete_updates: BTreeMap::new(),
        }
    }

    pub fn get_handle(&self) -> MgsHandle {
        MgsHandle { tx: self.tx.clone(), mgs_client: self.mgs_client.clone() }
    }

    pub async fn run(mut self) {
        let mut inventory_rx =
            poll_sps(&self.log, self.mgs_client.clone()).await;

        // Create a channel on which any SP update progress messages can be
        // delivered; we set the size to 36 to leave a slot for every SP,
        // although for the time being we really expect this to have at most one
        // element in it (since we plan to issue updates sequentially).
        //
        // Any time we spawn an updating task, we'll give it a clone of
        // `update_state_tx`.
        let (update_state_tx, mut update_state_rx) = mpsc::channel(36);

        loop {
            tokio::select! {
                // Poll MGS inventory
                Some(PollSps { changed_inventory, mgs_received }) = inventory_rx.recv() => {
                    let inventory = match (changed_inventory, self.inventory.take()) {
                        (Some(inventory), _) => inventory,
                        (None, Some((inventory, _))) => inventory,
                        (None, None) => continue,
                    };
                    self.inventory = Some((inventory, mgs_received));
                }

                // Handle update progress messages from `DeliverComponentUpdate`
                // tasks we've spawned.
                Some(new_state) = update_state_rx.recv() => {
                    self.handle_new_inflight_update_status(new_state);
                }

                // Handle requests from clients
                Some(request) = self.rx.recv() => {
                    match request {
                        MgsRequest::GetInventory {reply_tx, ..} => {
                            let response = GetInventoryResponse::new(self.inventory.clone());
                            let _ = reply_tx.send(response);
                        }
                        MgsRequest::StartComponentUpdate {
                            sp,
                            artifact,
                            update_slot,
                            data,
                            reply_tx,
                            completion_tx,
                        } => {
                            self.start_component_update(
                                sp,
                                artifact,
                                update_slot,
                                data,
                                reply_tx,
                                completion_tx,
                                &update_state_tx,
                            );
                        }
                        MgsRequest::UpdateStatusAll { reply_tx } => {
                            _ = reply_tx.send(self.update_status_all());
                        }
                    }
                }
            }
        }
    }

    fn start_component_update(
        &mut self,
        sp: SpIdentifier,
        artifact: UpdateArtifactId,
        update_slot: u16,
        data: BufList,
        reply_tx: oneshot::Sender<Result<(), StartComponentUpdateError>>,
        completion_tx: oneshot::Sender<ComponentUpdateTerminalState>,
        update_state_tx: &mpsc::Sender<ComponentUpdateStatus>,
    ) {
        // Map the artifact kind into the expected target SP type and the MGS
        // component.
        let (expected_type, component) = match artifact.kind {
            UpdateArtifactKind::GimletSp => {
                (SpType::Sled, SpComponent::SP_ITSELF.const_as_str())
            }
            UpdateArtifactKind::PscSp => {
                (SpType::Power, SpComponent::SP_ITSELF.const_as_str())
            }
            UpdateArtifactKind::SwitchSp => {
                (SpType::Switch, SpComponent::SP_ITSELF.const_as_str())
            }
            UpdateArtifactKind::GimletRot => {
                (SpType::Sled, SpComponent::ROT.const_as_str())
            }
            UpdateArtifactKind::PscRot => {
                (SpType::Power, SpComponent::ROT.const_as_str())
            }
            UpdateArtifactKind::SwitchRot => {
                (SpType::Switch, SpComponent::ROT.const_as_str())
            }
            UpdateArtifactKind::HostPhase1 => {
                (SpType::Sled, SpComponent::HOST_CPU_BOOT_FLASH.const_as_str())
            }
            UpdateArtifactKind::HostPhase2
            | UpdateArtifactKind::ControlPlane => {
                let _ = reply_tx.send(Err(
                    StartComponentUpdateError::CannotApplyUpdatesOfKind(
                        artifact.kind,
                    ),
                ));
                return;
            }
        };

        if expected_type != sp.type_ {
            let _ = reply_tx.send(Err(
                StartComponentUpdateError::CannotApplyUpdateToTargetType {
                    kind: artifact.kind,
                    required: expected_type,
                    target: sp.type_,
                },
            ));
            return;
        }

        // Reject updates going to this SP if it's currently processing an
        // update.
        let slot = match self.inflight_updates.entry(sp) {
            Entry::Vacant(slot) => slot,
            Entry::Occupied(_) => {
                let _ =
                    reply_tx.send(Err(StartComponentUpdateError::TargetSpBusy));
                return;
            }
        };

        let update_id = Uuid::new_v4();
        let initial_status = ComponentUpdateStatus {
            sp,
            artifact: artifact.clone(),
            update_id,
            state: ComponentUpdateState::Running(
                ComponentUpdateRunningState::IssuingRequestToMgs,
            ),
        };

        let updater = DeliverComponentUpdate {
            mgs_client: self.mgs_client.clone(),
            sp,
            artifact,
            update_id,
            component: component.to_string(),
            update_slot,
            update_state_tx: update_state_tx.clone(),
        };

        slot.insert(InFlightUpdate { status: initial_status, completion_tx });
        updater.start(data);
        let _ = reply_tx.send(Ok(()));
    }

    fn handle_new_inflight_update_status(
        &mut self,
        status: ComponentUpdateStatus,
    ) {
        match &status.state {
            ComponentUpdateState::Running(_) => {
                let sp = status.sp;
                self.inflight_updates
                    .get_mut(&sp)
                    .expect("missing inflight update for SP")
                    .status = status;
            }

            ComponentUpdateState::Terminal(state) => {
                let update = self
                    .inflight_updates
                    .remove(&status.sp)
                    .expect("missing inflight update for SP");
                _ = update.completion_tx.send(state.clone());
                self.complete_updates
                    .entry(status.sp)
                    .or_default()
                    .push(status);
            }
        }
    }

    fn update_status_all(&self) -> UpdateStatusAll {
        let mut in_flight = BTreeMap::new();
        for (sp, update) in &self.inflight_updates {
            let status = match &update.status.state {
                ComponentUpdateState::Running(state) => {
                    ComponentUpdateRunningStatus {
                        sp: update.status.sp,
                        artifact: update.status.artifact.clone(),
                        update_id: update.status.update_id,
                        state: state.clone(),
                    }
                }
                ComponentUpdateState::Terminal(_) => {
                    unreachable!("inflight_updates contains terminal state")
                }
            };

            let inner: &mut BTreeMap<_, _> =
                in_flight.entry(sp.type_).or_default();
            inner.insert(sp.slot, status);
        }

        let mut completed = BTreeMap::new();
        for (sp, statuses) in &self.complete_updates {
            let statuses = statuses
                .iter()
                .map(|status| match &status.state {
                    ComponentUpdateState::Terminal(state) => {
                        ComponentUpdateTerminalStatus {
                            sp: status.sp,
                            artifact: status.artifact.clone(),
                            update_id: status.update_id,
                            state: state.clone(),
                        }
                    }
                    ComponentUpdateState::Running(_) => {
                        unreachable!(
                            "complete_updates contains non-terminal state"
                        )
                    }
                })
                .collect();

            let inner: &mut BTreeMap<_, _> =
                completed.entry(sp.type_).or_default();
            inner.insert(sp.slot, statuses);
        }

        UpdateStatusAll { in_flight, completed }
    }
}

type InventoryMap = BTreeMap<SpIdentifier, SpInventory>;

// For the latest set of sps returned from MGS:
//  1. Update their state if it has changed
//  2. Remove any SPs in our current inventory that aren't in the new state
//
// Return `true` if inventory was updated, `false` otherwise
async fn update_inventory(
    log: &Logger,
    inventory: &mut InventoryMap,
    sps: Vec<SpInfo>,
    client: &gateway_client::Client,
) -> bool {
    let new_keys: BTreeSet<SpIdentifier> =
        sps.iter().map(|sp| sp.info.id).collect();

    let old_inventory_len = inventory.len();

    // Remove all keys that are not in the latest update
    inventory.retain(|k, _| new_keys.contains(k));

    // Did we remove any keys?
    let mut inventory_changed = inventory.len() != old_inventory_len;

    // Update any existing SPs that have changed state or add any new ones. For
    // each of these, keep track so we can fetch their ComponentInfo.
    let mut to_fetch: Vec<SpIdentifier> = vec![];
    for sp in sps.into_iter() {
        let state = sp.details;
        let id: SpIdentifier = sp.info.id;
        let ignition = sp.info.details;

        inventory
            .entry(id)
            .and_modify(|curr| {
                if curr.state != state || curr.components.is_none() {
                    to_fetch.push(id);
                }
                curr.state = state.clone();
                curr.ignition = ignition.clone();
            })
            .or_insert_with(|| {
                to_fetch.push(id);
                SpInventory::new(id, ignition, state)
            });
    }

    // Create futures to fetch `SpComponentInfo` for each SP concurrently
    let component_stream = to_fetch
        .into_iter()
        .map(|id| async move {
            let client = client.clone();
            (id, client.sp_component_list(id.type_, id.slot).await)
        })
        .collect::<FuturesUnordered<_>>();

    // Execute the futures
    let responses: BTreeMap<SpIdentifier, SpComponentList> = component_stream
        .filter_map(|(id, res)| async move {
            match res {
                Ok(val) => Some((id, val.into_inner())),
                Err(err) => {
                    warn!(
                        log,
                        "Failed to get component list for sp: {id:?}, {err})"
                    );
                    None
                }
            }
        })
        .collect()
        .await;

    if !responses.is_empty() {
        inventory_changed = true;
    }

    // Fill in the components for each given SpIdentifier
    for (id, sp_component_list) in responses {
        inventory.get_mut(&id).unwrap().components =
            Some(sp_component_list.components);
    }

    inventory_changed
}

async fn poll_sps(
    log: &Logger,
    client: gateway_client::Client,
) -> mpsc::Receiver<PollSps> {
    let log = log.clone();

    // We only want one outstanding inventory request at a time
    let (tx, rx) = mpsc::channel(1);

    // This is a BTreeMap version of inventory that we maintain for the lifetime
    // of the process.
    let mut inventory = InventoryMap::default();

    tokio::spawn(async move {
        let mut ticker = interval(MGS_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match client.sp_list().await {
                Ok(val) => {
                    let changed_inventory = update_inventory(
                        &log,
                        &mut inventory,
                        val.into_inner(),
                        &client,
                    )
                    .await
                    .then(|| RackV1Inventory {
                        sps: inventory.values().cloned().collect(),
                    });

                    let _ = tx
                        .send(PollSps {
                            changed_inventory,
                            mgs_received: Instant::now(),
                        })
                        .await;
                }
                Err(e) => {
                    warn!(log, "{e}");
                }
            }
        }
    });

    rx
}

#[derive(Debug)]
struct PollSps {
    changed_inventory: Option<RackV1Inventory>,
    mgs_received: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct ComponentUpdateStatus {
    pub(crate) sp: SpIdentifier,
    pub(crate) artifact: UpdateArtifactId,
    pub(crate) update_id: Uuid,
    pub(crate) state: ComponentUpdateState,
}

#[derive(Debug, Clone)]
pub(crate) enum ComponentUpdateState {
    Running(ComponentUpdateRunningState),
    Terminal(ComponentUpdateTerminalState),
}

#[derive(Debug)]
struct DeliverComponentUpdate {
    mgs_client: gateway_client::Client,
    sp: SpIdentifier,
    artifact: UpdateArtifactId,
    update_id: Uuid,
    component: String,
    update_slot: u16,
    update_state_tx: mpsc::Sender<ComponentUpdateStatus>,
}

impl DeliverComponentUpdate {
    fn start(self, data: BufList) {
        // Spawn a task and intentionally do not keep a handle to it: it's
        // important that this task is not cancelled (and important that it not
        // panic, which we'll catch forcefully below).
        tokio::spawn(async move {
            let sp = self.sp;
            let artifact = self.artifact.clone();
            let update_id = self.update_id;
            let update_state_tx = self.update_state_tx.clone();

            // Spawn an inner task to do the work: this may deliver non-terminal
            // state updates on `update_state_tx`, but we will always be the one
            // to deliver a terminal state when it completes.
            let inner_task = tokio::spawn(self.run_impl(data));

            let terminal_state = match inner_task.await {
                Ok(Ok(state)) => state,
                Ok(Err(err)) => ComponentUpdateTerminalState::Failed {
                    reason: format!("request to MGS failed: {err}"),
                },
                Err(err) => {
                    // We never cancel `inner_task`, so the only way it we
                    // should fail to await it is if it panics.
                    assert!(
                        err.is_panic(),
                        "update task died without panicking"
                    );
                    ComponentUpdateTerminalState::UpdateTaskPanicked
                }
            };

            // Send the terminal state to `MgsManager` so it knows this update
            // has finished.
            _ = update_state_tx
                .send(ComponentUpdateStatus {
                    sp,
                    artifact,
                    update_id,
                    state: ComponentUpdateState::Terminal(terminal_state),
                })
                .await;
        });
    }

    async fn run_impl(
        self,
        data: BufList,
    ) -> Result<ComponentUpdateTerminalState, GatewayClientError> {
        // How often we poll MGS for the progress of an update once it starts.
        const STATUS_POLL_FREQ: Duration = Duration::from_millis(300);

        // TODO Convert a BufList into a Vec<u8> - this is a little gross.
        // Should MGS's endpoint accept a BufList somehow? Should the artifact
        // store give us something more amenable to conversion?
        let mut image = Vec::with_capacity(data.num_bytes());
        for chunk in data {
            image.extend_from_slice(&*chunk);
        }

        let request =
            UpdateBody { id: self.update_id, image, slot: self.update_slot };

        self.mgs_client
            .sp_component_update(
                self.sp.type_,
                self.sp.slot,
                &self.component,
                &request,
            )
            .await?;
        self.send_state_update(ComponentUpdateRunningState::WaitingForStatus)
            .await;

        loop {
            let status = self
                .mgs_client
                .sp_component_update_status(
                    self.sp.type_,
                    self.sp.slot,
                    &self.component,
                )
                .await?
                .into_inner();

            let new_state = match status {
                // Happy path, non-terminal states
                SpUpdateStatus::Preparing { id, progress } => {
                    if id == self.update_id {
                        ComponentUpdateRunningState::Preparing { progress }
                    } else {
                        return Ok(ComponentUpdateTerminalState::Failed {
                            reason: format!(
                                "unexpected update preparing (ID: {id})"
                            ),
                        });
                    }
                }
                SpUpdateStatus::InProgress {
                    bytes_received,
                    id,
                    total_bytes,
                } => {
                    if id == self.update_id {
                        ComponentUpdateRunningState::InProgress {
                            bytes_received,
                            total_bytes,
                        }
                    } else {
                        return Ok(ComponentUpdateTerminalState::Failed {
                            reason: format!(
                                "unexpected update in progress (ID: {id})"
                            ),
                        });
                    }
                }

                // Happy path terminal state
                SpUpdateStatus::Complete { id } => {
                    if id == self.update_id {
                        return Ok(ComponentUpdateTerminalState::Complete);
                    } else {
                        return Ok(ComponentUpdateTerminalState::Failed {
                            reason: format!(
                                "unexpected update completed (ID: {id})"
                            ),
                        });
                    }
                }

                // Non-happy path states (all terminal)
                SpUpdateStatus::None => {
                    return Ok(ComponentUpdateTerminalState::Failed {
                        reason: "update no longer in progress (SP reset?)"
                            .to_string(),
                    });
                }
                SpUpdateStatus::Aborted { id } => {
                    let state = if id == self.update_id {
                        ComponentUpdateTerminalState::Failed {
                            reason: "update aborted".to_string(),
                        }
                    } else {
                        ComponentUpdateTerminalState::Failed {
                            reason: format!(
                                "unexpected update aborted (ID: {id})"
                            ),
                        }
                    };
                    return Ok(state);
                }
                SpUpdateStatus::Failed { code, id } => {
                    let state = if id == self.update_id {
                        ComponentUpdateTerminalState::Failed {
                            reason: format!("update failed (code {code})"),
                        }
                    } else {
                        ComponentUpdateTerminalState::Failed {
                            reason: format!(
                                "unexpected update failed (ID: {id}, code {code})"
                            ),
                        }
                    };
                    return Ok(state);
                }
            };

            self.send_state_update(new_state).await;
            tokio::time::sleep(STATUS_POLL_FREQ).await;
        }
    }

    // Helper method called by `run_impl` to send intermediate update progress
    // messages.
    async fn send_state_update(&self, new_state: ComponentUpdateRunningState) {
        let status = ComponentUpdateStatus {
            sp: self.sp,
            artifact: self.artifact.clone(),
            update_id: self.update_id,
            state: ComponentUpdateState::Running(new_state),
        };
        _ = self.update_state_tx.send(status).await;
    }
}
