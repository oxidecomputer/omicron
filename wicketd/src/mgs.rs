// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::http_entrypoints::{
    PostComponentUpdateResponse, SpComponentIdentifier,
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
use omicron_common::api::internal::nexus::UpdateArtifactKind;
use schemars::JsonSchema;
use serde::Serialize;
use slog::{info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use uuid::Uuid;

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

/// Possible errors from attempting to start a component update.
#[derive(Debug, Error)]
pub enum StartComponentUpdateError {
    #[error("cannot apply updates of kind {0:?}")]
    CannotApplyUpdatesOfKind(UpdateArtifactKind),
    #[error("artifact kind {kind:?} requires target of type {required:?}, but target is of type {target:?}")]
    CannotApplyUpdateToTargetType {
        kind: UpdateArtifactKind,
        required: SpType,
        target: SpType,
    },
    #[error(transparent)]
    MgsResponseError(gateway_client::Error<gateway_client::types::Error>),
}

impl From<StartComponentUpdateError> for HttpError {
    fn from(err: StartComponentUpdateError) -> Self {
        match err {
            StartComponentUpdateError::CannotApplyUpdatesOfKind(_)
            | StartComponentUpdateError::CannotApplyUpdateToTargetType {
                ..
            } => Self::for_bad_request(None, err.to_string()),
            StartComponentUpdateError::MgsResponseError(err) => {
                map_mgs_client_error(err)
            }
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
pub enum MgsRequest {
    GetInventory {
        etag: Option<String>,
        reply_tx: oneshot::Sender<GetInventoryResponse>,
    },
}

/// A mechanism for interacting with the  MgsManager
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

    pub async fn start_component_update(
        &self,
        target: SpIdentifier,
        update_slot: u16,
        kind: UpdateArtifactKind,
        data: BufList,
    ) -> Result<PostComponentUpdateResponse, StartComponentUpdateError> {
        let (expected_type, component) = match kind {
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
                return Err(
                    StartComponentUpdateError::CannotApplyUpdatesOfKind(kind),
                )
            }
        };
        if expected_type != target.type_ {
            return Err(
                StartComponentUpdateError::CannotApplyUpdateToTargetType {
                    kind,
                    required: expected_type,
                    target: target.type_,
                },
            );
        }

        // TODO Convert a BufList into a Vec<u8> - this is a little gross.
        // Should MGS's endpoint accept a BufList somehow? Should the artifact
        // store give us something more amenable to conversion?
        let mut image = Vec::with_capacity(data.num_bytes());
        for chunk in data {
            image.extend_from_slice(&*chunk);
        }

        let update_id = Uuid::new_v4();
        let request =
            UpdateBody { id: update_id.clone(), image, slot: update_slot };

        self.mgs_client
            .sp_component_update(target.type_, target.slot, component, &request)
            .await
            .map(|_ok| PostComponentUpdateResponse {
                update_id,
                component: component.to_string(),
            })
            .map_err(StartComponentUpdateError::MgsResponseError)
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
        let inventory = None;
        MgsManager { log, tx, rx, mgs_client, inventory }
    }

    pub fn get_handle(&self) -> MgsHandle {
        MgsHandle { tx: self.tx.clone(), mgs_client: self.mgs_client.clone() }
    }

    pub async fn run(mut self) {
        let mut inventory_rx =
            poll_sps(&self.log, self.mgs_client.clone()).await;

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

                // Handle requests from clients
                Some(request) = self.rx.recv() => {
                    match request {
                        MgsRequest::GetInventory {reply_tx, ..} => {
                            let response = GetInventoryResponse::new(self.inventory.clone());
                            let _ = reply_tx.send(response);
                        }
                    }
                }
            }
        }
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
