// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::inventory::RotInventory;
use crate::{RackV1Inventory, SpInventory};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use gateway_client::types::{
    SpComponentCaboose, SpComponentInfo, SpIdentifier, SpInfo,
};
use gateway_messages::SpComponent;
use schemars::JsonSchema;
use serde::Serialize;
use slog::{info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};

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

#[derive(Debug)]
enum MgsRequest {
    GetInventory {
        #[allow(dead_code)]
        etag: Option<String>,
        reply_tx: oneshot::Sender<GetInventoryResponse>,
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
    // The Instant indicates the moment at which the inventory was received.
    inventory: Option<(RackV1Inventory, Instant)>,
}

impl MgsManager {
    pub fn new(log: &Logger, mgs_addr: SocketAddrV6) -> MgsManager {
        let log = log.new(o!("component" => "wicketd MgsManager"));
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let mgs_client = make_mgs_client(log.clone(), mgs_addr);
        let inventory = None;
        MgsManager { log, tx, rx, mgs_client, inventory }
    }

    pub fn get_handle(&self) -> MgsHandle {
        MgsHandle { tx: self.tx.clone() }
    }

    pub async fn run(mut self) {
        let mgs_client = self.mgs_client;
        let mut inventory_rx = poll_sps(&self.log, mgs_client).await;

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

    // Build a stream of futures that fetch additional details about the SP's
    // state that require extra requests to MGS (e.g., to fetch the caboose). We
    // only update these details if either (1) the SP's state has changed (in
    // which case we discard all cached details we have) or (2) we don't yet
    // have the details (e.g., the state previously changed and our attempt to
    // fetch details at the time failed).
    let mut details_stream = sps
        .into_iter()
        .filter_map(|sp| {
            let state = sp.details;
            let id: SpIdentifier = sp.info.id;
            let ignition = sp.info.details;

            let curr = inventory
                .entry(id)
                .and_modify(|curr| {
                    if curr.state != state {
                        // state has changed - discard cached details
                        curr.components = None;
                        curr.caboose = None;
                        curr.rot.caboose = None;
                    }

                    curr.state = state.clone();
                    curr.ignition = ignition.clone();
                })
                .or_insert_with(|| SpInventory::new(id, ignition, state));

            fetch_sp_details_if_needed(curr, client, log)
        })
        .collect::<FuturesUnordered<_>>();

    // Wait for all our requests to come back - if any of them successfully
    // fetched any of the detail fields we needed, update our `inventory` and
    // note that it has changed.
    while let Some(details) = details_stream.next().await {
        let item = inventory.get_mut(&details.id).unwrap();
        if let Some(components) = details.components {
            item.components = Some(components);
            inventory_changed = true;
        }
        if let Some(sp_caboose) = details.caboose {
            item.caboose = Some(sp_caboose);
            inventory_changed = true;
        }
        if let Some(rot_caboose) = details.rot.caboose {
            item.rot.caboose = Some(rot_caboose);
            inventory_changed = true;
        }
    }

    inventory_changed
}

fn fetch_sp_details_if_needed<'a>(
    item: &SpInventory,
    client: &'a gateway_client::Client,
    log: &'a Logger,
) -> Option<impl Future<Output = SpDetails> + 'a> {
    let need_components = item.components.is_none();
    let need_sp_caboose = item.caboose.is_none();
    let need_rot_caboose = item.rot.caboose.is_none();

    // If all fields of `item` that we know how to populate are already set, we
    // have nothing to do.
    if !need_components && !need_sp_caboose && !need_rot_caboose {
        return None;
    }

    let id = item.id;
    Some(async move {
        let mut details = SpDetails {
            id,
            components: None,
            caboose: None,
            rot: RotInventory { caboose: None },
        };

        if need_components {
            match client.sp_component_list(id.type_, id.slot).await {
                Ok(val) => {
                    details.components = Some(val.into_inner().components);
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get component list for sp";
                        "sp" => ?id,
                        "err" => %err,
                    );
                }
            }
        }

        if need_sp_caboose {
            match client
                .sp_component_caboose_get(
                    id.type_,
                    id.slot,
                    SpComponent::SP_ITSELF.const_as_str(),
                )
                .await
            {
                Ok(val) => {
                    details.caboose = Some(val.into_inner());
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get caboose for sp";
                        "sp" => ?id,
                        "err" => %err,
                    );
                }
            }
        }

        if need_rot_caboose {
            match client
                .sp_component_caboose_get(
                    id.type_,
                    id.slot,
                    SpComponent::ROT.const_as_str(),
                )
                .await
            {
                Ok(val) => {
                    details.rot.caboose = Some(val.into_inner());
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get caboose for rot";
                        "sp" => ?id,
                        "err" => %err,
                    );
                }
            }
        }

        details
    })
}

// Container for details of an SP we have to fetch with separate MGS requests
// from the main `sp_list()`.
#[derive(Debug)]
struct SpDetails {
    id: SpIdentifier,
    caboose: Option<SpComponentCaboose>,
    components: Option<Vec<SpComponentInfo>>,
    rot: RotInventory,
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
