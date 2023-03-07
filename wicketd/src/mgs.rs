// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::{RackV1Inventory, SpInventory};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use gateway_client::types::{SpComponentList, SpIdentifier, SpInfo};
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
