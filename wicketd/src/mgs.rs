// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::{RackV1Inventory, SpId, SpInventory};
use gateway_client::types::SpInfo;
use slog::{debug, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, MissedTickBehavior};

const MGS_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MGS_TIMEOUT_MS: u32 = 3000; // 3 sec

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
pub enum MgsRequest {
    GetInventory {
        etag: Option<String>,
        reply_tx: oneshot::Sender<RackV1Inventory>,
    },
}

/// A mechanism for interacting with the  MgsManager
pub struct MgsHandle {
    tx: tokio::sync::mpsc::Sender<MgsRequest>,
}

impl MgsHandle {
    pub async fn get_inventory(
        &self,
    ) -> Result<RackV1Inventory, ShutdownInProgress> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let etag = None;
        self.tx
            .send(MgsRequest::GetInventory { etag, reply_tx })
            .await
            .map_err(|_| ShutdownInProgress)?;
        reply_rx.await.map_err(|_| ShutdownInProgress)
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
    inventory: RackV1Inventory,
}

impl MgsManager {
    pub fn new(log: &Logger, mgs_addr: SocketAddrV6) -> MgsManager {
        let log = log.new(o!("component" => "wicketd MgsManager"));
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let endpoint =
            format!("http://[{}]:{}", mgs_addr.ip(), mgs_addr.port());
        info!(log, "MGS Endpoint: {}", endpoint);
        let timeout = std::time::Duration::from_millis(MGS_TIMEOUT_MS.into());
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .unwrap();

        let mgs_client = gateway_client::Client::new_with_client(
            &endpoint,
            client,
            log.clone(),
        );
        let inventory = RackV1Inventory::default();
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
                Some(sps) = inventory_rx.recv() => {
                    update_inventory(&mut self.inventory, sps);
                }

                // Handle requests from clients
                Some(request) = self.rx.recv() => {
                    debug!(self.log, "{:?}", request);
                     match request {
                         MgsRequest::GetInventory {reply_tx, ..} => {
                            let _ = reply_tx.send(self.inventory.clone());
                         }
                     }
                }
            }
        }
    }
}

// For the latest set of sps returned from MGS:
//  1. Update their state if it has changed
//  2. Remove any SPs in our current inventory that aren't in the new state
fn update_inventory(inventory: &mut RackV1Inventory, sps: Vec<SpInfo>) {
    let new_keys: BTreeSet<SpId> =
        sps.iter().map(|sp| sp.info.id.clone().into()).collect();

    // Remove all keys that are not in the latest update
    let mut new_inventory: BTreeMap<SpId, SpInventory> = inventory
        .sps
        .iter()
        .filter_map(|sp| {
            if new_keys.contains(&sp.id) {
                Some((sp.id, sp.clone()))
            } else {
                None
            }
        })
        .collect();

    // Update any existing SPs that have changed state
    // or add any new ones.
    for sp in sps.into_iter() {
        let state = sp.details;
        let id: SpId = sp.info.id.into();
        let ignition = sp.info.details;

        new_inventory
            .entry(id)
            .and_modify(|curr| {
                // TODO: // Reset the components only if the state changes.
                // This is blocked waiting on some small progenitor changes
                // that will allow us to derive Eq/PartialEq, etc...
                //if curr.state != state {
                // Clear the components, so we can refetch them. We don't know
                // if the actual component has changed or if it has been upgraded, etc..
                //  curr.components = vec![];
                //}
                curr.state = state.clone();
                curr.ignition = ignition.clone();
            })
            .or_insert(SpInventory::new(id, ignition, state));
    }
    inventory.sps = new_inventory.into_values().collect();
}

async fn poll_sps(
    log: &Logger,
    client: gateway_client::Client,
) -> mpsc::Receiver<Vec<SpInfo>> {
    let log = log.clone();

    // We only want one outstanding inventory request at a time
    let (tx, rx) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut ticker = interval(MGS_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match client.sp_list(Some(MGS_TIMEOUT_MS)).await {
                Ok(val) => {
                    // TODO: Get components for each sp
                    let _ = tx.send(val.into_inner()).await;
                }
                Err(e) => {
                    warn!(log, "{e}");
                }
            }
        }
    });

    rx
}
