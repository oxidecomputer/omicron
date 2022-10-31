// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The collection of tasks used for interacting with MGS and maintaining
//! runtime state.

use crate::RackV1Inventory;
use gateway_client::types::SpInfo;
use slog::{debug, info, o, warn, Logger};
use std::net::SocketAddrV6;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration, MissedTickBehavior};

const MGS_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MGS_TIMEOUT: u32 = 3000; // 3 sec

// 32 sleds, 2 switches, 2 PSCs (at some point)
const MAX_COMPONENTS: Option<NonZeroU32> = NonZeroU32::new(36);

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
        reply_tx: oneshot::Sender<Arc<RackV1Inventory>>,
    },
}

/// A mechanism for interacting with the  MgsManager
pub struct MgsHandle {
    tx: tokio::sync::mpsc::Sender<MgsRequest>,
}

impl MgsHandle {
    pub async fn get_inventory(
        &self,
    ) -> Result<Arc<RackV1Inventory>, ShutdownInProgress> {
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
    // Remove the need to copy a potentially sizeable amount of data
    inventory: Arc<RackV1Inventory>,
}

impl MgsManager {
    pub fn new(log: &Logger, mgs_addr: SocketAddrV6) -> MgsManager {
        let log = log.new(o!("component" => "wicketd MgsManager"));
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let endpoint =
            format!("http://[{}]:{}", mgs_addr.ip(), mgs_addr.port());
        info!(log, "MGS Endpoint: {}", endpoint);
        let mgs_client = gateway_client::Client::new(&endpoint, log.clone());
        let inventory = Arc::new(RackV1Inventory::default());
        MgsManager { log, tx, rx, mgs_client, inventory }
    }

    pub fn get_handle(&self) -> MgsHandle {
        MgsHandle { tx: self.tx.clone() }
    }

    pub async fn run(mut self) {
        let mgs_client = self.mgs_client;
        let mut inventory_rx = poll_inventory(&self.log, mgs_client).await;

        loop {
            tokio::select! {
                // Poll MGS inventory
                Some(sps) = inventory_rx.recv() => {
                    self.inventory = Arc::new(RackV1Inventory {
                        sps: sps.into_iter().map(|sp| (sp, vec![])).collect()
                    });
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

pub async fn poll_inventory(
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
            match client.sp_list(MAX_COMPONENTS, None, Some(MGS_TIMEOUT)).await
            {
                Ok(val) => {
                    // TODO: Get components for each sp
                    let _ = tx.send(val.into_inner().items).await;
                }
                Err(e) => {
                    warn!(log, "{e}");
                }
            }
        }
    });

    rx
}
