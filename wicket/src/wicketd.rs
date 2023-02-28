// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code for talking to wicketd

use slog::{o, warn, Logger};
use std::net::SocketAddrV6;
use std::sync::mpsc::Sender;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use wicketd_client::types::RackV1Inventory;
use wicketd_client::GetInventoryResponse;

use crate::{Event, InventoryEvent};

const WICKETD_POLL_INTERVAL: Duration = Duration::from_millis(500);
const WICKETD_TIMEOUT: Duration = Duration::from_millis(1000);

// Assume that these requests are periodic on the order of seconds or the
// result of human interaction. In either case, this buffer should be plenty
// large.
const CHANNEL_CAPACITY: usize = 1000;

// Eventually this will be filled in with things like triggering updates from the UI
pub enum Request {}

#[allow(unused)]
pub struct WicketdHandle {
    tx: mpsc::Sender<Request>,
}

/// Wrapper around Wicketd clients used to poll inventory
/// and perform updates.
pub struct WicketdManager {
    log: Logger,

    //TODO: We'll use this onece we start implementing updates
    #[allow(unused)]
    rx: mpsc::Receiver<Request>,
    wizard_tx: Sender<Event>,
    inventory_client: wicketd_client::Client,
}

impl WicketdManager {
    pub fn new(
        log: &Logger,
        wizard_tx: Sender<Event>,
        wicketd_addr: SocketAddrV6,
    ) -> (WicketdHandle, WicketdManager) {
        let log = log.new(o!("component" => "WicketdManager"));
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        let inventory_client =
            create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);
        let handle = WicketdHandle { tx };
        let manager = WicketdManager { log, rx, wizard_tx, inventory_client };

        (handle, manager)
    }

    /// Manage interactions with wicketd on the same scrimlet
    ///
    /// * Send requests to wicketd
    /// * Receive responses / errors
    /// * Translate any responses/errors into [`Event`]s
    ///   that can be utilized by the UI.
    pub async fn run(self) {
        let mut inventory_rx =
            poll_inventory(&self.log, self.inventory_client).await;

        // TODO: Eventually there will be a tokio::select! here that also
        // allows issuing updates.
        while let Some(event) = inventory_rx.recv().await {
            // XXX: Should we log an error and exit here? This means the wizard
            // died and the process is exiting.
            let _ = self.wizard_tx.send(Event::Inventory(event));
        }
    }
}

pub(crate) fn create_wicketd_client(
    log: &Logger,
    wicketd_addr: SocketAddrV6,
    timeout: Duration,
) -> wicketd_client::Client {
    let endpoint =
        format!("http://[{}]:{}", wicketd_addr.ip(), wicketd_addr.port());
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .unwrap();

    wicketd_client::Client::new_with_client(&endpoint, client, log.clone())
}

async fn poll_inventory(
    log: &Logger,
    client: wicketd_client::Client,
) -> mpsc::Receiver<InventoryEvent> {
    let log = log.clone();

    // We only want one oustanding request at a time
    let (tx, rx) = mpsc::channel(1);
    let mut state = InventoryState::new(&log, tx);

    tokio::spawn(async move {
        let mut ticker = interval(WICKETD_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // TODO: We should really be using ETAGs here
            match client.get_inventory().await {
                Ok(val) => {
                    let new_inventory = val.into_inner();
                    state.send_if_changed(new_inventory.into()).await;
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
struct InventoryState {
    log: Logger,
    current_inventory: Option<RackV1Inventory>,
    tx: mpsc::Sender<InventoryEvent>,
}

impl InventoryState {
    fn new(log: &Logger, tx: mpsc::Sender<InventoryEvent>) -> Self {
        let log = log.new(o!("component" => "InventoryState"));
        Self { log, current_inventory: None, tx }
    }

    async fn send_if_changed(&mut self, new_inventory: GetInventoryResponse) {
        match (self.current_inventory.take(), new_inventory) {
            (
                current_inventory,
                GetInventoryResponse::Response {
                    inventory: new_inventory,
                    received_ago: mgs_received_ago,
                },
            ) => {
                let changed_inventory = (current_inventory.as_ref()
                    != Some(&new_inventory))
                .then(|| {
                    self.current_inventory = Some(new_inventory.clone());
                    new_inventory
                });

                let _ = self
                    .tx
                    .send(InventoryEvent::Inventory {
                        changed_inventory,
                        wicketd_received: Instant::now(),
                        mgs_received: libsw::Stopwatch::with_elapsed_started(
                            mgs_received_ago,
                        ),
                    })
                    .await;
            }
            (Some(_), GetInventoryResponse::Unavailable) => {
                // This is an illegal state transition -- wicketd can never return Unavailable after
                // returning a response.
                slog::error!(
                    self.log,
                    "Illegal state transition from response to unavailable"
                );
            }
            (None, GetInventoryResponse::Unavailable) => {
                // No response received by wicketd from MGS yet.
                let _ = self
                    .tx
                    .send(InventoryEvent::Unavailable {
                        wicketd_received: Instant::now(),
                    })
                    .await;
            }
        };
    }
}
