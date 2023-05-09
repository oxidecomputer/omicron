// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code for talking to wicketd

use slog::{o, warn, Logger};
use std::convert::From;
use std::net::SocketAddrV6;
use tokio::sync::mpsc::{self, Sender, UnboundedSender};
use tokio::time::{interval, Duration, MissedTickBehavior};
use wicketd_client::types::{
    GetInventoryParams, GetInventoryResponse, IgnitionCommand, SpIdentifier,
    SpType,
};

use crate::events::EventReportMap;
use crate::state::ComponentId;
use crate::Event;

impl From<ComponentId> for SpIdentifier {
    fn from(id: ComponentId) -> Self {
        match id {
            ComponentId::Sled(i) => {
                SpIdentifier { type_: SpType::Sled, slot: i as u32 }
            }
            ComponentId::Psc(i) => {
                SpIdentifier { type_: SpType::Power, slot: i as u32 }
            }
            ComponentId::Switch(i) => {
                SpIdentifier { type_: SpType::Switch, slot: i as u32 }
            }
        }
    }
}

const WICKETD_POLL_INTERVAL: Duration = Duration::from_millis(500);
const WICKETD_TIMEOUT: Duration = Duration::from_millis(1000);

// Assume that these requests are periodic on the order of seconds or the
// result of human interaction. In either case, this buffer should be plenty
// large.
const CHANNEL_CAPACITY: usize = 1000;

/// Requests driven by the UI and sent from [`crate::Runner`] to [`WicketdManager`]
#[allow(unused)]
#[derive(Debug)]
pub enum Request {
    StartUpdate(ComponentId),
    IgnitionCommand(ComponentId, IgnitionCommand),
}

pub struct WicketdHandle {
    pub tx: Sender<Request>,
}

/// Wrapper around Wicketd clients used to poll inventory
/// and perform updates.
pub struct WicketdManager {
    log: Logger,
    rx: mpsc::Receiver<Request>,
    events_tx: UnboundedSender<Event>,
    wicketd_addr: SocketAddrV6,
}

impl WicketdManager {
    pub fn new(
        log: &Logger,
        events_tx: UnboundedSender<Event>,
        wicketd_addr: SocketAddrV6,
    ) -> (WicketdHandle, WicketdManager) {
        let log = log.new(o!("component" => "WicketdManager"));
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        let handle = WicketdHandle { tx };
        let manager = WicketdManager { log, rx, events_tx, wicketd_addr };

        (handle, manager)
    }

    /// Manage interactions with wicketd on the same scrimlet
    ///
    /// * Send requests to wicketd
    /// * Receive responses / errors
    /// * Translate any responses/errors into [`Event`]s
    ///   that can be utilized by the UI.
    pub async fn run(mut self) {
        // When we perform operations that we expect to change the inventory, we
        // want to be able to trigger a poll of the inventory immediately
        // instead of waiting for the next tick. Create a depth-1 channel on
        // which we can push requests to fetch the inventory; we only need depth
        // 1 because if the channel already has a message in it, we've already
        // queued a request to poll the inventory ASAP.
        let (poll_interval_now_tx, poll_interval_now_rx) = mpsc::channel(1);

        self.poll_inventory(poll_interval_now_rx).await;
        self.poll_artifacts_and_event_reports().await;

        loop {
            tokio::select! {
                Some(request) = self.rx.recv() => {
                    slog::info!(self.log, "Got wicketd req: {:?}", request);
                    match request {
                        Request::StartUpdate(component_id) => {
                            self.start_update(component_id);
                        }
                        Request::IgnitionCommand(component_id, command) => {
                            self.start_ignition_command(
                                component_id,
                                command,
                                poll_interval_now_tx.clone(),
                            );
                        }
                    }
                }
                else => {
                    slog::info!(self.log, "Request receiver closed. Process must be exiting.");
                    break;
                }
            }
        }
    }

    fn start_update(&self, component_id: ComponentId) {
        let log = self.log.clone();
        let addr = self.wicketd_addr;
        tokio::spawn(async move {
            let update_client =
                create_wicketd_client(&log, addr, WICKETD_TIMEOUT);
            let sp: SpIdentifier = component_id.into();
            let res = update_client.post_start_update(sp.type_, sp.slot).await;
            // We don't return errors or success values, as there's nobody to
            // return them to. Instead, all updates are periodically polled
            // and global state mutated. This allows the update pane to
            // report current status to users in a more detailed and holistic
            // fashion.
            slog::info!(log, "Update response for {}: {:?}", component_id, res);
        });
    }

    fn start_ignition_command(
        &self,
        component_id: ComponentId,
        command: IgnitionCommand,
        poll_inventory_now: mpsc::Sender<SpIdentifier>,
    ) {
        let log = self.log.clone();
        let addr = self.wicketd_addr;
        tokio::spawn(async move {
            let client = create_wicketd_client(&log, addr, WICKETD_TIMEOUT);
            let sp: SpIdentifier = component_id.into();
            let res =
                client.post_ignition_command(sp.type_, sp.slot, command).await;
            // We don't return errors or success values, as there's nobody to
            // return them to. How do we relay this result to the user?
            slog::info!(
                log,
                "Ignition response for {} ({:?}): {:?}",
                component_id,
                command,
                res
            );
            // Try to poll the inventory now; if this fails we don't care (it
            // means either someone else has already queued up an inventory poll
            // or the polling task has died).
            _ = poll_inventory_now.try_send(sp);
        });
    }

    async fn poll_artifacts_and_event_reports(&self) {
        let log = self.log.clone();
        let tx = self.events_tx.clone();
        let addr = self.wicketd_addr;
        tokio::spawn(async move {
            let client = create_wicketd_client(&log, addr, WICKETD_TIMEOUT);
            let mut ticker = interval(WICKETD_POLL_INTERVAL * 2);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                // TODO: We should really be using ETAGs here
                match client.get_artifacts_and_event_reports().await {
                    Ok(val) => {
                        // TODO: Only send on changes
                        let rsp = val.into_inner();
                        let artifacts = rsp.artifacts;
                        let system_version = rsp.system_version;
                        let event_reports: EventReportMap = rsp.event_reports;
                        let _ = tx.send(Event::ArtifactsAndEventReports {
                            system_version,
                            artifacts,
                            event_reports,
                        });
                    }
                    Err(e) => {
                        warn!(log, "{e}");
                    }
                }
            }
        });
    }

    async fn poll_inventory(&self, mut poll_now: mpsc::Receiver<SpIdentifier>) {
        let log = self.log.clone();
        let tx = self.events_tx.clone();
        let addr = self.wicketd_addr;

        tokio::spawn(async move {
            let client = create_wicketd_client(&log, addr, WICKETD_TIMEOUT);
            let mut ticker = interval(WICKETD_POLL_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let force_refresh = tokio::select! {
                    _ = ticker.tick() => Vec::new(),
                    Some(sp) = poll_now.recv() => {
                        // We want to poll immediately; do so and reset our
                        // timer.
                        ticker.reset();
                        vec![sp]
                    }
                };

                let params = GetInventoryParams { force_refresh };
                // TODO: We should really be using ETAGs here
                match client.get_inventory(&params).await {
                    Ok(val) => match val.into_inner() {
                        GetInventoryResponse::Response {
                            inventory,
                            mgs_last_seen,
                        } => {
                            let _ = tx.send(Event::Inventory {
                                inventory,
                                mgs_last_seen,
                            });
                        }
                        GetInventoryResponse::Unavailable => {
                            // Nothing to do here. We keep a running total from
                            // the last successful response by processing
                            // ticks in the runner;
                        }
                    },
                    Err(err) => {
                        warn!(
                            log, "Getting inventory from wicketd failed";
                            "err" => %err,
                        );
                    }
                }
            }
        });
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
