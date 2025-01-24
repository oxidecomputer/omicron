// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fetching information from Dendrite.

use dpd_client::ClientState;
use slog::{debug, error, info, trace, warn, Logger};
use std::{net::SocketAddrV6, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};
use wicket_common::inventory::Transceiver;

// Queue size for handling requests to `dpd`.
const CHANNEL_CAPACITY: usize = 4;

// Requests to `dpd` should complete quickly, since we're just asking for state
// about the transceivers.
const DPD_TIMEOUT: Duration = Duration::from_secs(5);

// Duration on which we poll transceivers ourselves, independent of any
// requests.
const TRANSCEIVER_POLL_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum DpdRequest {
    GetTransceivers {
        #[allow(dead_code)]
        etag: Option<String>,
        reply_tx: oneshot::Sender<
            Result<GetTransceiversResponse, GetTransceiversError>,
        >,
    },
}

#[derive(Debug)]
pub enum GetTransceiversResponse {
    Response { transceivers: Vec<Transceiver>, dpd_last_seen: Duration },
    Unavailable,
}

#[derive(Debug)]
pub enum GetTransceiversError {
    ShutdownInProgress,
}

pub struct DpdHandle {
    tx: mpsc::Sender<DpdRequest>,
}

impl DpdHandle {
    /// Fetch the transceivers from `dpd`.
    pub(crate) async fn get_transceivers(
        &self,
    ) -> Result<GetTransceiversResponse, GetTransceiversError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let etag = None;
        self.tx
            .send(DpdRequest::GetTransceivers { etag, reply_tx })
            .await
            .map_err(|_| GetTransceiversError::ShutdownInProgress)?;
        reply_rx.await.map_err(|_| GetTransceiversError::ShutdownInProgress)?
    }
}

// The known state of transceivers.
struct TransceiverState {
    last_completed: Instant,
    transceivers: Vec<Transceiver>,
}

// Loop to collect transceivers on an interval.
async fn transceiver_fetch_task(
    log: Logger,
    client: dpd_client::Client,
    tx: watch::Sender<Option<TransceiverState>>,
) {
    let mut interval = tokio::time::interval(TRANSCEIVER_POLL_INTERVAL);
    loop {
        interval.tick().await;
        debug!(log, "fetching transceiver state");
        let transceivers = match client.transceivers_list().await {
            Ok(tr) => tr.into_inner(),
            Err(err) => {
                error!(
                    log,
                    "failed to list all transceivers";
                    "err" => %err,
                );
                continue;
            }
        };
        debug!(
            log,
            "fetched transceivers from `dpd`";
            "n_transceivers" => transceivers.len(),
        );

        // Fetch other transceiver state
        //
        // - optical power
        // - datapath

        let current_state = TransceiverState {
            last_completed: Instant::now(),
            transceivers: transceivers
                .into_iter()
                .map(|(port, _tr)| Transceiver { port, /* add other state */ })
                .collect(),
        };
        if tx.send(Some(current_state)).is_ok() {
            trace!(log, "posted new transceiver state on watch channel");
        } else {
            error!(
                log,
                "failed to send new transceiver state on watch channel",
            );
        }
    }
}

pub struct DpdManager {
    log: Logger,
    tx: mpsc::Sender<DpdRequest>,
    rx: mpsc::Receiver<DpdRequest>,
    dpd_client: dpd_client::Client,
}

impl DpdManager {
    pub(crate) fn new(log: &Logger, dpd_address: SocketAddrV6) -> Self {
        let log = log.new(slog::o!("component" => "wicked DpdManager"));
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let dpd_client = make_dpd_client(log.clone(), dpd_address);
        Self { log, tx, rx, dpd_client }
    }

    pub(crate) fn get_handle(&self) -> DpdHandle {
        DpdHandle { tx: self.tx.clone() }
    }

    pub(crate) async fn run(mut self) {
        debug!(self.log, "spawning transceiver fetching task");
        let (tx, mut transceivers) = watch::channel(None);
        tokio::spawn(transceiver_fetch_task(
            self.log.clone(),
            self.dpd_client.clone(),
            tx,
        ));

        debug!(self.log, "starting main loop");
        loop {
            tokio::select! {
                maybe_msg = self.rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        warn!(self.log, "request queue closed, exiting");
                        return;
                    };
                    match msg {
                        DpdRequest::GetTransceivers { reply_tx, .. } => {
                            let state = transceivers.borrow_and_update();
                            let response = match &*state {
                                Some(state) => {
                                    Ok(GetTransceiversResponse::Response {
                                        transceivers: state.transceivers.clone(),
                                        dpd_last_seen: state.last_completed.elapsed(),
                                    })
                                }
                                None => Ok(GetTransceiversResponse::Unavailable),
                            };
                            match reply_tx.send(response) {
                                Ok(_) => trace!(
                                    self.log, "sent transceiver response to client"
                                ),
                                Err(_) => error!(
                                    self.log,
                                    "failed to send transceiver response"
                                ),
                            }
                        }
                    }
                }
            }
        }
    }
}

fn make_dpd_client(log: Logger, addr: SocketAddrV6) -> dpd_client::Client {
    let endpoint = format!("http://[{}]:{}", addr.ip(), addr.port());
    info!(log, "Created dpd client"; "endpoint" => &endpoint);
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(DPD_TIMEOUT)
        .timeout(DPD_TIMEOUT)
        .build()
        .unwrap();
    let state = ClientState { tag: "wicketd".to_string(), log };
    dpd_client::Client::new_with_client(&endpoint, client, state)
}
