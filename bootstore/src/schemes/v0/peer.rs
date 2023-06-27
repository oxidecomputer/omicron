// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entrypoint of the v0 scheme for use by bootstrap agent

use crate::trust_quorum::RackSecret;

use super::{ApiError, Config as FsmConfig, Fsm, Output};
use slog::{error, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use uuid::Uuid;

use sled_hardware::Baseboard;

#[derive(Debug, Clone)]
pub struct Config {
    id: Baseboard,
    addr: SocketAddrV6,
    time_per_tick: Duration,
    learn_timeout: Duration,
    rack_init_timeout: Duration,
    rack_secret_request_timeout: Duration,
}

// An established connection operating in scheme v0
pub struct Connection {
    stream: TcpStream,
    addr: SocketAddrV6,
}

// An error response from a `PeerRequest`
pub enum PeerRequestError {
    // An `Init_` or `LoadRackSecret` request is already outstanding
    // We only allow one at a time.
    RequestAlreadyPending,

    // An error returned by the Fsm API
    Fsm(ApiError),
}

/// A request sent to the `Peer` task
pub enum PeerRequest {
    /// Initialize a rack at the behest of RSS running on the same scrimlet as this Peer
    InitRack {
        rack_uuid: Uuid,
        initial_membership: BTreeSet<Baseboard>,
        responder: oneshot::Sender<Result<(), PeerRequestError>>,
    },

    /// Initialize this peer as a learner.
    ///
    /// Return `()` from the responder when the learner has learned its share
    InitLearner { responder: oneshot::Sender<Result<(), PeerRequestError>> },

    /// Load the rack secret.
    ///
    /// This can only be successfully called when a peer has been initialized,
    /// either as initial member or learner who has learned its share.
    LoadRackSecret {
        responder: oneshot::Sender<Result<RackSecret, PeerRequestError>>,
    },

    /// Inform the peer of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    PeerAddresses(BTreeSet<SocketAddrV6>),
}

// A handle for interacting with a `Peer` task
pub struct PeerHandle {
    tx: mpsc::Sender<PeerRequest>,
}

/// A peer in the bootstore protocol
pub struct Peer {
    config: Config,
    fsm: Fsm,
    peers: BTreeSet<SocketAddrV6>,

    // Connections that have not yet completed handshakes via `Hello` and
    // `Identify` messages.
    negotiating_connections: BTreeMap<SocketAddrV6, TcpStream>,

    // Active connections participating in scheme v0
    connections: BTreeMap<Baseboard, Connection>,

    // Handle requests received from `PeerHandle`
    rx: mpsc::Receiver<PeerRequest>,

    // Used to respond to `InitRack` or `InitLearner` requests
    init_responder: Option<oneshot::Sender<Result<(), PeerRequestError>>>,

    // Used to respond to `LoadRackSecret` requests
    rack_secret_responder:
        Option<oneshot::Sender<Result<RackSecret, PeerRequestError>>>,

    log: Logger,
}

impl From<Config> for FsmConfig {
    fn from(value: Config) -> Self {
        FsmConfig {
            learn_timeout: (value.learn_timeout.as_millis()
                / value.time_per_tick.as_millis())
            .try_into()
            .unwrap(),
            rack_init_timeout: (value.rack_init_timeout.as_millis()
                / value.time_per_tick.as_millis())
            .try_into()
            .unwrap(),
            rack_secret_request_timeout: (value
                .rack_secret_request_timeout
                .as_millis()
                / value.time_per_tick.as_millis())
            .try_into()
            .unwrap(),
        }
    }
}

impl Peer {
    pub fn new(config: Config, log: &Logger) -> (Peer, PeerHandle) {
        // We only expect one outstanding request at a time for `Init_` or
        // `LoadRackSecret` requests, We can have one of those requests in
        // flight while allowing `PeerAddresses` updates.
        let (tx, rx) = mpsc::channel(3);
        let fsm =
            Fsm::new_uninitialized(config.id.clone(), config.clone().into());
        (
            Peer {
                config,
                fsm,
                peers: BTreeSet::new(),
                negotiating_connections: BTreeMap::new(),
                connections: BTreeMap::new(),
                rx,
                init_responder: None,
                rack_secret_responder: None,
                log: log.new(o!("component" => "bootstore")),
            },
            PeerHandle { tx },
        )
    }

    /// Run the main loop of the peer
    ///
    /// This should be spawned into its own tokio task
    pub async fn run(&mut self) {
        // select among timer tick/received messages
        let mut interval = interval(self.config.time_per_tick);
        loop {
            tokio::select! {
                Some(request) = self.rx.recv() => {
                    match request {
                        PeerRequest::InitRack {
                            rack_uuid,
                            initial_membership,
                            responder
                        } => {
                            if self.init_responder.is_some() {
                                let _ = responder.send(Err(PeerRequestError::RequestAlreadyPending));
                                continue;
                            }
                            self.init_responder = Some(responder);
                            let output = self.fsm.init_rack(rack_uuid, initial_membership);
                            self.handle_output(output);
                        }
                        PeerRequest::InitLearner{responder} => {
                            if self.init_responder.is_some() {
                                let _ = responder.send(Err(PeerRequestError::RequestAlreadyPending));
                                continue;
                            }
                            self.init_responder = Some(responder);
                            let output = self.fsm.init_learner();
                            self.handle_output(output);
                        }
                        PeerRequest::LoadRackSecret{responder} => {
                            if self.rack_secret_responder.is_some() {
                                let _ = responder.send(Err(PeerRequestError::RequestAlreadyPending));
                                continue;
                            }
                            self.rack_secret_responder = Some(responder);
                            let output = self.fsm.load_rack_secret();
                            self.handle_output(output);
                        }
                        PeerRequest::PeerAddresses(peers) => {
                            self.manage_connections(peers).await;
                        }
                    }
                }
                _ = interval.tick() => {
                    let output = self.fsm.tick();
                    self.handle_output(output);
                }
            }
        }
    }

    fn handle_output(&mut self, output: Output) {}

    async fn manage_connections(&mut self, peers: BTreeSet<SocketAddrV6>) {}
}
