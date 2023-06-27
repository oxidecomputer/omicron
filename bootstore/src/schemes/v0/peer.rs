// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entrypoint of the v0 scheme for use by bootstrap agent

use crate::trust_quorum::RackSecret;

use super::{ApiError, Config as FsmConfig, Fsm};
use slog::{error, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use uuid::Uuid;

use sled_hardware::Baseboard;

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

/// A request sent to the `Peer` task
pub enum PeerRequest {
    /// Initialize a rack at the behest of RSS running on the same scrimlet as this Peer
    InitRack {
        rack_uuid: Uuid,
        initial_membership: BTreeSet<Baseboard>,
        responder: oneshot::Sender<Result<(), ApiError>>,
    },

    /// Initialize this peer as a learner.
    ///
    /// Return `()` from the responder when the learner has learned its share
    InitLearner { responder: onseshot::Sender<()> },

    /// Load the rack secret.
    ///
    /// This can only be successfully called when a peer has been initialized,
    /// either as initial member or learner who has learned its share.
    LoadRackSecret { responder: oneshot::Sender<Result<RackSecret, ApiError>> },

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

    log: Logger,
}

impl From<Config> for FsmConfig {
    fn from(value: Config) -> Self {
        FsmConfig {
            learn_timeout: value.learn_timeout / value.time_per_tick,
            rack_init_timeout: value.rack_init_timeout / value.time_per_tick,
            rack_secret_request_timeout: value.rack_secret_request_timeout
                / value.time_per_tick,
        }
    }
}

impl Peer {
    pub fn new(config: Config, log: &Logger) -> (Peer, PeerHandle) {
        // A somewhat arbitrary channel size. We really only expect one
        // outstanding request at a time.
        let (tx, rx) = mpsc::channel(10);
        let fsm = Fsm::new_uninitialized(config.id, config.into());
        (
            PeerHandle { tx },
            Peer {
                config,
                fsm,
                peers: BTreeSet::new(),
                negotiating_connections: BTreeMap::new(),
                connections: BTreeMap::new(),
                rx,
                log: log.new(o!("component" => "bootstore")),
            },
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
                }
                let _ = interval.tick() => {
                    let output = self.fsm.tick();
                }
            }
        }
    }
}
