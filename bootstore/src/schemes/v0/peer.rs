// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entrypoint of the v0 scheme for use by bootstrap agent

use super::peer_networking::{
    spawn_client, spawn_server, AcceptedConnHandle, ConnToMainMsg,
    ConnToMainMsgInner, MainToConnMsg, Msg, PeerConnHandle,
};
use super::{ApiError, ApiOutput, Config as FsmConfig, Fsm, RackUuid};
use crate::trust_quorum::RackSecret;
use derive_more::From;
use sled_hardware::Baseboard;
use slog::{debug, error, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Instant, MissedTickBehavior};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Config {
    id: Baseboard,
    addr: SocketAddrV6,
    time_per_tick: Duration,
    learn_timeout: Duration,
    rack_init_timeout: Duration,
    rack_secret_request_timeout: Duration,
}

// An error response from a `PeerRequest`
#[derive(Debug, From)]
pub enum PeerRequestError {
    // An `Init_` or `LoadRackSecret` request is already outstanding
    // We only allow one at a time.
    RequestAlreadyPending,

    // An error returned by the Fsm API
    Fsm(ApiError),

    // The peer task shutdown
    Recv(oneshot::error::RecvError),

    // Failed to send to a connection management task
    Send(mpsc::error::SendError<PeerApiRequest>),
}

/// A request sent to the `Peer` task from the `PeerHandle`
pub enum PeerApiRequest {
    /// Initialize a rack at the behest of RSS running on the same scrimlet as this Peer
    InitRack {
        rack_uuid: RackUuid,
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

    /// Get the status of this peer
    GetStatus { responder: oneshot::Sender<Status> },

    /// Shutdown the peer
    Shutdown,
}

// A handle for interacting with a `Peer` task
pub struct PeerHandle {
    tx: mpsc::Sender<PeerApiRequest>,
}

impl PeerHandle {
    /// Initialize a rack at the behest of RSS running on the same scrimlet as
    /// this Peer
    pub async fn init_rack(
        &self,
        rack_uuid: RackUuid,
        initial_membership: BTreeSet<Baseboard>,
    ) -> Result<(), PeerRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(PeerApiRequest::InitRack {
                rack_uuid: RackUuid(Uuid::new_v4()),
                initial_membership,
                responder: tx,
            })
            .await?;
        let res = rx.await?;
        res
    }

    /// Initialize this peer as a learner
    pub async fn init_learner(&self) -> Result<(), PeerRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(PeerApiRequest::InitLearner { responder: tx }).await?;
        let res = rx.await?;
        res
    }

    /// Load the rack secret.
    ///
    /// This can only be successfully called when a peer has been initialized,
    /// either as initial member or learner who has learned its share.
    pub async fn load_rack_secret(
        &self,
    ) -> Result<RackSecret, PeerRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(PeerApiRequest::LoadRackSecret { responder: tx }).await?;
        let res = rx.await?;
        res
    }

    /// Inform the peer of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    pub async fn load_peer_addresses(
        &self,
        addrs: BTreeSet<SocketAddrV6>,
    ) -> Result<(), PeerRequestError> {
        self.tx.send(PeerApiRequest::PeerAddresses(addrs)).await?;
        Ok(())
    }

    /// Get the status of this peer
    pub async fn get_status(&self) -> Result<Status, PeerRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(PeerApiRequest::GetStatus { responder: tx }).await?;
        let res = rx.await?;
        Ok(res)
    }

    /// Shutdown the peer
    pub async fn shutdown(&self) -> Result<(), PeerRequestError> {
        self.tx.send(PeerApiRequest::Shutdown).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Status {
    fsm_state: &'static str,
    peers: BTreeSet<SocketAddrV6>,
    connections: BTreeMap<Baseboard, SocketAddrV6>,
    accepted_connections: BTreeSet<SocketAddrV6>,
    negotiating_connections: BTreeSet<SocketAddrV6>,
}

/// A peer in the bootstore protocol
pub struct Peer {
    config: Config,
    fsm: Fsm,
    peers: BTreeSet<SocketAddrV6>,
    handle_unique_id_counter: u64,

    // boolean set when a `PeerApiRequest::shutdown` is received
    shutdown: bool,

    // Connections that have been accepted, but where negotiation has not
    // finished At this point, we only know the client port of the connection,
    // and so cannot identify  it as a `Peer`.
    accepted_connections: BTreeMap<SocketAddrV6, AcceptedConnHandle>,

    // Connections that have not yet completed handshakes via `Hello` and
    // `Identify` messages.
    //
    // We only store the client (connecting) side, not server (accepting) side here.
    negotiating_connections: BTreeMap<SocketAddrV6, PeerConnHandle>,

    // Active connections participating in scheme v0
    //
    // This consists of both client and server connections
    connections: BTreeMap<Baseboard, PeerConnHandle>,

    // Handle requests received from `PeerHandle`
    rx: mpsc::Receiver<PeerApiRequest>,

    // Used to respond to `InitRack` or `InitLearner` requests
    init_responder: Option<oneshot::Sender<Result<(), PeerRequestError>>>,

    // Used to respond to `LoadRackSecret` requests
    rack_secret_responder:
        Option<oneshot::Sender<Result<RackSecret, PeerRequestError>>>,

    log: Logger,

    // Handle messages received from connection tasks
    conn_rx: mpsc::Receiver<ConnToMainMsg>,

    // Clone for use by connection tasks to send to the main peer task
    conn_tx: mpsc::Sender<ConnToMainMsg>,
}

impl From<Config> for FsmConfig {
    fn from(value: Config) -> Self {
        FsmConfig {
            learn_timeout: value.learn_timeout,
            rack_init_timeout: value.rack_init_timeout,
            rack_secret_request_timeout: value.rack_secret_request_timeout,
        }
    }
}

impl Peer {
    pub fn new(config: Config, log: &Logger) -> (Peer, PeerHandle) {
        // We only expect one outstanding request at a time for `Init_` or
        // `LoadRackSecret` requests, We can have one of those requests in
        // flight while allowing `PeerAddresses` updates.
        let (tx, rx) = mpsc::channel(3);

        // Up to 31 sleds sending messages with some extra room. These are mostly one at a time
        // for each sled, but we leave some extra room.
        let (conn_tx, conn_rx) = mpsc::channel(128);
        let fsm =
            Fsm::new_uninitialized(config.id.clone(), config.clone().into());
        let id_str = config.id.to_string();
        (
            Peer {
                config,
                fsm,
                peers: BTreeSet::new(),
                handle_unique_id_counter: 0,
                shutdown: false,
                accepted_connections: BTreeMap::new(),
                negotiating_connections: BTreeMap::new(),
                connections: BTreeMap::new(),
                rx,
                init_responder: None,
                rack_secret_responder: None,
                log: log
                    .new(o!("component" => "bootstore", "peer_id" => id_str)),
                conn_rx,
                conn_tx,
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
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let listener = TcpListener::bind(&self.config.addr).await.unwrap();
        while !self.shutdown {
            tokio::select! {
                res = listener.accept() => self.on_accept(res).await,
                Some(request) = self.rx.recv() => {
                    self.on_api_request(request).await;
                }
                Some(msg) = self.conn_rx.recv() => self.on_conn_msg(msg).await,
                _ = interval.tick() => {
                    if let Err(errors) = self.fsm.tick(Instant::now().into()) {
                        for (_, err) in errors {
                            self.handle_api_error(err).await;
                        }
                    }
                    // Even with errors there may be messages that need sending
                    self.deliver_envelopes().await;
                }
            }
        }
    }

    // Handle an accepted connection
    async fn on_accept(
        &mut self,
        res: Result<(TcpStream, SocketAddr), std::io::Error>,
    ) {
        match res {
            Ok((sock, addr)) => {
                let SocketAddr::V6(addr) = addr else {
                    warn!(self.log, "Got connection from IPv4 address {}", addr);
                    return;
                };
                // TODO: Log if a peer with a lower address connects?
                // Remove any existing connection
                self.remove_accepted_connection(&addr).await;
                info!(self.log, "Accepted connection from {}", addr);
                self.handle_unique_id_counter += 1;
                let handle = spawn_server(
                    self.handle_unique_id_counter,
                    self.config.id.clone(),
                    self.config.addr.clone(),
                    addr.clone(),
                    sock,
                    self.conn_tx.clone(),
                    &self.log,
                )
                .await;
                self.accepted_connections.insert(addr, handle);
            }
            Err(err) => {}
        }
    }

    // Handle API requests from the `PeerHandle`
    async fn on_api_request(&mut self, request: PeerApiRequest) {
        match request {
            PeerApiRequest::InitRack {
                rack_uuid,
                initial_membership,
                responder,
            } => {
                info!(self.log,
                    "Rack init started";
                    "rack_uuid" => rack_uuid.to_string()
                );
                if self.init_responder.is_some() {
                    let _ = responder
                        .send(Err(PeerRequestError::RequestAlreadyPending));
                    return;
                }
                if let Err(err) = self.fsm.init_rack(
                    Instant::now().into(),
                    rack_uuid,
                    initial_membership,
                ) {
                    let _ = responder.send(Err(err.into()));
                } else {
                    self.init_responder = Some(responder);
                    self.deliver_envelopes().await;
                }
            }
            PeerApiRequest::InitLearner { responder } => {
                if self.init_responder.is_some() {
                    let _ = responder
                        .send(Err(PeerRequestError::RequestAlreadyPending));
                    return;
                }
                if let Err(err) = self.fsm.init_learner(Instant::now().into()) {
                    let _ = responder.send(Err(err.into()));
                } else {
                    self.init_responder = Some(responder);
                    self.deliver_envelopes().await;
                }
            }
            PeerApiRequest::LoadRackSecret { responder } => {
                if self.rack_secret_responder.is_some() {
                    let _ = responder
                        .send(Err(PeerRequestError::RequestAlreadyPending));
                    return;
                }
                if let Err(err) =
                    self.fsm.load_rack_secret(Instant::now().into())
                {
                    let _ = responder.send(Err(err.into()));
                } else {
                    self.rack_secret_responder = Some(responder);
                    self.deliver_envelopes().await;
                }
            }
            PeerApiRequest::PeerAddresses(peers) => {
                info!(self.log, "Updated Peer Addresses: {:?}", peers);
                self.manage_connections(peers).await;
            }
            PeerApiRequest::GetStatus { responder } => {
                let status = Status {
                    fsm_state: self.fsm.state().name(),
                    peers: self.peers.clone(),
                    connections: self
                        .connections
                        .iter()
                        .map(|(id, handle)| (id.clone(), handle.addr))
                        .collect(),
                    accepted_connections: self
                        .accepted_connections
                        .keys()
                        .cloned()
                        .collect(),
                    negotiating_connections: self
                        .negotiating_connections
                        .keys()
                        .cloned()
                        .collect(),
                };
                let _ = responder.send(status);
            }
            PeerApiRequest::Shutdown => {
                self.shutdown = true;
                // Shutdown all connection processing tasks
                for (_, handle) in &self.accepted_connections {
                    let _ = handle.tx.send(MainToConnMsg::Close).await;
                }
                for (_, handle) in &self.negotiating_connections {
                    let _ = handle.tx.send(MainToConnMsg::Close).await;
                }
                for (_, handle) in &self.connections {
                    let _ = handle.tx.send(MainToConnMsg::Close).await;
                }
            }
        }
    }

    // Route messages to their destination connections
    async fn deliver_envelopes(&mut self) {
        for envelope in self.fsm.drain_envelopes() {
            if let Some(conn_handle) = self.connections.get(&envelope.to) {
                debug!(
                    self.log,
                    "Sending {:?} to {}", envelope.msg, envelope.to
                );
                if let Err(e) = conn_handle
                    .tx
                    .send(MainToConnMsg::Msg(Msg::Fsm(envelope.msg)))
                    .await
                {
                    warn!(self.log, "Failed to send {:?}", e);
                }
            } else {
                warn!(self.log, "Missing connection to {}", envelope.to);
            }
        }
    }

    // Perform any operations required by a given `ApiOutput`, such as
    // persisting state, and then inform any callers (via outstanding responders)
    // of the result.
    async fn handle_api_output(&mut self, output: ApiOutput) {
        info!(self.log, "Fsm output = {:?}", output);
        match output {
            // Initialization is mutually exclusive
            ApiOutput::PeerInitialized | ApiOutput::RackInitComplete => {
                if let Some(responder) = self.init_responder.take() {
                    let _ = responder.send(Ok(()));
                }
                // TODO: Persistence
            }
            ApiOutput::RackSecret { secret, .. } => {
                // We only allow one outstanding request currently, so no
                // need to get the `request_id` from destructuring above
                if let Some(responder) = self.rack_secret_responder.take() {
                    let _ = responder.send(Ok(secret));
                }
            }
            ApiOutput::ShareDistributedToLearner => {
                // TODO: Persistence
            }
            ApiOutput::LearningCompleted => {
                // TODO: Persistence
            }
        }
        self.deliver_envelopes().await;
    }

    // Inform any callers (via outstanding responders) of errors.
    async fn handle_api_error(&mut self, err: ApiError) {
        warn!(self.log, "Fsm error= {:?}", err);
        // TODO: Match on specific errors and return to responders
    }

    // Handle messages from connection management tasks
    async fn on_conn_msg(&mut self, msg: ConnToMainMsg) {
        let unique_id = msg.handle_unique_id;
        match msg.msg {
            ConnToMainMsgInner::ConnectedServer {
                accepted_addr,
                addr,
                peer_id,
            } => {
                // Do we need to worry about checking unique_id here? Is it even
                // possible to have a race?
                let Some(accepted_handle) =
                   self.accepted_connections.remove(&accepted_addr) else
                {
                    warn!(
                        self.log,
                        "Missing AcceptedConnHandle";
                        "accepted_addr" => accepted_addr.to_string(),
                        "addr" => addr.to_string(),
                        "remote_peer_id" => peer_id.to_string()
                    );
                    panic!("Missing AcceptedConnHandle");
                };
                // Gracefully close any old tasks for this peer if they exist
                self.remove_established_connection(&peer_id).await;

                // Move from `accepted_connections` to `connections`
                let handle = PeerConnHandle {
                    handle: accepted_handle.handle,
                    tx: accepted_handle.tx,
                    addr,
                    unique_id: accepted_handle.unique_id,
                };
                self.connections.insert(peer_id.clone(), handle);
                if let Err(e) =
                    self.fsm.on_connected(Instant::now().into(), peer_id)
                {
                    // This can only be a failure to init the rack, so we
                    // log it as an error and not a warning. It is unrecoverable
                    // without a rack reset.
                    error!(self.log, "Error on connection:  {e}");
                } else {
                    self.deliver_envelopes().await;
                }
            }
            ConnToMainMsgInner::ConnectedClient { addr, peer_id } => {
                let handle =
                    self.negotiating_connections.remove(&addr).unwrap();
                self.connections.insert(peer_id.clone(), handle);
                if let Err(e) =
                    self.fsm.on_connected(Instant::now().into(), peer_id)
                {
                    // This can only be a failure to init the rack, so we
                    // log it as an error and not a warning. It is unrecoverable
                    // without a rack reset.
                    error!(self.log, "Error on connection:  {e}");
                } else {
                    self.deliver_envelopes().await;
                }
            }
            ConnToMainMsgInner::Disconnected { peer_id } => {
                warn!(self.log, "peer disconnected {}", peer_id);
                self.connections.remove(&peer_id);
                self.fsm.on_disconnected(&peer_id);
            }
            ConnToMainMsgInner::Received { from, msg } => {
                match self.fsm.handle_msg(Instant::now().into(), from, msg) {
                    Ok(None) => self.deliver_envelopes().await,
                    Ok(Some(api_output)) => {
                        self.handle_api_output(api_output).await
                    }
                    Err(err) => self.handle_api_error(err).await,
                }
            }
            ConnToMainMsgInner::FailedServerHandshake {
                addr: SocketAddrV6,
            } => {}
        }
    }

    async fn manage_connections(&mut self, peers: BTreeSet<SocketAddrV6>) {
        if peers == self.peers {
            return;
        }

        let peers_to_remove: BTreeSet<_> =
            self.peers.difference(&peers).cloned().collect();
        let new_peers: BTreeSet<_> =
            peers.difference(&self.peers).cloned().collect();

        self.peers = peers;

        // Start a new client for each peer that has an addr < self.config.addr
        for addr in new_peers {
            if addr < self.config.addr {
                self.handle_unique_id_counter += 1;
                let handle = spawn_client(
                    self.handle_unique_id_counter,
                    self.config.id.clone(),
                    self.config.addr.clone(),
                    addr.clone(),
                    &self.log,
                    self.conn_tx.clone(),
                )
                .await;
                self.negotiating_connections.insert(addr, handle);
            }
        }

        // Remove each peer that we no longer need a connection to
        for addr in peers_to_remove {
            self.remove_peer(addr).await;
        }
    }

    async fn remove_accepted_connection(&mut self, addr: &SocketAddrV6) {
        if let Some(handle) = self.accepted_connections.remove(&addr) {
            // The connection has not yet completed its handshake
            let _ = handle.tx.send(MainToConnMsg::Close).await;
        }
    }

    async fn remove_peer(&mut self, addr: SocketAddrV6) {
        if let Some(handle) = self.negotiating_connections.remove(&addr) {
            // The connection has not yet completed its handshake
            let _ = handle.tx.send(MainToConnMsg::Close).await;
        } else {
            // Do we have an established connection?
            if let Some((id, handle)) =
                self.connections.iter().find(|(_, handle)| handle.addr == addr)
            {
                let _ = handle.tx.send(MainToConnMsg::Close).await;
                // probably a better way to avoid borrowck issues
                let id = id.clone();
                self.connections.remove(&id);
            }
        }
    }

    async fn remove_established_connection(&mut self, peer_id: &Baseboard) {
        if let Some(handle) = self.connections.remove(peer_id) {
            // Gracefully stop the task
            let _ = handle.tx.send(MainToConnMsg::Close).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slog::Drain;
    use tokio::time::sleep;

    fn initial_members() -> BTreeSet<Baseboard> {
        [("a", "1"), ("b", "1"), ("c", "1")]
            .iter()
            .map(|(id, model)| {
                Baseboard::new_pc(id.to_string(), model.to_string())
            })
            .collect()
    }

    fn initial_config() -> Vec<Config> {
        initial_members()
            .into_iter()
            .enumerate()
            .map(|(i, id)| Config {
                id,
                addr: format!("[::1]:3333{}", i).parse().unwrap(),
                time_per_tick: Duration::from_millis(20),
                learn_timeout: Duration::from_secs(5),
                rack_init_timeout: Duration::from_secs(10),
                rack_secret_request_timeout: Duration::from_secs(5),
            })
            .collect()
    }

    fn log() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[tokio::test]
    async fn basic_3_peers() {
        let log = log();
        let config = initial_config();
        let (mut peer0, handle0) = Peer::new(config[0].clone(), &log);
        let (mut peer1, handle1) = Peer::new(config[1].clone(), &log);
        let (mut peer2, handle2) = Peer::new(config[2].clone(), &log);

        let jh0 = tokio::spawn(async move {
            peer0.run().await;
        });
        let jh1 = tokio::spawn(async move {
            peer1.run().await;
        });
        let jh2 = tokio::spawn(async move {
            peer2.run().await;
        });

        // Inform each peer about the known addresses
        let addrs: BTreeSet<_> =
            config.iter().map(|c| c.addr.clone()).collect();
        for handle in [&handle0, &handle1, &handle2] {
            let _ = handle.load_peer_addresses(addrs.clone()).await;
        }

        let rack_uuid = RackUuid(Uuid::new_v4());
        let output = handle0.init_rack(rack_uuid, initial_members()).await;
        println!("output = {:?}", output);

        let status = handle0.get_status().await;
        println!("status = {:?}", status);

        let output = handle0.load_rack_secret().await.unwrap();
        println!("{:?}", output);
        let output = handle1.load_rack_secret().await.unwrap();
        println!("{:?}", output);
        let output = handle2.load_rack_secret().await.unwrap();
        println!("{:?}", output);

        // load the rack secret a second time on peer0
        let output = handle0.load_rack_secret().await.unwrap();
        println!("{:?}", output);

        for handle in [&handle0, &handle1, &handle2] {
            let err = handle.shutdown().await;
            println!("shutdown err = {:?}", err);
        }

        // Wait for the peer tasks to stop
        for jh in [jh0, jh1, jh2] {
            let _ = jh.await;
        }
    }
}
