// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entrypoint of the v0 scheme for use by bootstrap agent

use super::peer_networking::{
    spawn_accepted_connection_management_task, spawn_connection_initiator_task,
    AcceptedConnHandle, ConnToMainMsg, ConnToMainMsgInner, MainToConnMsg, Msg,
    PeerConnHandle,
};
use super::storage::{NetworkConfig, PersistentFsmState};
use super::{ApiError, ApiOutput, Fsm, FsmConfig, RackUuid};
use crate::trust_quorum::RackSecret;
use camino::Utf8PathBuf;
use derive_more::From;
use sled_hardware::Baseboard;
use slog::{debug, error, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Instant, MissedTickBehavior};

#[derive(Debug, Clone)]
pub struct Config {
    pub id: Baseboard,
    pub addr: SocketAddrV6,
    pub time_per_tick: Duration,
    pub learn_timeout: Duration,
    pub rack_init_timeout: Duration,
    pub rack_secret_request_timeout: Duration,
    pub fsm_state_ledger_paths: Vec<Utf8PathBuf>,
    pub network_config_ledger_paths: Vec<Utf8PathBuf>,
}

/// An error response from a `NodeApiRequest`
#[derive(Error, Debug, From, PartialEq)]
pub enum NodeRequestError {
    #[error("only one request allowed at a time")]
    RequestAlreadyPending,

    #[error("Fsm error: {0}")]
    Fsm(ApiError),

    #[error("failed to receive response from node task: {0}")]
    Recv(oneshot::error::RecvError),

    #[error("failed to send request to node task")]
    Send,

    #[error(
        "Network config update failed because it is out of date. Attempted
        update generation: {attempted_update_generation}, current generation: 
        {current_generation}"
    )]
    StaleNetworkConfig {
        attempted_update_generation: u64,
        current_generation: u64,
    },
}

/// A request sent to the `Node` task from the `NodeHandle`
pub enum NodeApiRequest {
    /// Initialize a rack at the behest of RSS running on the same scrimlet as
    /// this node
    InitRack {
        rack_uuid: RackUuid,
        initial_membership: BTreeSet<Baseboard>,
        responder: oneshot::Sender<Result<(), NodeRequestError>>,
    },

    /// Initialize this `Node` as a learner.
    ///
    /// Return `()` from the responder when the learner has learned its share
    InitLearner { responder: oneshot::Sender<Result<(), NodeRequestError>> },

    /// Load the rack secret.
    ///
    /// This can only be successfully called when this `Node` has been
    /// initialized, either as initial member or learner who has learned its
    /// share.
    LoadRackSecret {
        responder: oneshot::Sender<Result<RackSecret, NodeRequestError>>,
    },

    /// Inform the `Node` of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    PeerAddresses(BTreeSet<SocketAddrV6>),

    /// Get the status of this node
    GetStatus { responder: oneshot::Sender<Status> },

    /// Shutdown the node's tokio tasks
    Shutdown,

    /// Update Network Config used to bring up the control plane
    UpdateNetworkConfig {
        config: NetworkConfig,
        responder: oneshot::Sender<Result<(), NodeRequestError>>,
    },

    /// Retrieve the current network config
    GetNetworkConfig { responder: oneshot::Sender<Option<NetworkConfig>> },
}

/// A handle for interacting with a `Node` task
#[derive(Debug, Clone)]
pub struct NodeHandle {
    tx: mpsc::Sender<NodeApiRequest>,
}

impl NodeHandle {
    /// Initialize a rack at the behest of RSS running on the same scrimlet as
    /// this Node
    pub async fn init_rack(
        &self,
        rack_uuid: RackUuid,
        initial_membership: BTreeSet<Baseboard>,
    ) -> Result<(), NodeRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::InitRack {
                rack_uuid,
                initial_membership,
                responder: tx,
            })
            .await
            .map_err(|_| NodeRequestError::Send)?;
        let res = rx.await?;
        res
    }

    /// Initialize this node  as a learner
    pub async fn init_learner(&self) -> Result<(), NodeRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::InitLearner { responder: tx })
            .await
            .map_err(|_| NodeRequestError::Send)?;
        let res = rx.await?;
        res
    }

    /// Load the rack secret.
    ///
    /// This can only be successfully called when a node has been initialized,
    /// either as initial member or learner who has learned its share.
    pub async fn load_rack_secret(
        &self,
    ) -> Result<RackSecret, NodeRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::LoadRackSecret { responder: tx })
            .await
            .map_err(|_| NodeRequestError::Send)?;
        let res = rx.await?;
        res
    }

    /// Inform the node of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    pub async fn load_peer_addresses(
        &self,
        addrs: BTreeSet<SocketAddrV6>,
    ) -> Result<(), NodeRequestError> {
        self.tx
            .send(NodeApiRequest::PeerAddresses(addrs))
            .await
            .map_err(|_| NodeRequestError::Send)?;
        Ok(())
    }

    /// Get the status of this node
    pub async fn get_status(&self) -> Result<Status, NodeRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::GetStatus { responder: tx })
            .await
            .map_err(|_| NodeRequestError::Send)?;
        let res = rx.await?;
        Ok(res)
    }

    /// Shutdown the node's tokio tasks
    pub async fn shutdown(&self) -> Result<(), NodeRequestError> {
        self.tx
            .send(NodeApiRequest::Shutdown)
            .await
            .map_err(|_| NodeRequestError::Send)?;
        Ok(())
    }

    /// Update network config needed for bringing up the control plane
    pub async fn update_network_config(
        &self,
        config: NetworkConfig,
    ) -> Result<(), NodeRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::UpdateNetworkConfig { config, responder: tx })
            .await
            .map_err(|_| NodeRequestError::Send)?;
        rx.await?
    }

    /// Retrieve the current network config
    pub async fn get_network_config(
        &self,
    ) -> Result<Option<NetworkConfig>, NodeRequestError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::GetNetworkConfig { responder: tx })
            .await
            .map_err(|_| NodeRequestError::Send)?;
        let res = rx.await?;
        Ok(res)
    }
}

#[derive(Debug, Clone)]
pub struct Status {
    pub fsm_ledger_generation: u64,
    pub network_config_ledger_generation: Option<u64>,
    pub fsm_state: &'static str,
    pub peers: BTreeSet<SocketAddrV6>,
    pub connections: BTreeMap<Baseboard, SocketAddrV6>,
    pub accepted_connections: BTreeSet<SocketAddrV6>,
    pub negotiating_connections: BTreeSet<SocketAddrV6>,
}

/// A node in the bootstore protocol
///
/// This is the primary type for running the lrtq. There is one node running on
/// each sled on the rack. Each node runs in a tokio task, with separate tokio
/// tasks for each connection to other peer nodes. Nodes drive the lrtq protocol
/// via control of an underlying  `Fsm`.
pub struct Node {
    fsm_ledger_generation: u64,
    network_config: Option<NetworkConfig>,
    config: Config,
    fsm: Fsm,
    peers: BTreeSet<SocketAddrV6>,
    handle_unique_id_counter: u64,

    // boolean set when a `NodeApiRequest::shutdown` is received
    shutdown: bool,

    // Connections that have been accepted, but where handshake has not
    // finished. At this point, we only know the client port of the connection,
    // and so cannot identify it as a peer `Node`.
    accepted_connections: BTreeMap<SocketAddrV6, AcceptedConnHandle>,

    // Connections initiated from this node that have not yet completed
    // handshakes via `Hello` and `Identify` messages.
    //
    // We only store the initiating (connecting) side, not accepting side here.
    initiating_connections: BTreeMap<SocketAddrV6, PeerConnHandle>,

    // Active connections participating in scheme v0
    //
    // This consists of both initiating and accepting connections that have
    // completed handshake and are now logically equivalent.
    //
    // Note that we key `established_connections` by `Baseboard` as that is
    // how the underlying `Fsm`'s identify peers. We didn't know the mapping
    // of Baseboard to TCP connection until handshake completed, which
    // is why `accepted_connections` and `initiating_connections` key by
    // `SocketAddrV6`.
    established_connections: BTreeMap<Baseboard, PeerConnHandle>,

    // Handle requests received from `PeerHandle`
    rx: mpsc::Receiver<NodeApiRequest>,

    // Used to respond to `InitRack` or `InitLearner` requests
    init_responder: Option<oneshot::Sender<Result<(), NodeRequestError>>>,

    // Used to respond to `LoadRackSecret` requests
    rack_secret_responder:
        Option<oneshot::Sender<Result<RackSecret, NodeRequestError>>>,

    log: Logger,

    // Handle messages received from connection tasks
    conn_rx: mpsc::Receiver<ConnToMainMsg>,

    // Clone for use by connection tasks to send to the main node task
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

impl Node {
    pub async fn new(config: Config, log: &Logger) -> (Node, NodeHandle) {
        // We only expect one outstanding request at a time for `Init_` or
        // `LoadRackSecret` requests, We can have one of those requests in
        // flight while allowing `PeerAddresses` updates. We also allow status
        // requests in parallel. Just leave some room.
        let (tx, rx) = mpsc::channel(10);

        // There are up to 31 sleds sending messages. These are mostly one at a
        // time for each sled, but we leave some extra room.
        let (conn_tx, conn_rx) = mpsc::channel(128);

        let id_str = config.id.to_string();
        let log = log.new(o!("component" => "bootstore", "peer_id" => id_str));

        let (fsm, ledger_generation) = PersistentFsmState::load(
            &log,
            config.fsm_state_ledger_paths.clone(),
            config.id.clone(),
            config.clone().into(),
        )
        .await;
        let network_config = NetworkConfig::load(
            &log,
            config.network_config_ledger_paths.clone(),
        )
        .await;

        (
            Node {
                fsm_ledger_generation: ledger_generation,
                network_config,
                config,
                fsm,
                peers: BTreeSet::new(),
                handle_unique_id_counter: 0,
                shutdown: false,
                accepted_connections: BTreeMap::new(),
                initiating_connections: BTreeMap::new(),
                established_connections: BTreeMap::new(),
                rx,
                init_responder: None,
                rack_secret_responder: None,
                log,
                conn_rx,
                conn_tx,
            },
            NodeHandle { tx },
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
                }
            }
            self.deliver_envelopes().await;
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
                    warn!(self.log, "Got connection from IPv4 address {addr}");
                    return;
                };
                // Remove any existing connection
                self.remove_accepted_connection(&addr).await;
                info!(self.log, "Accepted connection from {addr}");
                self.handle_unique_id_counter += 1;
                let handle = spawn_accepted_connection_management_task(
                    self.handle_unique_id_counter,
                    self.config.id.clone(),
                    self.config.addr,
                    addr,
                    sock,
                    self.conn_tx.clone(),
                    &self.log,
                )
                .await;
                self.accepted_connections.insert(addr, handle);
            }
            Err(err) => {
                error!(self.log, "Failed to accept a connection: {err:?}");
            }
        }
    }

    // Handle API requests from the `NodeHandle`
    async fn on_api_request(&mut self, request: NodeApiRequest) {
        match request {
            NodeApiRequest::InitRack {
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
                        .send(Err(NodeRequestError::RequestAlreadyPending));
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
            NodeApiRequest::InitLearner { responder } => {
                info!(self.log, "InitLearner started");
                if self.init_responder.is_some() {
                    let _ = responder
                        .send(Err(NodeRequestError::RequestAlreadyPending));
                    return;
                }
                if let Err(err) = self.fsm.init_learner(Instant::now().into()) {
                    let _ = responder.send(Err(err.into()));
                } else {
                    self.init_responder = Some(responder);
                    self.deliver_envelopes().await;
                }
            }
            NodeApiRequest::LoadRackSecret { responder } => {
                info!(self.log, "LoadRackSecret started");
                if self.rack_secret_responder.is_some() {
                    let _ = responder
                        .send(Err(NodeRequestError::RequestAlreadyPending));
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
            NodeApiRequest::PeerAddresses(peers) => {
                info!(self.log, "Updated Peer Addresses: {peers:?}");
                self.manage_connections(peers).await;
            }
            NodeApiRequest::GetStatus { responder } => {
                let status = Status {
                    fsm_ledger_generation: self.fsm_ledger_generation,
                    network_config_ledger_generation: self
                        .network_config
                        .as_ref()
                        .map(|c| c.generation),
                    fsm_state: self.fsm.state().name(),
                    peers: self.peers.clone(),
                    connections: self
                        .established_connections
                        .iter()
                        .map(|(id, handle)| (id.clone(), handle.addr))
                        .collect(),
                    accepted_connections: self
                        .accepted_connections
                        .keys()
                        .cloned()
                        .collect(),
                    negotiating_connections: self
                        .initiating_connections
                        .keys()
                        .cloned()
                        .collect(),
                };
                let _ = responder.send(status);
            }
            NodeApiRequest::Shutdown => {
                info!(self.log, "Shutting down Node tokio tasks");
                self.shutdown = true;
                // Shutdown all connection processing tasks
                for (_, handle) in &self.accepted_connections {
                    let _ = handle.tx.send(MainToConnMsg::Close).await;
                }
                for (_, handle) in &self.initiating_connections {
                    let _ = handle.tx.send(MainToConnMsg::Close).await;
                }
                for (_, handle) in &self.established_connections {
                    let _ = handle.tx.send(MainToConnMsg::Close).await;
                }
            }
            NodeApiRequest::UpdateNetworkConfig { config, responder } => {
                let current_gen =
                    self.network_config.as_ref().map_or(0, |c| c.generation);
                info!(
                    self.log,
                    concat!(
                        "Attempting to update network config with ",
                        "generation: {}, current_generation: {}"
                    ),
                    config.generation,
                    current_gen,
                );
                if current_gen > config.generation {
                    error!(
                        self.log,
                        concat!(
                            "Attempted network config update with ",
                            "stale generation: attemped_update_generation: {}, ",
                            "current_generation: {}"
                        ),
                        config.generation,
                        current_gen,
                    );
                    let _ = responder.send(Err(
                        NodeRequestError::StaleNetworkConfig {
                            attempted_update_generation: config.generation,
                            current_generation: current_gen,
                        },
                    ));
                } else if current_gen == config.generation {
                    warn!(
                        self.log,
                        concat!(
                            "Not updating network config: generation ",
                            "{} is current"
                        ),
                        current_gen
                    );
                } else {
                    self.network_config = Some(config.clone());
                    NetworkConfig::save(
                        &self.log,
                        self.config.network_config_ledger_paths.clone(),
                        config,
                    )
                    .await;
                    // Broadacst the updated config. We only broadcast
                    // when we successfully update it so we don't trigger an
                    // endless broadcast storm.
                    self.broadcast_network_config(None).await;
                    let _ = responder.send(Ok(()));
                }
            }
            NodeApiRequest::GetNetworkConfig { responder } => {
                let _ = responder.send(self.network_config.clone());
            }
        }
    }

    // After we have updated our network config, we should send it out to all
    // peers, with the exception of the peer we received it from if this was not
    // a local update.
    async fn broadcast_network_config(
        &mut self,
        excluded_peer: Option<&Baseboard>,
    ) {
        // We only call this method when there has been an update. Otherwise we
        // have an invariant violation due to programmer error and should panic.
        let network_config = self.network_config.as_ref().unwrap();
        info!(
            self.log,
            "Broadcasting network config with generation {}",
            network_config.generation
        );
        for (id, handle) in self
            .established_connections
            .iter()
            .filter(|(id, _)| Some(*id) != excluded_peer)
        {
            debug!(
                self.log,
                "Sending network config with generation {} to {id}",
                network_config.generation
            );
            self.send_network_config(network_config.clone(), id, handle).await;
        }
    }

    // Send network config to a peer
    async fn send_network_config(
        &self,
        config: NetworkConfig,
        peer_id: &Baseboard,
        handle: &PeerConnHandle,
    ) {
        if let Err(e) =
            handle.tx.send(MainToConnMsg::Msg(Msg::NetworkConfig(config))).await
        {
            warn!(
                self.log,
                concat!(
                    "Failed to send network config to connection ",
                    "management task for {} {:?}"
                ),
                peer_id,
                e
            );
        }
    }

    // Route messages to their destination connections
    async fn deliver_envelopes(&mut self) {
        for envelope in self.fsm.drain_envelopes() {
            if let Some(conn_handle) =
                self.established_connections.get(&envelope.to)
            {
                debug!(
                    self.log,
                    "Sending {:?} to {}", envelope.msg, envelope.to
                );
                if let Err(e) = conn_handle
                    .tx
                    .send(MainToConnMsg::Msg(Msg::Fsm(envelope.msg)))
                    .await
                {
                    warn!(self.log, "Failed to send {e:?}");
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
        info!(self.log, "Fsm output = {output:?}");
        match output {
            // Initialization is mutually exclusive
            ApiOutput::PeerInitialized | ApiOutput::RackInitComplete => {
                if let Some(responder) = self.init_responder.take() {
                    let _ = responder.send(Ok(()));
                }
                self.fsm_ledger_generation = PersistentFsmState::save(
                    &self.log,
                    self.config.fsm_state_ledger_paths.clone(),
                    self.fsm_ledger_generation,
                    self.fsm.state().clone(),
                )
                .await;
            }
            ApiOutput::RackSecret { secret, .. } => {
                // We only allow one outstanding request currently, so no
                // need to get the `request_id` from destructuring above
                if let Some(responder) = self.rack_secret_responder.take() {
                    let _ = responder.send(Ok(secret));
                } else {
                    warn!(
                        self.log,
                        "Rack secret loaded, but no pending responder"
                    );
                }
            }
            ApiOutput::ShareDistributedToLearner => {
                self.fsm_ledger_generation = PersistentFsmState::save(
                    &self.log,
                    self.config.fsm_state_ledger_paths.clone(),
                    self.fsm_ledger_generation,
                    self.fsm.state().clone(),
                )
                .await;
            }
            ApiOutput::LearningCompleted => {
                if let Some(responder) = self.init_responder.take() {
                    let _ = responder.send(Ok(()));
                } else {
                    warn!(
                        self.log,
                        "Learning completed, but no pending responder"
                    );
                }
                self.fsm_ledger_generation = PersistentFsmState::save(
                    &self.log,
                    self.config.fsm_state_ledger_paths.clone(),
                    self.fsm_ledger_generation,
                    self.fsm.state().clone(),
                )
                .await;
            }
        }
    }

    // Inform any callers (via outstanding responders) of errors.
    async fn handle_api_error(&mut self, err: ApiError) {
        warn!(self.log, "Fsm error= {err:?}");
        match err {
            ApiError::AlreadyInitialized | ApiError::RackInitTimeout { .. } => {
                if let Some(responder) = self.init_responder.take() {
                    let _ = responder.send(Err(err.into()));
                }
            }
            ApiError::StillLearning
            | ApiError::NotInitialized
            | ApiError::RackSecretLoadTimeout
            | ApiError::RackInitFailed(_) => {
                if let Some(responder) = self.rack_secret_responder.take() {
                    let _ = responder.send(Err(err.into()));
                }
            }
            ApiError::FailedToReconstructRackSecret => {
                if let Some(responder) = self.rack_secret_responder.take() {
                    let _ = responder.send(Err(err.into()));
                }
            }
            // Nothing to do for these variants
            // We already loggged the error at the top of this method
            ApiError::FailedToDecryptExtraShares
            | ApiError::UnexpectedResponse { .. }
            | ApiError::ErrorResponseReceived { .. }
            | ApiError::InvalidShare { .. } => {}
        }
    }

    // Handle messages from connection management tasks
    async fn on_conn_msg(&mut self, msg: ConnToMainMsg) {
        let unique_id = msg.handle_unique_id;
        match msg.msg {
            ConnToMainMsgInner::ConnectedAcceptor {
                accepted_addr,
                addr,
                peer_id,
            } => {
                let Some(accepted_handle) =
                   self.accepted_connections.remove(&accepted_addr) else
                {
                    warn!(
                        self.log,
                        concat!(
                            "Missing AcceptedConnHandle: ",
                            "Stale ConnectedAcceptor msg"
                        );
                        "accepted_addr" => accepted_addr.to_string(),
                        "addr" => addr.to_string(),
                        "remote_peer_id" => peer_id.to_string()
                    );
                    return;
                };

                // Ignore the stale message if the unique_id doesn't match what
                // we have stored.
                if unique_id != accepted_handle.unique_id {
                    return;
                }

                // Gracefully close any old tasks for this peer if they exist
                self.remove_established_connection(&peer_id).await;

                // Move from `accepted_connections` to `established_connections`
                let handle = PeerConnHandle {
                    handle: accepted_handle.handle,
                    tx: accepted_handle.tx,
                    addr,
                    unique_id: accepted_handle.unique_id,
                };
                if let Some(network_config) = self.network_config.as_ref() {
                    self.send_network_config(
                        network_config.clone(),
                        &peer_id,
                        &handle,
                    )
                    .await;
                }

                self.established_connections.insert(peer_id.clone(), handle);
                if let Err(e) =
                    self.fsm.on_connected(Instant::now().into(), peer_id)
                {
                    // This can only be a failure to init the rack, so we
                    // log it as an error and not a warning. It is unrecoverable
                    // without a rack reset.
                    error!(self.log, "Error on connection: {e}");
                }
            }
            ConnToMainMsgInner::ConnectedInitiator { addr, peer_id } => {
                if let Some(handle) = self.initiating_connections.remove(&addr)
                {
                    // Ignore the stale message if the unique_id doesn't match what
                    // we have stored.
                    if unique_id != handle.unique_id {
                        return;
                    }

                    if let Some(network_config) = self.network_config.as_ref() {
                        self.send_network_config(
                            network_config.clone(),
                            &peer_id,
                            &handle,
                        )
                        .await;
                    }

                    self.established_connections
                        .insert(peer_id.clone(), handle);
                } else {
                    warn!(
                        self.log,
                        "Missing PeerConnHandle; Stale ConnectedInitiator msg";
                        "addr" => addr.to_string(),
                        "remote_peer_id" => peer_id.to_string()
                    );
                    return;
                }

                if let Err(e) =
                    self.fsm.on_connected(Instant::now().into(), peer_id)
                {
                    // This can only be a failure to init the rack, so we
                    // log it as an error and not a warning. It is unrecoverable
                    // without a rack reset. It's not an invariant violation
                    // though, so we don't panic.
                    error!(self.log, "Error on connection: {e}");
                }
            }
            ConnToMainMsgInner::Disconnected { peer_id } => {
                // Ignore the stale message if the unique_id doesn't match what
                // we have stored.
                if let Some(handle) = self.established_connections.get(&peer_id)
                {
                    if unique_id != handle.unique_id {
                        return;
                    }
                } else {
                    warn!(
                        self.log,
                        "Missing PeerConnHandle: Stale Disconnected msg";
                        "remote_peer_id" => peer_id.to_string()
                    );
                    return;
                }
                warn!(self.log, "peer disconnected {peer_id}");
                let handle =
                    self.established_connections.remove(&peer_id).unwrap();
                if peer_id < self.config.id {
                    // Put the connection handle back in initiating state
                    self.initiating_connections.insert(handle.addr, handle);
                }
                self.fsm.on_disconnected(&peer_id);
            }
            ConnToMainMsgInner::Received { from, msg } => {
                match self.fsm.handle_msg(Instant::now().into(), from, msg) {
                    Ok(None) => (),
                    Ok(Some(api_output)) => {
                        self.handle_api_output(api_output).await
                    }
                    Err(err) => self.handle_api_error(err).await,
                }
            }
            ConnToMainMsgInner::FailedAcceptorHandshake { addr } => {
                if let Some(handle) = self.accepted_connections.get(&addr) {
                    if handle.unique_id != unique_id {
                        return;
                    }
                }
                self.accepted_connections.remove(&addr);
            }
            ConnToMainMsgInner::ReceivedNetworkConfig { from, config } => {
                let current_gen =
                    self.network_config.as_ref().map_or(0, |c| c.generation);
                let generation = config.generation;
                info!(
                    self.log,
                    concat!(
                        "Received network config from {} with ",
                        "generation: {}, current generation: {}"
                    ),
                    from,
                    generation,
                    current_gen
                );
                if generation > current_gen {
                    self.network_config = Some(config.clone());
                    NetworkConfig::save(
                        &self.log,
                        self.config.network_config_ledger_paths.clone(),
                        config,
                    )
                    .await;
                    self.broadcast_network_config(Some(&from)).await;
                }
            }
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
                let handle = spawn_connection_initiator_task(
                    self.handle_unique_id_counter,
                    self.config.id.clone(),
                    self.config.addr,
                    addr,
                    &self.log,
                    self.conn_tx.clone(),
                )
                .await;
                info!(self.log, "Initiating connection to new peer: {addr}");
                self.initiating_connections.insert(addr, handle);
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
        if let Some(handle) = self.initiating_connections.remove(&addr) {
            // The connection has not yet completed its handshake
            info!(
                self.log,
                "Peer removed: deleting initiating connection";
                "remote_addr" => addr.to_string()
            );
            let _ = handle.tx.send(MainToConnMsg::Close).await;
        } else {
            // Do we have an established connection?
            if let Some((id, handle)) = self
                .established_connections
                .iter()
                .find(|(_, handle)| handle.addr == addr)
            {
                info!(
                    self.log,
                    "Peer removed: deleting established connection";
                    "remote_addr" => addr.to_string(),
                    "remote_peer_id" => id.to_string(),
                );
                let _ = handle.tx.send(MainToConnMsg::Close).await;
                // probably a better way to avoid borrowck issues
                let id = id.clone();
                self.established_connections.remove(&id);
            }
        }
    }

    async fn remove_established_connection(&mut self, peer_id: &Baseboard) {
        if let Some(handle) = self.established_connections.remove(peer_id) {
            // Gracefully stop the task
            let _ = handle.tx.send(MainToConnMsg::Close).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino_tempfile::Utf8TempDir;
    use slog::Drain;
    use tokio::time::sleep;
    use uuid::Uuid;

    fn initial_members() -> BTreeSet<Baseboard> {
        [("a", "1"), ("b", "1"), ("c", "1")]
            .iter()
            .map(|(id, model)| {
                Baseboard::new_pc(id.to_string(), model.to_string())
            })
            .collect()
    }

    fn initial_config(tempdir: &Utf8TempDir, port_start: u16) -> Vec<Config> {
        initial_members()
            .into_iter()
            .enumerate()
            .map(|(i, id)| {
                let fsm_file = format!("test-{i}-fsm-state-ledger");
                let network_file = format!("test-{i}-network-config-ledger");
                Config {
                    id,
                    addr: format!("[::1]:{}{}", port_start, i).parse().unwrap(),
                    time_per_tick: Duration::from_millis(20),
                    learn_timeout: Duration::from_secs(5),
                    rack_init_timeout: Duration::from_secs(10),
                    rack_secret_request_timeout: Duration::from_secs(1),
                    fsm_state_ledger_paths: vec![tempdir
                        .path()
                        .join(&fsm_file)],
                    network_config_ledger_paths: vec![tempdir
                        .path()
                        .join(&network_file)],
                }
            })
            .collect()
    }

    fn learner_id(n: usize) -> Baseboard {
        Baseboard::new_pc("learner".to_string(), n.to_string())
    }

    fn learner_config(
        tempdir: &Utf8TempDir,
        n: usize,
        port_start: u16,
    ) -> Config {
        let fsm_file = format!("test-learner-{n}-fsm-state-ledger");
        let network_file = format!("test-{n}-network-config-ledger");
        Config {
            id: learner_id(n),
            addr: format!("[::1]:{}{}", port_start, 3).parse().unwrap(),
            time_per_tick: Duration::from_millis(20),
            learn_timeout: Duration::from_secs(5),
            rack_init_timeout: Duration::from_secs(10),
            rack_secret_request_timeout: Duration::from_secs(1),
            fsm_state_ledger_paths: vec![tempdir.path().join(&fsm_file)],
            network_config_ledger_paths: vec![tempdir
                .path()
                .join(&network_file)],
        }
    }

    fn log() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[tokio::test]
    async fn basic_3_nodes() {
        let port_start = 3333;
        let tempdir = Utf8TempDir::new().unwrap();
        let log = log();
        let config = initial_config(&tempdir, port_start);
        let (mut node0, handle0) = Node::new(config[0].clone(), &log).await;
        let (mut node1, handle1) = Node::new(config[1].clone(), &log).await;
        let (mut node2, handle2) = Node::new(config[2].clone(), &log).await;

        let jh0 = tokio::spawn(async move {
            node0.run().await;
        });
        let jh1 = tokio::spawn(async move {
            node1.run().await;
        });
        let jh2 = tokio::spawn(async move {
            node2.run().await;
        });

        // Inform each node about the known addresses
        let mut addrs: BTreeSet<_> = config.iter().map(|c| c.addr).collect();
        for handle in [&handle0, &handle1, &handle2] {
            let _ = handle.load_peer_addresses(addrs.clone()).await;
        }

        let rack_uuid = RackUuid(Uuid::new_v4());
        handle0.init_rack(rack_uuid, initial_members()).await.unwrap();

        let status = handle0.get_status().await;
        println!("Status = {status:?}");

        // Ensure we can load the rack secret at all nodes
        handle0.load_rack_secret().await.unwrap();
        handle1.load_rack_secret().await.unwrap();
        handle2.load_rack_secret().await.unwrap();

        // load the rack secret a second time on node0
        handle0.load_rack_secret().await.unwrap();

        // Shutdown the node2 and make sure we can still load the rack
        // secret (threshold=2) at node0 and node1
        handle2.shutdown().await.unwrap();
        jh2.await.unwrap();
        handle0.load_rack_secret().await.unwrap();
        handle1.load_rack_secret().await.unwrap();

        // Add a learner node
        let learner_conf = learner_config(&tempdir, 1, port_start);
        let (mut learner, learner_handle) =
            Node::new(learner_conf.clone(), &log).await;
        let learner_jh = tokio::spawn(async move {
            learner.run().await;
        });
        // Inform the learner and node0 and node1 about all addresses including
        // the learner. This simulates DDM discovery
        addrs.insert(learner_conf.addr);
        let _ = learner_handle.load_peer_addresses(addrs.clone()).await;
        let _ = handle0.load_peer_addresses(addrs.clone()).await;
        let _ = handle1.load_peer_addresses(addrs.clone()).await;

        // Tell the learner to go ahead and learn its share.
        learner_handle.init_learner().await.unwrap();

        // Shutdown node1 and show that we can still load the rack secret at
        // node0 and the learner, because threshold=2 and it never changes.
        handle1.shutdown().await.unwrap();
        jh1.await.unwrap();
        handle0.load_rack_secret().await.unwrap();
        learner_handle.load_rack_secret().await.unwrap();

        // Now shutdown the learner and show that node0 cannot load the rack secret
        learner_handle.shutdown().await.unwrap();
        learner_jh.await.unwrap();
        handle0.load_rack_secret().await.unwrap_err();

        // Reload an node from persistent state and successfully reload the
        // rack secret.
        let (mut node1, handle1) = Node::new(config[1].clone(), &log).await;
        let jh1 = tokio::spawn(async move {
            node1.run().await;
        });
        let _ = handle1.load_peer_addresses(addrs.clone()).await;
        handle0.load_rack_secret().await.unwrap();

        // Add a second learner
        let peer0_gen =
            handle0.get_status().await.unwrap().fsm_ledger_generation;
        let peer1_gen =
            handle1.get_status().await.unwrap().fsm_ledger_generation;
        let learner_config = learner_config(&tempdir, 2, port_start);
        let (mut learner, learner_handle) =
            Node::new(learner_config.clone(), &log).await;
        let learner_jh = tokio::spawn(async move {
            learner.run().await;
        });

        // Inform the learner, node0, and node1 about all addresses including
        // the learner. This simulates DDM discovery
        addrs.insert(learner_config.addr);
        let _ = learner_handle.load_peer_addresses(addrs.clone()).await;
        let _ = handle0.load_peer_addresses(addrs.clone()).await;
        let _ = handle1.load_peer_addresses(addrs.clone()).await;

        // Tell the learner to go ahead and learn its share.
        learner_handle.init_learner().await.unwrap();

        // Get the new generation numbers
        let peer0_gen_new =
            handle0.get_status().await.unwrap().fsm_ledger_generation;
        let peer1_gen_new =
            handle1.get_status().await.unwrap().fsm_ledger_generation;

        // Ensure only one of the peers generation numbers gets bumped
        assert!(
            (peer0_gen_new == peer0_gen && peer1_gen_new == peer1_gen + 1)
                || (peer0_gen_new == peer0_gen + 1
                    && peer1_gen_new == peer1_gen)
        );

        // Wipe the learner ledger, restart the learner and instruct it to
        // relearn its share, and ensure that the neither generation number gets
        // bumped because persistence doesn't occur.
        learner_handle.shutdown().await.unwrap();
        learner_jh.await.unwrap();
        std::fs::remove_file(&learner_config.fsm_state_ledger_paths[0])
            .unwrap();
        let (mut learner, learner_handle) =
            Node::new(learner_config.clone(), &log).await;
        let learner_jh = tokio::spawn(async move {
            learner.run().await;
        });
        let _ = learner_handle.load_peer_addresses(addrs.clone()).await;
        learner_handle.init_learner().await.unwrap();
        let peer0_gen_new_2 =
            handle0.get_status().await.unwrap().fsm_ledger_generation;
        let peer1_gen_new_2 =
            handle1.get_status().await.unwrap().fsm_ledger_generation;

        // Ensure the peer's generation numbers don't get bumped. The learner
        // will ask the same sled for a share first, which it already handed
        // out.
        assert!(
            peer0_gen_new == peer0_gen_new_2
                && peer1_gen_new == peer1_gen_new_2
        );

        // Shutdown the new learner, node0, and node1
        learner_handle.shutdown().await.unwrap();
        learner_jh.await.unwrap();
        handle0.shutdown().await.unwrap();
        jh0.await.unwrap();
        handle1.shutdown().await.unwrap();
        jh1.await.unwrap();
    }

    #[tokio::test]
    async fn network_config() {
        let port_start = 4444;
        let tempdir = Utf8TempDir::new().unwrap();
        let log = log();
        let config = initial_config(&tempdir, port_start);
        let (mut node0, handle0) = Node::new(config[0].clone(), &log).await;
        let (mut node1, handle1) = Node::new(config[1].clone(), &log).await;
        let (mut node2, handle2) = Node::new(config[2].clone(), &log).await;

        let jh0 = tokio::spawn(async move {
            node0.run().await;
        });
        let jh1 = tokio::spawn(async move {
            node1.run().await;
        });
        let jh2 = tokio::spawn(async move {
            node2.run().await;
        });

        // Inform each node about the known addresses
        let mut addrs: BTreeSet<_> = config.iter().map(|c| c.addr).collect();
        for handle in [&handle0, &handle1, &handle2] {
            let _ = handle.load_peer_addresses(addrs.clone()).await;
        }

        // Ensure there is no network config at any of the nodes
        for handle in [&handle0, &handle1, &handle2] {
            assert_eq!(None, handle.get_network_config().await.unwrap());
        }

        // Update the network config at node0 and ensure it has taken effect
        let network_config = NetworkConfig {
            generation: 1,
            blob: b"Some network data".to_vec(),
        };
        handle0.update_network_config(network_config.clone()).await.unwrap();
        assert_eq!(
            Some(&network_config),
            handle0.get_network_config().await.unwrap().as_ref()
        );

        // Poll node1 and node2 until the network config update shows up
        // Timeout after 5 seconds
        const POLL_TIMEOUT: Duration = Duration::from_secs(5);
        let start = Instant::now();
        let mut node1_done = false;
        let mut node2_done = false;
        while !(node1_done && node2_done) {
            let timeout = POLL_TIMEOUT.saturating_sub(Instant::now() - start);
            tokio::select! {
                _ = sleep(timeout) => {
                    panic!("Network config not replicated");
                }
                res = handle1.get_network_config(), if !node1_done => {
                    if res.unwrap().as_ref() == Some(&network_config) {
                        node1_done = true;
                        continue;
                    }
                }
                res = handle2.get_network_config(), if !node2_done => {
                    if res.unwrap().as_ref() == Some(&network_config) {
                        node2_done = true;
                        continue;
                    }
                }
            }
        }

        // Bring a learner online
        let learner_conf = learner_config(&tempdir, 1, port_start);
        let (mut learner, learner_handle) =
            Node::new(learner_conf.clone(), &log).await;
        let learner_jh = tokio::spawn(async move {
            learner.run().await;
        });
        // Inform the learner and other nodes about all addresses including
        // the learner. This simulates DDM discovery.
        addrs.insert(learner_conf.addr);
        for handle in [&learner_handle, &handle0, &handle1, &handle2] {
            let _ = handle.load_peer_addresses(addrs.clone()).await;
        }

        // Poll the learner to ensure it gets the network config
        // Note that the learner doesn't even need to learn its share
        // for network config replication to work.
        let start = Instant::now();
        let mut done = false;
        while !done {
            let timeout = POLL_TIMEOUT.saturating_sub(Instant::now() - start);
            tokio::select! {
                _ = sleep(timeout) => {
                    panic!("Network config not replicated");
                }
                res = learner_handle.get_network_config() => {
                    if res.unwrap().as_ref() == Some(&network_config) {
                        done = true;
                    }
                }
            }
        }

        // Stop node0, bring it back online and ensure it still sees the config
        // at generation 1
        handle0.shutdown().await.unwrap();
        jh0.await.unwrap();
        let (mut node0, handle0) = Node::new(config[0].clone(), &log).await;
        let jh0 = tokio::spawn(async move {
            node0.run().await;
        });
        assert_eq!(
            Some(&network_config),
            handle0.get_network_config().await.unwrap().as_ref()
        );

        // Stop node0 again, update network config via node1, bring node0 back online,
        // and ensure all nodes see the latest configuration.
        let new_config = NetworkConfig {
            generation: 2,
            blob: b"Some more network data".to_vec(),
        };
        handle0.shutdown().await.unwrap();
        jh0.await.unwrap();
        handle1.update_network_config(new_config.clone()).await.unwrap();
        assert_eq!(
            Some(&new_config),
            handle1.get_network_config().await.unwrap().as_ref()
        );
        let (mut node0, handle0) = Node::new(config[0].clone(), &log).await;
        let jh0 = tokio::spawn(async move {
            node0.run().await;
        });
        let start = Instant::now();
        // These should all resolve instantly, so no real need for a select,
        // which is getting tedious.
        // We also want to repeatedly loop until all consistently have the same version
        // to give some assurance that the old version from node0 doesn't replicate
        'outer: loop {
            if Instant::now() - start > POLL_TIMEOUT {
                panic!("network config not replicated");
            }
            for h in [&handle0, &handle1, &handle2, &learner_handle] {
                if h.get_network_config().await.unwrap().as_ref()
                    != Some(&new_config)
                {
                    // We need to try again
                    continue 'outer;
                }
            }
            // Success
            break;
        }

        // Try to update node0 with an old config, and watch it fail
        let expected = Err(NodeRequestError::StaleNetworkConfig {
            attempted_update_generation: 1,
            current_generation: 2,
        });
        assert_eq!(
            handle0.update_network_config(network_config).await,
            expected
        );

        // Shut it all down
        for h in [handle0, handle1, handle2, learner_handle] {
            let _ = h.shutdown().await;
        }
        for jh in [jh0, jh1, jh2, learner_jh] {
            jh.await.unwrap();
        }
    }
}
