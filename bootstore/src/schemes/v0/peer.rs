// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entrypoint of the v0 scheme for use by bootstrap agent

use super::messages::Identify;
use super::{ApiError, Config as FsmConfig, Envelope, Fsm, Msg, Output};
use crate::schemes::Hello;
use crate::trust_quorum::RackSecret;
use bytes::Buf;
use derive_more::From;
use serde::Serialize;
use sled_hardware::Baseboard;
use slog::{debug, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Instant};
use uuid::Uuid;

const CONNECTION_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const CONN_BUF_SIZE: usize = 512 * 1024;
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_HEADER_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct Config {
    id: Baseboard,
    addr: SocketAddrV6,
    time_per_tick: Duration,
    learn_timeout: Duration,
    rack_init_timeout: Duration,
    rack_secret_request_timeout: Duration,
}

// A handle to a task managing a connection to a peer
struct PeerConnHandle {
    pub handle: JoinHandle<()>,
    pub tx: mpsc::Sender<MainToConnMsg>,
    pub addr: SocketAddrV6,
    // This is used to differentiate stale `ConnToMainMsg`s from cancelled tasks
    // with the same addr from each other
    pub unique_id: u64,
}

// A handle to a task of an accepted socket, pre-handshake
struct AcceptedConnHandle {
    pub handle: JoinHandle<()>,
    pub tx: mpsc::Sender<MainToConnMsg>,
    pub addr: SocketAddrV6,
    // This is used to differentiate stale `ConnToMainMsg`s from cancelled tasks
    // with the same addr from each other
    pub unique_id: u64,
}

// Serialize and write `msg` into `buf`, prefixed by a 4-byte big-endian size header
//
// Return the total amount of data written into `buf` including the 4-byte header
fn write_framed<T: Serialize + ?Sized>(
    msg: &T,
) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
    let mut cursor = Cursor::new(vec![]);
    // Write a size placeholder
    cursor.write(&[0u8; FRAME_HEADER_SIZE]);
    cursor.set_position(FRAME_HEADER_SIZE as u64);
    ciborium::into_writer(msg, &mut cursor)?;
    let size: u32 =
        (cursor.position() - FRAME_HEADER_SIZE as u64).try_into().unwrap();
    let mut buf = cursor.into_inner();
    buf[0..FRAME_HEADER_SIZE].copy_from_slice(&size.to_be_bytes());
    Ok(buf)
}

// Decode the 4-byte big-endian frame size header
fn read_frame_size(buf: [u8; FRAME_HEADER_SIZE]) -> usize {
    u32::from_be_bytes(buf) as usize
}

#[derive(Debug, From)]
enum HandshakeError {
    Serialization(ciborium::ser::Error<std::io::Error>),
    Deserialization(ciborium::de::Error<std::io::Error>),
    Io(tokio::io::Error),
    UnsupportedScheme,
    UnsupportedVersion,
    Timeout,
}

// Perform scheme/version negotiation and exchange peer_ids for scheme v0
async fn perform_handshake(
    sock: TcpStream,
    local_peer_id: &Baseboard,
    local_addr: SocketAddrV6,
    read_buf: &mut [u8],
    log: &Logger,
) -> Result<(OwnedReadHalf, OwnedWriteHalf, Identify), HandshakeError> {
    let (mut read_sock, mut write_sock) = sock.into_split();

    // Serialize and write the handshake messages into `write_buf`
    let mut hello_cursor = Cursor::new(Hello::default().serialize());
    let identify = write_framed(&Identify {
        id: local_peer_id.clone(),
        addr: local_addr,
    })?;
    let mut identify_cursor = Cursor::new(&identify);

    let handshake_start = Instant::now();

    // Read `Hello` and the frame size of `Identify`
    let initial_read = Hello::serialized_size() + FRAME_HEADER_SIZE;

    let mut total_read = 0;
    let mut identify_len = 0;
    let mut identify: Option<Identify> = None;

    loop {
        let timeout =
            KEEPALIVE_TIMEOUT.saturating_sub(Instant::now() - handshake_start);

        let end = initial_read + identify_len;

        let hello_written = !hello_cursor.has_remaining();

        if identify.is_some() && !identify_cursor.has_remaining() {
            return Ok((read_sock, write_sock, identify.unwrap()));
        }

        tokio::select! {
            _ = sleep(timeout) => {
                return Err(HandshakeError::Timeout);
            }
            _ = write_sock.writable(), if identify_cursor.has_remaining() => {
                if hello_cursor.has_remaining() {
                    write_sock.write_buf(&mut hello_cursor).await;
                } else {
                    write_sock.write_buf(&mut identify_cursor).await;
                }
            }
            res  = read_sock.read(&mut read_buf[total_read..end]), if identify.is_none() => {
                let n = res?;
                total_read += n;
                if total_read < initial_read {
                    continue;
                }
                if total_read == initial_read {
                    let hello =
                        Hello::from_bytes(&read_buf[..Hello::serialized_size()]).unwrap();
                    if hello.scheme != 0 {
                        return Err(HandshakeError::UnsupportedScheme);
                    }
                    if hello.version != 0 {
                        return Err(HandshakeError::UnsupportedVersion);
                    }
                    identify_len = read_frame_size(
                        read_buf[Hello::serialized_size()..initial_read]
                            .try_into()
                            .unwrap(),
                    );
                } else {
                    if total_read == end {
                        identify = Some(
                            ciborium::from_reader(&read_buf[initial_read..end])?
                        );
                    }
                }
            }
        }
    }
}

// Spawn a task that maintains a client connection to a peer
async fn spawn_client(
    unique_id: u64,
    my_peer_id: Baseboard,
    my_addr: SocketAddrV6,
    addr: SocketAddrV6,
    log: &Logger,
    main_tx: mpsc::Sender<ConnToMainMsg>,
) -> PeerConnHandle {
    // Create a channel for sending `MainToConnMsg`s to this connection task
    let (tx, rx) = mpsc::channel(2);
    let log = log.clone();

    let handle = tokio::spawn(async move {
        let mut read_buf = vec![0u8; CONN_BUF_SIZE];

        loop {
            let sock = match TcpStream::connect(addr).await {
                Ok(sock) => sock,
                Err(err) => {
                    // TODO: Throttle this?
                    warn!(log, "Failed to connect"; "addr" => addr.to_string());
                    // TODO: Sleep
                    sleep(CONNECTION_RETRY_TIMEOUT).await;
                    continue;
                }
            };

            info!(log, "Connected to peer"; "addr" => addr.to_string());

            let (read_sock, write_sock, identify) = match perform_handshake(
                sock,
                &my_peer_id,
                my_addr,
                &mut read_buf,
                &log,
            )
            .await
            {
                Ok(val) => val,
                Err(e) => {
                    warn!(log, "Handshake error: {:?}", e; "addr" => addr.to_string());
                    sleep(CONNECTION_RETRY_TIMEOUT).await;
                    continue;
                }
            };

            // Inform the main task that we have connected to a peer
            main_tx
                .send(ConnToMainMsg {
                    handle_unique_id: unique_id,
                    msg: ConnToMainMsgInner::ConnectedClient {
                        addr: addr.clone(),
                        peer_id: identify.id.clone(),
                    },
                })
                .await;

            // Test loopy
            loop {
                info!(
                    log,
                    "Still connected to addr: {}",
                    identify.addr.clone()
                );
                sleep(CONNECTION_RETRY_TIMEOUT).await;
            }

            /*loop {
                tokio::select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            MainToConnMsg::Close => return;
                            MainToConnMsg::Msg(msg) => {
                            }
                        }
                    }

                    // Read some data from the receive side

                    // Write some data
                    _ = write_sock.write_buf(&mut buf), if buf.has_remaining() => {
                    }
                }
            }
            */
        }
    });
    PeerConnHandle { handle, tx, addr, unique_id }
}

// Spawn a task that handles accepted connections from a peer
async fn spawn_server(
    unique_id: u64,
    my_peer_id: Baseboard,
    my_addr: SocketAddrV6,
    addr: SocketAddrV6,
    sock: TcpStream,
    main_tx: mpsc::Sender<ConnToMainMsg>,
    log: &Logger,
) -> AcceptedConnHandle {
    // Create a channel for sending `MainToConnMsg`s to this connection task
    let (tx, rx) = mpsc::channel(2);
    let log = log.clone();
    let handle = tokio::spawn(async move {
        let mut read_buf = vec![0u8; CONN_BUF_SIZE];

        let (read_sock, write_sock, identify) = match perform_handshake(
            sock,
            &my_peer_id,
            my_addr,
            &mut read_buf,
            &log,
        )
        .await
        {
            Ok(val) => val,
            Err(e) => {
                warn!(log, "Handshake error: {:?}", e; "addr" => addr.to_string());
                // This is a server so we bail and wait for a new connection
                // We must inform the main task so it can clean up any metadata.
                main_tx
                    .send(ConnToMainMsg {
                        handle_unique_id: unique_id,
                        msg: ConnToMainMsgInner::FailedServerHandshake { addr },
                    })
                    .await;
                return;
            }
        };

        // Inform the main task that we have connected to a peer
        main_tx
            .send(ConnToMainMsg {
                handle_unique_id: unique_id,
                msg: ConnToMainMsgInner::ConnectedServer {
                    accepted_addr: addr,
                    addr: identify.addr.clone(),
                    peer_id: identify.id.clone(),
                },
            })
            .await;

        // Test loopy
        loop {
            info!(log, "Still connected to addr: {}", identify.addr.clone());
            sleep(CONNECTION_RETRY_TIMEOUT).await;
        }
    });
    AcceptedConnHandle { handle, tx, addr, unique_id }
}

// Messages sent from connection managing tasks to the main peer task
//
// We include `handle_unique_id` to differentiate which task they come from so
// we can exclude requests from tasks that have been cancelled or have been told
// to shutdown.
#[derive(Debug, PartialEq)]
struct ConnToMainMsg {
    handle_unique_id: u64,
    msg: ConnToMainMsgInner,
}

#[derive(Debug, PartialEq)]
enum ConnToMainMsgInner {
    ConnectedServer {
        accepted_addr: SocketAddrV6,
        addr: SocketAddrV6,
        peer_id: Baseboard,
    },
    ConnectedClient {
        addr: SocketAddrV6,
        peer_id: Baseboard,
    },
    Disconnected {
        peer_id: Baseboard,
    },
    Forward(Envelope),
    FailedServerHandshake {
        addr: SocketAddrV6,
    },
}

#[derive(Debug, PartialEq)]
enum MainToConnMsg {
    Close,
    Msg(Msg),
}

// An error response from a `PeerRequest`
#[derive(Debug, PartialEq)]
pub enum PeerRequestError {
    // An `Init_` or `LoadRackSecret` request is already outstanding
    // We only allow one at a time.
    RequestAlreadyPending,

    // An error returned by the Fsm API
    Fsm(ApiError),
}

/// A request sent to the `Peer` task from the `PeerHandle`
pub enum PeerApiRequest {
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
    tx: mpsc::Sender<PeerApiRequest>,
}

// Some stats useful for operational purposes
#[derive(Debug, Default, Clone)]
pub struct Stats {
    server_accepted_connections: u64,
    server_closed_connections: u64,
    client_connections: u64,
    client_disconnections: u64,
    peer_updates: u64,
    failed_handshakes: u64,
}

#[derive(Debug, Clone)]
pub struct Status {
    stats: Stats,
    peers: BTreeMap<SocketAddrV6, PeerStatus>,
    fsm_state: String,
}

#[derive(Debug, Clone)]
pub enum PeerStatus {
    Negotiating,
    Connected(Baseboard),
    Disconnected,
}

/// A peer in the bootstore protocol
pub struct Peer {
    config: Config,
    fsm: Fsm,
    peers: BTreeSet<SocketAddrV6>,
    stats: Stats,
    handle_unique_id_counter: u64,

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
                stats: Stats::default(),
                handle_unique_id_counter: 0,
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
        let listener = TcpListener::bind(&self.config.addr).await.unwrap();
        loop {
            tokio::select! {
                res = listener.accept() => self.on_accept(res).await,
                Some(request) = self.rx.recv() => {
                    self.on_api_request(request).await;
                }
                Some(msg) = self.conn_rx.recv() => self.on_conn_msg(msg).await,
                _ = interval.tick() => {
                    let output = self.fsm.tick();
                    self.handle_output(output);
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
                if self.init_responder.is_some() {
                    let _ = responder
                        .send(Err(PeerRequestError::RequestAlreadyPending));
                    return;
                }
                self.init_responder = Some(responder);
                let output = self.fsm.init_rack(rack_uuid, initial_membership);
                self.handle_output(output);
            }
            PeerApiRequest::InitLearner { responder } => {
                if self.init_responder.is_some() {
                    let _ = responder
                        .send(Err(PeerRequestError::RequestAlreadyPending));
                    return;
                }
                self.init_responder = Some(responder);
                let output = self.fsm.init_learner();
                self.handle_output(output);
            }
            PeerApiRequest::LoadRackSecret { responder } => {
                if self.rack_secret_responder.is_some() {
                    let _ = responder
                        .send(Err(PeerRequestError::RequestAlreadyPending));
                    return;
                }
                self.rack_secret_responder = Some(responder);
                let output = self.fsm.load_rack_secret();
                self.handle_output(output);
            }
            PeerApiRequest::PeerAddresses(peers) => {
                info!(self.log, "Updated Peer Addresses: {:?}", peers);
                self.manage_connections(peers).await;
            }
        }
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
                self.remove_established_connection(&peer_id);

                // Move from `accepted_connections` to `connections`
                let handle = PeerConnHandle {
                    handle: accepted_handle.handle,
                    tx: accepted_handle.tx,
                    addr,
                    unique_id: accepted_handle.unique_id,
                };
                self.connections.insert(peer_id, handle);
            }
            ConnToMainMsgInner::ConnectedClient { addr, peer_id } => {}
            ConnToMainMsgInner::Disconnected { peer_id: Baseboard } => {}
            ConnToMainMsgInner::Forward(envelope) => {}
            ConnToMainMsgInner::FailedServerHandshake {
                addr: SocketAddrV6,
            } => {}
        }
    }

    fn handle_output(&mut self, output: Output) {}

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

        tokio::spawn(async move {
            peer0.run().await;
        });
        tokio::spawn(async move {
            peer1.run().await;
        });
        tokio::spawn(async move {
            peer2.run().await;
        });

        let addrs: BTreeSet<_> =
            config.iter().map(|c| c.addr.clone()).collect();
        for peer in [handle0, handle1, handle2] {
            peer.tx.send(PeerApiRequest::PeerAddresses(addrs.clone())).await;
        }

        sleep(Duration::from_secs(10)).await;
    }
}
