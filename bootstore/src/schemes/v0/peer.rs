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
use slog::{info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Instant};
use uuid::Uuid;

const CONNECTION_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const CONN_BUF_SIZE: usize = 512 * 1024;
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

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

// A handle to a task managing a connection to a peer
struct PeerConnHandle {
    handle: JoinHandle<()>,
    tx: mpsc::Sender<MainToConnMsg>,
    addr: SocketAddrV6,
}

// Serialize and write `msg` into `buf`, prefixed by a 4-byte big-endian size header
fn write_framed<T: Serialize + ?Sized>(
    msg: &T,
    buf: &mut [u8],
) -> Result<usize, bcs::Error> {
    let size = bcs::serialized_size(msg)?;
    if size + 4 > buf.len() {
        return Err(bcs::Error::ExceededMaxLen(size + 4));
    }
    let size: u32 = size.try_into().unwrap();
    buf[0..4].copy_from_slice(&size.to_be_bytes());
    bcs::serialize_into(&mut &mut buf[4..], msg).map(|_| size as usize)
}

// Decode the 4-byte big-endian frame size header
fn read_frame_size(buf: [u8; 4]) -> usize {
    u32::from_be_bytes(buf) as usize
}

#[derive(Debug, From)]
enum HandshakeError {
    Bcs(bcs::Error),
    Io(tokio::io::Error),
    UnsupportedScheme,
    UnsupportedVersion,
    Timeout,
}

// Perform scheme/version negotiation and exchange peer_ids for scheme v0
async fn perform_handshake(
    sock: TcpStream,
    local_peer_id: &Baseboard,
    write_buf: &mut [u8],
    read_buf: &mut [u8],
) -> Result<(OwnedReadHalf, OwnedWriteHalf, Baseboard), HandshakeError> {
    let (mut read_sock, mut write_sock) = sock.into_split();

    // Serialize and write the handshake messages into `write_buf`
    Hello::default().serialize_into(write_buf).unwrap();
    let identify_size = write_framed(
        &Identify(local_peer_id.clone()),
        &mut write_buf[Hello::serialized_size()..],
    )
    .unwrap();

    let handshake_start = Instant::now();
    let mut buf =
        Cursor::new(&write_buf[..Hello::serialized_size() + identify_size]);

    // Read `Hello` and the frame size of `Identify`
    let initial_read = Hello::serialized_size() + 4;

    let mut total_read = 0;
    let mut identify_len = 0;

    loop {
        let timeout =
            KEEPALIVE_TIMEOUT.saturating_sub(Instant::now() - handshake_start);

        let end = initial_read + identify_len;

        tokio::select! {
            _ = sleep(timeout) => {
                return Err(HandshakeError::Timeout);
            }
            _ = write_sock.write_buf(&mut buf), if buf.has_remaining() => {
            }
            res  = read_sock.read(&mut read_buf[total_read..end]) => {
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
                    total_read += n;
                    if total_read == end {
                        let identify: Identify =
                            bcs::from_bytes(&read_buf[total_read..end])?;
                        return Ok((read_sock, write_sock, identify.0));
                    }
                }
            }
        }
    }
}

// Spawn a task that maintains a client connection to a peer
async fn spawn_client(
    my_peer_id: Baseboard,
    addr: SocketAddrV6,
    log: &Logger,
) -> PeerConnHandle {
    // Create a channel for sending `MainToConnMsg`s to this connection task
    let (tx, rx) = mpsc::channel(2);
    let log = log.clone();

    // Create a write buffer, and read buffer
    let mut write_buf = vec![0u8; CONN_BUF_SIZE];
    let mut read_buf = vec![0u8; CONN_BUF_SIZE];

    let handle = tokio::spawn(async move {
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

            let (read_sock, write_sock, remote_peer_id) =
                match perform_handshake(
                    sock,
                    &my_peer_id,
                    &mut write_buf,
                    &mut read_buf,
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

            // Test loopy
            loop {
                info!(log, "Still connected to addr: {}", addr);
                sleep(CONNECTION_RETRY_TIMEOUT).await;
            }

            /* loop {
                    tokio::select! {
                        // Receive a message to be sent to the peer
                        Some(msg) = rx.recv() => {
                        }

                        // Read some data from the receive side

                        // Write some data
                    }
                }
            */
        }
    });
    PeerConnHandle { handle, tx, addr }
}

// Spawn a task that handles accepted connections from a peer
async fn spawn_server(addr: SocketAddrV6, sock: TcpStream) -> PeerConnHandle {
    let (tx, rx) = mpsc::channel(2);
    let addr2 = addr.clone();
    let handle = tokio::spawn(async move {});
    PeerConnHandle { handle, tx, addr }
}

// Messages sent from connection managing tasks to the main peer task
enum ConnToMainMsg {
    Connected { addr: SocketAddrV6, peer_id: Baseboard },
    Disconnected { peer_id: Baseboard },
    Forward(Envelope),
}

enum MainToConnMsg {
    Close,
    Msg(Msg),
}

// An error response from a `PeerRequest`
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

/// A peer in the bootstore protocol
pub struct Peer {
    config: Config,
    fsm: Fsm,
    peers: BTreeSet<SocketAddrV6>,

    // Connections that have not yet completed handshakes via `Hello` and
    // `Identify` messages.
    negotiating_connections: BTreeMap<SocketAddrV6, PeerConnHandle>,

    // Active connections participating in scheme v0
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
                res = listener.accept() => {
                    // If we already have a connection, we log this, abort the
                    // existing connection task, and add the new one.
                    //let (sock, addr) = res?;
                }
                Some(request) = self.rx.recv() => {
                    match request {
                        PeerApiRequest::InitRack {
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
                        PeerApiRequest::InitLearner{responder} => {
                            if self.init_responder.is_some() {
                                let _ = responder.send(Err(PeerRequestError::RequestAlreadyPending));
                                continue;
                            }
                            self.init_responder = Some(responder);
                            let output = self.fsm.init_learner();
                            self.handle_output(output);
                        }
                        PeerApiRequest::LoadRackSecret{responder} => {
                            if self.rack_secret_responder.is_some() {
                                let _ = responder.send(Err(PeerRequestError::RequestAlreadyPending));
                                continue;
                            }
                            self.rack_secret_responder = Some(responder);
                            let output = self.fsm.load_rack_secret();
                            self.handle_output(output);
                        }
                        PeerApiRequest::PeerAddresses(peers) => {
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

    async fn manage_connections(&mut self, peers: BTreeSet<SocketAddrV6>) {
        if peers == self.peers {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
