// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Async networking used by peer.rs

use super::messages::Identify;
use super::storage::NetworkConfig;
use super::Msg as FsmMsg;
use crate::schemes::Hello;
use bytes::Buf;
use derive_more::From;
use serde::{Deserialize, Serialize};
use sled_hardware_types::Baseboard;
use slog::{debug, error, info, o, warn, Logger};
use std::collections::VecDeque;
use std::io::Cursor;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Instant, MissedTickBehavior};

const CONN_BUF_SIZE: usize = 512 * 1024;
const CONNECTION_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const FRAME_HEADER_SIZE: usize = 4;
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const MSG_WRITE_QUEUE_CAPACITY: usize = 5;
const PING_INTERVAL: Duration = Duration::from_secs(1);
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

/// A superset of messages sent and received during an established connection
///
/// This does not include `Hello` and `Identify` messages, which are sent during
/// the handshake.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Msg {
    Ping,
    Fsm(FsmMsg),
    /// Message exchanged for reconciling network config used to bring up the
    /// control plane stored in the bootstore.
    NetworkConfig(NetworkConfig),
}

/// An error returned from an EstablishedConn
///
/// Also a great movie
#[derive(Debug)]
enum ConnErr {
    Retry,
    Close,
}

/// Messages sent from connection managing tasks to the main peer task
///
/// We include `handle_unique_id` to differentiate which task they come from so
/// we can exclude requests from tasks that have been cancelled or have been told
/// to shutdown.
#[derive(Debug, PartialEq)]
pub struct ConnToMainMsg {
    pub handle_unique_id: u64,
    pub msg: ConnToMainMsgInner,
}

#[derive(Debug, PartialEq)]
pub enum ConnToMainMsgInner {
    ConnectedAcceptor {
        accepted_addr: SocketAddrV6,
        addr: SocketAddrV6,
        peer_id: Baseboard,
    },
    ConnectedInitiator {
        addr: SocketAddrV6,
        peer_id: Baseboard,
    },
    Disconnected {
        peer_id: Baseboard,
    },
    Received {
        from: Baseboard,
        msg: FsmMsg,
    },
    FailedAcceptorHandshake {
        addr: SocketAddrV6,
    },
    ReceivedNetworkConfig {
        from: Baseboard,
        config: NetworkConfig,
    },
}

/// Messages sent from the main task to the connection managing tasks
#[derive(Debug, PartialEq)]
pub enum MainToConnMsg {
    Close,
    Msg(Msg),
}

// A handle to a task managing a connection to a peer
//
// This is either a negotiating (connecting client but not yet handshake
// complete), or established connection (handshake complete - whether connecting
// client or accepting server).
pub struct PeerConnHandle {
    pub handle: JoinHandle<()>,
    pub tx: mpsc::Sender<MainToConnMsg>,

    // The canonical IP:Port of the peer
    pub addr: SocketAddrV6,
    // This is used to differentiate stale `ConnToMainMsg`s from cancelled tasks
    // with the same addr from each other
    pub unique_id: u64,
}

// A handle to a task of an accepted socket, pre-handshake
pub struct AcceptedConnHandle {
    pub handle: JoinHandle<()>,
    pub tx: mpsc::Sender<MainToConnMsg>,

    // The canonical IP with ephemeral port of the peer
    pub addr: SocketAddrV6,
    // This is used to differentiate stale `ConnToMainMsg`s from cancelled tasks
    // with the same addr from each other
    pub unique_id: u64,
}

// Established connection management code running in its own task
struct EstablishedConn {
    peer_id: Baseboard,
    unique_id: u64,
    write_sock: OwnedWriteHalf,
    read_sock: OwnedReadHalf,
    main_tx: mpsc::Sender<ConnToMainMsg>,
    rx: mpsc::Receiver<MainToConnMsg>,
    log: Logger,
    read_buf: Box<[u8]>,
    total_read: usize,

    // Used for managing inactivity timeouts for the connection
    last_received_msg: Instant,

    // Keep a queue to write serialized messages into. We limit the queue
    // size, and if it gets exceeded it means the peer at the other
    // end isn't pulling data out fast enough. This should be basically
    // impossible to hit given the size and rate of message exchange
    // between peers. We go ahead and close the connection if the queue
    // fills.
    write_queue: VecDeque<Vec<u8>>,

    // The current serialized message being written if there is one
    current_write: Cursor<Vec<u8>>,
}

impl EstablishedConn {
    fn new(
        peer_id: Baseboard,
        unique_id: u64,
        write_sock: OwnedWriteHalf,
        read_sock: OwnedReadHalf,
        main_tx: mpsc::Sender<ConnToMainMsg>,
        rx: mpsc::Receiver<MainToConnMsg>,
        log: Logger,
    ) -> EstablishedConn {
        EstablishedConn {
            peer_id,
            unique_id,
            write_sock,
            read_sock,
            main_tx,
            rx,
            log,
            read_buf: vec![0u8; CONN_BUF_SIZE].into_boxed_slice(),
            total_read: 0,
            last_received_msg: Instant::now(),
            write_queue: VecDeque::with_capacity(MSG_WRITE_QUEUE_CAPACITY),
            current_write: Cursor::new(Vec::new()),
        }
    }

    // Run the main loop of the connection
    //
    // The task can only return a `ConnErr`, otherwise it runs forever
    async fn run(&mut self) -> ConnErr {
        let mut interval = interval(PING_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            if !self.current_write.has_remaining()
                && !self.write_queue.is_empty()
            {
                self.current_write =
                    Cursor::new(self.write_queue.pop_front().unwrap());
            }

            let res = tokio::select! {
                _ = interval.tick() => {
                    self.ping().await
                }
                Some(msg) = self.rx.recv() => {
                    self.on_msg_from_main(msg).await
                }
                res = self.read_sock.read(&mut self.read_buf[self.total_read..]) => {
                    self.on_read(res).await
                }
                res = self.write_sock.write_buf(&mut self.current_write),
                   if self.current_write.has_remaining() =>
                {
                   self.check_write_result(res).await
                }
            };

            if let Err(err) = res {
                warn!(self.log, "Connection error: {err:?}");
                return err;
            }
        }
    }

    async fn on_msg_from_main(
        &mut self,
        msg: MainToConnMsg,
    ) -> Result<(), ConnErr> {
        match msg {
            MainToConnMsg::Close => {
                let _ = self.close().await;
                return Err(ConnErr::Close);
            }
            MainToConnMsg::Msg(msg) => self.write_framed_to_queue(msg).await,
        }
    }

    async fn check_write_result(
        &mut self,
        res: Result<usize, std::io::Error>,
    ) -> Result<(), ConnErr> {
        match res {
            Ok(_) => {
                if !self.current_write.has_remaining() {
                    self.current_write = Cursor::new(Vec::new());
                }
                Ok(())
            }
            Err(e) => {
                warn!(self.log, "Closing connection: Failed to write: {e}");
                self.close().await
            }
        }
    }

    async fn on_read(
        &mut self,
        res: Result<usize, std::io::Error>,
    ) -> Result<(), ConnErr> {
        match res {
            Ok(n) => {
                self.total_read += n;
            }
            Err(e) => {
                warn!(self.log, "Closing connection: failed to read: {e}");
                return self.close().await;
            }
        }

        // We may have more than one message that has been read
        loop {
            if self.total_read < FRAME_HEADER_SIZE {
                return Ok(());
            }
            // Read frame size
            let size = read_frame_size(
                self.read_buf[..FRAME_HEADER_SIZE].try_into().unwrap(),
            );
            let end = size + FRAME_HEADER_SIZE;

            // If we haven't read the whole message yet, then return
            if end > self.total_read {
                return Ok(());
            }
            let msg: Msg = match ciborium::from_reader(
                &self.read_buf[FRAME_HEADER_SIZE..end],
            ) {
                Ok(msg) => {
                    // Move any remaining bytes to the beginning of the buffer.
                    self.read_buf.copy_within(end..self.total_read, 0);
                    self.total_read = self.total_read - end;
                    msg
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "Closing connection: failed to deserialize: {e}",
                    );
                    return self.close().await;
                }
            };
            self.last_received_msg = Instant::now();
            debug!(self.log, "Received {msg:?}");
            match msg {
                Msg::Fsm(msg) => {
                    if let Err(e) = self
                        .main_tx
                        .send(ConnToMainMsg {
                            handle_unique_id: self.unique_id,
                            msg: ConnToMainMsgInner::Received {
                                from: self.peer_id.clone(),
                                msg,
                            },
                        })
                        .await
                    {
                        warn!(
                            self.log,
                            "Failed to send received fsm msg to main task: {e:?}"
                        );
                    }
                }
                Msg::Ping => {
                    // Nothing to do here, since Ping is just to keep us alive and
                    // we updated self.last_received_msg above.
                }
                Msg::NetworkConfig(config) => {
                    let generation = config.generation;
                    if let Err(e) = self
                        .main_tx
                        .send(ConnToMainMsg {
                            handle_unique_id: self.unique_id,
                            msg: ConnToMainMsgInner::ReceivedNetworkConfig {
                                from: self.peer_id.clone(),
                                config,
                            },
                        })
                        .await
                    {
                        warn!(
                            self.log,
                            "Failed to send received NetworkConfig with
                             generation {generation} to main task: {e:?}"
                        );
                    }
                }
            }
        }
    }

    // Send ping messages and check for inactivity timeouts
    async fn ping(&mut self) -> Result<(), ConnErr> {
        if Instant::now() - self.last_received_msg > INACTIVITY_TIMEOUT {
            warn!(self.log, "Closing connection: inactivity timeout",);
            return self.close().await;
        }
        self.write_framed_to_queue(Msg::Ping).await
    }

    async fn write_framed_to_queue(&mut self, msg: Msg) -> Result<(), ConnErr> {
        if self.write_queue.len() == MSG_WRITE_QUEUE_CAPACITY {
            warn!(self.log, "Closing connection: write queue full");
            self.close().await
        } else {
            match write_framed(&msg) {
                Ok(msg) => {
                    self.write_queue.push_back(msg);
                    Ok(())
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "Closing connection: Failed to serialize msg: {}", e
                    );
                    self.close().await
                }
            }
        }
    }

    // Close and drop the connection.
    async fn close(&mut self) -> Result<(), ConnErr> {
        if let Err(e) = self
            .main_tx
            .send(ConnToMainMsg {
                handle_unique_id: self.unique_id,
                msg: ConnToMainMsgInner::Disconnected {
                    peer_id: self.peer_id.clone(),
                },
            })
            .await
        {
            warn!(self.log, "Failed to send to main task: {e:?}");
        }
        let _ = self.write_sock.shutdown().await;
        Err(ConnErr::Retry)
    }
}
/// Spawn a task that maintains a client connection to a peer
pub async fn spawn_connection_initiator_task(
    unique_id: u64,
    my_peer_id: Baseboard,
    my_addr: SocketAddrV6,
    addr: SocketAddrV6,
    log: &Logger,
    main_tx: mpsc::Sender<ConnToMainMsg>,
) -> PeerConnHandle {
    // Create a channel for sending `MainToConnMsg`s to this connection task
    let (tx, mut rx) = mpsc::channel(2);
    let log = log.clone();

    let handle = tokio::spawn(async move {
        loop {
            let sock = match TcpStream::connect(addr).await {
                Ok(sock) => sock,
                Err(err) => {
                    // TODO: Throttle this?
                    warn!(log, "Failed to connect: {err:?}"; "addr" => addr.to_string());
                    sleep(CONNECTION_RETRY_TIMEOUT).await;
                    continue;
                }
            };

            info!(log, "Connected to peer"; "addr" => addr.to_string());

            let (read_sock, write_sock, identify) = match perform_handshake(
                sock,
                &my_peer_id,
                my_addr,
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

            let log = log.new(o!("remote_peer_id" => identify.id.to_string()));

            // Inform the main task that we have connected to a peer
            let _ = main_tx
                .send(ConnToMainMsg {
                    handle_unique_id: unique_id,
                    msg: ConnToMainMsgInner::ConnectedInitiator {
                        addr,
                        peer_id: identify.id.clone(),
                    },
                })
                .await;

            let mut conn = EstablishedConn::new(
                identify.id.clone(),
                unique_id,
                write_sock,
                read_sock,
                main_tx.clone(),
                rx,
                log.clone(),
            );

            // We can only get errors back from `conn.run`
            match conn.run().await {
                ConnErr::Retry => {
                    // The only thing we need to keep is our receiver from main
                    rx = conn.rx;
                }
                ConnErr::Close => {
                    // The Main task told us to shutdown
                    return;
                }
            }
        }
    });
    PeerConnHandle { handle, tx, addr, unique_id }
}

/// Spawn a task that handles accepted connections from a peer
pub async fn spawn_accepted_connection_management_task(
    unique_id: u64,
    my_peer_id: Baseboard,
    my_addr: SocketAddrV6,
    client_addr: SocketAddrV6,
    sock: TcpStream,
    main_tx: mpsc::Sender<ConnToMainMsg>,
    log: &Logger,
) -> AcceptedConnHandle {
    // Create a channel for sending `MainToConnMsg`s to this connection task
    let (tx, rx) = mpsc::channel(2);
    let log = log.clone();
    let handle = tokio::spawn(async move {
        let (read_sock, write_sock, identify) = match perform_handshake(
            sock,
            &my_peer_id,
            my_addr,
        )
        .await
        {
            Ok(val) => val,
            Err(e) => {
                warn!(log, "Handshake error: {:?}", e; "addr" => client_addr.to_string());
                // This is a server so we bail and wait for a new connection.
                // We must inform the main task so it can clean up any metadata.
                let _ = main_tx
                    .send(ConnToMainMsg {
                        handle_unique_id: unique_id,
                        msg: ConnToMainMsgInner::FailedAcceptorHandshake {
                            addr: client_addr,
                        },
                    })
                    .await;
                return;
            }
        };

        // Connections are only supposed to come from peers with `ip:port`
        // values that sort higher than the peer they are connecting to. We
        // don't know the canonical port of the connecting peer until the
        // handshake completes though, as the connecting port is ephemeral.
        // Therefore we do the check here.
        //
        // Note: We don't do the check in `perform_hadnshake` because that
        // method is called for both the accept and connect side and we don't
        // need to do it for the connector. The connector, by definition,
        // shouldn't be connecting to a higher sorted peer.
        if identify.addr < my_addr {
            error!(
                log,
                concat!(
                    "Misbehaving peer: Connection from peer ",
                    "with lower valued address: {}"
                ),
                identify.addr
            );
            // This is a server so we bail and wait for a new connection.
            // We must inform the main task so it can clean up any metadata.
            let _ = main_tx
                .send(ConnToMainMsg {
                    handle_unique_id: unique_id,
                    msg: ConnToMainMsgInner::FailedAcceptorHandshake {
                        addr: client_addr,
                    },
                })
                .await;
            return;
        }

        // Inform the main task that we have connected to a peer
        let _ = main_tx
            .send(ConnToMainMsg {
                handle_unique_id: unique_id,
                msg: ConnToMainMsgInner::ConnectedAcceptor {
                    accepted_addr: client_addr,
                    addr: identify.addr,
                    peer_id: identify.id.clone(),
                },
            })
            .await;

        let mut conn = EstablishedConn::new(
            identify.id.clone(),
            unique_id,
            write_sock,
            read_sock,
            main_tx.clone(),
            rx,
            log.clone(),
        );

        // We always exit on server tasks, as the remote peer
        // will reconnect.
        let _ = conn.run().await;
    });

    AcceptedConnHandle { handle, tx, addr: client_addr, unique_id }
}

// Serialize and write `msg` into `buf`, prefixed by a 4-byte big-endian size header
//
// Return the total amount of data written into `buf` including the 4-byte header
fn write_framed<T: Serialize + ?Sized>(
    msg: &T,
) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
    let mut cursor = Cursor::new(vec![]);
    // Write a size placeholder
    std::io::Write::write(&mut cursor, &[0u8; FRAME_HEADER_SIZE])?;
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
    // Rust 1.77 warns on tuple variants not being used, but in reality these are
    // used for their Debug impl.
    Serialization(#[allow(dead_code)] ciborium::ser::Error<std::io::Error>),
    Deserialization(#[allow(dead_code)] ciborium::de::Error<std::io::Error>),
    Io(#[allow(dead_code)] tokio::io::Error),
    UnsupportedScheme,
    UnsupportedVersion,
    Timeout,
}

// Perform scheme/version negotiation and exchange peer_ids for scheme v0
async fn perform_handshake(
    sock: TcpStream,
    local_peer_id: &Baseboard,
    local_addr: SocketAddrV6,
) -> Result<(OwnedReadHalf, OwnedWriteHalf, Identify), HandshakeError> {
    // Enough to hold the `Hello` and `Identify` messages
    let mut read_buf = [0u8; 128];
    let (mut read_sock, mut write_sock) = sock.into_split();

    // Serialize and write the handshake + identity messages to a local buffer
    let out: Vec<u8> = Hello::default()
        .serialize()
        .into_iter()
        .chain(write_framed(&Identify {
            id: local_peer_id.clone(),
            addr: local_addr,
        })?)
        .collect();
    let mut out_cursor = Cursor::new(&out);

    let handshake_start = Instant::now();

    // Read `Hello` and the frame size of `Identify`
    const INITIAL_READ: usize = Hello::serialized_size() + FRAME_HEADER_SIZE;

    let mut total_read = 0;
    let mut identify_len = 0;
    let mut identify: Option<Identify> = None;

    loop {
        let timeout =
            KEEPALIVE_TIMEOUT.saturating_sub(Instant::now() - handshake_start);

        let end = INITIAL_READ + identify_len;

        // Clippy for Rust 1.73 warns on this but there doesn't seem to be a
        // better way to write it?
        #[allow(clippy::unnecessary_unwrap)]
        if identify.is_some() && !out_cursor.has_remaining() {
            return Ok((read_sock, write_sock, identify.unwrap()));
        }

        tokio::select! {
            _ = sleep(timeout) => {
                return Err(HandshakeError::Timeout);
            }
            _ = write_sock.writable(), if out_cursor.has_remaining() => {
                    write_sock.write_buf(&mut out_cursor).await?;
            }
            res = read_sock.read(&mut read_buf[total_read..end]), if identify.is_none() => {
                let n = res?;
                total_read += n;
                if total_read < INITIAL_READ {
                    continue;
                }
                if total_read == INITIAL_READ {
                    let hello =
                        Hello::from_bytes(&read_buf[..Hello::serialized_size()]).unwrap();
                    if hello.scheme != 0 {
                        return Err(HandshakeError::UnsupportedScheme);
                    }
                    if hello.version != 0 {
                        return Err(HandshakeError::UnsupportedVersion);
                    }
                    identify_len = read_frame_size(
                        read_buf[Hello::serialized_size()..INITIAL_READ]
                            .try_into()
                            .unwrap(),
                    );
                } else {
                    if total_read == end {
                        identify = Some(
                            ciborium::from_reader(&read_buf[INITIAL_READ..end])?
                        );
                    }
                }
            }
        }
    }
}
