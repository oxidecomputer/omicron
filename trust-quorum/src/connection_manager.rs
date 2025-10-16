// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for maintaining a full mesh of trust quorum node connections

use crate::{BaseboardId, PeerMsg};
// TODO: Move or copy this to this crate?
use bootstore::schemes::v0::NetworkConfig;
use bytes::Buf;
use camino::Utf8PathBuf;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use slog::{Logger, debug, error, info, o, warn};
use slog_error_chain::SlogInlineError;
use sprockets_tls::keys::SprocketsConfig;
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf, split};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval};

/// We only expect one outstanding request at a time for `Init_` or
/// `LoadRackSecret` requests, We can have one of those requests in
/// flight while allowing `PeerAddresses` updates. We also allow status
/// requests in parallel. Just leave some room.
const CHANNEL_BOUND: usize = 10;

/// Max buffer size of a connection
const CONN_BUF_SIZE: usize = 1024 * 1024;

/// Each message starts with a 4 bytes size header
const FRAME_HEADER_SIZE: usize = 4;

/// The number of serialized messages to queue for writing before closing the socket.
/// This means the remote side is very slow.
///
/// TODO: Alternatively we could drop the oldest message.
const MSG_WRITE_QUEUE_CAPACITY: usize = 5;

// Timing parameters for keeping the connection healthy
const PING_INTERVAL: Duration = Duration::from_secs(1);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

/// An error returned from `ConnMgr::accept`
#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum AcceptError {
    #[error("Accepted connection from IPv4 address {addr}. Only IPv6 allowed.")]
    Ipv4Accept { addr: SocketAddrV4 },

    #[error("sprockets error")]
    Sprockets(
        #[from]
        #[source]
        sprockets_tls::Error,
    ),
}

/// A mechanism for uniquely identifying a task managing a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new(id: u64) -> TaskId {
        TaskId(id)
    }

    /// Increment the ID and then return the value before the increment
    pub fn inc(&mut self) -> TaskId {
        let id = *self;
        self.0 += 1;
        id
    }
}

/// Messages sent from the main task to the connection managing tasks
#[derive(Debug, PartialEq)]
pub enum MainToConnMsg {
    Close,
    Msg(WireMsg),
}

/// All possible messages sent over established connections
///
/// This include trust quorum related `PeerMsg`s, but also ancillary network
/// messages used for other purposes.
///
/// All `WireMsg`s sent between nodes is prefixed with a 4 byte size header used
/// for framing.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum WireMsg {
    /// Used for connection keep alive
    Ping,
    /// Trust quorum peer messages
    Tq(PeerMsg),
    /// Early network configuration to enable NTP timesync
    ///
    /// Technically this is not part of the trust quorum protocol. However it is
    /// necessary to gossip this information to all nodes on the system so that
    /// each can establish NTP sync required for the rest of the control plane
    /// to boot. In short, we can't have rack unlock without this information,
    /// even if we can decrypt the drives. For simplicity, we just piggyback
    /// this information on the trust quorum connections. This is why the
    /// implementation of LRTQ lived inside the `bootstore` directory in the
    /// `omicron` repo. This is technically an eventually consistent database
    /// of tiny information layered on top of trust quorum. You can still think
    /// of it as a bootstore, although, we no longer use that name.
    NetworkConfig(NetworkConfig),
}

/// Messages sent from connection managing tasks to the main peer task
///
/// We include `handle_unique_id` to differentiate which task they come from so
/// we can exclude requests from tasks that have been cancelled or have been told
/// to shutdown.
#[derive(Debug, PartialEq)]
pub struct ConnToMainMsg {
    pub task_id: TaskId,
    pub msg: ConnToMainMsgInner,
}

#[derive(Debug, PartialEq)]
pub enum ConnToMainMsgInner {
    Accepted { addr: SocketAddrV6, peer_id: BaseboardId },
    Connected { addr: SocketAddrV6, peer_id: BaseboardId },
    Disconnected { peer_id: BaseboardId },
    Received { from: BaseboardId, msg: PeerMsg },
    //  FailedAcceptorHandshake { addr: SocketAddrV6 },
    ReceivedNetworkConfig { from: BaseboardId, config: NetworkConfig },
}

pub struct TaskHandle {
    task_id: TaskId,
    tx: mpsc::Sender<MainToConnMsg>,
    conn_type: ConnectionType,
}

pub enum ConnectionType {
    Connected(SocketAddrV6),
    Accepted(SocketAddrV6),
}

pub struct ConnMgr {
    log: Logger,

    /// A channel for sending messages from a connection task to the main task
    main_tx: mpsc::Sender<ConnToMainMsg>,

    /// The sprockets server
    server: sprockets_tls::Server,

    /// The address the sprockets server listens on
    listen_addr: SocketAddrV6,

    // A unique, monotonically incrementing id for each task to help map tasks
    // to their handles in case the task aborts, or there is a new connection
    // accepted and established for an existing `BaseboardId`.
    next_task_id: TaskId,

    /// `JoinHandle`s to all tasks that can be polled for crashes
    join_handles: FuturesUnordered<JoinHandle<TaskId>>,

    /// All known addresses on the bootstrap network, learned via DDMD
    bootstrap_addrs: BTreeSet<SocketAddrV6>,

    /// All tasks currently connecting to remote nodes and attempting a
    /// sprockets handshake.
    connecting: BTreeMap<SocketAddrV6, TaskHandle>,

    /// All tasks with an accepted TCP connnection performing a sprockets handshake
    accepting: BTreeMap<SocketAddrV6, TaskHandle>,

    // All tasks that have moved from `Connecting` to `Established`
    // This is separate from `Established` because established nodes contain
    // accepted connections also, and we don't know the canonical address since
    // they use an ephemeral port.
    connected: BTreeMap<SocketAddrV6, TaskId>,

    /// All tasks containing established connections that can be used to communicate
    /// with other nodes.
    established: BTreeMap<BaseboardId, TaskHandle>,
}

impl ConnMgr {
    pub async fn new(
        log: &Logger,
        listen_addr: SocketAddrV6,
        sprockets_config: SprocketsConfig,
        main_tx: mpsc::Sender<ConnToMainMsg>,
    ) -> ConnMgr {
        let log = log.new(o!(
            "component" => "trust-quorum-conn-mgr"));

        let server = sprockets_tls::Server::new(
            sprockets_config,
            listen_addr,
            log.clone(),
        )
        .await
        .expect("sprockets server can listen");

        ConnMgr {
            log,
            main_tx,
            server,
            listen_addr,
            next_task_id: TaskId::new(0),
            join_handles: Default::default(),
            bootstrap_addrs: BTreeSet::new(),
            connecting: BTreeMap::new(),
            accepting: BTreeMap::new(),
            connected: BTreeMap::new(),
            established: BTreeMap::new(),
        }
    }

    pub async fn accept(
        &mut self,
        corpus: Vec<Utf8PathBuf>,
    ) -> Result<(), AcceptError> {
        let acceptor = self.server.accept(corpus).await?;
        let addr = match acceptor.addr() {
            SocketAddr::V4(addr) => {
                return Err(AcceptError::Ipv4Accept { addr });
            }
            SocketAddr::V6(addr) => addr,
        };
        let log = self.log.clone();
        let task_id = self.next_task_id.inc();
        let (tx, rx) = mpsc::channel(CHANNEL_BOUND);
        let task_handle = TaskHandle {
            task_id,
            tx,
            conn_type: ConnectionType::Accepted(addr),
        };
        let main_tx = self.main_tx.clone();
        let join_handle = tokio::spawn(async move {
            match acceptor.handshake().await {
                Ok((stream, _)) => {
                    let platform_id =
                        stream.peer_platform_id().as_str().unwrap();
                    let baseboard_id = platform_id_to_baseboard_id(platform_id);

                    // TODO: Conversion between `PlatformId` and `BaseboardId` should
                    // happen in `sled-agent-types`. This is waiting on an update
                    // to the `dice-mfg-msgs` crate.
                    let log =
                        log.new(o!("baseboard_id" => baseboard_id.to_string()));
                    info!(log, "Accepted sprockets connection"; "addr" => %addr);

                    let mut conn = EstablishedConn::new(
                        baseboard_id,
                        task_id,
                        stream,
                        main_tx,
                        rx,
                        log,
                    );

                    conn.run().await;
                }
                Err(err) => {
                    error!(log, "Failed to accept a connection"; &err);
                }
            }
            task_id
        });
        self.join_handles.push(join_handle);
        self.accepting.insert(addr, task_handle);
        Ok(())
    }

    /// The set of known addresses on the bootstrap network has changed
    pub fn update_bootstrap_addrs(&mut self, addrs: BTreeSet<SocketAddrV6>) {
        if self.bootstrap_addrs == addrs {
            return;
        }

        // We don't try to compare addresses from accepted nodes. If DDMD
        // loses an accepting address we assume that the connection will go
        // away soon, if it hasn't already. We can't compare without an extra
        // handshake message to identify the listen address of the remote
        // connection because clients use ephemeral ports. We always compare
        // on the full `SocketAddrV6` which includes the port, which helps when
        // testing on localhost.
        let to_connect: BTreeSet<_> = addrs
            .difference(&self.bootstrap_addrs)
            .filter(|&&addr| self.listen_addr > addr)
            .cloned()
            .collect();
        let to_disconnect: BTreeSet<_> = self
            .bootstrap_addrs
            .difference(&addrs)
            .filter(|&&addr| self.listen_addr > addr)
            .cloned()
            .collect();
    }
}

/// An error from within an `EstablishedConn` that triggers connection close
///
/// Also a great movie
#[derive(Debug, thiserror::Error, SlogInlineError)]
enum ConnErr {
    #[error("Main task insructed this connection to close")]
    Close,
    #[error("Failed to write")]
    FailedWrite(#[source] std::io::Error),
    #[error("Failed to read")]
    FailedRead(#[source] std::io::Error),
    #[error("Failed to deserialize wire message")]
    DeserializeWireMsg(#[from] ciborium::de::Error<std::io::Error>),
    #[error("Failed to serialize wire message")]
    SerializeWireMsg(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("Write queue filled with serialized messages")]
    WriteQueueFull,
    #[error("Inactivity timeout")]
    InactivityTimeout,
}

// Container for code running in its own task per sprockets connection
struct EstablishedConn {
    peer_id: BaseboardId,
    task_id: TaskId,
    reader: ReadHalf<sprockets_tls::Stream<TcpStream>>,
    writer: WriteHalf<sprockets_tls::Stream<TcpStream>>,
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
        peer_id: BaseboardId,
        task_id: TaskId,
        stream: sprockets_tls::Stream<TcpStream>,
        main_tx: mpsc::Sender<ConnToMainMsg>,
        rx: mpsc::Receiver<MainToConnMsg>,
        log: Logger,
    ) -> EstablishedConn {
        let (reader, writer) = split(stream);
        EstablishedConn {
            peer_id,
            task_id,
            reader,
            writer,
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

    async fn run(&mut self) {
        let mut interval = interval(PING_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // This is the main loop of the connection
        //
        // Continuously process messages until the connection closes
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
                res = self.reader.read(&mut self.read_buf[self.total_read..]) => {
                    self.on_read(res).await
                }
                res = self.writer.write_buf(&mut self.current_write),
                   if self.current_write.has_remaining() =>
                {
                   self.check_write_result(res).await
                }
            };

            if let Err(err) = res {
                warn!(self.log, "Closing connection"; &err);
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
                return Err(ConnErr::FailedRead(e));
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
            let msg: WireMsg =
                ciborium::from_reader(&self.read_buf[FRAME_HEADER_SIZE..end])?;
            // Move any remaining bytes to the beginning of the buffer.
            self.read_buf.copy_within(end..self.total_read, 0);
            self.total_read = self.total_read - end;

            self.last_received_msg = Instant::now();
            debug!(self.log, "Received {msg:?}");
            match msg {
                WireMsg::Tq(msg) => {
                    if let Err(e) = self
                        .main_tx
                        .send(ConnToMainMsg {
                            task_id: self.task_id,
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
                WireMsg::Ping => {
                    // Nothing to do here, since Ping is just to keep us alive and
                    // we updated self.last_received_msg above.
                }
                WireMsg::NetworkConfig(config) => {
                    let generation = config.generation;
                    if let Err(e) = self
                        .main_tx
                        .send(ConnToMainMsg {
                            task_id: self.task_id,
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
                let _ = self.writer.shutdown().await;
                Err(ConnErr::FailedWrite(e))
            }
        }
    }

    async fn on_msg_from_main(
        &mut self,
        msg: MainToConnMsg,
    ) -> Result<(), ConnErr> {
        match msg {
            MainToConnMsg::Close => {
                return Err(ConnErr::Close);
            }
            MainToConnMsg::Msg(msg) => self.write_framed_to_queue(msg).await,
        }
    }

    async fn write_framed_to_queue(
        &mut self,
        msg: WireMsg,
    ) -> Result<(), ConnErr> {
        if self.write_queue.len() == MSG_WRITE_QUEUE_CAPACITY {
            return Err(ConnErr::WriteQueueFull);
        } else {
            let msg = write_framed(&msg)?;
            self.write_queue.push_back(msg);
            Ok(())
        }
    }

    async fn ping(&mut self) -> Result<(), ConnErr> {
        if Instant::now() - self.last_received_msg > INACTIVITY_TIMEOUT {
            return Err(ConnErr::InactivityTimeout);
        }
        self.write_framed_to_queue(WireMsg::Ping).await
    }
}

fn platform_id_to_baseboard_id(platform_id: &str) -> BaseboardId {
    let mut platform_id_iter = platform_id.split(":");
    let part_number = platform_id_iter.nth(1).unwrap().to_string();
    let serial_number = platform_id_iter.skip(1).next().unwrap().to_string();
    BaseboardId { part_number, serial_number }
}

// Decode the 4-byte big-endian frame size header
fn read_frame_size(buf: [u8; FRAME_HEADER_SIZE]) -> usize {
    u32::from_be_bytes(buf) as usize
}

/// Serialize and write `msg` into `buf`, prefixed by a 4-byte big-endian size
/// header
///
/// Return the total amount of data written into `buf` including the 4-byte
/// header.
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
