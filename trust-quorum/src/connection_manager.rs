// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for maintaining a full mesh of trust quorum node connections

use crate::established_conn::EstablishedConn;
use crate::{BaseboardId, PeerMsg};
// TODO: Move or copy this to this crate?
use bootstore::schemes::v0::NetworkConfig;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use slog::{Logger, debug, error, info, o, warn};
use slog_error_chain::SlogInlineError;
use sprockets_tls::keys::SprocketsConfig;
use sprockets_tls::server::SprocketsAcceptor;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::{self, JoinSet};
use tokio::time::{Interval, MissedTickBehavior, interval};

/// We only expect a handful of concurrent requests at most.
const CHANNEL_BOUND: usize = 10;

// Time between checks to see if we need to reconnect to to any peers
pub const RECONNECT_TIME: Duration = Duration::from_secs(5);

/// An error returned from `ConnMgr::accept`
#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum AcceptError {
    #[error("accepted connection from IPv4 address {addr}. Only IPv6 allowed.")]
    Ipv4Accept { addr: SocketAddrV4 },

    #[error("sprockets error")]
    Sprockets(
        #[from]
        #[source]
        sprockets_tls::Error,
    ),
}

/// Messages sent from the main task to the connection managing tasks
#[derive(Debug, PartialEq)]
pub enum MainToConnMsg {
    Close,
    #[expect(unused)]
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
/// We include `task_id` to differentiate which task they come from so we can
/// exclude requests from tasks that have been cancelled or have been told to
/// shutdown.
#[derive(Debug, PartialEq)]
pub struct ConnToMainMsg {
    pub task_id: task::Id,
    pub msg: ConnToMainMsgInner,
}

#[derive(Debug, PartialEq)]
pub enum ConnToMainMsgInner {
    Accepted { addr: SocketAddrV6, peer_id: BaseboardId },
    Connected { addr: SocketAddrV6, peer_id: BaseboardId },
    Received { from: BaseboardId, msg: PeerMsg },
    ReceivedNetworkConfig { from: BaseboardId, config: NetworkConfig },
    Disconnected { peer_id: BaseboardId },
}

pub struct TaskHandle {
    pub task_id: task::Id,
    pub tx: mpsc::Sender<MainToConnMsg>,
    pub conn_type: ConnectionType,
}

impl TaskHandle {
    pub fn addr(&self) -> SocketAddrV6 {
        self.conn_type.addr()
    }
}

pub enum ConnectionType {
    Connected(SocketAddrV6),
    Accepted(SocketAddrV6),
}

impl ConnectionType {
    pub fn addr(&self) -> SocketAddrV6 {
        match self {
            Self::Connected(addr) => *addr,
            Self::Accepted(addr) => *addr,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnState {
    Connecting,
    Accepting,
    Established(BaseboardId),
}

/// Information about a single connection task
#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub state: ConnState,
    pub addr: SocketAddrV6,
    pub task_id: task::Id,
}

/// Status information useful for debugging
#[derive(Debug, Clone)]
pub struct ConnMgrStatus {
    pub bootstrap_addrs: BTreeSet<SocketAddrV6>,
    pub connections: Vec<ConnInfo>,
    pub num_conn_tasks: u64,
    pub total_tasks_spawned: u64,
}

/// A structure to manage all sprockets connections to peer nodes
///
/// Each sprockets connection runs in its own task which communicates with the
/// main `NodeTask`. All methods on the `ConnMgr` run inside the main `NodeTask`
/// as `ConnMgr` is a member field of `NodeTask`. This allows isolating the
/// connection management logic from the main node message handling logic
/// without adding yet another task.
pub struct ConnMgr {
    log: Logger,

    /// A channel for sending messages from a connection task to the main task
    main_tx: mpsc::Sender<ConnToMainMsg>,

    /// The sprockets config
    config: SprocketsConfig,

    /// The sprockets server
    server: sprockets_tls::Server,

    /// The address the sprockets server listens on
    listen_addr: SocketAddrV6,

    /// A mechanism for spawning connection tasks
    join_set: JoinSet<()>,

    /// All known addresses on the bootstrap network, learned via DDMD
    bootstrap_addrs: BTreeSet<SocketAddrV6>,

    /// All tasks currently connecting to remote nodes and attempting a
    /// sprockets handshake.
    connecting: BTreeMap<SocketAddrV6, TaskHandle>,

    /// All tasks with an accepted TCP connnection performing a sprockets handshake
    accepting: BTreeMap<SocketAddrV6, TaskHandle>,

    /// All tasks containing established connections that can be used to communicate
    /// with other nodes.
    established: BTreeMap<BaseboardId, TaskHandle>,

    /// An interval for reconnect operations
    reconnect_interval: Interval,

    /// The number of total connection tasks spawned
    total_tasks_spawned: u64,
}

impl ConnMgr {
    pub async fn new(
        log: &Logger,
        mut listen_addr: SocketAddrV6,
        sprockets_config: SprocketsConfig,
        main_tx: mpsc::Sender<ConnToMainMsg>,
    ) -> ConnMgr {
        let log = log.new(o!("component" => "trust-quorum-conn-mgr"));

        let config = sprockets_config.clone();
        let server = sprockets_tls::Server::new(
            sprockets_config,
            listen_addr,
            log.clone(),
        )
        .await
        .expect("sprockets server can listen");

        // If the listen port was 0, we want to update our addr to use
        // the actual port This is really only useful for testing, but the
        // connection manager won't work properly without doing this because it
        // will never trigger connections since its own address will always sort
        // lower than other addresses if only the ports differ.
        let listen_port = server.listen_addr().unwrap().port();

        if listen_port != listen_addr.port() {
            listen_addr.set_port(listen_port);
        }

        info!(
            log,
            "Started listening";
            "local_addr" => %listen_addr
        );

        let mut reconnect_interval = interval(RECONNECT_TIME);
        reconnect_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        ConnMgr {
            log,
            main_tx,
            config,
            server,
            listen_addr,
            join_set: JoinSet::new(),
            bootstrap_addrs: BTreeSet::new(),
            connecting: BTreeMap::new(),
            accepting: BTreeMap::new(),
            established: BTreeMap::new(),
            reconnect_interval,
            total_tasks_spawned: 0,
        }
    }

    pub fn status(&self) -> ConnMgrStatus {
        let connections = self
            .connecting
            .iter()
            .map(|(addr, task_handle)| ConnInfo {
                state: ConnState::Connecting,
                addr: *addr,
                task_id: task_handle.task_id,
            })
            .chain(self.accepting.iter().map(|(addr, task_handle)| ConnInfo {
                state: ConnState::Accepting,
                addr: *addr,
                task_id: task_handle.task_id,
            }))
            .chain(self.established.iter().map(
                |(baseboard_id, task_handle)| ConnInfo {
                    state: ConnState::Established(baseboard_id.clone()),
                    addr: task_handle.addr(),
                    task_id: task_handle.task_id,
                },
            ))
            .collect();

        ConnMgrStatus {
            bootstrap_addrs: self.bootstrap_addrs.clone(),
            connections,
            num_conn_tasks: self.join_set.len() as u64,
            total_tasks_spawned: self.total_tasks_spawned,
        }
    }

    pub fn listen_addr(&self) -> SocketAddrV6 {
        self.listen_addr
    }

    /// Perform any polling related operations that the connection
    /// manager must perform concurrently.
    pub async fn step(
        &mut self,
        corpus: Vec<Utf8PathBuf>,
    ) -> Result<(), AcceptError> {
        tokio::select! {
            acceptor = self.server.accept(corpus.clone()) => {
                self.accept(acceptor?).await?;
            }
            Some(res) = self.join_set.join_next_with_id() => {
                match res {
                    Ok((task_id, _)) => {
                        self.on_task_exit(task_id).await;
                    }
                    Err(err) => {
                        error!(self.log, "Connection task panic: {err}");
                        self.on_task_exit(err.id()).await;
                    }

                }
            }
            _ = self.reconnect_interval.tick() => {
                self.reconnect(corpus.clone()).await;
            }
        }

        Ok(())
    }

    pub async fn accept(
        &mut self,
        acceptor: SprocketsAcceptor,
    ) -> Result<(), AcceptError> {
        let addr = match acceptor.addr() {
            SocketAddr::V4(addr) => {
                return Err(AcceptError::Ipv4Accept { addr });
            }
            SocketAddr::V6(addr) => addr,
        };
        let log = self.log.clone();
        let (tx, rx) = mpsc::channel(CHANNEL_BOUND);
        let main_tx = self.main_tx.clone();
        let abort_handle = self.join_set.spawn(async move {
            let stream = match acceptor.handshake().await {
                Ok((stream, _)) => stream,

                Err(err) => {
                    error!(log, "Failed to accept a connection"; &err);
                    return;
                }
            };
            let platform_id = stream.peer_platform_id().as_str().unwrap();
            let baseboard_id = platform_id_to_baseboard_id(platform_id);

            // TODO: Conversion between `PlatformId` and `BaseboardId` should
            // happen in `sled-agent-types`. This is waiting on an update
            // to the `dice-mfg-msgs` crate.
            let log = log.new(o!(
                "peer_id" => baseboard_id.to_string(),
                "peer_addr" => addr.to_string()
            ));
            info!(log, "Accepted sprockets connection");

            let mut conn = EstablishedConn::new(
                baseboard_id.clone(),
                task::id(),
                stream,
                main_tx.clone(),
                rx,
                &log,
            );

            // Inform the main task that accepted connection is established
            if let Err(e) = main_tx
                .send(ConnToMainMsg {
                    task_id: task::id(),
                    msg: ConnToMainMsgInner::Accepted {
                        addr,
                        peer_id: baseboard_id,
                    },
                })
                .await
            {
                // The system is shutting down
                // Just bail from this task
                warn!(log, "Failed to send 'accepted' msg to main task: {e:?}");
            } else {
                conn.run().await;
            }
        });
        self.total_tasks_spawned += 1;
        let task_handle = TaskHandle {
            task_id: abort_handle.id(),
            tx,
            conn_type: ConnectionType::Accepted(addr),
        };
        self.accepting.insert(addr, task_handle);
        Ok(())
    }

    pub async fn server_handshake_completed(
        &mut self,
        task_id: task::Id,
        addr: SocketAddrV6,
        peer_id: BaseboardId,
    ) {
        if let Some(task_handle) = self.accepting.remove(&addr) {
            info!(
                self.log,
                "Established server connection";
                "task_id" => ?task_id,
                "peer_addr" => %addr,
                "peer_id" => %peer_id
            );
            let already_established =
                self.established.insert(peer_id, task_handle);
            assert!(already_established.is_none());
        }
    }

    pub async fn client_handshake_completed(
        &mut self,
        task_id: task::Id,
        addr: SocketAddrV6,
        peer_id: BaseboardId,
    ) {
        if let Some(task_handle) = self.connecting.remove(&addr) {
            info!(
                self.log,
                "Established client connection";
                "task_id" => ?task_id,
                "peer_addr" => %addr,
                "peer_id" => %peer_id
            );
            let already_established =
                self.established.insert(peer_id, task_handle);

            assert!(already_established.is_none());
        } else {
            error!(self.log, "Client handshake completed, but no client addr in map";
                "task_id" => ?task_id,
                "peer_addr" => %addr,
                "peer_id" => %peer_id
            );
        }
    }

    /// The established connection task has asynchronously exited.
    pub async fn on_disconnected(
        &mut self,
        task_id: task::Id,
        peer_id: BaseboardId,
    ) {
        if let Some(task_handle) = self.established.get(&peer_id) {
            if task_handle.task_id != task_id {
                // This was a stale disconnect
                return;
            }
        }
        warn!(self.log, "peer disconnected"; "peer_id" => %peer_id);
        let _ = self.established.remove(&peer_id);
    }

    /// Initiate connections if a corresponding task doesn't already exist. This
    /// must be called periodically to handle transient disconnections which
    /// cause tasks to exit.
    pub async fn reconnect(&mut self, corpus: Vec<Utf8PathBuf>) {
        debug!(self.log, "Reconnect called");
        let mut to_connect = vec![];
        for addr in
            self.bootstrap_addrs.iter().filter(|&&addr| self.listen_addr > addr)
        {
            if self.connecting.contains_key(addr) {
                continue;
            }

            if self
                .established
                .values()
                .any(|task_handle| task_handle.addr() == *addr)
            {
                continue;
            }

            to_connect.push(*addr);
        }

        for addr in to_connect {
            // We don't have an existing connection
            self.connect_client(corpus.clone(), addr).await
        }
    }

    /// The set of known addresses on the bootstrap network has changed
    ///
    /// We only want a single connection between known peers at a time. The
    /// easiest way to achieve this is to only connect to peers with addresses
    /// that sort less than our own and tear down any connections that no longer
    /// exist in `addrs`.
    pub async fn update_bootstrap_connections(
        &mut self,
        addrs: BTreeSet<SocketAddrV6>,
        corpus: Vec<Utf8PathBuf>,
    ) {
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

        self.bootstrap_addrs = addrs;

        for addr in to_connect {
            self.connect_client(corpus.clone(), addr).await;
        }

        for addr in to_disconnect {
            self.disconnect_client(addr).await;
        }
    }

    /// Spawn a task to estalbish a sprockets connection for the given address
    async fn connect_client(
        &mut self,
        corpus: Vec<Utf8PathBuf>,
        addr: SocketAddrV6,
    ) {
        let (tx, rx) = mpsc::channel(CHANNEL_BOUND);
        info!(self.log, "Initiating connection to new peer: {addr}");
        let main_tx = self.main_tx.clone();
        let log = self.log.clone();
        let config = self.config.clone();
        let abort_handle = self.join_set.spawn(async move {
            let stream = match sprockets_tls::Client::connect(
                config,
                addr,
                corpus.clone(),
                log.clone(),
            )
            .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    warn!(log, "Failed to connect"; "peer_addr"=> %addr, &err);
                    return ();
                }
            };
            let platform_id = stream.peer_platform_id().as_str().unwrap();
            let baseboard_id = platform_id_to_baseboard_id(platform_id);

            // TODO: Conversion between `PlatformId` and `BaseboardId` should
            // happen in `sled-agent-types`. This is waiting on an update
            // to the `dice-mfg-msgs` crate.
            let log = log.new(o!(
                "peer_id" => baseboard_id.to_string(),
                "peer_addr" => addr.to_string()
            ));
            info!(log, "Sprockets connection established");

            let mut conn = EstablishedConn::new(
                baseboard_id.clone(),
                task::id(),
                stream,
                main_tx.clone(),
                rx,
                &log,
            );
            // Inform the main task that the client connection is
            // established.
            if let Err(e) = main_tx
                .send(ConnToMainMsg {
                    task_id: task::id(),
                    msg: ConnToMainMsgInner::Connected {
                        addr,
                        peer_id: baseboard_id,
                    },
                })
                .await
            {
                // The system is shutting down
                // Just bail from this task
                error!(
                    log,
                    "Failed to send 'connected' msg to main task: {e:?}"
                );
            } else {
                conn.run().await;
            }
        });
        self.total_tasks_spawned += 1;
        let task_handle = TaskHandle {
            task_id: abort_handle.id(),
            tx,
            conn_type: ConnectionType::Connected(addr),
        };
        self.connecting.insert(addr, task_handle);
    }

    /// Remove any information about a sprockets client connection and inform
    /// the corresponding task to stop.
    ///
    /// We don't tear down server connections this way as we don't know their
    /// listen port, just the ephemeral port.
    async fn disconnect_client(&mut self, addr: SocketAddrV6) {
        if let Some(handle) = self.connecting.remove(&addr) {
            // The connection has not yet completed its handshake
            info!(
                self.log,
                "Deleting initiating connection";
                "remote_addr" => %addr
            );
            let _ = handle.tx.send(MainToConnMsg::Close).await;
        } else {
            if let Some((id, handle)) = self
                .established
                .iter()
                .find(|(_, handle)| handle.addr() == addr)
            {
                info!(
                    self.log,
                    "Deleting established connection";
                    "peer_addr" => %addr,
                    "peer_id" => %id
                );
                let _ = handle.tx.send(MainToConnMsg::Close).await;
                // probably a better way to avoid borrowck issues
                let id = id.clone();
                self.established.remove(&id);
            }
        }
    }

    /// Remove any references to the given task
    async fn on_task_exit(&mut self, task_id: task::Id) {
        // We're most likely to find the task as established so we start with that
        if let Some((id, handle)) = self
            .established
            .iter()
            .find(|(_, handle)| handle.task_id == task_id)
        {
            info!(
                self.log,
                "Established connection task exited";
                "task_id" => ?task_id,
                "peer_addr" => %handle.addr(),
                "peer_id" => %id
            );
            // probably a better way to avoid borrowck issues
            let id = id.clone();
            self.established.remove(&id);
        } else if let Some((addr, handle)) =
            self.accepting.iter().find(|(_, handle)| handle.task_id == task_id)
        {
            info!(
                self.log,
                "Accepting task exited";
                "task_id" => ?task_id,
                "peer_addr" => %handle.addr()
            );
            let addr = *addr;
            self.accepting.remove(&addr);
        } else if let Some((addr, handle)) =
            self.connecting.iter().find(|(_, handle)| handle.task_id == task_id)
        {
            info!(
                self.log,
                "Connecting task exited";
                "task_id" => ?task_id,
                "peer_addr" => %handle.addr()
            );
            let addr = *addr;
            self.connecting.remove(&addr);
        } else {
            info!(
                self.log,
                "Task exited. No cleanup required.";
                "task_id" => ?task_id
            );
        }
    }
}

// TODO: Eventually this will go away, once we pull in and use the latest
// `dice-util` code.
pub fn platform_id_to_baseboard_id(platform_id: &str) -> BaseboardId {
    let mut platform_id_iter = platform_id.split(":");
    let part_number = platform_id_iter.nth(1).unwrap().to_string();
    let serial_number = platform_id_iter.nth(1).unwrap().to_string();
    BaseboardId { part_number, serial_number }
}
