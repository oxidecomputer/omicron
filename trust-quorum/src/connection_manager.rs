// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for maintaining a full mesh of trust quorum node connections

use crate::BaseboardId;
use camino::Utf8PathBuf;
use futures::stream::FuturesUnordered;
use slog::{Logger, error, info, o, warn};
use slog_error_chain::SlogInlineError;
use sprockets_tls::keys::SprocketsConfig;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};

/// We only expect one outstanding request at a time for `Init_` or
/// `LoadRackSecret` requests, We can have one of those requests in
/// flight while allowing `PeerAddresses` updates. We also allow status
/// requests in parallel. Just leave some room.
const CHANNEL_BOUND: usize = 10;

// Timing parameters for keeping the connection healthy
const PING_INTERVAL: Duration = Duration::from_secs(1);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

/// An error returned from the `ConnMgr`
#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum ConnMgrError {
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
    // TODO: Fill this in
    Msg,
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
    ) -> Result<(), ConnMgrError> {
        let acceptor = self.server.accept(corpus).await?;
        let addr = match acceptor.addr() {
            SocketAddr::V4(addr) => {
                return Err(ConnMgrError::Ipv4Accept { addr });
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

                    // This is the main loop of the connection
                    let mut interval = interval(PING_INTERVAL);
                    interval
                        .set_missed_tick_behavior(MissedTickBehavior::Delay);
                }
                Err(err) => {
                    error!(log, "Failed to accept a connection: {err:?}");
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

fn platform_id_to_baseboard_id(platform_id: &str) -> BaseboardId {
    let mut platform_id_iter = platform_id.split(":");
    let part_number = platform_id_iter.nth(1).unwrap().to_string();
    let serial_number = platform_id_iter.skip(1).next().unwrap().to_string();
    BaseboardId { part_number, serial_number }
}
