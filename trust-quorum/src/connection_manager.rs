// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for maintaining a full mesh of trust quorum node connections

use crate::BaseboardId;
use futures::stream::FuturesUnordered;
use slog::{Logger, o};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddrV6;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A mechanism for uniquely identifying a task managing a connection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new(id: u64) -> TaskId {
        TaskId(id)
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

    listen_addr: SocketAddrV6,

    // A unique, monotonically incrementing id for each task to help map tasks
    // to their handles in case the task aborts, or there is a new connection
    // accepted and established for an existing `BaseboardId`.
    next_task_id: u64,

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
    pub fn new(log: &Logger, listen_addr: SocketAddrV6) -> ConnMgr {
        let log = log.new(o!(
            "component" => "trust-quorum-conn-mgr"));
        ConnMgr {
            log,
            listen_addr,
            next_task_id: 0,
            join_handles: Default::default(),
            bootstrap_addrs: BTreeSet::new(),
            connecting: BTreeMap::new(),
            accepting: BTreeMap::new(),
            connected: BTreeMap::new(),
            established: BTreeMap::new(),
        }
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
