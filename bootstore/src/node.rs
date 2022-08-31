// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual bootstore node.
//!
//! Most logic is contained here, but networking sits on top.
//! This allows easier testing of clusters and failure situations.

use slog::Logger;
use slog::{info, o};
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;

use crate::db::Db;
use sprockets_host::Ed25519Certificate;

/// Configuration for an individual node
pub struct Config {
    log: Logger,
    db_path: String,
    // TODO: This will live inside the certificate eventually
    serial_number: String,
    device_id_cert: Ed25519Certificate,
}

/// A node of the bootstore
///
/// A Node contains all the logic of the bootstore and stores relevant
/// information  in [`Db`]. The [`BootstrapAgent`] establishes sprockets
/// sessions, and utilizes its local `Node` to manage any messages received
/// over these sessions.
///
/// Messages are received over sprockets sessions from either peer nodes
/// during rack unlock, or from a [`Coordinator`] during rack initialization
/// or reconfiguration.
pub struct Node {
    config: Config,
    db: Db,
}

impl Node {
    /// Create a new Node
    pub fn new(config: Config) -> Node {
        let db = Db::open(config.log.clone(), &config.db_path).unwrap();
        Node { config, db }
    }
}
