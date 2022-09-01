// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual bootstore node.
//!
//! Most logic is contained here, but networking sits on top.
//! This allows easier testing of clusters and failure situations.

use slog::Logger;
use slog::{info, o};
use sprockets_host::Ed25519Certificate;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::db::Db;
use crate::messages::*;
use crate::trust_quorum::SerializableShareDistribution;

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

    /// Handle a message received over sprockets from another [`Node`] or
    /// the [`Coordinator`].
    pub fn handle(&mut self, req: NodeRequest) -> NodeResponse {
        if req.version != 1 {
            return NodeResponse {
                version: req.version,
                id: req.id,
                op: NodeOpResult::Error(NodeError::UnsupportedVersion(
                    req.version,
                )),
            };
        }

        let result = match req.op {
            NodeOp::GetShare { epoch } => self.handle_get_share(epoch),
            NodeOp::Initialize { rack_uuid, share_distribution } => {
                self.handle_initialize(rack_uuid, share_distribution)
            }
            NodeOp::KeySharePrepare {
                rack_uuid,
                epoch,
                share_distribution,
            } => self.handle_key_share_prepare(
                rack_uuid,
                epoch,
                share_distribution,
            ),
            NodeOp::KeyShareCommit { rack_uuid, epoch } => {
                self.handle_key_share_commit(rack_uuid, epoch)
            }
        };

        let op_result = match result {
            Ok(op_result) => op_result,
            Err(err) => NodeOpResult::Error(err),
        };

        NodeResponse { version: req.version, id: req.id, op: op_result }
    }

    fn handle_get_share(
        &mut self,
        epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }

    fn handle_initialize(
        &mut self,
        rack_uuid: Uuid,
        share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }

    fn handle_key_share_prepare(
        &mut self,
        rack_uuid: Uuid,
        epoch: i32,
        share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }

    fn handle_key_share_commit(
        &mut self,
        rack_uuid: Uuid,
        epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }
}
