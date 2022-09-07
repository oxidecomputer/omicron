// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual bootstore node.
//!
//! Most logic is contained here, but networking sits on top.
//! This allows easier testing of clusters and failure situations.

use diesel::SqliteConnection;
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use uuid::Uuid;

use crate::db;
use crate::db::Db;
use crate::messages::*;
use crate::trust_quorum::SerializableShareDistribution;

/// Configuration for an individual node
pub struct Config {
    log: Logger,
    db_path: String,
    // TODO: This will live inside the certificate eventually
    _serial_number: String,
    _device_id_cert: Ed25519Certificate,
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
//
// Temporary until the using code is written
#[allow(dead_code)]
pub struct Node {
    config: Config,
    db: Db,

    // For now we just use a single connection
    // This *may* change once there is an async connection handling mechanism on top.
    conn: SqliteConnection,
}

impl Node {
    /// Create a new Node
    pub fn new(config: Config) -> Node {
        let (db, conn) = Db::init(&config.log, &config.db_path).unwrap();
        Node { config, db, conn }
    }

    /// Handle a message received over sprockets from another [`Node`] or
    /// the [`Coordinator`].
    pub fn handle(&mut self, req: NodeRequest) -> NodeResponse {
        if req.version != 1 {
            return NodeResponse {
                version: req.version,
                id: req.id,
                result: Err(NodeError::UnsupportedVersion(req.version)),
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

        NodeResponse { version: req.version, id: req.id, result }
    }

    fn handle_get_share(
        &mut self,
        epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        match self.db.get_committed_share(&mut self.conn, epoch) {
            Ok(share) => {
                Ok(NodeOpResult::Share { epoch, share: share.0.share })
            }
            Err(err) => {
                if let db::Error::Db(diesel::result::Error::NotFound) = err {
                    Err(NodeError::KeyShareDoesNotExist { epoch })
                } else {
                    Err(err.into())
                }
            }
        }
    }

    fn handle_initialize(
        &mut self,
        rack_uuid: Uuid,
        share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        self.db.initialize(&mut self.conn, &rack_uuid, share_distribution)?;
        Ok(NodeOpResult::CoordinatorAck)
    }

    fn handle_key_share_prepare(
        &mut self,
        _rack_uuid: Uuid,
        _epoch: i32,
        _share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }

    fn handle_key_share_commit(
        &mut self,
        _rack_uuid: Uuid,
        _epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }
}
