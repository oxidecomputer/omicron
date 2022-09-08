// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual bootstore node.
//!
//! Most logic is contained here, but networking sits on top.
//! This allows easier testing of clusters and failure situations.

use diesel::SqliteConnection;
use slog::Logger;
//use sprockets_host::Ed25519Certificate;
use uuid::Uuid;

use crate::db::Db;
use crate::messages::*;
use crate::trust_quorum::SerializableShareDistribution;

/// Configuration for an individual node
pub struct Config {
    log: Logger,
    db_path: String,
    // TODO: This will live inside the certificate eventually
    //_serial_number: String,
    //_device_id_cert: Ed25519Certificate,
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
                self.handle_initialize(&rack_uuid, share_distribution)
            }
            NodeOp::KeySharePrepare {
                rack_uuid,
                epoch,
                share_distribution,
            } => self.handle_key_share_prepare(
                &rack_uuid,
                epoch,
                share_distribution,
            ),
            NodeOp::KeyShareCommit { rack_uuid, epoch } => {
                self.handle_key_share_commit(rack_uuid, epoch)
            }
        };

        NodeResponse { version: req.version, id: req.id, result }
    }

    // Handle `GetShare` messages from another node
    fn handle_get_share(
        &mut self,
        epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        let share = self.db.get_committed_share(&mut self.conn, epoch)?;
        Ok(NodeOpResult::Share { epoch, share: share.0.share })
    }

    // Handle `Initialize` messages from the coordinator
    fn handle_initialize(
        &mut self,
        rack_uuid: &Uuid,
        share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        self.db.initialize(&mut self.conn, rack_uuid, share_distribution)?;
        Ok(NodeOpResult::CoordinatorAck)
    }

    // Handle `KeySharePrepare` messages from the coordinator
    fn handle_key_share_prepare(
        &mut self,
        rack_uuid: &Uuid,
        epoch: i32,
        share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        if epoch == 0 {
            return Err(NodeError::KeySharePrepareForEpoch0);
        }

        self.db.prepare_share(
            &mut self.conn,
            rack_uuid,
            epoch,
            share_distribution,
        )?;

        Ok(NodeOpResult::CoordinatorAck)
    }

    // Handle `KeyShareCommit` messages from the coordinator
    fn handle_key_share_commit(
        &mut self,
        _rack_uuid: Uuid,
        _epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::KeyShare;
    use crate::db::tests::new_shares;
    use omicron_test_utils::dev::test_setup_log;
    use uuid::Uuid;

    #[test]
    fn initialize_can_run_again_if_epoch_0_is_not_committed() {
        let logctx = test_setup_log("test_db");
        let config =
            Config { log: logctx.log.clone(), db_path: ":memory:".to_string() };
        let mut node = Node::new(config);
        let shares = new_shares();
        let expected = Ok(NodeOpResult::CoordinatorAck);
        assert_eq!(
            expected,
            node.handle_initialize(&Uuid::new_v4(), shares[0].clone().into())
        );
        // We can re-initialize with a new uuid
        let rack_uuid = Uuid::new_v4();
        assert_eq!(
            expected,
            node.handle_initialize(&rack_uuid, shares[0].clone().into())
        );

        // We can re-initialize with a the same uuid
        assert_eq!(
            expected,
            node.handle_initialize(&rack_uuid, shares[0].clone().into())
        );

        // Committing the key share for epoch 0 means we cannot initialize again
        let epoch = 0;
        let prepare = KeyShare::new(epoch, shares[0].clone().into()).unwrap();
        node.db
            .commit_share(&mut node.conn, epoch, prepare.share_digest.into())
            .unwrap();

        assert!(node
            .handle_initialize(&rack_uuid, shares[0].clone().into())
            .is_err());

        logctx.cleanup_successful();
    }
}
