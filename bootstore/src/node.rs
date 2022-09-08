// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual bootstore node.
//!
//! Most logic is contained here, but networking sits on top.
//! This allows easier testing of clusters and failure situations.

use diesel::SqliteConnection;
use slog::Logger;
use sprockets_common::Sha3_256Digest;
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
            NodeOp::KeyShareCommit {
                rack_uuid,
                epoch,
                prepare_share_distribution_digest,
            } => self.handle_key_share_commit(
                &rack_uuid,
                epoch,
                prepare_share_distribution_digest,
            ),
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
        rack_uuid: &Uuid,
        epoch: i32,
        prepare_shared_distribution_digest: Sha3_256Digest,
    ) -> Result<NodeOpResult, NodeError> {
        self.db.commit_share(
            &mut self.conn,
            rack_uuid,
            epoch,
            prepare_shared_distribution_digest,
        )?;

        Ok(NodeOpResult::CoordinatorAck)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::models::KeyShare;
    use crate::db::tests::new_shares;
    use crate::trust_quorum::ShareDistribution;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_test_utils::dev::LogContext;
    use uuid::Uuid;

    fn setup() -> (LogContext, Node, Vec<ShareDistribution>) {
        let logctx = test_setup_log("test_db");
        let config =
            Config { log: logctx.log.clone(), db_path: ":memory:".to_string() };
        let node = Node::new(config);
        (logctx, node, new_shares())
    }

    #[test]
    fn initialize_can_run_again_if_epoch_0_is_not_committed() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let expected = Ok(NodeOpResult::CoordinatorAck);
        assert_eq!(
            expected,
            node.handle_initialize(&Uuid::new_v4(), sd.clone())
        );
        // We can re-initialize with a new uuid
        let rack_uuid = Uuid::new_v4();
        assert_eq!(expected, node.handle_initialize(&rack_uuid, sd.clone()));

        // We can re-initialize with a the same uuid
        assert_eq!(expected, node.handle_initialize(&rack_uuid, sd.clone()));

        // Committing the key share for epoch 0 means we cannot initialize again
        let epoch = 0;
        let prepare = KeyShare::new(epoch, sd.clone()).unwrap();
        node.db
            .commit_share(
                &mut node.conn,
                &rack_uuid,
                epoch,
                prepare.share_digest.into(),
            )
            .unwrap();

        let expected = Err(NodeError::Db(db::Error::AlreadyInitialized(
            rack_uuid.to_string(),
        )));
        assert_eq!(expected, node.handle_initialize(&rack_uuid, sd));

        logctx.cleanup_successful();
    }

    #[test]
    fn initialize_and_reconfigure() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let ok_ack = Ok(NodeOpResult::CoordinatorAck);
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization - The initialize request is the prepare
        // Then we commit it. In the future, we will also prepare and commit a plan
        // as well as a share distribution. In that case we will likely have a separate
        // `InitializeCommit` message and will likely change the `Initialize` message to
        // `InitializePrepare`.
        assert_eq!(ok_ack, node.handle_initialize(&rack_uuid, sd));
        assert_eq!(
            ok_ack,
            node.handle_key_share_commit(&rack_uuid, epoch, sd_digest)
        );

        // Let's simulate a successful reconfiguration
        let sd: SerializableShareDistribution = new_shares()[0].clone().into();
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 1;
        assert_eq!(
            ok_ack,
            node.handle_key_share_prepare(&rack_uuid, epoch, sd)
        );
        assert_eq!(
            ok_ack,
            node.handle_key_share_commit(&rack_uuid, epoch, sd_digest)
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn cannot_prepare_if_not_initialized() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let epoch = 1;

        // We don't have even a prepared key share at epoch 0 yet
        assert_eq!(
            Err(NodeError::Db(db::Error::RackNotInitialized)),
            node.handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
        );

        // Create a prepare, but don't commit it
        let ok_ack = Ok(NodeOpResult::CoordinatorAck);
        assert_eq!(ok_ack, node.handle_initialize(&rack_uuid, sd.clone()));

        // Try (and fail) to issue a prepare for epoch 1 again. The actual contents of the
        // share distribution doesn't matter.
        assert_eq!(
            Err(NodeError::Db(db::Error::RackNotInitialized)),
            node.handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn cannot_prepare_with_bad_rack_uuid() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let ok_ack = Ok(NodeOpResult::CoordinatorAck);
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization
        assert_eq!(ok_ack, node.handle_initialize(&rack_uuid, sd.clone()));
        assert_eq!(
            ok_ack,
            node.handle_key_share_commit(&rack_uuid, epoch, sd_digest)
        );

        // Attempt to prepare with an invalid rack uuid
        let bad_uuid = Uuid::new_v4();
        let epoch = 1;
        assert_eq!(
            Err(NodeError::Db(db::Error::RackUuidMismatch {
                expected: bad_uuid.to_string(),
                actual: Some(rack_uuid.to_string())
            })),
            node.handle_key_share_prepare(&bad_uuid, epoch, sd.clone())
        );

        logctx.cleanup_successful();
    }
}
