// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual bootstore node.
//!
//! Most logic is contained here, but networking sits on top.
//! This allows easier testing of clusters and failure situations.

use slog::Logger;
use sprockets_common::Sha3_256Digest;
use uuid::Uuid;

use crate::db::Db;
use crate::messages::*;
use crate::trust_quorum::SerializableShareDistribution;

/// Configuration for an individual node
pub struct Config {
    pub log: Logger,
    pub db_path: String,
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
}

impl Node {
    /// Create a new Node
    pub fn new(config: Config) -> Node {
        let db = Db::init(&config.log, &config.db_path).unwrap();
        Node { config, db }
    }

    /// Handle a message received over sprockets from another [`Node`] or
    /// the [`Coordinator`].
    pub fn handle(&mut self, req: NodeRequest) -> NodeResponse {
        if req.version != 1 {
            return NodeResponse {
                version: req.version,
                coordinator_id: req.coordinator_id,
                result: Err(NodeError::UnsupportedVersion(req.version)),
            };
        }

        // TODO: Check coordinator id before handling requests
        // This requires storing the coordinator id in the Db

        let result = match req.op {
            NodeOp::GetShare { rack_uuid, epoch } => {
                self.handle_get_share(&rack_uuid, epoch)
            }
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

        NodeResponse {
            version: req.version,
            coordinator_id: req.coordinator_id,
            result,
        }
    }

    pub fn has_key_share_prepare(
        &self,
        rack_uuid: &Uuid,
        epoch: i32,
    ) -> Result<bool, NodeError> {
        let res = self.db.has_key_share_prepare(rack_uuid, epoch)?;
        Ok(res)
    }

    pub fn is_initialized(&self, rack_uuid: &Uuid) -> Result<bool, NodeError> {
        let res = self.db.is_initialized(rack_uuid)?;
        Ok(res)
    }

    // Handle `GetShare` messages from another node
    fn handle_get_share(
        &self,
        rack_uuid: &Uuid,
        epoch: i32,
    ) -> Result<NodeOpResult, NodeError> {
        let share = self.db.get_committed_share(rack_uuid, epoch)?;
        Ok(NodeOpResult::Share { epoch, share: share.0.share.clone() })
    }

    // Handle `Initialize` messages from the coordinator
    fn handle_initialize(
        &mut self,
        rack_uuid: &Uuid,
        share_distribution: SerializableShareDistribution,
    ) -> Result<NodeOpResult, NodeError> {
        self.db.initialize(rack_uuid, share_distribution)?;
        Ok(NodeOpResult::PrepareOk { rack_uuid: *rack_uuid, epoch: 0 })
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

        self.db.prepare_share(rack_uuid, epoch, share_distribution)?;

        Ok(NodeOpResult::PrepareOk { rack_uuid: *rack_uuid, epoch })
    }

    // Handle `KeyShareCommit` messages from the coordinator
    fn handle_key_share_commit(
        &mut self,
        rack_uuid: &Uuid,
        epoch: i32,
        prepare_shared_distribution_digest: Sha3_256Digest,
    ) -> Result<NodeOpResult, NodeError> {
        self.db.commit_share(
            rack_uuid,
            epoch,
            prepare_shared_distribution_digest,
        )?;

        Ok(NodeOpResult::CommitOk { rack_uuid: *rack_uuid, epoch })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::models::KeyShare;
    use crate::db::tests::new_shares;
    use crate::trust_quorum::ShareDistribution;
    use assert_matches::assert_matches;
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
        assert_matches!(
            node.handle_initialize(&Uuid::new_v4(), sd.clone()),
            Ok(NodeOpResult::PrepareOk { .. })
        );
        // We can re-initialize with a new uuid
        let rack_uuid = Uuid::new_v4();
        assert_matches!(
            node.handle_initialize(&rack_uuid, sd.clone()),
            Ok(NodeOpResult::PrepareOk { .. })
        );

        // We can re-initialize with the same uuid
        assert_matches!(
            node.handle_initialize(&rack_uuid, sd.clone()),
            Ok(NodeOpResult::PrepareOk { .. })
        );

        // Committing the key share for epoch 0 means we cannot initialize again
        let epoch = 0;
        let prepare = KeyShare::new(epoch, sd.clone()).unwrap();
        node.db
            .commit_share(&rack_uuid, epoch, prepare.share_digest.into())
            .unwrap();

        let expected =
            Err(NodeError::Db(db::Error::AlreadyInitialized(rack_uuid)));
        assert_eq!(expected, node.handle_initialize(&rack_uuid, sd));

        logctx.cleanup_successful();
    }

    #[test]
    fn initialize_and_reconfigure() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization - The initialize request is the prepare
        // Then we commit it. In the future, we will also prepare and commit a plan
        // as well as a share distribution. In that case we will likely have a separate
        // `InitializeCommit` message and will likely change the `Initialize` message to
        // `InitializePrepare`.
        assert!(node.handle_initialize(&rack_uuid, sd).is_ok());
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

        // Let's simulate a successful reconfiguration
        let sd: SerializableShareDistribution = new_shares()[0].clone().into();
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 1;
        assert!(node.handle_key_share_prepare(&rack_uuid, epoch, sd).is_ok());
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

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
        assert!(node.handle_initialize(&rack_uuid, sd.clone()).is_ok());

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
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization
        assert!(node.handle_initialize(&rack_uuid, sd.clone()).is_ok());
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

        // Attempt to prepare with an invalid rack uuid
        let bad_uuid = Uuid::new_v4();
        let epoch = 1;
        assert_eq!(
            Err(NodeError::Db(db::Error::RackUuidMismatch {
                expected: bad_uuid,
                actual: Some(rack_uuid)
            })),
            node.handle_key_share_prepare(&bad_uuid, epoch, sd.clone())
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn prepare_is_idempotent() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization
        assert!(node.handle_initialize(&rack_uuid, sd.clone()).is_ok());
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

        // Prepare succeeds
        let epoch = 1;
        assert!(node
            .handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
            .is_ok());

        // Same prepare succeeds
        assert!(node
            .handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
            .is_ok());

        logctx.cleanup_successful();
    }

    #[test]
    fn prepares_for_old_epochs_fail() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization
        assert!(node.handle_initialize(&rack_uuid, sd.clone()).is_ok());
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

        // Prepare succeeds
        let epoch = 1;
        assert!(node
            .handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
            .is_ok());

        // Prepare succeeds
        let epoch = 2;
        assert!(node
            .handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
            .is_ok());

        // Prepare fails
        let epoch = 1;
        assert_eq!(
            NodeError::Db(db::Error::OldKeySharePrepare {
                epoch: 1,
                stored_epoch: 2
            }),
            node.handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
                .unwrap_err()
        );

        // Idempotent prepare still succeeds
        let epoch = 2;
        assert!(node
            .handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
            .is_ok());

        // Commit the prepare
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

        // Old prepare still fails the same way
        let epoch = 1;
        assert_eq!(
            NodeError::Db(db::Error::OldKeySharePrepare {
                epoch: 1,
                stored_epoch: 2
            }),
            node.handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
                .unwrap_err()
        );

        // Now committed prepare fails
        let epoch = 2;
        assert_eq!(
            NodeError::Db(db::Error::KeyShareAlreadyCommitted { epoch }),
            node.handle_key_share_prepare(&rack_uuid, epoch, sd.clone())
                .unwrap_err()
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn mismatched_prepares_fail() {
        let (logctx, mut node, share_distributions) = setup();
        let sd: SerializableShareDistribution =
            share_distributions[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        let sd_digest =
            KeyShare::share_distribution_digest(&sd).unwrap().into();
        let epoch = 0;

        // Successful rack initialization
        assert!(node.handle_initialize(&rack_uuid, sd.clone()).is_ok());
        assert!(node
            .handle_key_share_commit(&rack_uuid, epoch, sd_digest)
            .is_ok());

        // Prepare succeeds
        let epoch = 1;
        assert!(node.handle_key_share_prepare(&rack_uuid, epoch, sd).is_ok());

        // Prepare with a different share_distribution fails
        let sd2: SerializableShareDistribution =
            share_distributions[1].clone().into();
        assert_eq!(
            NodeError::Db(db::Error::KeySharePrepareAlreadyExists { epoch }),
            node.handle_key_share_prepare(&rack_uuid, epoch, sd2).unwrap_err()
        );

        logctx.cleanup_successful();
    }
}
