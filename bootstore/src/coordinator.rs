// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A coordinator for bootstore transactions
//!
//! Bootstore transactions are performed via 2-phase commit (2PC), where the
//! [`Coordinator`] issues Prepare and Accept statements to [`Node`]s. The
//! coordinator itself does not persist state, but instead receives requests via
//! its API that are driven ultimately by RSS and Nexus and returns results that
//! are persisted in CockroachDB.

use crate::messages::{
    NodeError, NodeOp, NodeOpResult, NodeRequest, NodeResponse,
};
use crate::trust_quorum::{
    RackSecret, SerializableShareDistribution, ShareDistribution,
};
use sha3::{Digest, Sha3_256};
use slog::{error, o, Logger};
use sprockets_host::Ed25519Certificate;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

// TODO: It would be nice to have a printable ID in the cert other than
// the pub key. This will change when we use X509.v3 certs in sprockets.
#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("Failed to split secret: {0:?}")]
    FailedToSplitSecret(vsss_rs::Error),

    #[error("BCS serialization error: {err}")]
    Bcs { err: String },

    #[error(transparent)]
    NodeResponseError(#[from] NodeError),

    #[error(
        "PrepareOk received with wrong rack UUID: Expected: {expected}, \
    Actual: {actual}"
    )]
    PrepareOkBadRackUuid {
        from: Ed25519Certificate,
        expected: Uuid,
        actual: Uuid,
    },

    #[error(
        "PrepareOk received with wrong epoch: Expected: {expected}, Actual: \
{actual}"
    )]
    PrepareOkBadEpoch { from: Ed25519Certificate, expected: i32, actual: i32 },

    #[error(
        "CommitOk received with wrong rack UUID: Expected: {expected}, Actual: \
{actual}"
    )]
    CommitOkBadRackUuid {
        from: Ed25519Certificate,
        expected: Uuid,
        actual: Uuid,
    },

    #[error(
        "CommitOk received with wrong epoch: Expected: {expected}, Actual: \
{actual}"
    )]
    CommitOkBadEpoch { from: Ed25519Certificate, expected: i32, actual: i32 },

    #[error("Unexpected share received for epoch {epoch}")]
    UnexpectedShare { from: Ed25519Certificate, epoch: i32 },

    #[error(
        "Unexpected PrepareOk received for epoch {epoch} with rack uuid: \
{rack_uuid}"
    )]
    UnexpectedPrepareOk {
        from: Ed25519Certificate,
        epoch: i32,
        rack_uuid: Uuid,
    },

    #[error(
        "Unexpected PrepareOk received for epoch {epoch} with rack uuid: \
{rack_uuid}"
    )]
    UnexpectedCommitOk { from: Ed25519Certificate, epoch: i32, rack_uuid: Uuid },

    // Response received from a Node that is not a member
    #[error(
        "Response received from node that is not a member for epoch {epoch} \
with rack uuid: {rack_uuid}"
    )]
    NotAMember { from: Ed25519Certificate, epoch: i32, rack_uuid: Uuid },
}

/// The internal state of a [`Coordinator`] shared among all
/// [`CoordinatorOperation`]s.
struct CoordinatorState {
    // A monotonic counter incremented and maintained by nexus everytime a new
    // coordinator takes over.
    id: u64,
    log: Logger,
    rack_uuid: Uuid,
    total_nodes: usize,
    ackd_prepares: BTreeSet<Ed25519Certificate>,
    ackd_commits: BTreeSet<Ed25519Certificate>,
}

impl CoordinatorState {
    /// Return true if all nodes have acknowledged the prepare
    pub fn prepare_complete(&self) -> bool {
        self.ackd_prepares.len() == self.total_nodes
    }

    /// Return true if all nodes have acknowledged the commit
    pub fn commit_complete(&self) -> bool {
        self.ackd_commits.len() == self.total_nodes
    }
}

/// A [`Coordinator`] is only used for one transaction
///
/// A specific [`CoordinatorOperation`] is used for each transaction.
trait CoordinatorOperation {
    fn prepare(
        &mut self,
        state: &mut CoordinatorState,
    ) -> Result<BTreeMap<Ed25519Certificate, NodeOp>, Error>;
    fn commit(
        &mut self,
        state: &mut CoordinatorState,
    ) -> Result<BTreeMap<Ed25519Certificate, NodeOp>, Error>;

    fn handle(
        &mut self,
        state: &mut CoordinatorState,
        from: Ed25519Certificate,
        result: NodeOpResult,
    ) -> Result<bool, Error>;
}

/// A [`CoordinatorOperation`] for rack initialization
struct InitializeOperation {
    members: BTreeSet<Ed25519Certificate>,
    share_distributions: BTreeMap<Ed25519Certificate, ShareDistribution>,
}

impl CoordinatorOperation for InitializeOperation {
    /// Return a [`NodeOp::Initialize`] request for each node that has not yet acked
    fn prepare(
        &mut self,
        state: &mut CoordinatorState,
    ) -> Result<BTreeMap<Ed25519Certificate, NodeOp>, Error> {
        Ok(self
            .share_distributions
            .iter()
            .filter(|(cert, _)| !state.ackd_prepares.contains(cert))
            .map(|(cert, sd)| {
                (
                    *cert,
                    NodeOp::Initialize {
                        rack_uuid: state.rack_uuid,
                        share_distribution: sd.clone().into(),
                    },
                )
            })
            .collect())
    }

    fn commit(
        &mut self,
        state: &mut CoordinatorState,
    ) -> Result<BTreeMap<Ed25519Certificate, NodeOp>, Error> {
        let mut output = BTreeMap::new();
        for (cert, sd) in self
            .share_distributions
            .iter()
            .filter(|(cert, _)| !state.ackd_commits.contains(cert))
        {
            let sd: SerializableShareDistribution = sd.clone().into();
            let bytes = bcs::to_bytes(&sd)
                .map_err(|err| Error::Bcs { err: err.to_string() })?;
            let prepare_share_distribution_digest =
                sprockets_common::Sha3_256Digest(
                    Sha3_256::digest(&bytes).into(),
                );

            output.insert(
                *cert,
                NodeOp::KeyShareCommit {
                    rack_uuid: state.rack_uuid,
                    epoch: 0,
                    prepare_share_distribution_digest,
                },
            );
        }
        Ok(output)
    }

    fn handle(
        &mut self,
        state: &mut CoordinatorState,
        from: Ed25519Certificate,
        result: NodeOpResult,
    ) -> Result<bool, Error> {
        if !self.members.contains(&from) {
            return Err(Error::NotAMember {
                from,
                epoch: 0,
                rack_uuid: state.rack_uuid,
            });
        }

        if !state.prepare_complete() {
            // We're expecting a `PrepareOk`
            match result {
                NodeOpResult::PrepareOk { rack_uuid, epoch } => {
                    if rack_uuid != state.rack_uuid {
                        return Err(Error::PrepareOkBadRackUuid {
                            from,
                            expected: state.rack_uuid,
                            actual: rack_uuid,
                        });
                    }
                    if epoch != 0 {
                        return Err(Error::PrepareOkBadEpoch {
                            from,
                            expected: 0,
                            actual: epoch,
                        });
                    }
                    state.ackd_prepares.insert(from);
                    return Ok(false);
                }
                NodeOpResult::CommitOk { rack_uuid, epoch } => {
                    return Err(Error::UnexpectedCommitOk {
                        from,
                        epoch,
                        rack_uuid,
                    });
                }
                NodeOpResult::Share { epoch, .. } => {
                    return Err(Error::UnexpectedShare { from, epoch });
                }
            }
        }

        // Handle CommitOk messages
        match result {
            NodeOpResult::CommitOk { rack_uuid, epoch } => {
                if rack_uuid != state.rack_uuid {
                    return Err(Error::CommitOkBadRackUuid {
                        from,
                        expected: state.rack_uuid,
                        actual: rack_uuid,
                    });
                }
                if epoch != 0 {
                    return Err(Error::CommitOkBadEpoch {
                        from,
                        expected: 0,
                        actual: epoch,
                    });
                }
                state.ackd_commits.insert(from);
            }
            NodeOpResult::PrepareOk { rack_uuid, epoch } => {
                return Err(Error::UnexpectedPrepareOk {
                    from,
                    epoch,
                    rack_uuid,
                });
            }
            NodeOpResult::Share { epoch, .. } => {
                return Err(Error::UnexpectedShare { from, epoch });
            }
        }

        Ok(state.commit_complete())
    }
}

/// A [`CoordinatorOperation`] for rack reconfiguration
//
// TODO: Not used yet
#[allow(dead_code)]
struct ReconfigureOperation {
    old_epoch: i32,
    old_members: Vec<Ed25519Certificate>,
    new_epoch: i32,
    new_members: Vec<Ed25519Certificate>,
    share_distributions: BTreeMap<Ed25519Certificate, ShareDistribution>,
}

/// A coordinator for the bootstore's 2PC protocol
///
/// The coordinatore is responsible for creating the [`trust_quorum::RackSecret`]
/// splitting it into share sand distributing those shares along with the
/// relevant information to the participant nodes.
pub struct Coordinator {
    state: CoordinatorState,
    op: Box<dyn CoordinatorOperation>,
}

impl Coordinator {
    /// Create a coordinator used to initialize a rack
    pub fn new_initialize(
        log: Logger,
        rack_uuid: Uuid,
        members: BTreeSet<Ed25519Certificate>,
    ) -> Result<Coordinator, Error> {
        let log = log.new(o!(
            "component" => "BootstoreCoordinator"
        ));
        let total_nodes = members.len();
        let threshold = Self::threshold(total_nodes);
        let secret = RackSecret::new();
        let (shares, verifier) = secret
            .split(threshold, total_nodes)
            .map_err(Error::FailedToSplitSecret)?;
        let share_distributions = members
            .iter()
            .cloned()
            .zip(shares.into_iter().map(|share| ShareDistribution {
                threshold,
                verifier: verifier.clone(),
                share,
                member_device_id_certs: members.clone(),
            }))
            .collect();
        let state = CoordinatorState {
            id: 0,
            log,
            rack_uuid,
            total_nodes,
            ackd_prepares: BTreeSet::new(),
            ackd_commits: BTreeSet::new(),
        };
        Ok(Coordinator {
            state,
            op: Box::new(InitializeOperation { members, share_distributions }),
        })
    }

    /// Handle responses from nodes.
    ///
    /// Return `Ok(true)` if the transaction is complete, `Ok(false)` if
    /// the response was handled fine, but the transaction is not complete.
    pub fn handle(
        &mut self,
        from: Ed25519Certificate,
        rsp: NodeResponse,
    ) -> Result<bool, Error> {
        // TODO: Should  these conditions be passed up as errors?
        // If we pass them up, should we also log them?
        if rsp.version != 1 {
            error!(
                self.state.log,
                "Invalid version for response: {}", rsp.version
            );
        }

        if rsp.coordinator_id != self.state.id {
            error!(
                self.state.log,
                "Invalid Coordinator ID. Expected: {}, Actual: {}",
                self.state.id,
                rsp.coordinator_id
            );
        }

        match rsp.result {
            Ok(result) => self.op.handle(&mut self.state, from, result),
            Err(err) => Err(Error::NodeResponseError(err)),
        }
    }

    /// Return a set of [`NodeRequests`] to send to all members that haven't acked yet.
    /// A higher "networking" layer is responsible for sending these messages over
    /// sprockets channels and receiving responses to be handled by the `Coordinator`.
    pub fn next_requests(
        &mut self,
    ) -> Result<BTreeMap<Ed25519Certificate, NodeRequest>, Error> {
        let ops = if !self.prepare_complete() {
            self.op.prepare(&mut self.state)?
        } else if !self.commit_complete() {
            self.op.commit(&mut self.state)?
        } else {
            // We are done
            BTreeMap::new()
        };

        Ok(ops
            .into_iter()
            .map(|(cert, op)| {
                (
                    cert,
                    NodeRequest {
                        version: 1,
                        coordinator_id: self.state.id,
                        op,
                    },
                )
            })
            .collect())
    }

    pub fn prepare_complete(&self) -> bool {
        self.state.prepare_complete()
    }

    pub fn commit_complete(&self) -> bool {
        self.state.commit_complete()
    }

    // Return the trust quorum threshold required to unlock the rack.
    fn threshold(total_nodes: usize) -> usize {
        assert!(total_nodes > 1);
        if total_nodes < 6 {
            2
        } else {
            total_nodes / 2 - 1
        }
    }
}
