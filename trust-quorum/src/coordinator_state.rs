// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State of a reconfiguration coordinator inside a [`crate::Node`]

use crate::crypto::{LrtqShare, Sha3_256Digest, ShareDigestLrtq};
use crate::messages::{PeerMsg, PrepareMsg};
use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{Configuration, Envelope, Epoch, PlatformId};
use gfss::shamir::{SecretShares, Share};
use secrecy::ExposeSecret;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

/// The state of a reconfiguration coordinator.
///
/// A coordinator can be any trust quorum node that is a member of both the old
/// and new group. The coordinator is chosen by Nexus for a given epoch when a
/// trust quorum reconfiguration is triggered. Reconfiguration is only performed
/// when the control plane is up, as we use Nexus to persist prepares and ensure
/// commitment happens, even if the system crashes while committing. If a
/// rack crash (such as a power outage) occurs before nexus is informed of the
/// prepares, nexus will  skip the epoch and start a new reconfiguration. This
/// allows progress to always be made with a full linearization of epochs.
///
/// We allow some unused fields before we complete the coordination code
#[allow(unused)]
pub struct CoordinatorState {
    /// A copy of the platform_id from [`Node`] purely for ergonomics
    platform_id: PlatformId,

    /// When the reconfiguration started
    pub start_time: Instant,

    /// A copy of the message used to start this reconfiguration
    pub reconfigure_msg: ValidatedReconfigureMsg,

    /// Configuration that will get persisted inside a `Prepare` message in a
    /// `Node`s `PersistentState`, once it is possible to create the Prepare.
    pub configuration: Configuration,

    /// What is the coordinator currently doing
    pub op: CoordinatorOperation,

    /// When to resend prepare messages next
    pub retry_deadline: Instant,
}

impl CoordinatorState {
    /// Start coordinating a reconfiguration for a brand new trust quorum
    ///
    /// Return the newly constructed `CoordinatorState` along with this node's
    /// `PrepareMsg` so that it can be persisted.
    ///
    /// Precondition: This node must be a member of the new configuration
    /// or this method will panic. This is ensured as part of passing in a
    /// `ValidatedReconfigureMsg`.
    pub fn new_uninitialized(
        my_platform_id: PlatformId,
        now: Instant,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(CoordinatorState, PrepareMsg), ReconfigurationError> {
        // Create a configuration for this epoch
        let (config, shares) =
            Configuration::new(my_platform_id.clone(), &msg)?;

        let shares_by_member: BTreeMap<PlatformId, Share> = config
            .members
            .keys()
            .cloned()
            .zip(shares.shares.expose_secret().iter().cloned())
            .collect();

        let mut prepares = BTreeMap::new();
        let mut my_prepare_msg: Option<PrepareMsg> = None;
        for (platform_id, share) in shares_by_member.into_iter() {
            let prepare_msg = PrepareMsg { config: config.clone(), share };
            if platform_id == my_platform_id {
                // The prepare message to add to our `PersistentState`
                my_prepare_msg = Some(prepare_msg);
            } else {
                // Create a message that requires sending
                prepares.insert(platform_id, prepare_msg);
            }
        }
        let op = CoordinatorOperation::Prepare {
            prepares,
            prepare_acks: BTreeSet::new(),
        };

        let state = CoordinatorState::new(my_platform_id, now, msg, config, op);
        Ok((state, my_prepare_msg.unwrap()))
    }

    /// A reconfiguration from one group to another
    pub fn new_reconfiguration(
        my_platform_id: PlatformId,
        now: Instant,
        msg: ValidatedReconfigureMsg,
        last_committed_config: &Configuration,
    ) -> Result<CoordinatorState, ReconfigurationError> {
        let (config, new_shares) =
            Configuration::new(my_platform_id.clone(), &msg)?;

        // We must collect shares from the last configuration
        // so we can recompute the old rack secret.
        let op = CoordinatorOperation::CollectShares {
            epoch: last_committed_config.epoch,
            members: last_committed_config.members.clone(),
            collected_shares: BTreeMap::new(),
            new_shares,
        };

        Ok(CoordinatorState::new(my_platform_id, now, msg, config, op))
    }

    // Intentionallly private!
    fn new(
        platform_id: PlatformId,
        now: Instant,
        reconfigure_msg: ValidatedReconfigureMsg,
        configuration: Configuration,
        op: CoordinatorOperation,
    ) -> CoordinatorState {
        // We always set the retry deadline to `now` so that we will send
        // messages upon new construction. This field gets updated after
        // prepares are sent.
        let retry_deadline = now;
        CoordinatorState {
            platform_id,
            start_time: now,
            reconfigure_msg,
            configuration,
            op,
            retry_deadline,
        }
    }

    // Send any required messages as a reconfiguration coordinator
    //
    // This varies depending upon the current `CoordinatorState`.
    //
    // In some cases a `PrepareMsg` will be added locally to the
    // `PersistentState`, requiring persistence from the caller. In this case we
    // will return a copy of it.
    //
    // This method is "in progress" - allow unused parameters for now
    #[allow(unused)]
    pub fn send_msgs(&mut self, now: Instant, outbox: &mut Vec<Envelope>) {
        match &self.op {
            CoordinatorOperation::CollectShares {
                epoch,
                members,
                collected_shares,
                ..
            } => {}
            CoordinatorOperation::CollectLrtqShares { members, shares } => {}
            CoordinatorOperation::Prepare { prepares, prepare_acks } => {
                for (platform_id, prepare) in prepares.clone().into_iter() {
                    outbox.push(Envelope {
                        to: platform_id,
                        from: self.platform_id.clone(),
                        msg: PeerMsg::Prepare(prepare),
                    });
                }
            }
        }
    }
}

/// What should the coordinator be doing?
///
/// We haven't started implementing upgrade from LRTQ yet
#[allow(unused)]
pub enum CoordinatorOperation {
    CollectShares {
        epoch: Epoch,
        members: BTreeMap<PlatformId, Sha3_256Digest>,
        collected_shares: BTreeMap<PlatformId, Share>,
        new_shares: SecretShares,
    },
    // Epoch is always 0
    CollectLrtqShares {
        members: BTreeMap<PlatformId, ShareDigestLrtq>,
        shares: BTreeMap<PlatformId, LrtqShare>,
    },
    Prepare {
        /// The set of Prepares to send to each node
        prepares: BTreeMap<PlatformId, PrepareMsg>,

        /// Acknowledgements that the prepare has been received
        prepare_acks: BTreeSet<PlatformId>,
    },
}
