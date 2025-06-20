// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State of a reconfiguration coordinator inside a [`crate::Node`]

use crate::crypto::{LrtqShare, Sha3_256Digest, ShareDigestLrtq};
use crate::messages::PeerMsg;
use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{Configuration, Envelope, Epoch, PeerMsgKind, PlatformId};
use gfss::shamir::Share;
use slog::{Logger, o, warn};
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
pub struct CoordinatorState {
    log: Logger,

    /// When the reconfiguration started
    #[expect(unused)]
    start_time: Instant,

    /// A copy of the message used to start this reconfiguration
    reconfigure_msg: ValidatedReconfigureMsg,

    /// Configuration that will get persisted inside a `Prepare` message in a
    /// `Node`s `PersistentState`, once it is possible to create the Prepare.
    configuration: Configuration,

    /// What is the coordinator currently doing
    op: CoordinatorOperation,

    /// When to resend prepare messages next
    retry_deadline: Instant,
}

impl CoordinatorState {
    /// Start coordinating a reconfiguration for a brand new trust quorum
    ///
    /// Return the newly constructed `CoordinatorState` along with this node's
    /// `PrepareMsg` so that it can be persisted.
    pub fn new_uninitialized(
        log: Logger,
        now: Instant,
        msg: ValidatedReconfigureMsg,
    ) -> Result<(CoordinatorState, Configuration, Share), ReconfigurationError>
    {
        // Create a configuration for this epoch
        let (config, shares) = Configuration::new(&msg)?;

        let mut prepares = BTreeMap::new();
        // `my_config` and `my_share` are optional only so that we can fill them
        // in via the loop. They will always become `Some`, as a `Configuration`
        // always contains the coordinator as a member as validated by
        // construction of `ValidatedReconfigureMsg`.
        let mut my_config: Option<Configuration> = None;
        let mut my_share: Option<Share> = None;
        for (platform_id, share) in shares.into_iter() {
            if platform_id == *msg.coordinator_id() {
                // The data to add to our `PersistentState`
                my_config = Some(config.clone());
                my_share = Some(share);
            } else {
                // Create a message that requires sending
                prepares.insert(platform_id, (config.clone(), share));
            }
        }
        let op = CoordinatorOperation::Prepare {
            prepares,
            prepare_acks: BTreeSet::new(),
        };

        let state = CoordinatorState::new(log, now, msg, config, op);

        // Safety: Construction of a `ValidatedReconfigureMsg` ensures that
        // `my_platform_id` is part of the new configuration and has a share.
        // We can therefore safely unwrap here.
        Ok((state, my_config.unwrap(), my_share.unwrap()))
    }

    /// A reconfiguration from one group to another
    pub fn new_reconfiguration(
        log: Logger,
        now: Instant,
        msg: ValidatedReconfigureMsg,
        last_committed_config: &Configuration,
    ) -> Result<CoordinatorState, ReconfigurationError> {
        let (config, new_shares) = Configuration::new(&msg)?;

        // We must collect shares from the last configuration
        // so we can recompute the old rack secret.
        let op = CoordinatorOperation::CollectShares {
            epoch: last_committed_config.epoch,
            members: last_committed_config.members.clone(),
            collected_shares: BTreeMap::new(),
            new_shares,
        };

        Ok(CoordinatorState::new(log, now, msg, config, op))
    }

    // Intentionally private!
    //
    // The public constructors `new_uninitialized` and `new_reconfiguration` are
    // more specific, and perform validation of arguments.
    fn new(
        log: Logger,
        now: Instant,
        reconfigure_msg: ValidatedReconfigureMsg,
        configuration: Configuration,
        op: CoordinatorOperation,
    ) -> CoordinatorState {
        // We want to send any pending messages immediately
        let retry_deadline = now;
        CoordinatorState {
            log: log.new(o!("component" => "tq-coordinator-state")),
            start_time: now,
            reconfigure_msg,
            configuration,
            op,
            retry_deadline,
        }
    }

    // Return the `ValidatedReconfigureMsg` that started this reconfiguration
    pub fn reconfigure_msg(&self) -> &ValidatedReconfigureMsg {
        &self.reconfigure_msg
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
    #[expect(unused)]
    pub fn send_msgs(&mut self, now: Instant, outbox: &mut Vec<Envelope>) {
        if now < self.retry_deadline {
            return;
        }
        self.retry_deadline = now + self.reconfigure_msg.retry_timeout();
        match &self.op {
            CoordinatorOperation::CollectShares {
                epoch,
                members,
                collected_shares,
                ..
            } => {}
            CoordinatorOperation::CollectLrtqShares { members, shares } => {}
            CoordinatorOperation::Prepare { prepares, prepare_acks } => {
                let rack_id = self.reconfigure_msg.rack_id();
                for (platform_id, (config, share)) in
                    prepares.clone().into_iter()
                {
                    outbox.push(Envelope {
                        to: platform_id,
                        from: self.reconfigure_msg.coordinator_id().clone(),
                        msg: PeerMsg {
                            rack_id,
                            kind: PeerMsgKind::Prepare { config, share },
                        },
                    });
                }
            }
        }
    }

    /// Record a `PrepareAck` from another node as part of tracking
    /// quorum for the prepare phase of the trust quorum protocol.
    pub fn ack_prepare(&mut self, from: PlatformId) {
        match &mut self.op {
            CoordinatorOperation::Prepare {
                prepares, prepare_acks, ..
            } => {
                if !self.configuration.members.contains_key(&from) {
                    warn!(
                        self.log,
                        "PrepareAck from node that is not a cluster member";
                        "epoch" => %self.configuration.epoch,
                        "from" => %from
                    );
                    return;
                }

                // Remove the responder so we don't ask it again
                prepares.remove(&from);

                // Save the ack for quorum purposes
                prepare_acks.insert(from);
            }
            op => {
                warn!(
                    self.log,
                    "Ack received when coordinator is not preparing";
                    "op" => op.name(),
                    "from" => %from
                );
            }
        }
    }
}

/// What should the coordinator be doing?
pub enum CoordinatorOperation {
    // We haven't started implementing this yet
    #[expect(unused)]
    CollectShares {
        epoch: Epoch,
        members: BTreeMap<PlatformId, Sha3_256Digest>,
        collected_shares: BTreeMap<PlatformId, Share>,
        new_shares: BTreeMap<PlatformId, Share>,
    },
    // We haven't started implementing this yet
    // Epoch is always 0
    #[allow(unused)]
    CollectLrtqShares {
        members: BTreeMap<PlatformId, ShareDigestLrtq>,
        shares: BTreeMap<PlatformId, LrtqShare>,
    },
    Prepare {
        /// The set of Prepares to send to each node
        prepares: BTreeMap<PlatformId, (Configuration, Share)>,

        /// Acknowledgements that the prepare has been received
        prepare_acks: BTreeSet<PlatformId>,
    },
}

impl CoordinatorOperation {
    pub fn name(&self) -> &'static str {
        match self {
            CoordinatorOperation::CollectShares { .. } => "collect shares",
            CoordinatorOperation::CollectLrtqShares { .. } => {
                "collect lrtq shares"
            }
            CoordinatorOperation::Prepare { .. } => "prepare",
        }
    }
}
