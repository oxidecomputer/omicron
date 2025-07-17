// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the [`crate::FsmState`] for coordinating reconfigurations

use crate::crypto::{LrtqShare, Sha3_256Digest, ShareDigestLrtq};
use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{
    Configuration, Epoch, FsmCtxApi, FsmState, PeerMsg, PlatformId,
    ReconfigureMsg,
};
use gfss::shamir::Share;
use slog::{Logger, o};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

/// The [`FsmState`] for coordinating a reconfiguration
///
/// A coordinator can be any trust quorum node that is a member of both the old
/// and new group. The coordinator is chosen by Nexus for a given epoch when a
/// trust quorum reconfiguration is triggered. Reconfiguration is only performed
/// when the control plane is up, as we use Nexus to persist prepares and ensure
/// commitment happens, even if the system crashes while committing. If a
/// rack crash (such as a power outage) occurs before nexus is informed of the
/// prepares, nexus will  skip the epoch and start a new reconfiguration. This
/// allows progress to always be made with a full linearization of epochs.
pub struct Coordinating {
    log: Logger,

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

impl Coordinating {
    /// Start coordinating a reconfiguration for a brand new trust quorum
    ///
    /// Return the newly constructed `CoordinatorState` along with this node's
    /// `PrepareMsg` so that it can be persisted.
    pub fn new_uninitialized(
        log: &Logger,
        ctx: &mut dyn FsmCtxApi,
        msg: ValidatedReconfigureMsg,
    ) -> Result<Self, ReconfigurationError> {
        // Create a configuration for this epoch
        let (config, shares) = Configuration::new(&msg)?;

        let mut prepares = BTreeMap::new();

        // SAFETY: We know that the this config is new because it has already
        // because we have a `ValidatedReconfigurationMsg`. The expect is solely
        // an assertion to catch programmer bugs.
        ctx.persistent_state_mut()
            .configs
            .insert_unique(config.clone())
            .expect("empty state");

        for (platform_id, share) in shares.into_iter() {
            if platform_id == *msg.coordinator_id() {
                // This conditional will always be entered since a
                // `ValidatedReconfigureMsg` ensures that this node is a member
                // of the configuration.
                ctx.persistent_state_mut().shares.insert(config.epoch, share);
            } else {
                // Create a message that requires sending
                //
                // We only send to nodes other than ourself
                prepares.insert(platform_id, (config.clone(), share));
            }
        }

        let op = CoordinatorOperation::Prepare {
            prepares,
            prepare_acks: BTreeSet::new(),
        };

        let state = Self {
            log: log.new(o!("state" => "coordinating")),
            reconfigure_msg: msg,
            configuration: config,
            op,
            retry_deadline: ctx.now(),
        };

        Ok(state)
    }
}

impl FsmState for Coordinating {
    fn name(&self) -> &'static str {
        "coordinating"
    }

    fn handle(
        &mut self,
        ctx: &mut dyn FsmCtxApi,
        from: PlatformId,
        msg: PeerMsg,
    ) {
        todo!();
    }

    fn tick(&mut self, ctx: &mut dyn super::FsmCtxApi) {
        todo!()
    }

    fn coordinate_reconfiguration(
        &mut self,
        ctx: &mut dyn FsmCtxApi,
        msg: ReconfigureMsg,
    ) -> Result<(), ReconfigurationError> {
        let Some(validated_msg) = ValidatedReconfigureMsg::new(
            &self.log,
            ctx.platform_id(),
            msg,
            ctx.persistent_state().into(),
            Some(&self.reconfigure_msg),
        )?
        else {
            // This was an idempotent (duplicate) request.
            return Ok(());
        };

        // TODO: Handle a new reconfiguration

        todo!()
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
