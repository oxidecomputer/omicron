// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanism to track and compute this node's key share for a configuration
//!
//! When a node learns of a committed configuration but does not have a key
//! share for that configuration it must collect a threshold of key shares from
//! other nodes so  that it can compute its own key share.

use crate::{
    Alarm, Configuration, Epoch, NodeHandlerCtx, PeerMsgKind, BaseboardId,
};
use gfss::gf256::Gf256;
use gfss::shamir::{self, Share};
use slog::{Logger, error, o};
use std::collections::BTreeMap;

/// In memory state that tracks retrieval of key shares in order to compute
/// this node's key share for a given configuration.
#[derive(Debug, Clone)]
pub struct KeyShareComputer {
    log: Logger,

    // A copy of the configuration stored in persistent state
    config: Configuration,

    collected_shares: BTreeMap<BaseboardId, Share>,
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl PartialEq for KeyShareComputer {
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
            && self.collected_shares == other.collected_shares
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl Eq for KeyShareComputer {}

impl KeyShareComputer {
    pub fn new(
        log: &Logger,
        ctx: &mut impl NodeHandlerCtx,
        config: Configuration,
    ) -> KeyShareComputer {
        let log = log.new(o!("component" => "tq-key-share-computer"));

        for id in config.members.keys() {
            if ctx.connected().contains(id) {
                ctx.send(id.clone(), PeerMsgKind::GetShare(config.epoch));
            }
        }

        KeyShareComputer { log, config, collected_shares: BTreeMap::new() }
    }

    pub fn config(&self) -> &Configuration {
        &self.config
    }

    pub fn on_connect(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        peer: BaseboardId,
    ) {
        if self.config.members.contains_key(&peer)
            && !self.collected_shares.contains_key(&peer)
        {
            ctx.send(peer, PeerMsgKind::GetShare(self.config.epoch));
        }
    }

    /// We received a key share
    ///
    /// Return true if we have computed and saved our key share to the
    /// persistent state, false otherwise.
    pub fn handle_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: BaseboardId,
        epoch: Epoch,
        share: Share,
    ) -> bool {
        if !crate::validate_share(&self.log, &self.config, &from, epoch, &share)
        {
            // Logging done inside `validate_share`
            return false;
        };

        // A valid share was received. Is it new?
        if self.collected_shares.insert(from, share).is_some() {
            return false;
        }

        // Do we have enough shares to compute our rack share?
        if self.collected_shares.len() < self.config.threshold.0 as usize {
            return false;
        }

        // Share indices are assigned according the configuration membership's
        // key order, when the configuration is constructed.
        //
        // What index are we in the configuration? This is our "x-coordinate"
        // for our key share calculation. We always start indexing from 1, since
        // 0 is the rack secret.
        //
        let index =
            self.config.members.keys().position(|id| id == ctx.platform_id());

        let Some(index) = index else {
            let msg = concat!(
                "Failed to get index for ourselves in current configuration. ",
                "We are not a member, and must have been expunged."
            );
            error!(
                self.log,
                "{msg}";
                "platform_id" => %ctx.platform_id(),
                "config" => ?self.config
            );
            return false;
        };

        let x_coordinate =
            Gf256::new(u8::try_from(index + 1).expect("index fits in u8"));

        let shares: Vec<_> = self.collected_shares.values().cloned().collect();

        match shamir::compute_share(&shares, x_coordinate) {
            Ok(our_share) => {
                ctx.update_persistent_state(|ps| {
                    let inserted_share =
                        ps.shares.insert(epoch, our_share).is_none();
                    let inserted_commit = ps.commits.insert(epoch);
                    inserted_share || inserted_commit
                });
                true
            }
            Err(err) => {
                error!(self.log, "Failed to compute share: {}", err);
                ctx.raise_alarm(Alarm::ShareComputationFailed { epoch, err });
                false
            }
        }
    }
}
