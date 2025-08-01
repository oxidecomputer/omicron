// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanism to track and compute this node's key share for a configuration
//!
//! When a node learns of a committed configuration but does not have a key
//! share for that configuration it must collect a threshold of key shares from
//! other nodes so  that it can compute its own key share.

use crate::crypto::Sha3_256Digest;
use crate::{
    Alarm, Configuration, Epoch, NodeHandlerCtx, PeerMsgKind, PlatformId,
};
use gfss::gf256::Gf256;
use gfss::shamir::{self, Share};
use slog::{Logger, error, o, warn};
use std::collections::BTreeMap;

/// In memory state that tracks retrieval of key shares in order to compute
/// this node's key share for a given configuration.
pub struct KeyShareComputer {
    log: Logger,

    // A copy of the configuration stored in persistent state
    config: Configuration,

    collected_shares: BTreeMap<PlatformId, Share>,
}

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
        peer: PlatformId,
    ) {
        if !self.collected_shares.contains_key(&peer) {
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
        from: PlatformId,
        epoch: Epoch,
        share: Share,
    ) -> bool {
        // Are we trying to retrieve shares for `epoch`?
        if epoch != self.config.epoch {
            warn!(
                self.log,
                "Received Share from node with wrong epoch";
                "received_epoch" => %epoch,
                "from" => %from
            );
            return false;
        }

        // Is the sender a member of the configuration `epoch`?
        // Was the sender a member of the configuration at `old_epoch`?
        let Some(expected_digest) = self.config.members.get(&from) else {
            warn!(
                self.log,
                "Received Share from unexpected node";
                "epoch" => %epoch,
                "from" => %from
            );
            return false;
        };

        // Does the share hash match what we expect?
        let mut digest = Sha3_256Digest::default();
        share.digest::<sha3::Sha3_256>(&mut digest.0);
        if digest != *expected_digest {
            error!(
                self.log,
                "Received share with invalid digest";
                "epoch" => %epoch,
                "from" => %from
            );
        }

        // A valid share was received. Is it new?
        if self.collected_shares.insert(from, share).is_some() {
            return false;
        }

        // Do we have enough shares to computer our rack share?
        if self.collected_shares.len() < self.config.threshold.0 as usize {
            return false;
        }

        // What index are we in the configuration? This is our "x-coordinate"
        // for our key share calculation. We always start indexing from 1, since
        // 0 is the rack secret.
        let index = self
            .config
            .members
            .keys()
            .position(|id| id == ctx.platform_id())
            .expect("node exists");
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
