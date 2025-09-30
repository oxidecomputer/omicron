// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The component responsible for loading rack secrets for various
//! configurations.

use std::collections::BTreeMap;

use crate::crypto::ReconstructedRackSecret;
use crate::{
    Alarm, BaseboardId, Configuration, Epoch, NodeHandlerCtx, PeerMsgKind,
    RackSecret, Share,
};
use daft::{BTreeMapDiff, Diffable, Leaf};
use slog::{Logger, error, info, o};

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum LoadRackSecretError {
    #[error("no committed configurations")]
    NoCommittedConfigurations,
    #[error("configuration at epoch {0} is not committed")]
    NotCommitted(Epoch),
    #[error("alarm: configuration went backwards")]
    Alarm,
    #[error("rack secret for epoch {0} is no longer available")]
    NotAvailable(Epoch),
}

/// Manage retrieval of key shares to load various rack secrets
#[derive(Debug, Clone, Diffable)]
pub struct RackSecretLoader {
    #[daft(ignore)]
    log: Logger,
    loaded: BTreeMap<Epoch, ReconstructedRackSecret>,
    // We can only collect shares for the latest committed epoch. We then derive
    // a key from the computed rack secret to decrypt rack secrets for prior
    // configurations.
    #[daft(leaf)]
    collector: Option<ShareCollector>,
}

impl<'daft> RackSecretLoaderDiff<'daft> {
    pub fn loaded(
        &self,
    ) -> &BTreeMapDiff<'daft, Epoch, ReconstructedRackSecret> {
        &self.loaded
    }

    pub fn collector(&self) -> Leaf<&'daft Option<ShareCollector>> {
        self.collector
    }
}

impl RackSecretLoader {
    pub fn new(log: &Logger) -> RackSecretLoader {
        let log = log.new(o!("component" => "tq-rack-secret-loader"));
        RackSecretLoader { log, loaded: BTreeMap::new(), collector: None }
    }

    pub fn is_collecting_shares_for_rack_secret(&self, epoch: Epoch) -> bool {
        let Some(c) = &self.collector else {
            return false;
        };
        // We collect for the latest committed epoch which must be greater than
        // or equal to the epoch for the rack secret we are interested in.
        c.config.epoch >= epoch
    }

    pub fn load(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        epoch: Epoch,
    ) -> Result<Option<ReconstructedRackSecret>, LoadRackSecretError> {
        // Return the rack secret if we have it
        if let Some(secret) = self.loaded.get(&epoch) {
            return Ok(Some(secret.clone()));
        }

        let Some(latest_committed_epoch) =
            ctx.persistent_state().latest_committed_epoch()
        else {
            // There aren't any committed configs
            return Err(LoadRackSecretError::NoCommittedConfigurations);
        };

        if epoch > latest_committed_epoch {
            return Err(LoadRackSecretError::NotCommitted(epoch));
        }

        // If we have loaded the latest committed epoch, then we have loaded all
        // possible rack secrets. Secrets for prior epochs are unavailable.
        if self.loaded.contains_key(&latest_committed_epoch) {
            if epoch < latest_committed_epoch {
                return Err(LoadRackSecretError::NotAvailable(epoch));
            } else {
                unreachable!(
                    "epoch comparisons for epoch({epoch}) \
                    <= latest_committed_epoch({latest_committed_epoch}) \
                    already handled"
                );
            }
        }

        // We don't have the rack secret for the latest committed epoch yet. Are
        // we still collecting shares?
        match &mut self.collector {
            None => {
                // Start collecting
                self.collector = Some(ShareCollector::new(&self.log, ctx)?);
                Ok(None)
            }
            Some(collector) => {
                // Are we already collecting for the latest committed epoch?
                // If so, we are done for now.
                if collector.config.epoch == latest_committed_epoch {
                    Ok(None)
                } else if collector.config.epoch < latest_committed_epoch {
                    // Stop collecting for the old config. Start again for
                    // new config.
                    self.collector = Some(ShareCollector::new(&self.log, ctx)?);
                    Ok(None)
                } else {
                    let m = "collecting shares for a \
                               committed configuration that no longer exists";
                    error!(
                        self.log, "{m}";
                        "latest_committed_epoch"
                           => %latest_committed_epoch,
                        "collecting_epoch" => %collector.config.epoch
                    );

                    ctx.raise_alarm(Alarm::CommittedConfigurationLost {
                        latest_committed_epoch,
                        collecting_epoch: collector.config.epoch,
                    });
                    Err(LoadRackSecretError::Alarm)
                }
            }
        }
    }

    pub fn handle_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: BaseboardId,
        epoch: Epoch,
        share: Share,
    ) {
        // If we have already reconstructed the rack secret for `epoch`, we can
        // ignore this share.
        if self.loaded.contains_key(&epoch) {
            return;
        }

        let Some(collector) = &mut self.collector else {
            // If we are not currently collecting shares, we can ignore this
            // share.
            return;
        };

        if let Some(loaded) = collector.handle_share(ctx, from, epoch, share) {
            // We are done collecting shares for `epoch`
            //
            // All possible rack secrets were decrypted and loaded
            self.loaded = loaded;
            self.collector = None;
        }
    }

    /// Remove all in-memory RackSecrets and stop collecting shares
    pub fn clear_secrets(&mut self) {
        self.loaded = BTreeMap::new();
        self.collector = None;
    }

    pub fn on_connect(&self, ctx: &mut impl NodeHandlerCtx, peer: BaseboardId) {
        if let Some(collector) = &self.collector {
            collector.on_connect(ctx, peer);
        }
    }
}

// Pub only for use in daft
#[derive(Debug, Clone, Diffable)]
pub struct ShareCollector {
    #[daft(ignore)]
    log: Logger,
    // A copy of the configuration stored in persistent state
    #[daft(leaf)]
    config: Configuration,
    shares: BTreeMap<BaseboardId, Share>,
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl PartialEq for ShareCollector {
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config && self.shares == other.shares
    }
}

#[cfg(feature = "danger_partial_eq_ct_wrapper")]
impl Eq for ShareCollector {}

impl<'daft> ShareCollectorDiff<'daft> {
    pub fn config(&self) -> Leaf<&'daft Configuration> {
        self.config
    }

    pub fn shares(&self) -> &BTreeMapDiff<'daft, BaseboardId, Share> {
        &self.shares
    }
}

impl ShareCollector {
    pub fn new(
        log: &Logger,
        ctx: &mut impl NodeHandlerCtx,
    ) -> Result<ShareCollector, LoadRackSecretError> {
        let log =
            log.new(o!("component" => "tq-rack-secret-loader-share-collector"));
        let config = ctx
            .persistent_state()
            .latest_committed_configuration()
            .ok_or(LoadRackSecretError::NoCommittedConfigurations)?
            .clone();

        let my_id = ctx.platform_id().clone();
        let my_share = ctx
            .persistent_state()
            .shares
            .get(&config.epoch)
            .expect("share exists because config is committed")
            .clone();
        let shares = [(my_id, my_share)].into_iter().collect();

        // Request shares for all connected nodes
        let collector = ShareCollector { log, config, shares };
        collector.send_get_share_requests(ctx);

        Ok(collector)
    }

    /// Handle a received `Share` from another node
    ///
    /// Ensure that this share was valid and requested and save it if so.
    ///
    /// Return `Some(epoch, rack_secret)` once enough shares have been collected
    /// and the rack secret has been computed, `None` otherwise.
    pub fn handle_share(
        &mut self,
        ctx: &mut impl NodeHandlerCtx,
        from: BaseboardId,
        epoch: Epoch,
        share: Share,
    ) -> Option<BTreeMap<Epoch, ReconstructedRackSecret>> {
        if !crate::validate_share(&self.log, &self.config, &from, epoch, &share)
        {
            // Logging done inside `validate_share`
            return None;
        };

        // A valid share was received. Is it new?
        if self.shares.insert(from, share).is_some() {
            return None;
        }

        // Do we have enough shares to compute our rack share?
        if self.shares.len() < self.config.threshold.0 as usize {
            return None;
        }

        // Reconstruct the new rack secret from the shares we created
        // at coordination start time.
        let shares: Vec<_> = self.shares.values().cloned().collect();
        match RackSecret::reconstruct(&shares) {
            Ok(secret) => {
                info!(
                    self.log,
                    "Successfully reconstructed rack secret";
                    "epoch" => %self.config.epoch
                );
                if let Some(encrypted) = &self.config.encrypted_rack_secrets {
                    match encrypted.decrypt(
                        self.config.rack_id,
                        self.config.epoch,
                        &secret,
                    ) {
                        Ok(plaintext) => {
                            let mut plaintext = plaintext.into_inner();
                            plaintext.insert(self.config.epoch, secret);
                            Some(plaintext)
                        }
                        Err(err) => {
                            error!(
                                self.log,
                                "rack secret decryption error";
                                "epoch" => %self.config.epoch,
                                &err
                            );
                            ctx.raise_alarm(
                                Alarm::RackSecretDecryptionFailed {
                                    epoch,
                                    err,
                                },
                            );
                            None
                        }
                    }
                } else {
                    Some([(self.config.epoch, secret)].into_iter().collect())
                }
            }
            Err(err) => {
                error!(
                    self.log,
                    "Failed to reconstruct rack secret";
                    "epoch" => %self.config.epoch,
                    &err
                );
                ctx.raise_alarm(Alarm::RackSecretReconstructionFailed {
                    epoch,
                    err,
                });
                None
            }
        }
    }

    /// Send `GetShare` messages to all connected nodes in the current
    /// configuration.
    fn send_get_share_requests(&self, ctx: &mut impl NodeHandlerCtx) {
        for to in self.config.members.keys() {
            if ctx.connected().contains(to) {
                ctx.send(to.clone(), PeerMsgKind::GetShare(self.config.epoch));
                info!(
                    self.log,
                    "Sent GetShare";
                    "epoch" => %self.config.epoch,
                    "to" => %to
                );
            }
        }
    }

    /// A peer node has connected to this one
    pub fn on_connect(&self, ctx: &mut impl NodeHandlerCtx, peer: BaseboardId) {
        if self.config.members.contains_key(&peer) {
            info!(
                self.log,
                "Sent GetShare";
                "epoch" => %self.config.epoch,
                "to" => %peer
            );
            ctx.send(peer, PeerMsgKind::GetShare(self.config.epoch));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NodeCallerCtx, NodeCommonCtx, NodeCtx, Sha3_256Digest, Threshold,
        crypto::PlaintextRackSecrets,
    };
    use assert_matches::assert_matches;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::RackUuid;
    use secrecy::ExposeSecret;
    use sha3::digest::const_oid::db::rfc1274::LAST_MODIFIED_BY;
    use std::collections::BTreeSet;

    const NUM_INITIAL_MEMBERS: u8 = 5;

    pub fn initial_config()
    -> (Configuration, RackSecret, BTreeMap<BaseboardId, Share>) {
        let threshold = Threshold(3);
        let initial_members: BTreeSet<_> = (0..NUM_INITIAL_MEMBERS)
            .map(|serial| BaseboardId {
                part_number: "test".into(),
                serial_number: serial.to_string(),
            })
            .collect();
        let initial_rack_secret = RackSecret::new();
        let shares: BTreeMap<_, _> = initial_members
            .iter()
            .cloned()
            .zip(
                initial_rack_secret
                    .split(threshold, NUM_INITIAL_MEMBERS)
                    .unwrap()
                    .shares
                    .expose_secret()
                    .iter()
                    .cloned(),
            )
            .collect();
        let digests: BTreeMap<_, _> = shares
            .iter()
            .map(|(id, share)| {
                let mut digest = Sha3_256Digest::default();
                share.digest::<sha3::Sha3_256>(&mut digest.0);
                (id.clone(), digest)
            })
            .collect();

        let config = Configuration {
            rack_id: RackUuid::new_v4(),
            epoch: Epoch(1),
            coordinator: initial_members.first().unwrap().clone(),
            members: digests,
            threshold,
            encrypted_rack_secrets: None,
        };

        (config, initial_rack_secret, shares)
    }

    /// Create a new Configuration by adding a member to `old_config`
    fn new_config(
        new_member_serial: u8,
        old_config: &Configuration,
        old_rack_secret: ReconstructedRackSecret,
    ) -> (Configuration, ReconstructedRackSecret, BTreeMap<BaseboardId, Share>)
    {
        let threshold = Threshold(3);
        let rack_secret = RackSecret::new();

        let mut new_members: BTreeSet<_> =
            old_config.members.keys().cloned().collect();
        new_members.insert(BaseboardId {
            part_number: "test".into(),
            serial_number: new_member_serial.to_string(),
        });
        let num_members = new_members.len() as u8;
        let shares: BTreeMap<_, _> = new_members
            .into_iter()
            .zip(
                rack_secret
                    .split(threshold, num_members)
                    .unwrap()
                    .shares
                    .expose_secret()
                    .iter()
                    .cloned(),
            )
            .collect();
        let digests: BTreeMap<_, _> = shares
            .iter()
            .map(|(id, share)| {
                let mut digest = Sha3_256Digest::default();
                share.digest::<sha3::Sha3_256>(&mut digest.0);
                (id.clone(), digest)
            })
            .collect();

        let rack_secret: ReconstructedRackSecret = rack_secret.into();
        let mut plaintext = PlaintextRackSecrets::new();
        plaintext.insert(Epoch(1), old_rack_secret);
        let encrypted = plaintext
            .encrypt(old_config.rack_id, Epoch(2), &rack_secret)
            .unwrap();

        let mut config = old_config.clone();
        config.epoch = Epoch(2);
        config.members = digests;
        config.encrypted_rack_secrets = Some(encrypted);

        (config, rack_secret, shares)
    }

    #[test]
    fn test_load_rack_secrets() {
        let logctx = test_setup_log("load_rack_secret");
        let (initial_config, initial_rack_secret, initial_shares) =
            initial_config();
        let mut ctx =
            NodeCtx::new(initial_config.members.keys().next().unwrap().clone());
        ctx.update_persistent_state(|ps| {
            ps.shares.insert(
                initial_config.epoch,
                initial_shares.first_key_value().unwrap().1.clone(),
            );
            ps.configs.insert_unique(initial_config.clone()).is_ok()
        });

        let mut loader = RackSecretLoader::new(&logctx.log);

        // We have an uncommittted configuration in persistent state
        assert_eq!(
            LoadRackSecretError::NoCommittedConfigurations,
            loader.load(&mut ctx, Epoch(1)).unwrap_err()
        );

        // Commit the configuration
        ctx.update_persistent_state(|ps| ps.commits.insert(Epoch(1)));

        // We have the committed configuration for epoch 1, but need
        // to retrieve the shares.
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(1)));

        // No messages should have been sent because no nodes are connected yet
        assert_eq!(0, ctx.num_envelopes());

        // Connect to all nodes, except ourself, and watch some `GetShare`
        // requests get sent
        for id in initial_shares.keys().skip(1) {
            ctx.add_connection(id.clone());
            loader.on_connect(&mut ctx, id.clone());
        }

        for (e, to) in ctx.envelopes().zip(ctx.connected()) {
            assert_eq!(&e.from, ctx.platform_id());
            assert_eq!(&e.to, to);
            assert_matches!(e.msg.kind, PeerMsgKind::GetShare(Epoch(1)));
        }
        let _ = ctx.drain_envelopes();

        // Feed enough shares (Threshold - 1) into the loader to get back
        // the rack secret.
        //
        // First share returns none, as not enough were retrieved.
        let mut iter = initial_shares.iter().skip(1);
        let (from, share) = iter.next().unwrap();
        loader.handle_share(&mut ctx, from.clone(), Epoch(1), share.clone());
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(1)));

        // Second share should load the secret.
        // threshold = 3 (2 retrieved shares plus ourself)
        let (from, share) = iter.next().unwrap();
        loader.handle_share(&mut ctx, from.clone(), Epoch(1), share.clone());
        let rs = loader.load(&mut ctx, Epoch(1)).unwrap().unwrap();
        assert_eq!(rs.expose_secret(), initial_rack_secret.expose_secret());

        // Loading the same secret again succeeds
        let rs = loader.load(&mut ctx, Epoch(1)).unwrap().unwrap();
        assert_eq!(rs.expose_secret(), initial_rack_secret.expose_secret());

        // Clearing the secret results in `None` being returned on load.
        // `GetShare` messages are sent again.
        loader.clear_secrets();
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(1)));
        for (e, to) in ctx.envelopes().zip(ctx.connected()) {
            assert_eq!(&e.from, ctx.platform_id());
            assert_eq!(&e.to, to);
            assert_matches!(e.msg.kind, PeerMsgKind::GetShare(Epoch(1)));
        }
        let _ = ctx.drain_envelopes();

        // Create a new configuration for epoch 2
        let (config_2, rack_secret_2, shares_2) =
            new_config(NUM_INITIAL_MEMBERS, &initial_config, rs);

        // Save and commit the config
        ctx.update_persistent_state(|ps| {
            ps.shares.insert(
                config_2.epoch,
                shares_2.first_key_value().unwrap().1.clone(),
            );
            ps.configs.insert_unique(config_2.clone()).unwrap();
            ps.commits.insert(Epoch(2));
            true
        });

        // We can't load the old epoch, since we cleared the loaded secrets
        // previously. But collection has started, so this isn't an error.
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(1)));

        // Trying to load the new epoch results in resetting the collector.
        // We still can't load the old epoch.
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(2)));
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(1)));

        // We should see GetShare messages sent for the new epoch to all
        // members except ourselves and the new member, which isn't yet connected
        for (e, to) in ctx.envelopes().zip(ctx.connected()) {
            assert_eq!(&e.from, ctx.platform_id());
            assert_eq!(&e.to, to);
            assert_matches!(e.msg.kind, PeerMsgKind::GetShare(Epoch(2)));
        }
        let _ = ctx.drain_envelopes();

        // Handling enough shares results in both the new and old secrets
        // becoming available.
        let mut iter = shares_2.iter().skip(1);
        let (from, share) = iter.next().unwrap();
        loader.handle_share(&mut ctx, from.clone(), Epoch(2), share.clone());
        let (from, share) = iter.next().unwrap();
        loader.handle_share(&mut ctx, from.clone(), Epoch(2), share.clone());

        let rs = loader.load(&mut ctx, Epoch(2)).unwrap().unwrap();
        assert_eq!(rs, rack_secret_2);

        let rs = loader.load(&mut ctx, Epoch(1)).unwrap().unwrap();
        assert_eq!(rs.expose_secret(), initial_rack_secret.expose_secret());

        // Clear the secrets in preparation for the next check
        loader.clear_secrets();

        // Hack the persistent state to make the rack secret for epoch 1
        // no longer available
        ctx.update_persistent_state(|ps| {
            ps.configs.get_mut(&Epoch(2)).unwrap().encrypted_rack_secrets =
                None;
            true
        });

        // Start loading the secret for epoch 2 and feed in shares
        assert_eq!(Ok(None), loader.load(&mut ctx, Epoch(2)));
        let mut iter = shares_2.iter().skip(1);
        let (from, share) = iter.next().unwrap();
        loader.handle_share(&mut ctx, from.clone(), Epoch(2), share.clone());
        let (from, share) = iter.next().unwrap();
        loader.handle_share(&mut ctx, from.clone(), Epoch(2), share.clone());

        // Loading the secret for epoch 2 succeeds
        let rs = loader.load(&mut ctx, Epoch(2)).unwrap().unwrap();
        assert_eq!(rs, rack_secret_2);

        // Loading the secret for epoch 1 fails
        assert_eq!(
            LoadRackSecretError::NotAvailable(Epoch(1)),
            loader.load(&mut ctx, Epoch(1)).unwrap_err()
        );

        logctx.cleanup_successful();
    }
}
