// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combined TQ or LRTQ secret retriever
//!
//! This retriever dynamically switches between LRTQ and TQ based on whether
//! Trust Quorum has committed epochs. It handles three scenarios:
//!
//! 1. **LRTQ only**: TQ is not yet initialized; continue using LRTQ
//! 2. **Upgraded from LRTQ**: TQ starts at epoch 2; LRTQ owns epoch 1
//! 3. **Fresh install with TQ**: TQ starts at epoch 1; all epochs go to TQ
//!
//! Once TQ becomes active (commits exist), the state transitions are one-way
//! and permanent for the lifetime of the retriever.

use async_trait::async_trait;
use bootstore::schemes::v0::NodeHandle;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};
use std::sync::RwLock;
use trust_quorum::NodeTaskHandle;

use super::lrtq::LrtqSecretRetriever;
use super::tq::TqSecretRetriever;

/// The current state of secret retrieval, determining how epochs are routed.
///
/// This is a one-way state machine: once we transition away from `Lrtq`, we
/// never go back. The TQ upgrade is permanent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TqUpgraded {
    /// Still using LRTQ, TQ not yet active (no commits in TQ).
    NotYet,

    /// Upgraded from LRTQ to TQ. Epoch 1 goes to LRTQ, epoch 2+ goes to TQ.
    ///
    /// This state occurs when an existing rack running LRTQ upgrades to TQ.
    Upgraded,

    /// Fresh install with TQ. All epochs go to TQ.
    ///
    /// This state occurs when a new rack is initialized directly with TQ
    /// (no prior LRTQ epochs exist). TQ starts at epoch 1.
    FreshInstall,
}

/// A [`key_manager::SecretRetriever`] that dynamically routes requests between
/// LRTQ and TQ based on the rack's initialization history.
///
/// For deployed systems that started with LRTQ, this retriever continues using
/// LRTQ until TQ commits its first epoch, then switches to TQ while preserving
/// access to the LRTQ epoch 1 secret for existing encrypted data.
///
/// For fresh installs with TQ, all secret retrieval goes through TQ.
pub struct TqOrLrtqSecretRetriever {
    tq: TqSecretRetriever,
    lrtq: LrtqSecretRetriever,
    upgraded: RwLock<TqUpgraded>,
}

impl TqOrLrtqSecretRetriever {
    /// Create a new `TqOrLrtqSecretRetriever`.
    ///
    /// The retriever starts in `TqUpgraded::NotYet` state and will transition
    /// to TQ-based states when TQ commits are detected.
    pub fn new(
        salt: [u8; 32],
        tq_handle: NodeTaskHandle,
        lrtq_handle: NodeHandle,
    ) -> Self {
        TqOrLrtqSecretRetriever {
            tq: TqSecretRetriever::new(salt, tq_handle),
            lrtq: LrtqSecretRetriever::new(salt, lrtq_handle),
            upgraded: RwLock::new(TqUpgraded::NotYet),
        }
    }

    /// Check if TQ upgrade has occurred.
    ///
    /// The status is cached to avoid repeated status checks once TQ is active.
    async fn upgraded(&self) -> Result<TqUpgraded, SecretRetrieverError> {
        // Fast path: check cached state. Once we've transitioned away from
        // LRTQ, we never need to check again.
        {
            let upgraded = self.upgraded.read().unwrap();
            if *upgraded != TqUpgraded::NotYet {
                return Ok(*upgraded);
            }
        }

        // Our cached state says we haven't upgraded, but it could be stale:
        // check if TQ has become active
        let status =
            self.tq.handle.status().await.map_err(|e| {
                SecretRetrieverError::TrustQuorum(e.to_string())
            })?;

        let commits = &status.persistent_state.commits;
        if commits.is_empty() {
            // TQ has no commits yet, stay in LRTQ mode
            return Ok(TqUpgraded::NotYet);
        }

        // TQ is active: determine if this is a fresh install or upgrade
        // by checking the minimum committed epoch.
        let min_epoch = commits.iter().min().unwrap();
        let new_state = if min_epoch.0 == 1 {
            // TQ starts at epoch 1: fresh install, no LRTQ data
            TqUpgraded::FreshInstall
        } else {
            // TQ starts at epoch 2+: upgraded from LRTQ
            TqUpgraded::Upgraded
        };

        // Update cached state. This is a one-way transition that never
        // reverts back to LRTQ mode.
        *self.upgraded.write().unwrap() = new_state;
        Ok(new_state)
    }
}

#[async_trait]
impl SecretRetriever for TqOrLrtqSecretRetriever {
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError> {
        match self.upgraded().await? {
            TqUpgraded::NotYet => self.lrtq.get_latest().await,
            TqUpgraded::Upgraded | TqUpgraded::FreshInstall => {
                self.tq.get_latest().await
            }
        }
    }

    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        match self.upgraded().await? {
            TqUpgraded::NotYet => {
                // Still using LRTQ, all epochs go there
                self.lrtq.get(epoch).await
            }
            TqUpgraded::FreshInstall => {
                // Fresh install: TQ owns all epochs
                self.tq.get(epoch).await
            }
            TqUpgraded::Upgraded => {
                // Upgraded: LRTQ owns epoch 1, TQ owns epoch 2+
                if epoch == 0 {
                    // There is never such a thing as "epoch 0" in production systems!
                    Err(SecretRetrieverError::NoSuchEpoch(0))
                } else if epoch == 1 {
                    self.lrtq.get(epoch).await
                } else {
                    self.tq.get(epoch).await
                }
            }
        }
    }
}
