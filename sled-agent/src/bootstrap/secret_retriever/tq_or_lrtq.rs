// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combined TQ or LRTQ secret retriever
//!
//! This retriever dynamically switches between LRTQ and TQ based on whether
//! Trust Quorum has committed epochs.

use async_trait::async_trait;
use bootstore::schemes::v0::NodeHandle;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};
use trust_quorum::NodeTaskHandle;

use super::lrtq::LrtqSecretRetriever;
use super::tq::TqSecretRetriever;

/// A [`key_manager::SecretRetriever`] that dynamically routes requests between
/// LRTQ and TQ based on whether TQ is active.
///
/// For deployed systems that started with LRTQ, this retriever continues using
/// LRTQ until TQ commits its first epoch, then switches to TQ, which implicitly
/// preserves any rack secret that was created under LRTQ.
///
/// For fresh installs with TQ, all secret retrieval goes through TQ.
pub struct TqOrLrtqSecretRetriever {
    /// The inner state of the secret retriever.
    state: State,
}

/// The current inner state of the secret retriever: if there's a pending
/// trust-quorum secret retriever, we should repeatedly poll it to see whether
/// it has upgraded, then swap it in for active and start using it.
struct State {
    /// The pending TQ secret retriever, only present if not yet activated.
    pending_tq: Option<TqSecretRetriever>,
    /// The current active secret retriever, which *MUST* be either
    /// `LrtqSecretRetriever` or `TqSecretRetriever`.
    active: Box<dyn SecretRetriever>,
}

impl TqOrLrtqSecretRetriever {
    /// Create a new `TqOrLrtqSecretRetriever`.
    pub fn new(
        salt: [u8; 32],
        tq_handle: NodeTaskHandle,
        lrtq_handle: NodeHandle,
    ) -> Self {
        TqOrLrtqSecretRetriever {
            state: State {
                pending_tq: Some(TqSecretRetriever::new(salt, tq_handle)),
                active: Box::new(LrtqSecretRetriever::new(salt, lrtq_handle)),
            },
        }
    }

    /// Get a mutable reference to the currently active secret retriever,
    /// potentially switching internally from LRTQ to TQ in the process if TQ is
    /// found to have become active.
    ///
    /// This is a one-way transition: once TQ is active, we never go back.
    async fn retriever(
        &mut self,
    ) -> Result<&mut dyn SecretRetriever, SecretRetrieverError> {
        // Fast path: if `pending_tq` is None, we've already switched.
        if let Some(pending_tq) = &self.state.pending_tq {
            // Check if TQ has any commits yet.
            let commits = pending_tq
                .handle
                .status()
                .await
                .map_err(|e| SecretRetrieverError::TrustQuorum(e.to_string()))?
                .persistent_state
                .commits;

            // Switch to TQ if it has any commits.
            if !commits.is_empty() {
                let tq = self
                    .state
                    .pending_tq
                    .take()
                    .expect("pending_tq must be Some when switching to TQ");
                self.state.active = Box::new(tq);
            }
        }

        Ok(self.state.active.as_mut())
    }
}

#[async_trait]
impl SecretRetriever for TqOrLrtqSecretRetriever {
    async fn get_latest(
        &mut self,
    ) -> Result<VersionedIkm, SecretRetrieverError> {
        self.retriever().await?.get_latest().await
    }

    async fn get(
        &mut self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        self.retriever().await?.get(epoch).await
    }
}
