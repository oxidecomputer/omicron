// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combined TQ or LRTQ secret retriever
//!
//! This retriever dynamically switches between LRTQ and TQ based on whether
//! Trust Quorum has committed epochs.

use better_as_any::DowncastRef;
use tokio::sync::{RwLock, RwLockReadGuard};

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
    state: RwLock<State>,
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
    /// The salt used for key derivation, for idempotency checking.
    pub async fn salt(&self) -> [u8; 32] {
        let active = &self.state.read().await.active;
        if let Some(lrtq) = active.downcast_ref::<LrtqSecretRetriever>() {
            lrtq.salt
        } else if let Some(tq) = active.downcast_ref::<TqSecretRetriever>() {
            tq.salt
        } else {
            unreachable!("inner secret retriever is not TQ or LRTQ");
        }
    }

    /// Create a new `TqOrLrtqSecretRetriever`.
    pub fn new(
        salt: [u8; 32],
        tq_handle: NodeTaskHandle,
        lrtq_handle: NodeHandle,
    ) -> Self {
        TqOrLrtqSecretRetriever {
            state: RwLock::new(State {
                pending_tq: Some(TqSecretRetriever::new(salt, tq_handle)),
                active: Box::new(LrtqSecretRetriever::new(salt, lrtq_handle)),
            }),
        }
    }

    /// Get a guard to the (dynamic) currently active secret retriever,
    /// potentially switching internally from LRTQ to TQ in the process if TQ is
    /// found to have become active.
    ///
    /// This is a one-way transition: once TQ is active, we never go back.
    async fn retriever(
        &self,
    ) -> Result<
        RwLockReadGuard<'_, Box<dyn SecretRetriever>>,
        SecretRetrieverError,
    > {
        // Fast path: if `pending_tq` is None, we've already switched, so do
        // nothing. We extract the handle here (if any) so we don't hold the
        // read lock across the async status check below.
        let pending_tq_handle = self
            .state
            .read()
            .await
            .pending_tq
            .as_ref()
            .map(|tq| tq.handle.clone());

        // Determine if we need to switch to the pending TQ by checking whether
        // the handle reports any commits.
        let should_switch = match pending_tq_handle {
            None => false,
            Some(handle) => {
                // Get all commits from the pending TQ:
                let commits = handle
                    .status()
                    .await
                    .map_err(|e| {
                        SecretRetrieverError::TrustQuorum(e.to_string())
                    })?
                    .persistent_state
                    .commits;

                // Switch to the pending TQ if it has any commits.
                !commits.is_empty()
            }
        };

        if should_switch {
            // Attempt to atomically swap in the pending TQ for the active
            // retriever. If someone else has already raced ahead and done this
            // by the time we try, that's fine, because the action is the same
            // regardless of who is performing it.
            let State { pending_tq, active } = &mut *self.state.write().await;
            if let Some(tq) = pending_tq.take() {
                *active = Box::new(tq);
            }
        }

        // Project out the active secret retriever inside a read lock so we can
        // return it to the caller, who can then invoke method(s) on it.
        Ok(RwLockReadGuard::map(self.state.read().await, |s| &s.active))
    }
}

#[async_trait]
impl SecretRetriever for TqOrLrtqSecretRetriever {
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError> {
        self.retriever().await?.get_latest().await
    }

    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        self.retriever().await?.get(epoch).await
    }
}
