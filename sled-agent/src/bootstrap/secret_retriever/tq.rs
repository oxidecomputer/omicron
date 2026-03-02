// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust Quorum (TQ) secret retriever
//!
//! This module implements secret retrieval using the full Trust Quorum
//! protocol, supporting arbitrary epochs. Until all deployed systems are
//! updated past LRTQ, this module shouldn't be used directly, but instead
//! within the dynamically-switching
//! [`TqOrLrtqRetriever`](super::TqOrLrtqSecretRetriever) to allow LRTQ to
//! function during its deprecation period.

use async_trait::async_trait;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};
use secrecy::ExposeSecret;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use trust_quorum::NodeTaskHandle;
use trust_quorum_protocol::ReconstructedRackSecret;
use trust_quorum_types::types::Epoch;

/// Polling interval for share collection (500ms).
///
/// When `load_rack_secret` returns `None`, the retriever polls at this
/// interval until the secret becomes available.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Timeout for share collection (2 minutes).
///
/// If shares cannot be collected within this time, an error is returned.
const POLL_TIMEOUT: Duration = Duration::from_secs(120);

/// A [`key_manager::SecretRetriever`] for use with full Trust Quorum (TQ).
pub(super) struct TqSecretRetriever {
    pub(super) salt: [u8; 32],
    pub(super) handle: NodeTaskHandle,
}

impl TqSecretRetriever {
    /// Create a new `TqSecretRetriever`.
    ///
    /// The `salt` should come from `hash_rack_id()` (sha3_256 of rack_id),
    /// ensuring keys are rack-specific.
    pub fn new(salt: [u8; 32], handle: NodeTaskHandle) -> Self {
        TqSecretRetriever { salt, handle }
    }

    /// Poll for the latest rack secret until available.
    ///
    /// This method polls `load_latest_rack_secret` with a 500ms interval until
    /// the secret is available. There is no timeout; if shares cannot be
    /// collected, this will block indefinitely.
    async fn load_latest_with_retry(
        &self,
    ) -> Result<(Epoch, ReconstructedRackSecret), SecretRetrieverError> {
        timeout(POLL_TIMEOUT, async {
            loop {
                match self.handle.load_latest_rack_secret().await {
                    Ok(Some((epoch, secret))) => return Ok((epoch, secret)),
                    Ok(None) => {
                        // Share collection in progress, keep polling
                        sleep(POLL_INTERVAL).await;
                    }
                    Err(e) => {
                        return Err(SecretRetrieverError::TrustQuorum(
                            e.to_string(),
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| SecretRetrieverError::Timeout)?
    }

    /// Poll for a specific epoch's rack secret until available.
    ///
    /// Used when retrieving secrets for old epochs during reconfiguration.
    async fn load_secret_with_retry(
        &self,
        epoch: Epoch,
    ) -> Result<ReconstructedRackSecret, SecretRetrieverError> {
        timeout(POLL_TIMEOUT, async {
            loop {
                match self.handle.load_rack_secret(epoch).await {
                    Ok(Some(secret)) => return Ok(secret),
                    Ok(None) => {
                        // Share collection in progress, keep polling
                        sleep(POLL_INTERVAL).await;
                    }
                    Err(e) => {
                        return Err(SecretRetrieverError::TrustQuorum(
                            e.to_string(),
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| SecretRetrieverError::Timeout)?
    }
}

#[async_trait]
impl SecretRetriever for TqSecretRetriever {
    async fn get_latest(
        &mut self,
    ) -> Result<VersionedIkm, SecretRetrieverError> {
        // Use atomic load_latest_rack_secret to avoid TOCTOU between
        // checking the latest epoch and loading its secret.
        let (epoch, secret) = self.load_latest_with_retry().await?;
        Ok(VersionedIkm::new(epoch.0, self.salt, secret.expose_secret()))
    }

    async fn get(
        &mut self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        let requested_epoch = Epoch(epoch);

        // First, get the secret for the requested epoch. If the epoch
        // doesn't exist, this will return an error.
        let secret = self.load_secret_with_retry(requested_epoch).await?;
        let ikm = VersionedIkm::new(epoch, self.salt, secret.expose_secret());

        // Then check what's currently latest. By checking latest *after*
        // getting the requested secret, we get the freshest possible view of
        // whether the requested epoch is current or needs rotation. This
        // doesn't *prevent* the possibility of stale data being returned, but
        // it reduces the window of opportunity, and in this case, such stale
        // data would be benign since the reconciler will retry.
        let (latest_epoch, latest_secret) =
            self.load_latest_with_retry().await?;

        if requested_epoch == latest_epoch {
            Ok(SecretState::Current(ikm))
        } else if requested_epoch < latest_epoch {
            // Requested epoch is older than latest: return both secrets so the
            // caller can decrypt with old and re-encrypt with new.
            let new_ikm = VersionedIkm::new(
                latest_epoch.0,
                self.salt,
                latest_secret.expose_secret(),
            );
            Ok(SecretState::Reconfiguration { old: ikm, new: new_ikm })
        } else {
            panic!("Requested epoch is newer than latest known epoch");
        }
    }
}
