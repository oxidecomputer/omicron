// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Key retrieval mechanisms for use by [`key-manager::KeyManager`]

use async_trait::async_trait;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};

/// A [`key-manager::SecretRetriever`] for use before trust quorum is production ready
///
/// The local retriever only returns keys for epoch 0
pub struct LocalSecretRetriever {}

#[async_trait]
impl SecretRetriever for LocalSecretRetriever {
    /// TODO: Figure out where we want our local key material to come from
    /// See RFD 388
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError> {
        let epoch = 0;
        let salt = [0u8; 32];
        let secret = [0x1d; 32];

        Ok(VersionedIkm::new(epoch, salt, &secret))
    }

    /// We don't plan to do any key rotation before trust quorum is ready
    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        if epoch != 0 {
            return Err(SecretRetrieverError::NoSuchEpoch(epoch));
        }
        Ok(SecretState::Current(self.get_latest().await?))
    }
}
