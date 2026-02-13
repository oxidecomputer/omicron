// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Hardcoded secret retriever for use before trust quorum is production ready

use async_trait::async_trait;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};

/// A [`key_manager::SecretRetriever`] for use before trust quorum is production
/// ready
///
/// The local retriever only returns keys for epoch 0
#[derive(Debug)]
pub struct HardcodedSecretRetriever {}

impl HardcodedSecretRetriever {
    pub fn new() -> Self {
        HardcodedSecretRetriever {}
    }
}

#[async_trait]
impl SecretRetriever for HardcodedSecretRetriever {
    async fn get_latest(
        &mut self,
    ) -> Result<VersionedIkm, SecretRetrieverError> {
        let epoch = 0;
        let salt = [0u8; 32];
        let secret = [0x1d; 32];

        Ok(VersionedIkm::new(epoch, salt, &secret))
    }

    /// We don't plan to do any key rotation before trust quorum is ready
    async fn get(
        &mut self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        if epoch != 0 {
            return Err(SecretRetrieverError::NoSuchEpoch(epoch));
        }
        Ok(SecretState::Current(self.get_latest().await?))
    }
}
