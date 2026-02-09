// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! LRTQ (Low Rent Trust Quorum) secret retriever

use async_trait::async_trait;
use bootstore::schemes::v0::NodeHandle;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};
use slog_error_chain::InlineErrorChain;

/// A [`key_manager::SecretRetriever`] for use with LRTQ
///
/// The LRTQ retriever only returns keys for epoch 1
#[derive(Debug)]
pub(super) struct LrtqSecretRetriever {
    pub(super) salt: [u8; 32],
    bootstore: NodeHandle,
}

impl LrtqSecretRetriever {
    pub fn new(salt: [u8; 32], bootstore: NodeHandle) -> Self {
        LrtqSecretRetriever { salt, bootstore }
    }
}

#[async_trait]
impl SecretRetriever for LrtqSecretRetriever {
    async fn get_latest(
        &mut self,
    ) -> Result<VersionedIkm, SecretRetrieverError> {
        let epoch = 1;
        let rack_secret = self
            .bootstore
            .load_rack_secret()
            .await
            .map_err(|e| SecretRetrieverError::Bootstore(InlineErrorChain::new(&e).to_string()))?;
        let secret = rack_secret.expose_secret().as_bytes();
        Ok(VersionedIkm::new(epoch, self.salt, secret))
    }

    async fn get(
        &mut self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        if epoch != 1 {
            return Err(SecretRetrieverError::NoSuchEpoch(epoch));
        }
        Ok(SecretState::Current(self.get_latest().await?))
    }
}
