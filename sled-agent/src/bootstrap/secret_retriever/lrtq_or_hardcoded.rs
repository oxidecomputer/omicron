// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Combined LRTQ or Hardcoded secret retriever

use async_trait::async_trait;
use bootstore::schemes::v0::NodeHandle;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};
use std::sync::OnceLock;

use super::hardcoded::HardcodedSecretRetriever;
use super::lrtq::LrtqSecretRetriever;

static MAYBE_LRTQ_RETRIEVER: OnceLock<LrtqOrHardcodedSecretRetrieverInner> =
    OnceLock::new();

/// A [`key_manager::SecretRetriever`] that either uses a
/// [`HardcodedSecretRetriever`] or [`LrtqSecretRetriever`] under the
/// hood depending upon how many sleds are in the cluster at rack init time.
pub struct LrtqOrHardcodedSecretRetriever {}

impl LrtqOrHardcodedSecretRetriever {
    pub fn new() -> LrtqOrHardcodedSecretRetriever {
        LrtqOrHardcodedSecretRetriever {}
    }

    /// Set the type of secret retriever to `HardcodedSecretRetriever`
    ///
    /// Panics if a non-idempotent call is made
    pub fn init_hardcoded() {
        if let Err(_) = MAYBE_LRTQ_RETRIEVER
            .set(LrtqOrHardcodedSecretRetrieverInner::new_hardcoded())
        {
            // We know `unwrap` is safe because we only get an error if a value
            // exists in `MAYBE_LRTQ_RETRIEVER`
            if !MAYBE_LRTQ_RETRIEVER.get().unwrap().is_hardcoded() {
                panic!("SecretRetriever already set: call was not idempotent")
            }
        }
    }

    /// Set the type of secret retriever to `LrtqSecretRetriever`
    ///
    /// Panics if a non-idempotent call is made
    pub fn init_lrtq(salt: [u8; 32], bootstore: NodeHandle) {
        if let Err(_) = MAYBE_LRTQ_RETRIEVER
            .set(LrtqOrHardcodedSecretRetrieverInner::new_lrtq(salt, bootstore))
        {
            // We know `unwrap` is safe because we only get an error if a value
            // exists in `MAYBE_LRTQ_RETRIEVER`
            match MAYBE_LRTQ_RETRIEVER.get().unwrap() {
                // We assume the `bootstore::NodeHandle` is fine. There can
                // only be one that gets cloned and we can't compare them for
                // equality.
                LrtqOrHardcodedSecretRetrieverInner::Lrtq(retriever)
                    if retriever.salt == salt =>
                {
                    ()
                }
                _ => panic!(
                    "SecretRetriever already set: call was not idempotent"
                ),
            }
        }
    }
}

#[async_trait]
impl SecretRetriever for LrtqOrHardcodedSecretRetriever {
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError> {
        match MAYBE_LRTQ_RETRIEVER.get() {
            Some(retriever) => retriever.get_latest().await,
            None => Err(SecretRetrieverError::RackNotInitialized),
        }
    }

    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        match MAYBE_LRTQ_RETRIEVER.get() {
            Some(retriever) => retriever.get(epoch).await,
            None => Err(SecretRetrieverError::RackNotInitialized),
        }
    }
}

#[derive(Debug)]
enum LrtqOrHardcodedSecretRetrieverInner {
    Lrtq(LrtqSecretRetriever),
    Hardcoded(HardcodedSecretRetriever),
}

impl LrtqOrHardcodedSecretRetrieverInner {
    pub fn new_hardcoded() -> Self {
        Self::Hardcoded(HardcodedSecretRetriever::new())
    }

    pub fn new_lrtq(salt: [u8; 32], bootstore: NodeHandle) -> Self {
        Self::Lrtq(LrtqSecretRetriever::new(salt, bootstore))
    }

    pub fn is_hardcoded(&self) -> bool {
        if let Self::Hardcoded(_) = self { true } else { false }
    }
}

#[async_trait]
impl SecretRetriever for LrtqOrHardcodedSecretRetrieverInner {
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError> {
        match self {
            LrtqOrHardcodedSecretRetrieverInner::Lrtq(retriever) => {
                retriever.get_latest().await
            }
            LrtqOrHardcodedSecretRetrieverInner::Hardcoded(retriever) => {
                retriever.get_latest().await
            }
        }
    }

    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        match self {
            LrtqOrHardcodedSecretRetrieverInner::Lrtq(retriever) => {
                retriever.get(epoch).await
            }
            LrtqOrHardcodedSecretRetrieverInner::Hardcoded(retriever) => {
                retriever.get(epoch).await
            }
        }
    }
}
