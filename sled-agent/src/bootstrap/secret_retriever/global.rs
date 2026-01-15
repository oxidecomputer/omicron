// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Global secret retriever
//!
//! Provides a globally-accessible secret retriever that can be initialized
//! with different concrete retrievers depending on rack configuration.

use std::sync::OnceLock;

use async_trait::async_trait;
use better_as_any::DowncastRef;
use bootstore::schemes::v0::NodeHandle;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};

use super::hardcoded::HardcodedSecretRetriever;
use super::tq_or_lrtq::TqOrLrtqSecretRetriever;

static GLOBAL_RETRIEVER: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();

/// A [`key_manager::SecretRetriever`] that either uses a
/// [`HardcodedSecretRetriever`] or [`TqOrLrtqSecretRetriever`] under the
/// hood depending upon how many sleds are in the cluster at rack init time.
pub struct GlobalSecretRetriever {}

impl GlobalSecretRetriever {
    pub fn new() -> GlobalSecretRetriever {
        GlobalSecretRetriever {}
    }

    /// Set the type of secret retriever to `HardcodedSecretRetriever`
    ///
    /// Panics if a non-idempotent call is made
    pub fn init_hardcoded() {
        if GLOBAL_RETRIEVER
            .set(Box::new(HardcodedSecretRetriever::new()))
            .is_err()
        {
            // We know `unwrap` is safe because we only get an error if a value
            // exists in `GLOBAL_RETRIEVER`
            if !GLOBAL_RETRIEVER.get().unwrap().is::<HardcodedSecretRetriever>()
            {
                panic!("SecretRetriever already set: call was not idempotent")
            }
        }
    }

    /// Set the type of secret retriever to `TqOrLrtqSecretRetriever`
    ///
    /// Panics if a non-idempotent call is made
    pub fn init_trust_quorum(
        salt: [u8; 32],
        tq_handle: trust_quorum::NodeTaskHandle,
        lrtq_handle: NodeHandle,
    ) {
        if GLOBAL_RETRIEVER
            .set(Box::new(TqOrLrtqSecretRetriever::new(
                salt,
                tq_handle,
                lrtq_handle,
            )))
            .is_err()
        {
            // We know `unwrap` is safe because we only get an error if a value
            // exists in `GLOBAL_RETRIEVER`.
            match GLOBAL_RETRIEVER
                .get()
                .unwrap()
                .downcast_ref::<TqOrLrtqSecretRetriever>()
            {
                // We assume the node handles are fine. There can only be one of
                // each that gets cloned and we can't compare them for equality.
                Some(retriever) if retriever.tq.salt == salt => (),
                _ => {
                    panic!(
                        "SecretRetriever already set: call was not idempotent"
                    )
                }
            }
        }
    }
}

#[async_trait]
impl SecretRetriever for GlobalSecretRetriever {
    async fn get_latest(&self) -> Result<VersionedIkm, SecretRetrieverError> {
        match GLOBAL_RETRIEVER.get() {
            Some(retriever) => retriever.get_latest().await,
            None => Err(SecretRetrieverError::RackNotInitialized),
        }
    }

    async fn get(
        &self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        match GLOBAL_RETRIEVER.get() {
            Some(retriever) => retriever.get(epoch).await,
            None => Err(SecretRetrieverError::RackNotInitialized),
        }
    }
}
