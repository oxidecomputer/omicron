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
        Self::init_hardcoded_with(&GLOBAL_RETRIEVER)
    }

    /// Set the type of secret retriever to `HardcodedSecretRetriever` using
    /// the provided cell.
    ///
    /// This is separated from [`GlobalSecretRetriever::init_hardcoded`] to
    /// allow testing the idempotency logic with non-static `OnceLock`
    /// instances.
    ///
    /// Panics if a non-idempotent call is made
    fn init_hardcoded_with(cell: &OnceLock<Box<dyn SecretRetriever>>) {
        if cell.set(Box::new(HardcodedSecretRetriever::new())).is_err() {
            // We know `unwrap` is safe because we only get an error if a value
            // exists in `cell`
            if !cell.get().unwrap().is::<HardcodedSecretRetriever>() {
                panic!("SecretRetriever already set: call was not idempotent")
            }
        }
    }

    /// Set the type of secret retriever to `TqOrLrtqSecretRetriever`
    ///
    /// Panics if a non-idempotent call is made
    pub async fn init_trust_quorum(
        salt: [u8; 32],
        tq_handle: trust_quorum::NodeTaskHandle,
        lrtq_handle: NodeHandle,
    ) {
        Self::init_trust_quorum_with(
            &GLOBAL_RETRIEVER,
            salt,
            tq_handle,
            lrtq_handle,
        )
        .await
    }

    /// Set the type of secret retriever to `TqOrLrtqSecretRetriever` using
    /// the provided cell.
    ///
    /// This is separated from [`GlobalSecretRetriever::init_trust_quorum`] to
    /// allow testing the idempotency logic with non-static `OnceLock`
    /// instances.
    ///
    /// Panics if a non-idempotent call is made
    async fn init_trust_quorum_with(
        cell: &OnceLock<Box<dyn SecretRetriever>>,
        salt: [u8; 32],
        tq_handle: trust_quorum::NodeTaskHandle,
        lrtq_handle: NodeHandle,
    ) {
        if cell
            .set(Box::new(TqOrLrtqSecretRetriever::new(
                salt,
                tq_handle,
                lrtq_handle,
            )))
            .is_err()
        {
            // We know `unwrap` is safe because we only get an error if a value
            // exists in `cell`.
            match cell.get().unwrap().downcast_ref::<TqOrLrtqSecretRetriever>()
            {
                // We assume the node handles are fine. There can only be one of
                // each that gets cloned and we can't compare them for equality.
                Some(retriever) if retriever.salt().await == salt => (),
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

#[cfg(test)]
mod tests {
    use super::*;

    // These tests ensure that the dynamic idemopotency assertions work
    // correctly: they would be easy to get wrong because they require the
    // programmer to specify the expected type.

    #[test]
    fn init_hardcoded_first_call_succeeds() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        // Should not panic
        GlobalSecretRetriever::init_hardcoded_with(&cell);
        assert!(cell.get().unwrap().is::<HardcodedSecretRetriever>());
    }

    #[test]
    fn init_hardcoded_idempotent_call_succeeds() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        GlobalSecretRetriever::init_hardcoded_with(&cell);
        // Second call with same type should succeed (idempotent)
        GlobalSecretRetriever::init_hardcoded_with(&cell);
        assert!(cell.get().unwrap().is::<HardcodedSecretRetriever>());
    }

    #[test]
    #[should_panic(expected = "call was not idempotent")]
    fn init_hardcoded_after_trust_quorum_panics() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        let salt = [0u8; 32];
        let tq_handle = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle = NodeHandle::new_for_test();
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt,
            tq_handle,
            lrtq_handle,
        );
        // This should panic: TQ was set, but we're trying to set hardcoded
        GlobalSecretRetriever::init_hardcoded_with(&cell);
    }

    #[test]
    fn init_trust_quorum_first_call_succeeds() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        let salt = [1u8; 32];
        let tq_handle = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle = NodeHandle::new_for_test();
        // Should not panic
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt,
            tq_handle,
            lrtq_handle,
        );
        assert!(cell.get().unwrap().is::<TqOrLrtqSecretRetriever>());
    }

    #[test]
    fn init_trust_quorum_idempotent_call_same_salt_succeeds() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        let salt = [2u8; 32];

        let tq_handle1 = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle1 = NodeHandle::new_for_test();
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt,
            tq_handle1,
            lrtq_handle1,
        );

        // Second call with same salt should succeed (idempotent)
        let tq_handle2 = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle2 = NodeHandle::new_for_test();
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt,
            tq_handle2,
            lrtq_handle2,
        );

        assert!(cell.get().unwrap().is::<TqOrLrtqSecretRetriever>());
    }

    #[test]
    #[should_panic(expected = "call was not idempotent")]
    fn init_trust_quorum_different_salt_panics() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        let salt1 = [3u8; 32];
        let salt2 = [4u8; 32];

        let tq_handle1 = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle1 = NodeHandle::new_for_test();
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt1,
            tq_handle1,
            lrtq_handle1,
        );

        // This should panic: different salt is not idempotent
        let tq_handle2 = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle2 = NodeHandle::new_for_test();
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt2,
            tq_handle2,
            lrtq_handle2,
        );
    }

    #[test]
    #[should_panic(expected = "call was not idempotent")]
    fn init_trust_quorum_after_hardcoded_panics() {
        let cell: OnceLock<Box<dyn SecretRetriever>> = OnceLock::new();
        GlobalSecretRetriever::init_hardcoded_with(&cell);

        // This should panic: hardcoded was set, but we're trying to set TQ
        let salt = [5u8; 32];
        let tq_handle = trust_quorum::NodeTaskHandle::new_for_test();
        let lrtq_handle = NodeHandle::new_for_test();
        GlobalSecretRetriever::init_trust_quorum_with(
            &cell,
            salt,
            tq_handle,
            lrtq_handle,
        );
    }
}
