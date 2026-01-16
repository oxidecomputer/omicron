// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A secret retriever that waits to be configured before use.
//!
//! Created early in boot (before we know which retriever type to use),
//! configured later once we have the sled agent request.

use async_trait::async_trait;
use key_manager::{
    SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// A secret retriever that waits to be configured before use.
///
/// Created early in boot (before we know which retriever type to use),
/// configured later once we have the sled agent request.
pub struct ConfigurableSecretRetriever {
    inner: Option<Box<dyn SecretRetriever>>,
    config_rx: Option<oneshot::Receiver<Box<dyn SecretRetriever>>>,
}

/// Handle to configure a [`PendingSecretRetriever`].
///
/// Cloneable, but `configure` can only succeed once (panics on second call).
#[derive(Clone)]
pub struct ConfigurableSecretRetrieverHandle {
    #[allow(clippy::type_complexity)]
    tx: Arc<Mutex<Option<oneshot::Sender<Box<dyn SecretRetriever>>>>>,
}

impl ConfigurableSecretRetriever {
    pub fn new() -> (Self, ConfigurableSecretRetrieverHandle) {
        let (tx, rx) = oneshot::channel();
        (
            Self { inner: None, config_rx: Some(rx) },
            ConfigurableSecretRetrieverHandle {
                tx: Arc::new(Mutex::new(Some(tx))),
            },
        )
    }
}

impl ConfigurableSecretRetrieverHandle {
    /// Configure the pending retriever with the actual implementation.
    ///
    /// Panics if called twice or if the corresponding
    /// [`PendingSecretRetriever`] was dropped.
    pub fn init(&self, retriever: impl SecretRetriever) {
        self.tx
            .lock()
            .unwrap()
            .take()
            .expect("PendingSecretRetriever already configured")
            .send(Box::new(retriever))
            .unwrap_or_else(|_| {
                panic!("PendingSecretRetriever dropped before configure")
            });
    }
}

#[async_trait]
impl SecretRetriever for ConfigurableSecretRetriever {
    async fn get_latest(
        &mut self,
    ) -> Result<VersionedIkm, SecretRetrieverError> {
        self.ensure_configured().await?;
        self.inner.as_mut().unwrap().get_latest().await
    }

    async fn get(
        &mut self,
        epoch: u64,
    ) -> Result<SecretState, SecretRetrieverError> {
        self.ensure_configured().await?;
        self.inner.as_mut().unwrap().get(epoch).await
    }
}

impl ConfigurableSecretRetriever {
    async fn ensure_configured(&mut self) -> Result<(), SecretRetrieverError> {
        if self.inner.is_none() {
            let rx = self
                .config_rx
                .take()
                .ok_or(SecretRetrieverError::RackNotInitialized)?;
            self.inner = Some(
                rx.await
                    .map_err(|_| SecretRetrieverError::RackNotInitialized)?,
            );
        }
        Ok(())
    }
}
