// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of `crate::artifact_store::StorageBackend` for our simulated
//! storage.

use std::sync::Arc;

use camino_tempfile::Utf8TempDir;
use futures::lock::Mutex;
use sled_storage::error::Error as StorageError;

use super::storage::Storage;
use crate::artifact_store::DatasetsManager;

pub(super) struct SimArtifactStorage {
    root: Utf8TempDir,
    backend: Arc<Mutex<Storage>>,
}

impl SimArtifactStorage {
    pub(super) fn new(backend: Arc<Mutex<Storage>>) -> SimArtifactStorage {
        SimArtifactStorage {
            root: camino_tempfile::tempdir().unwrap(),
            backend,
        }
    }
}

impl DatasetsManager for SimArtifactStorage {
    async fn artifact_storage_paths(
        &self,
    ) -> Result<impl Iterator<Item = camino::Utf8PathBuf> + '_, StorageError>
    {
        let config = self
            .backend
            .lock()
            .await
            .datasets_config_list()
            .await
            .map_err(|_| StorageError::LedgerNotFound)?;
        Ok(crate::artifact_store::filter_dataset_mountpoints(
            config,
            self.root.path(),
        ))
    }
}
