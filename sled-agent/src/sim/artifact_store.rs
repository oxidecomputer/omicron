// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of `crate::artifact_store::StorageBackend` for our simulated
//! storage.

use std::sync::Arc;

use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use futures::lock::Mutex;
use omicron_common::disk::DatasetsConfig;

use super::storage::Storage;
use crate::artifact_store::{Error, StorageBackend};

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

impl StorageBackend for SimArtifactStorage {
    async fn datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        self.backend
            .lock()
            .await
            .datasets_config_list()
            .await
            .map_err(|_| Error::NoUpdateDataset)
    }

    fn mountpoint_root(&self) -> &Utf8Path {
        self.root.path()
    }
}
