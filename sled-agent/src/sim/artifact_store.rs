// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of `crate::artifact_store::StorageBackend` for our simulated
//! storage.

use std::sync::Arc;

use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use dropshot::{
    Body, ConfigDropshot, FreeformBody, HttpError, HttpResponseOk, HttpServer,
    Path, RequestContext, ServerBuilder,
};
use omicron_common::api::external::Generation;
use repo_depot_api::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::artifact_store::{ArtifactStore, DatasetsManager};

// Semaphore mostly uses usize but in `acquire_many` it unfortunately uses u32.
const MAX_PERMITS: u32 = u32::MAX >> 3;

#[derive(Clone)]
pub struct SimArtifactStorage {
    // We simulate the two M.2s with two separate temporary directories.
    dirs: Arc<[Utf8TempDir; 2]>,

    // Semaphore to keep track of how many copy requests are in flight, and to
    // be able to await on their completion. Used in integration tests.
    copy_semaphore: Arc<Semaphore>,

    // Watch channel to be able to await on the delete reconciler completing in
    // integration tests.
    delete_done_tx: watch::Sender<Generation>,
    delete_done_rx: watch::Receiver<Generation>,
}

impl SimArtifactStorage {
    pub(super) fn new() -> SimArtifactStorage {
        let (delete_done_tx, delete_done_rx) = watch::channel(0u32.into());
        SimArtifactStorage {
            dirs: Arc::new([
                camino_tempfile::tempdir().unwrap(),
                camino_tempfile::tempdir().unwrap(),
            ]),
            copy_semaphore: Arc::new(
                const { Semaphore::const_new(MAX_PERMITS as usize) },
            ),
            delete_done_tx,
            delete_done_rx,
        }
    }
}

impl DatasetsManager for SimArtifactStorage {
    async fn artifact_storage_paths(
        &self,
    ) -> impl Iterator<Item = camino::Utf8PathBuf> + '_ {
        self.dirs.iter().map(|tempdir| tempdir.path().to_owned())
    }

    async fn copy_permit(&self) -> Option<OwnedSemaphorePermit> {
        Some(self.copy_semaphore.clone().acquire_owned().await.unwrap())
    }

    fn signal_delete_done(&self, generation: Generation) {
        self.delete_done_tx.send_if_modified(|old| {
            let modified = *old != generation;
            *old = generation;
            modified
        });
    }
}

impl ArtifactStore<SimArtifactStorage> {
    pub(super) fn start(
        self,
        log: &slog::Logger,
        dropshot_config: &ConfigDropshot,
    ) -> HttpServer<Self> {
        ServerBuilder::new(
            repo_depot_api_mod::api_description::<RepoDepotImpl>()
                .expect("registered entrypoints"),
            self,
            log.new(o!("component" => "dropshot (Repo Depot)")),
        )
        .config(dropshot_config.clone())
        .start()
        .unwrap()
    }

    pub fn storage_paths(&self) -> impl Iterator<Item = &Utf8Path> {
        self.storage.dirs.iter().map(|p| p.path())
    }

    pub async fn wait_for_copy_tasks(&self) {
        // Acquire a permit for MAX_PERMITS, which requires that all copy tasks
        // have dropped their permits. Then immediately drop it.
        let _permit = self
            .storage
            .copy_semaphore
            .acquire_many(MAX_PERMITS)
            .await
            .unwrap();
    }

    pub fn create_delete_watcher(&self) -> watch::Receiver<Generation> {
        let mut watcher = self.storage.delete_done_rx.clone();
        watcher.mark_unchanged();
        watcher
    }
}

/// Implementation of the Repo Depot API backed by an
/// `ArtifactStore<SimArtifactStorage>`.
pub(super) enum RepoDepotImpl {}

impl RepoDepotApi for RepoDepotImpl {
    type Context = ArtifactStore<SimArtifactStorage>;

    async fn artifact_get_by_sha256(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParams>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
        let sha256 = path_params.into_inner().sha256;
        let file = rqctx.context().get(sha256).await?;
        let file_access = hyper_staticfile::vfs::TokioFileAccess::new(file);
        let file_stream =
            hyper_staticfile::util::FileBytesStream::new(file_access);
        let body = Body::wrap(hyper_staticfile::Body::Full(file_stream));
        Ok(HttpResponseOk(FreeformBody(body)))
    }
}
