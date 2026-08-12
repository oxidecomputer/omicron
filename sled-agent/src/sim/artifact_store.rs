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
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, watch};

use crate::artifact_store::{ArtifactStore, DatasetsManager, Error};

// Semaphore mostly uses usize but in `acquire_many` it unfortunately uses u32.
const MAX_PERMITS: u32 = u32::MAX >> 3;

#[derive(Clone)]
pub struct SimArtifactStorage {
    // We simulate the two M.2s with two separate temporary directories.
    dirs: Arc<[Utf8TempDir; 2]>,

    // Semaphore to keep track of how many writes are in flight, and to be
    // able to await on their completion. Used to wait for copies to complete
    // in integration tests and to wait for writes to complete finish before
    // dropping sled-agent, to avoid a race condition where files are moved
    // to their final path while the drop implementation for `Utf8TempDir` is
    // running.
    write_semaphore: Arc<Semaphore>,

    // Watch channel to be able to await on the delete reconciler completing in
    // integration tests.
    delete_done: watch::Sender<Generation>,
}

impl SimArtifactStorage {
    pub(super) fn new() -> SimArtifactStorage {
        SimArtifactStorage {
            dirs: Arc::new(std::array::from_fn(|_| {
                camino_tempfile::Builder::new()
                    .prefix("artifact-store")
                    .tempdir()
                    .unwrap()
            })),
            write_semaphore: Arc::new(Semaphore::new(MAX_PERMITS as usize)),
            delete_done: watch::Sender::new(0u32.into()),
        }
    }
}

impl DatasetsManager for SimArtifactStorage {
    type PermitError = Error;

    async fn artifact_storage_paths(
        &self,
    ) -> impl Iterator<Item = camino::Utf8PathBuf> + '_ {
        self.dirs.iter().map(|tempdir| tempdir.path().to_owned())
    }

    async fn write_permit(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, Error> {
        match self.write_semaphore.clone().acquire_owned().await {
            Ok(permit) => Ok(Some(permit)),
            Err(err) => {
                let _err: AcquireError = err;
                // `ArtifactStore::stop_writers` was called and the semaphore is
                // closed, so claim that no update datasets are available.
                Err(Error::NoUpdateDataset)
            }
        }
    }

    fn signal_delete_done(&self, generation: Generation) {
        self.delete_done.send_if_modified(|old| {
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
        .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
            dropshot::ClientSpecifiesVersionInHeader::new(
                omicron_common::api::VERSION_HEADER,
                repo_depot_api::latest_version(),
            ),
        )))
        .start()
        .unwrap()
    }

    pub fn storage_paths(&self) -> impl Iterator<Item = &Utf8Path> {
        self.storage.dirs.iter().map(|p| p.path())
    }

    pub async fn wait_for_writers(&self) {
        // Acquire a permit for MAX_PERMITS, which requires that all write tasks
        // have dropped their permits. Then immediately drop it.
        let _permit_or_closed =
            self.storage.write_semaphore.acquire_many(MAX_PERMITS).await;
    }

    /// Waits for all current writers to complete, then closes the semaphore to
    /// disallow any new writers from starting.
    ///
    /// This cannot take an owned `self` because it would require moving this
    /// struct out of an `Arc`.
    pub async fn stop_writers(&self) {
        // Acquire a permit for MAX_PERMITS, which requires that all write
        // tasks have dropped their permits. While holding the permit, close the
        // semaphore to prevent any new write tasks from starting.
        let _permit_or_closed =
            self.storage.write_semaphore.acquire_many(MAX_PERMITS).await;
        self.storage.write_semaphore.close();
    }

    pub fn subscribe_delete_done(&self) -> watch::Receiver<Generation> {
        self.storage.delete_done.subscribe()
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
