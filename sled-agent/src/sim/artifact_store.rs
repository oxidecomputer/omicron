// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of `crate::artifact_store::StorageBackend` for our simulated
//! storage.

use camino_tempfile::Utf8TempDir;
use dropshot::{
    Body, ConfigDropshot, FreeformBody, HttpError, HttpResponseOk, HttpServer,
    Path, RequestContext, ServerBuilder,
};
use repo_depot_api::*;
use sled_storage::error::Error as StorageError;
use std::sync::Arc;

use crate::artifact_store::{ArtifactStore, DatasetsManager};

#[derive(Clone)]
pub(super) struct SimArtifactStorage {
    dirs: Arc<(Utf8TempDir, Utf8TempDir)>,
}

impl SimArtifactStorage {
    pub(super) fn new() -> SimArtifactStorage {
        SimArtifactStorage {
            dirs: Arc::new((
                camino_tempfile::tempdir().unwrap(),
                camino_tempfile::tempdir().unwrap(),
            )),
        }
    }
}

impl DatasetsManager for SimArtifactStorage {
    async fn artifact_storage_paths(
        &self,
    ) -> Result<impl Iterator<Item = camino::Utf8PathBuf> + '_, StorageError>
    {
        Ok([self.dirs.0.path().to_owned(), self.dirs.1.path().to_owned()]
            .into_iter())
    }
}

impl ArtifactStore<SimArtifactStorage> {
    pub(super) fn start(
        &self,
        log: &slog::Logger,
        dropshot_config: &ConfigDropshot,
    ) -> HttpServer<Self> {
        let mut config = dropshot_config.clone();
        config.bind_address.set_port(0);
        ServerBuilder::new(
            repo_depot_api_mod::api_description::<RepoDepotImpl>()
                .expect("registered entrypoints"),
            self.clone(),
            log.new(o!("component" => "dropshot (Repo Depot)")),
        )
        .config(config)
        .start()
        .unwrap()
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
