// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages TUF artifacts stored on this sled. The implementation is a very
//! basic content-addressed object store.
//!
//! GET operations are handled by the "Repo Depot" API, which is deliberately
//! a separate Dropshot service from the rest of Sled Agent. This is to avoid a
//! circular logical dependency, because we expect Sled Agent to fetch artifacts
//! it does not have from another Repo Depot that does have them (at Nexus's
//! direction). This API's implementation is also part of this module.
//!
//! POST, PUT, and DELETE operations are handled by the Sled Agent API.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::net::SocketAddrV6;
use std::str::FromStr;
use std::time::Duration;

use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::{NamedUtf8TempFile, Utf8TempPath};
use dropshot::{
    Body, ConfigDropshot, FreeformBody, HttpError, HttpResponseOk,
    HttpServerStarter, Path, RequestContext, StreamingBody,
};
use futures::{Stream, TryStreamExt};
use http::StatusCode;
use omicron_common::address::REPO_DEPOT_PORT;
use omicron_common::disk::{DatasetKind, DatasetsConfig};
use omicron_common::update::ArtifactHash;
use repo_depot_api::*;
use sha2::{Digest, Sha256};
use sled_storage::dataset::M2_ARTIFACT_DATASET;
use sled_storage::error::Error as StorageError;
use sled_storage::manager::StorageHandle;
use slog::{error, info, Logger};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

const TEMP_SUBDIR: &str = "tmp";

/// Content-addressable local storage for software artifacts.
///
/// Storage for artifacts is backed by datasets that are explicitly designated
/// for this purpose. The `T: DatasetsManager` parameter, which varies between
/// the real sled agent, the simulated sled agent, and unit tests, specifies
/// exactly which datasets are available for artifact storage. That's the only
/// thing `T` is used for. The behavior of storing artifacts as files under
/// one or more paths is identical for all callers (i.e., both the real and
/// simulated sled agents).
///
/// A given artifact is generally stored on both datasets designated for
/// artifact storage across both M.2 devices, but we attempt to be resilient to
/// a failing or missing M.2 device. This means:
///
/// - for PUT, we try to write to all datasets, logging errors as we go; if we
///   successfully write the artifact to at least one, we return OK.
/// - for GET, we look in each dataset until we find it.
/// - for DELETE, we attempt to delete it from each dataset, logging errors as
///   we go, and failing if we saw any errors.
#[derive(Clone)]
pub(crate) struct ArtifactStore<T: DatasetsManager> {
    log: Logger,
    reqwest_client: reqwest::Client,
    storage: T,
}

impl<T: DatasetsManager> ArtifactStore<T> {
    pub(crate) fn new(log: &Logger, storage: T) -> ArtifactStore<T> {
        ArtifactStore {
            log: log.new(slog::o!("component" => "ArtifactStore")),
            reqwest_client: reqwest::ClientBuilder::new()
                .connect_timeout(Duration::from_secs(15))
                .read_timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
            storage,
        }
    }
}

impl ArtifactStore<StorageHandle> {
    pub(crate) async fn start(
        self,
        sled_address: SocketAddrV6,
        dropshot_config: &ConfigDropshot,
    ) -> Result<dropshot::HttpServer<ArtifactStore<StorageHandle>>, StartError>
    {
        // In the real sled agent, the update datasets are durable and may
        // retain temporary files leaked during a crash. Upon startup, we
        // attempt to remove the subdirectory we store temporary files in,
        // logging an error if that fails.
        //
        // (This function is part of `start` instead of `new` out of
        // convenience: this function already needs to be async and fallible,
        // but `new` doesn't; and all the sled agent implementations that don't
        // call this function also don't need to run cleanup.)
        for mountpoint in self
            .storage
            .artifact_storage_paths()
            .await
            .map_err(StartError::DatasetConfig)?
        {
            let path = mountpoint.join(TEMP_SUBDIR);
            if let Err(err) = tokio::fs::remove_dir_all(&path).await {
                if err.kind() != ErrorKind::NotFound {
                    // We log an error here because we expect that if we are
                    // having disk I/O errors, something else (fmd?) will
                    // identify those issues and bubble them up to the operator.
                    // (As of writing this comment that is not true but we
                    // expect this to exist in the limit, and refusing to start
                    // Sled Agent because of a problem with a single FRU seems
                    // inappropriate.)
                    error!(
                        &self.log,
                        "Failed to remove stale temporary artifacts";
                        "error" => &err,
                        "path" => path.as_str(),
                    );
                }
            }
        }

        let mut depot_address = sled_address;
        depot_address.set_port(REPO_DEPOT_PORT);

        let log = self.log.new(o!("component" => "dropshot (Repo Depot)"));
        Ok(HttpServerStarter::new(
            &ConfigDropshot {
                bind_address: depot_address.into(),
                ..dropshot_config.clone()
            },
            repo_depot_api_mod::api_description::<RepoDepotImpl>()
                .expect("registered entrypoints"),
            self,
            &log,
        )
        .map_err(StartError::Dropshot)?
        .start())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("Error retrieving dataset configuration")]
    DatasetConfig(#[source] sled_storage::error::Error),

    #[error("Dropshot error while starting Repo Depot service")]
    Dropshot(#[source] Box<dyn std::error::Error + Send + Sync>),
}

macro_rules! log_and_store {
    ($last_error:expr, $log:expr, $verb:literal, $path:expr, $err:expr) => {{
        error!(
            $log,
            concat!("Failed to ", $verb, " path");
            "error" => &$err,
            "path" => $path.as_str(),
        );
        $last_error = Some(Error::File { verb: $verb, path: $path, err: $err });
    }};
}

impl<T: DatasetsManager> ArtifactStore<T> {
    /// GET operation (served by Repo Depot API)
    ///
    /// We try all datasets, returning early if we find the artifact, logging
    /// errors as we go. If we don't find it we return the most recent error we
    /// logged or a NotFound.
    pub(crate) async fn get(
        &self,
        sha256: ArtifactHash,
    ) -> Result<File, Error> {
        let sha256 = sha256.to_string();
        let mut last_error = None;
        for mountpoint in self.storage.artifact_storage_paths().await? {
            let path = mountpoint.join(&sha256);
            match File::open(&path).await {
                Ok(file) => {
                    info!(
                        &self.log,
                        "Retrieved artifact";
                        "sha256" => &sha256,
                        "path" => path.as_str(),
                    );
                    return Ok(file);
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    log_and_store!(last_error, &self.log, "open", path, err);
                }
            }
        }
        if let Some(last_error) = last_error {
            Err(last_error)
        } else {
            Err(Error::NotFound { sha256 })
        }
    }

    /// List operation (served by Sled Agent API)
    ///
    /// We try all datasets, logging errors as we go; if we're experiencing I/O
    /// errors, Nexus should still be aware of the artifacts we think we have.
    pub(crate) async fn list(
        &self,
    ) -> Result<BTreeMap<ArtifactHash, usize>, Error> {
        let mut map = BTreeMap::new();
        let mut any_datasets = false;
        for mountpoint in self.storage.artifact_storage_paths().await? {
            any_datasets = true;
            let mut read_dir = match tokio::fs::read_dir(&mountpoint).await {
                Ok(read_dir) => read_dir,
                Err(err) => {
                    error!(
                        &self.log,
                        "Failed to read dir";
                        "error" => &err,
                        "path" => mountpoint.as_str(),
                    );
                    continue;
                }
            };
            // The semantics of tokio::fs::ReadDir are weird. At least with
            // `std::fs::ReadDir`, we know when the end of the iterator is,
            // because `.next()` returns `Option<Result<DirEntry>>`; we could
            // theoretically log the error and continue trying to retrieve
            // elements from the iterator (but whether this makes sense to do
            // is not documented and likely system-dependent).
            //
            // The Tokio version returns `Result<Option<DirEntry>>`, which
            // has no indication of whether there might be more items in
            // the stream! (The stream adapter in tokio-stream simply calls
            // `Result::transpose()`, so in theory an error is not the end of
            // the stream.)
            //
            // For lack of any direction we stop reading entries from the stream
            // on the first error. That way we at least don't get stuck retrying
            // an operation that will always fail.
            loop {
                match read_dir.next_entry().await {
                    Ok(Some(entry)) => {
                        if let Ok(file_name) = entry.file_name().into_string() {
                            if let Ok(hash) = ArtifactHash::from_str(&file_name)
                            {
                                *map.entry(hash).or_default() += 1;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        error!(
                            &self.log,
                            "Failed to read dir";
                            "error" => &err,
                            "path" => mountpoint.as_str(),
                        );
                        break;
                    }
                }
            }
        }
        if any_datasets {
            Ok(map)
        } else {
            Err(Error::NoUpdateDataset)
        }
    }

    /// Common implementation for all artifact write operations that creates
    /// a temporary file on all datasets.
    ///
    /// Errors are logged and ignored unless a temporary file already exists
    /// (another task is writing to this artifact) or no temporary files could
    /// be created.
    async fn put_writer(
        &self,
        sha256: ArtifactHash,
    ) -> Result<MultiWriter, Error> {
        let mut inner = Vec::new();
        let mut last_error = None;
        for mountpoint in self.storage.artifact_storage_paths().await? {
            let temp_dir = mountpoint.join(TEMP_SUBDIR);
            if let Err(err) = tokio::fs::create_dir(&temp_dir).await {
                if err.kind() != ErrorKind::AlreadyExists {
                    log_and_store!(
                        last_error, &self.log, "create", temp_dir, err
                    );
                    continue;
                }
            }

            let temp_path =
                Utf8TempPath::from_path(temp_dir.join(sha256.to_string()));
            let file = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .await
            {
                Ok(file) => file,
                Err(err) => {
                    if err.kind() == ErrorKind::AlreadyExists {
                        return Err(Error::AlreadyInProgress { sha256 });
                    } else {
                        let path = temp_path.to_path_buf();
                        log_and_store!(
                            last_error, &self.log, "create", path, err
                        );
                        continue;
                    }
                }
            };
            let file = NamedUtf8TempFile::from_parts(file, temp_path);

            inner.push((file, mountpoint, sha256));
        }
        if inner.is_empty() {
            Err(last_error.unwrap_or(Error::NoUpdateDataset))
        } else {
            Ok(MultiWriter { inner })
        }
    }

    /// Common implementation for all artifact write operations that copies a
    /// generic stream of bytes to a set of temporary files, then persists those
    /// files.
    async fn put_impl(
        &self,
        multi_writer: Option<MultiWriter>,
        sha256: ArtifactHash,
        stream: impl Stream<Item = Result<impl AsRef<[u8]>, Error>>,
    ) -> Result<(), Error> {
        let multi_writer = match multi_writer {
            Some(multi_writer) => multi_writer,
            None => self.put_writer(sha256).await?,
        };

        let (multi_writer, hasher) = stream
            .try_fold(
                (multi_writer, Sha256::new()),
                |(mut multi_writer, mut hasher), chunk| async move {
                    hasher.update(&chunk);
                    multi_writer.write(&self.log, chunk).await?;
                    Ok((multi_writer, hasher))
                },
            )
            .await?;

        let digest = hasher.finalize();
        if digest.as_slice() != sha256.as_ref() {
            return Err(Error::HashMismatch {
                expected: sha256,
                actual: ArtifactHash(digest.into()),
            });
        }

        multi_writer.finalize(&self.log).await?;
        info!(
            &self.log,
            "Wrote artifact";
            "sha256" => &sha256.to_string(),
        );
        Ok(())
    }

    /// PUT operation (served by Sled Agent API) which takes a [`StreamingBody`]
    pub(crate) async fn put_body(
        &self,
        sha256: ArtifactHash,
        body: StreamingBody,
    ) -> Result<(), Error> {
        self.put_impl(None, sha256, body.into_stream().map_err(Error::Body))
            .await
    }

    /// POST operation (served by Sled Agent API)
    pub(crate) async fn copy_from_depot(
        &self,
        sha256: ArtifactHash,
        depot_base_url: &str,
    ) -> Result<(), Error> {
        let client = repo_depot_client::Client::new_with_client(
            depot_base_url,
            self.reqwest_client.clone(),
            self.log.new(slog::o!(
                "component" => "Repo Depot client (ArtifactStore)",
                "base_url" => depot_base_url.to_owned(),
            )),
        );
        // Check that there's no conflict before we send the upstream request.
        let multi_writer = self.put_writer(sha256).await?;
        let stream = client
            .artifact_get_by_sha256(&sha256.to_string())
            .await
            .map_err(|err| Error::DepotCopy {
                sha256,
                base_url: depot_base_url.to_owned(),
                err,
            })?
            .into_inner()
            .into_inner()
            .map_err(|err| Error::DepotCopy {
                sha256,
                base_url: depot_base_url.to_owned(),
                err: repo_depot_client::ClientError::ResponseBodyError(err),
            });
        self.put_impl(Some(multi_writer), sha256, stream).await
    }

    /// DELETE operation (served by Sled Agent API)
    ///
    /// We attempt to delete the artifact in all datasets, logging errors as we
    /// go. If any errors occurred we return the most recent error we logged.
    pub(crate) async fn delete(
        &self,
        sha256: ArtifactHash,
    ) -> Result<(), Error> {
        let sha256 = sha256.to_string();
        let mut any_datasets = false;
        let mut last_error = None;
        for mountpoint in self.storage.artifact_storage_paths().await? {
            any_datasets = true;
            let path = mountpoint.join(&sha256);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    info!(
                        &self.log,
                        "Removed artifact";
                        "sha256" => &sha256,
                        "path" => path.as_str(),
                    );
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    log_and_store!(last_error, &self.log, "remove", path, err);
                }
            }
        }
        if let Some(last_error) = last_error {
            Err(last_error)
        } else if any_datasets {
            Ok(())
        } else {
            // If we're here because there aren't any update datasets, we should
            // report Service Unavailable instead of a successful result.
            Err(Error::NoUpdateDataset)
        }
    }
}

/// Abstracts over what kind of sled agent we are; each of the real sled agent,
/// simulated sled agent, and this module's unit tests have different ways of
/// keeping track of the datasets on the system.
pub(crate) trait DatasetsManager {
    async fn artifact_storage_paths(
        &self,
    ) -> Result<impl Iterator<Item = Utf8PathBuf> + '_, StorageError>;
}

/// Iterator `.filter().map()` common to `DatasetsManager` implementations.
pub(crate) fn filter_dataset_mountpoints(
    config: DatasetsConfig,
    root: &Utf8Path,
) -> impl Iterator<Item = Utf8PathBuf> + '_ {
    config
        .datasets
        .into_values()
        .filter(|dataset| *dataset.name.dataset() == DatasetKind::Update)
        .map(|dataset| dataset.name.mountpoint(root))
}

impl DatasetsManager for StorageHandle {
    async fn artifact_storage_paths(
        &self,
    ) -> Result<impl Iterator<Item = Utf8PathBuf> + '_, StorageError> {
        // TODO: When datasets are managed by Reconfigurator (#6229),
        // this should be changed to use `self.datasets_config_list()` and
        // `filter_dataset_mountpoints`.
        Ok(self
            .get_latest_disks()
            .await
            .all_m2_mountpoints(M2_ARTIFACT_DATASET)
            .into_iter())
    }
}

/// Abstraction for `ArtifactStore::put_impl` that handles writing to several
/// temporary files.
struct MultiWriter {
    inner: Vec<(NamedUtf8TempFile<File>, Utf8PathBuf, ArtifactHash)>,
}

impl MultiWriter {
    /// Write `chunk` to all files. If an error occurs, it is logged and the
    /// temporary file is dropped. If there are no files left to write to, the
    /// most recently-seen error is returned.
    async fn write(
        &mut self,
        log: &Logger,
        chunk: impl AsRef<[u8]>,
    ) -> Result<(), Error> {
        // We would rather use `Vec::retain_mut` here but we'd need an async
        // version...
        let to_write = std::mem::take(&mut self.inner);
        self.inner.reserve_exact(to_write.len());
        let mut last_error = None;
        for (mut file, mountpoint, sha256) in to_write {
            match file.as_file_mut().write_all(chunk.as_ref()).await {
                Ok(()) => self.inner.push((file, mountpoint, sha256)),
                Err(err) => {
                    let path = file.path().to_owned();
                    log_and_store!(last_error, log, "write to", path, err);
                    // `file` and `final_path` are dropped here, cleaning up
                    // the file
                }
            }
        }

        if self.inner.is_empty() {
            Err(last_error.unwrap_or(Error::NoUpdateDataset))
        } else {
            Ok(())
        }
    }

    /// Rename all files to their final paths. If an error occurs, it is logged.
    /// If none of the files are renamed successfully, the most recently-seen
    /// error is returned.
    async fn finalize(self, log: &Logger) -> Result<(), Error> {
        let mut last_error = None;
        let mut any_success = false;
        for (mut file, mountpoint, sha256) in self.inner {
            // 1. Open the mountpoint and its temp dir so we can fsync them at
            // the end.
            let parent_dir = match File::open(&mountpoint).await {
                Ok(dir) => dir,
                Err(err) => {
                    log_and_store!(last_error, log, "open", mountpoint, err);
                    continue;
                }
            };
            let temp_dir_path = mountpoint.join(TEMP_SUBDIR);
            let temp_dir = match File::open(&temp_dir_path).await {
                Ok(dir) => dir,
                Err(err) => {
                    log_and_store!(last_error, log, "open", temp_dir_path, err);
                    continue;
                }
            };
            // 2. fsync the file.
            if let Err(err) = file.as_file_mut().sync_all().await {
                let path = file.path().to_owned();
                log_and_store!(last_error, log, "sync", path, err);
                continue;
            }
            // 3. Rename temporary file.
            let final_path = mountpoint.join(sha256.to_string());
            let moved_final_path = final_path.clone();
            if let Err(err) = tokio::task::spawn_blocking(move || {
                file.persist(&moved_final_path)
            })
            .await?
            {
                error!(
                    log,
                    "Failed to rename temporary file";
                    "error" => &err.error,
                    "from" => err.file.path().as_str(),
                    "to" => final_path.as_str(),
                );
                last_error = Some(Error::FileRename {
                    from: err.file.path().to_owned(),
                    to: final_path,
                    err: err.error,
                });
                continue;
            }
            // 4. fsync the parent directory for both the final path and its
            // previous path.
            if let Err(err) = parent_dir.sync_all().await {
                log_and_store!(last_error, log, "sync", mountpoint, err);
                continue;
            }
            if let Err(err) = temp_dir.sync_all().await {
                log_and_store!(last_error, log, "sync", temp_dir_path, err);
                continue;
            }

            any_success = true;
        }

        if any_success {
            Ok(())
        } else {
            Err(last_error.unwrap_or(Error::NoUpdateDataset))
        }
    }
}

/// Implementation of the Repo Depot API backed by an
/// `ArtifactStore<StorageHandle>`.
enum RepoDepotImpl {}

impl RepoDepotApi for RepoDepotImpl {
    type Context = ArtifactStore<StorageHandle>;

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

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Another task is already writing artifact {sha256}")]
    AlreadyInProgress { sha256: ArtifactHash },

    #[error("Error while reading request body")]
    Body(dropshot::HttpError),

    #[error("Error retrieving dataset configuration")]
    DatasetConfig(#[from] sled_storage::error::Error),

    #[error("Error fetching artifact {sha256} from depot at {base_url}")]
    DepotCopy {
        sha256: ArtifactHash,
        base_url: String,
        #[source]
        err: repo_depot_client::ClientError,
    },

    #[error("Failed to {verb} `{path}`")]
    File {
        verb: &'static str,
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to rename `{from}` to `{to}`")]
    FileRename {
        from: Utf8PathBuf,
        to: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("Digest mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: ArtifactHash, actual: ArtifactHash },

    #[error("Blocking task failed")]
    Join(#[from] tokio::task::JoinError),

    #[error("Artifact {sha256} not found")]
    NotFound { sha256: String },

    #[error("No update datasets present")]
    NoUpdateDataset,
}

impl From<Error> for HttpError {
    fn from(err: Error) -> HttpError {
        match err {
            Error::AlreadyInProgress { .. } => HttpError::for_client_error(
                None,
                StatusCode::CONFLICT,
                err.to_string(),
            ),
            Error::Body(inner) => inner,
            Error::DatasetConfig(_) | Error::NoUpdateDataset => {
                HttpError::for_unavail(None, err.to_string())
            }
            Error::DepotCopy { .. }
            | Error::File { .. }
            | Error::FileRename { .. }
            | Error::Join(_) => HttpError::for_internal_error(err.to_string()),
            Error::HashMismatch { .. } => {
                HttpError::for_bad_request(None, err.to_string())
            }
            Error::NotFound { .. } => {
                HttpError::for_not_found(None, err.to_string())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use camino_tempfile::Utf8TempDir;
    use futures::stream;
    use hex_literal::hex;
    use omicron_common::disk::{
        DatasetConfig, DatasetKind, DatasetName, DatasetsConfig,
    };
    use omicron_common::update::ArtifactHash;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::{DatasetUuid, ZpoolUuid};
    use sled_storage::error::Error as StorageError;
    use tokio::io::AsyncReadExt;

    use super::{ArtifactStore, DatasetsManager, Error};

    struct TestBackend {
        datasets: DatasetsConfig,
        mountpoint_root: Utf8TempDir,
    }

    impl TestBackend {
        fn new(len: usize) -> TestBackend {
            let mountpoint_root = camino_tempfile::tempdir().unwrap();

            let mut datasets = DatasetsConfig::default();
            if len > 0 {
                datasets.generation = datasets.generation.next();
            }
            for _ in 0..len {
                let dataset = DatasetConfig {
                    id: DatasetUuid::new_v4(),
                    name: DatasetName::new(
                        ZpoolName::new_external(ZpoolUuid::new_v4()),
                        DatasetKind::Update,
                    ),
                    compression: Default::default(),
                    quota: None,
                    reservation: None,
                };
                let mountpoint =
                    dataset.name.mountpoint(mountpoint_root.path());
                std::fs::create_dir_all(mountpoint).unwrap();
                datasets.datasets.insert(dataset.id, dataset);
            }

            TestBackend { datasets, mountpoint_root }
        }
    }

    impl DatasetsManager for TestBackend {
        async fn artifact_storage_paths(
            &self,
        ) -> Result<impl Iterator<Item = camino::Utf8PathBuf> + '_, StorageError>
        {
            Ok(super::filter_dataset_mountpoints(
                self.datasets.clone(),
                self.mountpoint_root.path(),
            ))
        }
    }

    const TEST_ARTIFACT: &[u8] = b"I'm an artifact!\n";
    const TEST_HASH: ArtifactHash = ArtifactHash(hex!(
        "ab3581cd62f6645518f61a8e4391af6c062d5d60111edb0e51b37bd84827f5b4"
    ));

    #[tokio::test]
    async fn list_get_put_delete() {
        let log = test_setup_log("get_put_delete");
        let backend = TestBackend::new(2);
        let store = ArtifactStore::new(&log.log, backend);

        // list succeeds with an empty result
        assert!(store.list().await.unwrap().is_empty());
        // get fails, because it doesn't exist yet
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));
        // delete does not fail because we don't fail if the artifact is not
        // present
        assert!(matches!(store.delete(TEST_HASH).await, Ok(())));

        // test several things here:
        // 1. put succeeds
        // 2. put is idempotent (we don't care if it clobbers a file as long as
        //    the hash is okay)
        // 3. we don't fail trying to create TEMP_SUBDIR twice
        for _ in 0..2 {
            store
                .put_impl(
                    None,
                    TEST_HASH,
                    stream::once(async { Ok(TEST_ARTIFACT) }),
                )
                .await
                .unwrap();
            // list lists the file
            assert!(store
                .list()
                .await
                .unwrap()
                .into_iter()
                .eq([(TEST_HASH, 2)]));
            // get succeeds, file reads back OK
            let mut file = store.get(TEST_HASH).await.unwrap();
            let mut vec = Vec::new();
            file.read_to_end(&mut vec).await.unwrap();
            assert_eq!(vec, TEST_ARTIFACT);
        }

        // all datasets should have the artifact
        for mountpoint in store.storage.artifact_storage_paths().await.unwrap()
        {
            assert_eq!(
                tokio::fs::read(mountpoint.join(TEST_HASH.to_string()))
                    .await
                    .unwrap(),
                TEST_ARTIFACT
            );
        }

        // delete succeeds and is idempotent
        for _ in 0..2 {
            store.delete(TEST_HASH).await.unwrap();
            // list succeeds with an empty result
            assert!(store.list().await.unwrap().is_empty());
            // get now fails because it no longer exists
            assert!(matches!(
                store.get(TEST_HASH).await,
                Err(Error::NotFound { .. })
            ));
        }

        log.cleanup_successful();
    }

    #[tokio::test]
    async fn no_dataset() {
        // If there are no update datasets, all gets should fail with
        // `Error::NotFound`, and all other operations should fail with
        // `Error::NoUpdateDataset`.

        let log = test_setup_log("no_dataset");
        let backend = TestBackend::new(0);
        let store = ArtifactStore::new(&log.log, backend);

        assert!(matches!(
            store
                .put_impl(
                    None,
                    TEST_HASH,
                    stream::once(async { Ok(TEST_ARTIFACT) })
                )
                .await,
            Err(Error::NoUpdateDataset)
        ));
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(store.list().await, Err(Error::NoUpdateDataset)));
        assert!(matches!(
            store.delete(TEST_HASH).await,
            Err(Error::NoUpdateDataset)
        ));

        log.cleanup_successful();
    }
}
