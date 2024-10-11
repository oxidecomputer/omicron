// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages update artifacts stored on this sled. The implementation is a very
//! basic content-addressed object store.
//!
//! GET operations are handled by the "Repo Depot" API, which is deliberately
//! a separate Dropshot service from the rest of Sled Agent. This is to avoid a
//! circular logical dependency, because we expect Sled Agent to fetch artifacts
//! it does not have from another Repo Depot that does have them (at Nexus's
//! direction). This API's implementation is also part of this module.
//!
//! POST, PUT, and DELETE operations are handled by the Sled Agent API.

use std::io::ErrorKind;
use std::net::SocketAddrV6;
use std::sync::LazyLock;
use std::time::Duration;

use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::NamedUtf8TempFile;
use dropshot::{
    Body, ConfigDropshot, FreeformBody, HttpError, HttpResponseOk,
    HttpServerStarter, Path, RequestContext, StreamingBody,
};
use futures::{Stream, TryStreamExt};
use illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT;
use omicron_common::address::REPO_DEPOT_PORT;
use omicron_common::disk::{DatasetKind, DatasetsConfig};
use omicron_common::update::ArtifactHash;
use repo_depot_api::*;
use sha2::{Digest, Sha256};
use sled_storage::manager::StorageHandle;
use slog::{error, info, Logger};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

const TEMP_SUBDIR: &str = "tmp";

/// Content-addressable local storage for software artifacts.
///
/// Storage for artifacts is backed by datasets that are explicitly designated
/// for this purpose. The `T: StorageBackend` parameter, which varies between
/// the real sled agent, the simulated sled agent, and unit tests, specifies
/// exactly which datasets are available for artifact storage. That's the only
/// thing `T` is used for. The behavior of storing artifacts as files under
/// one or more paths is identical for all callers (i.e., both the real and
/// simulated sled agents).
///
/// A given artifact is generally stored on exactly one of the datasets
/// designated for artifact storage. There's no way to know from its key which
/// dataset it's stored on. This means:
///
/// - for PUT, we pick a dataset and store it there
/// - for GET, we look in every dataset for it until we find it
/// - for DELETE, we attempt to delete it from every dataset (even after we've
///   found it in one of them)
#[derive(Clone)]
pub(crate) struct ArtifactStore<T: StorageBackend = StorageHandle> {
    log: Logger,
    storage: T,
}

impl<T: StorageBackend> ArtifactStore<T> {
    pub(crate) fn new(log: &Logger, storage: T) -> ArtifactStore<T> {
        ArtifactStore {
            log: log.new(slog::o!("component" => "ArtifactStore")),
            storage,
        }
    }
}

impl ArtifactStore {
    pub(crate) async fn start(
        self,
        sled_address: SocketAddrV6,
        dropshot_config: &ConfigDropshot,
    ) -> Result<dropshot::HttpServer<ArtifactStore>, StartError> {
        // This function is only called when the real Sled Agent starts up (it
        // is only defined over `ArtifactStore<StorageHandle>`). In the real
        // Sled Agent, these datasets are durable and may retain temporary
        // files leaked during a crash. Upon startup, we attempt to remove the
        // subdirectory we store temporary files in, logging an error if that
        // fails.

        // This `datasets_config_list` call is clear enough to the
        // compiler, but perhaps not to the person reading this code,
        // because there's two relevant methods in scope: the concrete
        // `StorageHandle::datasets_config_list` (because T = StorageHandle) and
        // the generic `<StorageHandle as StorageBackend>::datasets_config_list`.
        // The reason the concrete function is selected is because these
        // functions return two different error types, and the `.map_err`
        // implies that we want a `sled_storage::error::Error`.
        let config = self
            .storage
            .datasets_config_list()
            .await
            .map_err(StartError::DatasetConfig)?;
        for mountpoint in
            update_dataset_mountpoints(config, ZPOOL_MOUNTPOINT_ROOT.into())
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
    #[error("Error retrieving dataset configuration: {0}")]
    DatasetConfig(#[source] sled_storage::error::Error),

    #[error("Dropshot error while starting Repo Depot service: {0}")]
    Dropshot(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl<T: StorageBackend> ArtifactStore<T> {
    /// GET operation (served by Repo Depot API)
    pub(crate) async fn get(
        &self,
        sha256: ArtifactHash,
    ) -> Result<File, Error> {
        let sha256 = sha256.to_string();
        let mut last_error = None;
        for mountpoint in self.dataset_mountpoints().await? {
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
                    error!(
                        &self.log,
                        "Failed to open file";
                        "error" => &err,
                        "path" => path.as_str(),
                    );
                    last_error = Some(Error::File { verb: "open", path, err });
                }
            }
        }
        if let Some(last_error) = last_error {
            Err(last_error)
        } else {
            Err(Error::NotFound { sha256 })
        }
    }

    /// Common implementation for all artifact write operations, generic over a
    /// stream of bytes.
    async fn put_impl(
        &self,
        sha256: ArtifactHash,
        stream: impl Stream<Item = Result<impl AsRef<[u8]>, Error>>,
    ) -> Result<(), Error> {
        let mountpoint = self
            .dataset_mountpoints()
            .await?
            .next()
            .ok_or(Error::NoUpdateDataset)?;
        let final_path = mountpoint.join(sha256.to_string());

        let temp_dir = mountpoint.join(TEMP_SUBDIR);
        if let Err(err) = tokio::fs::create_dir(&temp_dir).await {
            if err.kind() != ErrorKind::AlreadyExists {
                return Err(Error::File {
                    verb: "create directory",
                    path: temp_dir,
                    err,
                });
            }
        }
        let (file, temp_path) = tokio::task::spawn_blocking(move || {
            NamedUtf8TempFile::new_in(&temp_dir).map_err(|err| Error::File {
                verb: "create temporary file in",
                path: temp_dir,
                err,
            })
        })
        .await??
        .into_parts();
        let file =
            NamedUtf8TempFile::from_parts(File::from_std(file), temp_path);

        let (mut file, hasher) = stream
            .try_fold(
                (file, Sha256::new()),
                |(mut file, mut hasher), chunk| async move {
                    hasher.update(&chunk);
                    match file.as_file_mut().write_all(chunk.as_ref()).await {
                        Ok(()) => Ok((file, hasher)),
                        Err(err) => Err(Error::File {
                            verb: "write to",
                            path: file.path().to_owned(),
                            err,
                        }),
                    }
                },
            )
            .await?;
        file.as_file_mut().sync_data().await.map_err(|err| Error::File {
            verb: "sync",
            path: final_path.clone(),
            err,
        })?;

        let digest = hasher.finalize();
        if digest.as_slice() != sha256.as_ref() {
            return Err(Error::HashMismatch {
                expected: sha256,
                actual: ArtifactHash(digest.into()),
            });
        }

        let moved_final_path = final_path.clone();
        tokio::task::spawn_blocking(move || {
            file.persist(&moved_final_path).map_err(|err| Error::File {
                verb: "rename temporary file to",
                path: moved_final_path,
                err: err.error,
            })
        })
        .await??;
        info!(
            &self.log,
            "Wrote artifact";
            "sha256" => &sha256.to_string(),
            "path" => final_path.as_str(),
        );
        Ok(())
    }

    /// PUT operation (served by Sled Agent API) which takes a [`StreamingBody`]
    pub(crate) async fn put_body(
        &self,
        sha256: ArtifactHash,
        body: StreamingBody,
    ) -> Result<(), Error> {
        self.put_impl(sha256, body.into_stream().map_err(Error::Body)).await
    }

    /// POST operation (served by Sled Agent API)
    pub(crate) async fn copy_from_depot(
        &self,
        sha256: ArtifactHash,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
            reqwest::ClientBuilder::new()
                .connect_timeout(Duration::from_secs(15))
                .read_timeout(Duration::from_secs(15))
                .build()
                .unwrap()
        });

        let client = repo_depot_client::Client::new_with_client(
            &format!("http://{address}"),
            CLIENT.clone(),
            self.log.new(
                slog::o!("component" => "Repo Depot client (ArtifactStore)"),
            ),
        );
        let stream = client
            .artifact_get_by_sha256(&sha256.to_string())
            .await
            .map_err(|err| Error::DepotCopy { sha256, address, err })?
            .into_inner()
            .into_inner()
            .map_err(|err| Error::DepotCopy {
                sha256,
                address,
                err: repo_depot_client::ClientError::ResponseBodyError(err),
            });
        self.put_impl(sha256, stream).await
    }

    /// DELETE operation (served by Sled Agent API)
    pub(crate) async fn delete(
        &self,
        sha256: ArtifactHash,
    ) -> Result<(), Error> {
        let sha256 = sha256.to_string();
        let mut any_datasets = false;
        let mut last_error = None;
        for mountpoint in self.dataset_mountpoints().await? {
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
                    error!(
                        &self.log,
                        "Failed to remove file";
                        "error" => &err,
                        "path" => path.as_str(),
                    );
                    last_error =
                        Some(Error::File { verb: "remove", path, err });
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

    async fn dataset_mountpoints(
        &self,
    ) -> Result<impl Iterator<Item = Utf8PathBuf> + '_, Error> {
        let config = self.storage.datasets_config_list().await?;
        Ok(update_dataset_mountpoints(config, self.storage.mountpoint_root()))
    }
}

fn update_dataset_mountpoints(
    config: DatasetsConfig,
    root: &Utf8Path,
) -> impl Iterator<Item = Utf8PathBuf> + '_ {
    config
        .datasets
        .into_values()
        .filter(|dataset| *dataset.name.dataset() == DatasetKind::Update)
        .map(|dataset| dataset.name.mountpoint(root))
}

enum RepoDepotImpl {}

impl RepoDepotApi for RepoDepotImpl {
    type Context = ArtifactStore;

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
    #[error(transparent)]
    Body(dropshot::HttpError),

    #[error("Error retrieving dataset configuration: {0}")]
    DatasetConfig(#[source] sled_storage::error::Error),

    #[error("Error fetching artifact {sha256} from depot {address}: {err}")]
    DepotCopy {
        sha256: ArtifactHash,
        address: SocketAddrV6,
        #[source]
        err: repo_depot_client::ClientError,
    },

    #[error("Failed to {verb} `{path}`: {err}")]
    File {
        verb: &'static str,
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("Digest mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: ArtifactHash, actual: ArtifactHash },

    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    #[error("Artifact {sha256} not found")]
    NotFound { sha256: String },

    #[error("No update datasets present")]
    NoUpdateDataset,
}

impl From<Error> for HttpError {
    fn from(err: Error) -> HttpError {
        match err {
            Error::Body(inner) => inner,
            Error::DatasetConfig(_) | Error::NoUpdateDataset => {
                HttpError::for_unavail(None, err.to_string())
            }
            Error::DepotCopy { .. } | Error::File { .. } | Error::Join(_) => {
                HttpError::for_internal_error(err.to_string())
            }
            Error::HashMismatch { .. } => {
                HttpError::for_bad_request(None, err.to_string())
            }
            Error::NotFound { .. } => {
                HttpError::for_not_found(None, err.to_string())
            }
        }
    }
}

pub(crate) trait StorageBackend {
    async fn datasets_config_list(&self) -> Result<DatasetsConfig, Error>;
    fn mountpoint_root(&self) -> &Utf8Path;
}

impl StorageBackend for StorageHandle {
    async fn datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        self.datasets_config_list().await.map_err(Error::DatasetConfig)
    }

    fn mountpoint_root(&self) -> &Utf8Path {
        ZPOOL_MOUNTPOINT_ROOT.into()
    }
}

#[cfg(test)]
mod test {
    use camino::Utf8Path;
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
    use tokio::io::AsyncReadExt;

    use super::{ArtifactStore, Error, StorageBackend};

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

    impl StorageBackend for TestBackend {
        async fn datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
            Ok(self.datasets.clone())
        }

        fn mountpoint_root(&self) -> &Utf8Path {
            self.mountpoint_root.path()
        }
    }

    const TEST_ARTIFACT: &[u8] = b"I'm an artifact!\n";
    const TEST_HASH: ArtifactHash = ArtifactHash(hex!(
        "ab3581cd62f6645518f61a8e4391af6c062d5d60111edb0e51b37bd84827f5b4"
    ));

    #[tokio::test]
    async fn get_put_delete() {
        let log = test_setup_log("get_put_delete");
        let backend = TestBackend::new(1);
        let store = ArtifactStore::new(&log.log, backend);

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
                .put_impl(TEST_HASH, stream::once(async { Ok(TEST_ARTIFACT) }))
                .await
                .unwrap();
            // get succeeds, file reads back OK
            let mut file = store.get(TEST_HASH).await.unwrap();
            let mut vec = Vec::new();
            file.read_to_end(&mut vec).await.unwrap();
            assert_eq!(vec, TEST_ARTIFACT);
        }

        // delete succeeds and is idempotent
        for _ in 0..2 {
            store.delete(TEST_HASH).await.unwrap();
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
                .put_impl(TEST_HASH, stream::once(async { Ok(TEST_ARTIFACT) }))
                .await,
            Err(Error::NoUpdateDataset)
        ));
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            store.delete(TEST_HASH).await,
            Err(Error::NoUpdateDataset)
        ));

        log.cleanup_successful();
    }
}
