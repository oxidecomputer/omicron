// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages TUF artifacts stored on this sled. The implementation is a
//! content-addressed object store.
//!
//! See docs/tuf-artifact-replication.adoc for an architectural overview of the
//! TUF artifact replication system.
//!
//! GET operations are handled by the "Repo Depot" API, which is deliberately
//! a separate Dropshot service from the rest of Sled Agent. This is to avoid a
//! circular logical dependency, because we expect Sled Agent to fetch artifacts
//! it does not have from another Repo Depot that does have them (at Nexus's
//! direction). This API's implementation is also part of this module.
//!
//! Operations that list or modify artifacts or the configuration are called by
//! Nexus and handled by the Sled Agent API.

use std::future::Future;
use std::io::{ErrorKind, Write};
use std::net::SocketAddrV6;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use atomicwrites::{AtomicFile, OverwriteBehavior};
use bytes::Bytes;
use camino::Utf8PathBuf;
use dropshot::{
    Body, ConfigDropshot, FreeformBody, HttpError, HttpResponseOk, Path,
    RequestContext, ServerBuilder, StreamingBody,
};
use futures::{Stream, TryStreamExt};
use omicron_common::address::REPO_DEPOT_PORT;
use omicron_common::api::external::Generation;
use omicron_common::ledger::Ledger;
use repo_depot_api::*;
use sha2::{Digest, Sha256};
use sled_agent_api::{
    ArtifactConfig, ArtifactListResponse, ArtifactPutResponse,
};
use sled_agent_config_reconciler::ConfigReconcilerHandle;
use sled_agent_config_reconciler::InternalDisksReceiver;
use slog::{Logger, error, info};
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use tokio::fs::File;
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tufaceous_artifact::ArtifactHash;

// These paths are defined under the artifact storage dataset. They
// cannot conflict with any artifact paths because all artifact paths are
// hexadecimal-encoded SHA-256 checksums.
const LEDGER_PATH: &str = "artifact-config.json";
const TEMP_SUBDIR: &str = "tmp";

/// Content-addressable local storage for software artifacts.
///
/// If you need to read a file managed by the artifact store from somewhere else
/// in Sled Agent, use [`ArtifactStore::get`].
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
pub struct ArtifactStore<T: DatasetsManager> {
    log: Logger,
    reqwest_client: reqwest::Client,
    ledger_tx: mpsc::Sender<LedgerManagerRequest>,
    config: watch::Receiver<Option<ArtifactConfig>>,
    pub(crate) storage: T,
}

impl<T: DatasetsManager> ArtifactStore<T> {
    pub(crate) async fn new(
        log: &Logger,
        storage: T,
        config_reconciler: Option<Arc<ConfigReconcilerHandle>>,
    ) -> ArtifactStore<T> {
        let log = log.new(slog::o!("component" => "ArtifactStore"));

        let mut ledger_paths = Vec::new();
        for mountpoint in storage.artifact_storage_paths().await {
            ledger_paths.push(mountpoint.join(LEDGER_PATH));

            // Attempt to remove any in-progress artifacts stored in the
            // dataset's temporary directory, possibly left behind from a
            // crashed sled agent.
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
                        &log,
                        "Failed to remove stale temporary artifacts";
                        "error" => &err,
                        "path" => path.as_str(),
                    );
                }
            }
        }

        let config = Ledger::new(&log, ledger_paths.clone())
            .await
            .map(Ledger::into_inner);
        let (config_tx, config) = watch::channel(config);
        // Somewhat arbitrary bound size, large enough that we should never hit it.
        let (ledger_tx, ledger_rx) = mpsc::channel(256);
        tokio::task::spawn(ledger_manager(
            log.clone(),
            ledger_paths,
            config_reconciler,
            ledger_rx,
            config_tx,
        ));

        tokio::task::spawn(delete_reconciler(
            log.clone(),
            storage.clone(),
            config.clone(),
        ));

        ArtifactStore {
            log,
            reqwest_client: reqwest::ClientBuilder::new()
                .connect_timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
            ledger_tx,
            config,
            storage,
        }
    }
}

impl ArtifactStore<InternalDisksReceiver> {
    pub(crate) async fn start(
        self: Arc<Self>,
        sled_address: SocketAddrV6,
        dropshot_config: &ConfigDropshot,
    ) -> Result<
        dropshot::HttpServer<Arc<ArtifactStore<InternalDisksReceiver>>>,
        StartError,
    > {
        let mut depot_address = sled_address;
        depot_address.set_port(REPO_DEPOT_PORT);

        let log = self.log.new(o!("component" => "dropshot (Repo Depot)"));
        ServerBuilder::new(
            repo_depot_api_mod::api_description::<RepoDepotImpl>()
                .expect("registered entrypoints"),
            self,
            log,
        )
        .config(ConfigDropshot {
            bind_address: depot_address.into(),
            ..dropshot_config.clone()
        })
        .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
            dropshot::ClientSpecifiesVersionInHeader::new(
                omicron_common::api::VERSION_HEADER,
                repo_depot_api::latest_version(),
            ),
        )))
        .start()
        .map_err(StartError::Dropshot)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("Dropshot error while starting Repo Depot service")]
    Dropshot(#[source] dropshot::BuildError),
}

macro_rules! log_io_err {
    ($log:expr, $verb:literal, $path:expr, $err:expr) => {
        error!(
            $log,
            concat!("Failed to ", $verb, " path");
            "error" => &$err,
            "path" => $path.as_str(),
        )
    };
}

macro_rules! log_io_err_into {
    ($log:expr, $verb:literal, $path:expr, $err:expr) => {{
        log_io_err!($log, $verb, $path, $err);
        Error::File { verb: $verb, path: $path, err: $err }
    }};
}

macro_rules! log_and_store {
    ($last_error:expr, $log:expr, $verb:literal, $path:expr, $err:expr) => {
        $last_error = Some(log_io_err_into!($log, $verb, $path, $err))
    };
}

impl<T: DatasetsManager> ArtifactStore<T> {
    /// Get the current [`ArtifactConfig`].
    pub(crate) fn get_config(&self) -> Option<ArtifactConfig> {
        self.config.borrow().clone()
    }

    /// Set a new [`ArtifactConfig`].
    ///
    /// Rejects the configuration with an error if the configuration was
    /// modified without increasing the generation number.
    pub(crate) async fn put_config(
        &self,
        new_config: ArtifactConfig,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.ledger_tx
            .send((new_config, tx))
            .await
            .map_err(|_| Error::LedgerChannel)?;
        rx.await.map_err(|_| Error::LedgerChannel)?
    }

    /// Open an artifact file by hash.
    ///
    /// Also the GET operation (served by Repo Depot API).
    ///
    /// We try all datasets, returning early if we find the artifact, logging
    /// errors as we go. If we don't find it we return the most recent error we
    /// logged or a NotFound.
    pub(crate) async fn get(
        &self,
        sha256: ArtifactHash,
    ) -> Result<File, Error> {
        Self::get_from_storage(&self.storage, &self.log, sha256).await
    }

    /// Open an artifact file by hash from a storage handle.
    ///
    /// This is the same as [ArtifactStore::get], but can be called with only
    /// a storage implementation.
    pub(crate) async fn get_from_storage(
        storage: &T,
        log: &Logger,
        sha256: ArtifactHash,
    ) -> Result<File, Error> {
        let sha256_str = sha256.to_string();
        let mut last_error = None;
        for mountpoint in storage.artifact_storage_paths().await {
            let path = mountpoint.join(&sha256_str);
            match File::open(&path).await {
                Ok(file) => {
                    info!(
                        &log,
                        "Retrieved artifact";
                        "sha256" => &sha256_str,
                        "path" => path.as_str(),
                    );
                    return Ok(file);
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    log_and_store!(last_error, &log, "open", path, err);
                }
            }
        }
        Err(last_error.unwrap_or(Error::NotFound { sha256 }))
    }

    /// List operation (served by Sled Agent API)
    ///
    /// We try all datasets, logging errors as we go; if we're experiencing I/O
    /// errors, Nexus should still be aware of the artifacts we think we have.
    pub(crate) async fn list(&self) -> Result<ArtifactListResponse, Error> {
        let mut response = if let Some(config) = self.config.borrow().as_ref() {
            ArtifactListResponse {
                generation: config.generation,
                list: config.artifacts.iter().map(|hash| (*hash, 0)).collect(),
            }
        } else {
            return Err(Error::NoConfig);
        };
        let mut any_datasets = false;
        for mountpoint in self.storage.artifact_storage_paths().await {
            any_datasets = true;
            for (hash, count) in &mut response.list {
                let path = mountpoint.join(hash.to_string());
                match tokio::fs::try_exists(&path).await {
                    Ok(true) => *count += 1,
                    Ok(false) => {}
                    Err(err) => {
                        log_io_err!(&self.log, "check existence of", path, err)
                    }
                }
            }
        }
        if any_datasets {
            response.list.retain(|_, count| *count > 0);
            Ok(response)
        } else {
            Err(Error::NoUpdateDataset)
        }
    }

    /// Common implementation for all artifact write operations that creates
    /// a temporary file on all datasets. Returns an [`ArtifactWriter`] that
    /// can be used to write the artifact to all temporary files, then move all
    /// temporary files to their final paths.
    ///
    /// Most errors during the write process are considered non-fatal errors,
    /// which are logged instead of immediately returned.
    ///
    /// In this method, possible fatal errors are:
    /// - No temporary files could be created.
    /// - A temporary file already exists (another task is writing to this
    ///   artifact).
    async fn writer(
        &self,
        sha256: ArtifactHash,
        attempted_generation: Generation,
    ) -> Result<ArtifactWriter, Error> {
        if let Some(config) = self.config.borrow().as_ref() {
            if attempted_generation != config.generation {
                return Err(Error::GenerationPut {
                    attempted_generation,
                    current_generation: config.generation,
                });
            }
            if !config.artifacts.contains(&sha256) {
                return Err(Error::NotInConfig {
                    sha256,
                    generation: config.generation,
                });
            }
        } else {
            return Err(Error::NoConfig);
        }

        let mut writer = ArtifactWriter::new(self.log.clone(), sha256);
        let mut last_error = None;
        for mountpoint in self.storage.artifact_storage_paths().await {
            let temp_dir = mountpoint.join(TEMP_SUBDIR);
            if let Err(err) = tokio::fs::create_dir(&temp_dir).await {
                if err.kind() != ErrorKind::AlreadyExists {
                    log_and_store!(
                        last_error, &self.log, "create", temp_dir, err
                    );
                    continue;
                }
            }
            writer.add_path(mountpoint, temp_dir);
        }
        if writer.write_tasks.is_empty() {
            Err(last_error.unwrap_or(Error::NoUpdateDataset))
        } else {
            Ok(writer)
        }
    }

    /// PUT operation (served by Sled Agent API) which takes a [`StreamingBody`]
    pub(crate) async fn put_body(
        &self,
        sha256: ArtifactHash,
        generation: Generation,
        body: StreamingBody,
    ) -> Result<ArtifactPutResponse, Error> {
        self.writer(sha256, generation)
            .await?
            .write_stream(Box::pin(body.into_stream().map_err(Error::Body)))
            .await
    }

    /// POST operation (served by Sled Agent API)
    pub(crate) async fn copy_from_depot(
        &self,
        sha256: ArtifactHash,
        generation: Generation,
        depot_base_url: &str,
    ) -> Result<(), Error> {
        // Check that there's no conflict before we send the upstream request.
        let writer = self.writer(sha256, generation).await?;
        let permit = self.storage.copy_permit().await;

        let client = repo_depot_client::Client::new_with_client(
            depot_base_url,
            self.reqwest_client.clone(),
            self.log.new(slog::o!(
                "component" => "Repo Depot client (ArtifactStore)",
                "base_url" => depot_base_url.to_owned(),
            )),
        );
        let response = client
            .artifact_get_by_sha256(&sha256.to_string())
            .await
            .map_err(|err| Error::DepotCopy {
                sha256,
                base_url: depot_base_url.to_owned(),
                err,
            })?;
        // Copy from the stream on its own task and immediately return.
        let log = self.log.clone();
        let base_url = depot_base_url.to_owned();
        tokio::task::spawn(async move {
            let _permit = permit;
            let stream = response.into_inner().into_inner().map_err(|err| {
                Error::DepotCopy {
                    sha256,
                    base_url: base_url.clone(),
                    err: repo_depot_client::ClientError::ResponseBodyError(err),
                }
            });
            if let Err(err) = writer.write_stream(stream).await {
                error!(
                    &log,
                    "Failed to write artifact";
                    "err" => &err,
                );
            }
        });
        Ok(())
    }
}

type LedgerManagerRequest =
    (ArtifactConfig, oneshot::Sender<Result<(), Error>>);

/// Receives requests via an [`mpsc`] channel, responding via the [`oneshot`]
/// channel sent by the requester. Updates the configuration in a [`watch`]
/// channel so that the artifact store can use it and the delete reconciler can
/// be notified of changes.
async fn ledger_manager(
    log: Logger,
    ledger_paths: Vec<Utf8PathBuf>,
    config_reconciler: Option<Arc<ConfigReconcilerHandle>>,
    mut rx: mpsc::Receiver<LedgerManagerRequest>,
    config_channel: watch::Sender<Option<ArtifactConfig>>,
) {
    let config_reconciler = config_reconciler.as_ref();
    let handle_request = async |new_config: ArtifactConfig| {
        if ledger_paths.is_empty() {
            return Err(Error::NoUpdateDataset);
        }
        let mut ledger = if let Some(mut ledger) =
            Ledger::<ArtifactConfig>::new(&log, ledger_paths.clone()).await
        {
            if new_config.generation > ledger.data().generation {
                // New config generation. First check that the configuration
                // is valid against the current ledgered sled config.
                if let Some(config_reconciler) = config_reconciler {
                    config_reconciler
                        .validate_artifact_config(new_config.clone())
                        .await??;
                }

                // Everything looks okay; update the ledger.
                *ledger.data_mut() = new_config;
                ledger
            } else if new_config == *ledger.data() {
                // Be idempotent and do nothing.
                return Ok(());
            } else {
                // Either we were asked to use an older generation, or the same
                // generation with a different config.
                return Err(Error::GenerationConfig {
                    attempted_generation: new_config.generation,
                    current_generation: ledger.data().generation,
                });
            }
        } else {
            Ledger::new_with(&log, ledger_paths.clone(), new_config)
        };
        ledger.commit().await?;
        // If we successfully wrote to the ledger, update the watch channel.
        config_channel.send_replace(Some(ledger.data().clone()));
        Ok(())
    };

    while let Some((new_config, tx)) = rx.recv().await {
        tx.send(handle_request(new_config).await).ok();
    }
    warn!(log, "All ledger manager request senders dropped");
}

async fn delete_reconciler<T: DatasetsManager>(
    log: Logger,
    storage: T,
    mut receiver: watch::Receiver<Option<ArtifactConfig>>,
) {
    while let Ok(()) = receiver.changed().await {
        let generation = match receiver.borrow_and_update().as_ref() {
            Some(config) => config.generation,
            None => continue,
        };
        info!(
            &log,
            "Starting delete reconciler";
            "generation" => &generation,
        );
        for mountpoint in storage.artifact_storage_paths().await {
            let mut read_dir = match tokio::fs::read_dir(&mountpoint).await {
                Ok(read_dir) => read_dir,
                Err(err) => {
                    error!(
                        log,
                        "Failed to read dir";
                        "error" => &err,
                        "path" => mountpoint.as_str(),
                    );
                    continue;
                }
            };
            while let Some(result) = read_dir.next_entry().await.transpose() {
                let entry = match result {
                    Ok(entry) => entry,
                    Err(err) => {
                        error!(
                            log,
                            "Failed to read dir";
                            "error" => &err,
                            "path" => mountpoint.as_str(),
                        );
                        // It's not clear whether we should expect future calls
                        // to `next_entry` to work after the first error; we
                        // take the conservative approach and stop iterating.
                        break;
                    }
                };
                let Ok(file_name) = entry.file_name().into_string() else {
                    // Content-addressed paths are ASCII-only, so this is
                    // clearly not a hash.
                    continue;
                };
                let Ok(hash) = ArtifactHash::from_str(&file_name) else {
                    continue;
                };
                if let Some(config) = receiver.borrow().as_ref() {
                    if config.artifacts.contains(&hash) {
                        continue;
                    }
                } else {
                    continue;
                }
                let sha256 = hash.to_string();
                let path = mountpoint.join(&sha256);
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {
                        info!(
                            &log,
                            "Removed artifact";
                            "sha256" => &sha256,
                            "path" => path.as_str(),
                        );
                    }
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => {
                        log_io_err!(&log, "remove", path, err);
                    }
                }
            }
        }
        storage.signal_delete_done(generation);
    }
    warn!(log, "Delete reconciler sender dropped");
}

/// Abstracts over what kind of sled agent we are; each of the real sled agent,
/// simulated sled agent, and this module's unit tests have different ways of
/// keeping track of the datasets on the system.
pub trait DatasetsManager: Clone + Send + Sync + 'static {
    fn artifact_storage_paths(
        &self,
    ) -> impl Future<Output = impl Iterator<Item = Utf8PathBuf> + Send + '_> + Send;

    #[expect(async_fn_in_trait)]
    async fn copy_permit(&self) -> Option<OwnedSemaphorePermit> {
        None
    }

    fn signal_delete_done(&self, _generation: Generation) {}
}

impl DatasetsManager for InternalDisksReceiver {
    async fn artifact_storage_paths(
        &self,
    ) -> impl Iterator<Item = Utf8PathBuf> + '_ {
        self.current().all_artifact_datasets().collect::<Vec<_>>().into_iter()
    }
}

/// Abstraction that handles writing to several temporary files.
#[derive(Debug)]
struct ArtifactWriter {
    senders: Vec<mpsc::Sender<Bytes>>,
    write_tasks: JoinSet<Result<(), Error>>,
    log: Logger,
    sha256: ArtifactHash,
}

impl ArtifactWriter {
    fn new(log: Logger, sha256: ArtifactHash) -> ArtifactWriter {
        ArtifactWriter {
            senders: Vec::new(),
            write_tasks: JoinSet::new(),
            log,
            sha256,
        }
    }

    fn add_path(&mut self, mountpoint: Utf8PathBuf, temp_dir: Utf8PathBuf) {
        let log = self.log.clone();
        let path = mountpoint.join(self.sha256.to_string());
        let atomic_file = AtomicFile::new_with_tmpdir(
            &path,
            OverwriteBehavior::AllowOverwrite,
            temp_dir,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let expected = self.sha256;
        self.senders.push(tx);
        self.write_tasks.spawn_blocking(move || {
            let moved_path = path.clone();
            atomic_file
                .write(|file| {
                    let mut hasher = Sha256::new();
                    while let Some(bytes) = rx.blocking_recv() {
                        hasher.update(&bytes);
                        if let Err(err) = file.write_all(&bytes) {
                            return Err(log_io_err_into!(
                                log,
                                "write to temporary file for",
                                moved_path,
                                err
                            ));
                        }
                    }
                    let actual = ArtifactHash(hasher.finalize().into());
                    if expected == actual {
                        Ok(())
                    } else {
                        Err(Error::HashMismatch { expected, actual })
                    }
                })
                .map_err(|err| match err {
                    atomicwrites::Error::Internal(err) => {
                        log_io_err_into!(
                            log,
                            "create or persist temporary file for",
                            path,
                            err
                        )
                    }
                    atomicwrites::Error::User(err) => err,
                })
        });
    }

    async fn write_stream(
        mut self,
        mut stream: impl Stream<Item = Result<Bytes, Error>> + Unpin,
    ) -> Result<ArtifactPutResponse, Error> {
        let mut swap = Vec::with_capacity(self.senders.len());
        while let Some(chunk) = stream.try_next().await? {
            // Send the chunk to all the write tasks, pruning any that failed
            // because the other end hung up.
            let mut join_set = self
                .senders
                .drain(..)
                .zip(std::iter::repeat(chunk))
                .map(async |(sender, chunk)| {
                    sender.send(chunk).await.ok().map(|()| sender)
                })
                .collect::<JoinSet<_>>();
            while let Some(maybe_sender) = join_set.join_next().await {
                if let Some(sender) = maybe_sender.map_err(Error::Join)? {
                    swap.push(sender);
                }
            }
            if swap.is_empty() {
                // All tasks have hung up!
                warn!(
                    &self.log,
                    "All writer tasks have hung up";
                    "sha256" => &self.sha256.to_string()
                );
                break;
            }
            std::mem::swap(&mut self.senders, &mut swap);
        }
        // Drop the senders so that tasks stop waiting for new chunks.
        drop(self.senders);
        // `swap` should be empty, but drop it early just in case.
        debug_assert!(swap.is_empty());
        drop(swap);

        let datasets = self.write_tasks.len();
        let mut successful_writes = 0;
        let mut last_error = None;
        while let Some(result) = self.write_tasks.join_next().await {
            match result
                .map_err(|err| {
                    error!(
                        &self.log,
                        "Failed to join artifact write task";
                        "error" => InlineErrorChain::new(&err),
                        "sha256" => &self.sha256.to_string(),
                    );
                    Error::Join(err)
                })
                .and_then(|r| r)
            {
                Ok(()) => successful_writes += 1,
                Err(err) => last_error = Some(err),
            }
        }
        if successful_writes > 0 {
            info!(
                &self.log,
                "Wrote artifact";
                "sha256" => &self.sha256.to_string(),
                "datasets" => datasets,
                "successful_writes" => successful_writes,
            );
            Ok(ArtifactPutResponse { datasets, successful_writes })
        } else {
            Err(last_error.unwrap_or(Error::NoUpdateDataset))
        }
    }
}

/// Implementation of the Repo Depot API backed by an
/// `ArtifactStore<InternalDisksReceiver>`.
enum RepoDepotImpl {}

impl RepoDepotApi for RepoDepotImpl {
    type Context = Arc<ArtifactStore<InternalDisksReceiver>>;

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

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum Error {
    #[error("Error while reading request body")]
    Body(dropshot::HttpError),

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

    #[error(
        "Attempt to modify config to generation {attempted_generation} \
        while at {current_generation}"
    )]
    GenerationConfig {
        attempted_generation: Generation,
        current_generation: Generation,
    },

    #[error(
        "Attempt to put object with generation {attempted_generation} \
        while at {current_generation}"
    )]
    GenerationPut {
        attempted_generation: Generation,
        current_generation: Generation,
    },

    #[error("Digest mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: ArtifactHash, actual: ArtifactHash },

    #[error("New config is invalid per current sled config")]
    InvalidPerSledConfig(
        #[from] sled_agent_config_reconciler::LedgerArtifactConfigError,
    ),

    #[error("Cannot validate incoming config against sled config")]
    CannotValidateAgainstSledConfig(
        #[from] sled_agent_config_reconciler::LedgerTaskError,
    ),

    #[error("Blocking task failed")]
    Join(#[source] tokio::task::JoinError),

    #[error("Failed to commit ledger")]
    LedgerCommit(#[from] omicron_common::ledger::Error),

    #[error("Ledger manager task dropped its end of the channel")]
    LedgerChannel,

    #[error("No artifact configuration present")]
    NoConfig,

    #[error("No update datasets present")]
    NoUpdateDataset,

    #[error("Artifact {sha256} not found")]
    NotFound { sha256: ArtifactHash },

    #[error(
        "Attempt to put artifact {sha256} not in config generation {generation}"
    )]
    NotInConfig { sha256: ArtifactHash, generation: Generation },
}

impl From<Error> for HttpError {
    fn from(err: Error) -> HttpError {
        match err {
            // 4xx errors
            Error::HashMismatch { .. }
            | Error::InvalidPerSledConfig { .. }
            | Error::NoConfig
            | Error::NotInConfig { .. } => {
                HttpError::for_bad_request(None, err.to_string())
            }
            Error::NotFound { .. } => {
                HttpError::for_not_found(None, err.to_string())
            }
            Error::GenerationConfig { .. } => HttpError::for_client_error(
                Some("CONFIG_GENERATION".to_string()),
                dropshot::ClientErrorStatusCode::CONFLICT,
                err.to_string(),
            ),
            Error::GenerationPut { .. } => HttpError::for_client_error(
                None,
                dropshot::ClientErrorStatusCode::CONFLICT,
                err.to_string(),
            ),

            // 5xx errors: ensure the error chain is logged
            Error::Body(inner) => inner,
            Error::NoUpdateDataset => HttpError::for_unavail(
                None,
                InlineErrorChain::new(&err).to_string(),
            ),
            Error::DepotCopy { .. }
            | Error::File { .. }
            | Error::Join(_)
            | Error::LedgerCommit(_)
            | Error::LedgerChannel
            | Error::CannotValidateAgainstSledConfig(_) => {
                HttpError::for_internal_error(
                    InlineErrorChain::new(&err).to_string(),
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use bytes::Bytes;
    use camino::Utf8PathBuf;
    use camino_tempfile::Utf8TempDir;
    use futures::stream::{self, StreamExt};
    use hex_literal::hex;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use sled_agent_api::ArtifactConfig;
    use tokio::io::AsyncReadExt;
    use tokio::sync::oneshot;
    use tokio::sync::watch;
    use tufaceous_artifact::ArtifactHash;

    use super::{ArtifactStore, DatasetsManager, Error};

    #[derive(Clone)]
    struct TestBackend {
        delete_done_tx: watch::Sender<Generation>,
        delete_done_rx: watch::Receiver<Generation>,
        datasets: Vec<Utf8PathBuf>,
        _tempdir: Arc<Utf8TempDir>,
    }

    impl TestBackend {
        fn new(len: usize) -> TestBackend {
            let tempdir = Arc::new(camino_tempfile::tempdir().unwrap());

            let mut datasets = Vec::new();
            for i in 0..len {
                let dataset =
                    tempdir.path().join(format!("test_backed_dataset_{i}"));
                std::fs::create_dir_all(&dataset).unwrap();
                datasets.push(dataset)
            }

            let (delete_done_tx, delete_done_rx) = watch::channel(0u32.into());
            TestBackend {
                delete_done_tx,
                delete_done_rx,
                datasets,
                _tempdir: tempdir,
            }
        }
    }

    impl DatasetsManager for TestBackend {
        async fn artifact_storage_paths(
            &self,
        ) -> impl Iterator<Item = camino::Utf8PathBuf> + '_ {
            self.datasets.iter().cloned()
        }

        fn signal_delete_done(&self, generation: Generation) {
            self.delete_done_tx.send_if_modified(|old| {
                let modified = *old != generation;
                *old = generation;
                modified
            });
        }
    }

    const TEST_ARTIFACT: Bytes = Bytes::from_static(b"I'm an artifact!\n");
    const TEST_HASH: ArtifactHash = ArtifactHash(hex!(
        "ab3581cd62f6645518f61a8e4391af6c062d5d60111edb0e51b37bd84827f5b4"
    ));

    #[tokio::test]
    async fn generations() {
        macro_rules! assert_generation_err {
            ($f:expr, $attempted:expr, $current:expr) => {{
                let err = $f.await.unwrap_err();
                match err {
                    Error::GenerationConfig {
                        attempted_generation,
                        current_generation,
                    } => {
                        assert_eq!(
                            attempted_generation, $attempted,
                            "attempted generation does not match"
                        );
                        assert_eq!(
                            current_generation, $current,
                            "current generation does not match"
                        );
                    }
                    err => panic!("wrong error: {err:?}"),
                }
            }};
        }

        let log = test_setup_log("generations");
        let backend = TestBackend::new(2);
        let store = ArtifactStore::new(&log.log, backend, None).await;

        // get_config returns None
        assert!(store.get_config().is_none());
        // put our first config
        let mut config = ArtifactConfig {
            generation: 1u32.into(),
            artifacts: BTreeSet::new(),
        };
        store.put_config(config.clone()).await.unwrap();
        assert_eq!(store.get_config().unwrap(), config);

        // putting an unmodified config from the same generation succeeds (puts
        // are idempotent)
        store.put_config(config.clone()).await.unwrap();
        assert_eq!(store.get_config().unwrap(), config);
        // putting an unmodified config from an older generation fails
        config.generation = 0u32.into();
        assert_generation_err!(
            store.put_config(config.clone()),
            0u32.into(),
            1u32.into()
        );
        // putting an unmodified config from a newer generation succeeds
        config.generation = 2u32.into();
        store.put_config(config.clone()).await.unwrap();

        // putting a modified config from the same generation fails
        config = store.get_config().unwrap();
        config.artifacts.insert(TEST_HASH);
        assert_generation_err!(
            store.put_config(config.clone()),
            2u32.into(),
            2u32.into()
        );
        // putting a modified config from an older generation fails
        config.generation = 0u32.into();
        assert_generation_err!(
            store.put_config(config.clone()),
            0u32.into(),
            2u32.into()
        );
        // putting a modified config from a newer generation succeeds
        config.generation = store.get_config().unwrap().generation.next();
        store.put_config(config.clone()).await.unwrap();

        log.cleanup_successful();
    }

    #[tokio::test]
    async fn list_get_put() {
        let log = test_setup_log("list_get_put");
        let backend = TestBackend::new(2);
        let mut store = ArtifactStore::new(&log.log, backend, None).await;

        // get fails, because it doesn't exist yet
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));
        // list/put fail, no config
        assert!(matches!(store.list().await.unwrap_err(), Error::NoConfig));
        assert!(matches!(
            store.writer(TEST_HASH, 1u32.into()).await.unwrap_err(),
            Error::NoConfig
        ));

        // put our first config
        let mut config = ArtifactConfig {
            generation: 1u32.into(),
            artifacts: BTreeSet::new(),
        };
        config.artifacts.insert(TEST_HASH);
        store.put_config(config.clone()).await.unwrap();

        // list succeeds with an empty result
        let response = store.list().await.unwrap();
        assert_eq!(response.generation, 1u32.into());
        assert!(response.list.is_empty());
        // get fails, because it doesn't exist yet
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));

        // put with the wrong generation fails
        for generation in [0u32, 2] {
            let err =
                store.writer(TEST_HASH, generation.into()).await.unwrap_err();
            match err {
                Error::GenerationPut {
                    attempted_generation,
                    current_generation,
                } => {
                    assert_eq!(attempted_generation, generation.into());
                    assert_eq!(current_generation, 1u32.into());
                }
                err => panic!("wrong error: {err}"),
            }
        }

        // test several things here:
        // 1. put succeeds
        // 2. put is idempotent (we don't care if it clobbers a file as long as
        //    the hash is okay)
        // 3. we don't fail trying to create TEMP_SUBDIR twice
        for _ in 0..2 {
            store
                .writer(TEST_HASH, 1u32.into())
                .await
                .unwrap()
                .write_stream(Box::pin(stream::once(async {
                    Ok(TEST_ARTIFACT)
                })))
                .await
                .unwrap();
            // list lists the file
            assert!(
                store
                    .list()
                    .await
                    .unwrap()
                    .list
                    .into_iter()
                    .eq([(TEST_HASH, 2)])
            );
            // get succeeds, file reads back OK
            let mut file = store.get(TEST_HASH).await.unwrap();
            let mut vec = Vec::new();
            file.read_to_end(&mut vec).await.unwrap();
            assert_eq!(vec, TEST_ARTIFACT);
        }

        // all datasets should have the artifact
        for mountpoint in store.storage.artifact_storage_paths().await {
            assert_eq!(
                tokio::fs::read(mountpoint.join(TEST_HASH.to_string()))
                    .await
                    .unwrap(),
                TEST_ARTIFACT
            );
        }

        // clear `delete_done` so we can synchronize with the delete reconciler
        store.storage.delete_done_rx.mark_unchanged();
        // put a new config that says we don't want the artifact anymore.
        config.generation = config.generation.next();
        config.artifacts.remove(&TEST_HASH);
        store.put_config(config.clone()).await.unwrap();
        // list succeeds with an empty result, regardless of whether deletion
        // has actually occurred yet
        assert!(store.list().await.unwrap().list.is_empty());
        // wait for deletion to actually complete
        store.storage.delete_done_rx.changed().await.unwrap();
        // get fails, because it has been deleted
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));
        // all datasets should no longer have the artifact
        for mountpoint in store.storage.artifact_storage_paths().await {
            assert!(!mountpoint.join(TEST_HASH.to_string()).exists());
        }

        log.cleanup_successful();
    }

    #[tokio::test]
    async fn no_dataset() {
        // If there are no update datasets:
        // - all gets should fail with `Error::NotFound`
        // - putting a config should fail with `Error::NoUpdateDataset`

        let log = test_setup_log("no_dataset");
        let backend = TestBackend::new(0);
        let store = ArtifactStore::new(&log.log, backend, None).await;

        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));

        let mut config = ArtifactConfig {
            generation: 1u32.into(),
            artifacts: BTreeSet::new(),
        };
        config.artifacts.insert(TEST_HASH);
        assert!(matches!(
            store.put_config(config).await,
            Err(Error::NoUpdateDataset)
        ));

        log.cleanup_successful();
    }

    #[tokio::test]
    async fn wrong_hash() {
        const ACTUAL: ArtifactHash = ArtifactHash(hex!(
            "4d27a9d1ddb65e0f2350a400cf73157e42ae2ca687a4220aa0a73b9bb2d211f7"
        ));

        let log = test_setup_log("wrong_hash");
        let backend = TestBackend::new(2);
        let store = ArtifactStore::new(&log.log, backend, None).await;
        let mut config = ArtifactConfig {
            generation: 1u32.into(),
            artifacts: BTreeSet::new(),
        };
        config.artifacts.insert(TEST_HASH);
        store.put_config(config).await.unwrap();
        let err = store
            .writer(TEST_HASH, 1u32.into())
            .await
            .unwrap()
            .write_stream(Box::pin(stream::once(async {
                Ok(Bytes::from_static(b"This isn't right at all."))
            })))
            .await
            .unwrap_err();
        match err {
            Error::HashMismatch { expected, actual } => {
                assert_eq!(expected, TEST_HASH);
                assert_eq!(actual, ACTUAL);
            }
            err => panic!("wrong error: {err}"),
        }
        assert!(matches!(
            store.get(TEST_HASH).await,
            Err(Error::NotFound { .. })
        ));

        log.cleanup_successful();
    }

    #[tokio::test]
    async fn issue_7796() {
        // Tests a specific multi-writer issue described in omicron#7796.
        // Previously, creating a writer would create temporary files at
        // `tmp/{sha256}`, returning an error if there was already a writer in
        // progress. However a `tempfile::TempPath` was created before this and
        // dropped when an error was returned, deleting the temporary file in
        // use by the previous writer.
        //
        // One manifestation of the issue is:
        // 1. Writer A is created and starts writing.
        // 2. Writer B fails to create, returning `AlreadyInProgress`. This
        //    triggers a logic error where a `TempPath` is dropped, unlinking
        //    one of writer A's temporary files.
        // 3. Writer C fails to create, returning `AlreadyInProgress`. Similarly
        //    to writer B, this unlinks the other of writer A's temporary files.
        // 4. Writer D is created successfully because writer A's files are no
        //    longer present.
        // 5. Writer A finishes and incorrectly persists writer C's incomplete
        //    files.
        // 6. Writer D finishes and fails because its files have already been
        //    moved.
        //
        // We no longer use a temporary file with a known name or fail if
        // another writer is already in progress, but it's good to have
        // regression tests.

        let log = test_setup_log("issue_7796");
        let backend = TestBackend::new(2);
        let store = ArtifactStore::new(&log.log, backend, None).await;

        let mut config = ArtifactConfig {
            generation: 1u32.into(),
            artifacts: BTreeSet::new(),
        };
        config.artifacts.insert(TEST_HASH);
        store.put_config(config.clone()).await.unwrap();

        // Start the first writer.
        let first_writer = store.writer(TEST_HASH, 1u32.into()).await.unwrap();
        // Start two additional writers and immediately drop them. Currently
        // both are successful. In the omicron#7796 implementation both fail
        // with `AlreadyInProgress`. Two writers are necessary to delete both of
        // the temporary files from the first writer.
        for _ in 0..2 {
            let _ = store.writer(TEST_HASH, 1u32.into()).await;
        }
        // Start a fourth writer and partially write to it.
        let fourth_writer = store.writer(TEST_HASH, 1u32.into()).await.unwrap();
        let (tx, rx) = oneshot::channel();
        let stream = stream::once(async { Ok(Bytes::from_static(b"I'm an ")) })
            .chain(stream::once(async {
                rx.await.unwrap();
                Ok(Bytes::from_static(b"artifact!\n"))
            }));
        let handle =
            tokio::task::spawn(fourth_writer.write_stream(Box::pin(stream)));
        // Write to the first writer.
        first_writer
            .write_stream(Box::pin(stream::once(async { Ok(TEST_ARTIFACT) })))
            .await
            .unwrap();
        // The artifacts should be complete.
        for mountpoint in store.storage.artifact_storage_paths().await {
            assert_eq!(
                tokio::fs::read(mountpoint.join(TEST_HASH.to_string()))
                    .await
                    .unwrap(),
                TEST_ARTIFACT
            );
        }
        // Allow the fourth writer to finish. It should not have failed.
        tx.send(()).unwrap();
        handle.await.unwrap().unwrap();
        // The artifacts should still be complete.
        for mountpoint in store.storage.artifact_storage_paths().await {
            assert_eq!(
                tokio::fs::read(mountpoint.join(TEST_HASH.to_string()))
                    .await
                    .unwrap(),
                TEST_ARTIFACT
            );
        }

        log.cleanup_successful();
    }
}
