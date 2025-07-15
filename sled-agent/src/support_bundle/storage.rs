// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of and access to Support Bundles

use async_trait::async_trait;
use bytes::Bytes;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use dropshot::Body;
use dropshot::HttpError;
use futures::Stream;
use futures::StreamExt;
use illumos_utils::zfs::DatasetProperties;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use range_requests::PotentialRange;
use range_requests::SingleRange;
use sha2::Digest;
use sled_agent_api::*;
use sled_agent_config_reconciler::ConfigReconcilerHandle;
use sled_agent_config_reconciler::DatasetTaskError;
use sled_agent_config_reconciler::InventoryError;
use sled_agent_config_reconciler::NestedDatasetDestroyError;
use sled_agent_config_reconciler::NestedDatasetEnsureError;
use sled_agent_config_reconciler::NestedDatasetListError;
use sled_agent_config_reconciler::NestedDatasetMountError;
use sled_agent_types::support_bundle::BUNDLE_FILE_NAME;
use sled_agent_types::support_bundle::BUNDLE_TMP_FILE_NAME;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::io::Write;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tufaceous_artifact::ArtifactHash;
use zip::result::ZipError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    HttpError(#[from] HttpError),

    #[error("Hash mismatch accessing bundle")]
    HashMismatch,

    #[error("Not a file")]
    NotAFile,

    #[error("Dataset not found")]
    DatasetNotFound,

    #[error("Could not look up dataset")]
    DatasetLookup(#[source] anyhow::Error),

    #[error("Cannot access dataset {dataset:?} which is not mounted")]
    DatasetNotMounted { dataset: DatasetName },

    #[error(
        "Dataset exists, but has an invalid configuration: (wanted {wanted}, saw {actual})"
    )]
    DatasetExistsBadConfig { wanted: DatasetUuid, actual: DatasetUuid },

    #[error(
        "Dataset exists, but appears on the wrong zpool (wanted {wanted}, saw {actual})"
    )]
    DatasetExistsOnWrongZpool { wanted: ZpoolUuid, actual: ZpoolUuid },

    #[error("Bundle not found")]
    BundleNotFound,

    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    #[error(transparent)]
    Storage(#[from] sled_storage::error::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Range(#[from] range_requests::Error),

    #[error(transparent)]
    Zip(#[from] ZipError),

    #[error(transparent)]
    DatasetTask(#[from] DatasetTaskError),

    #[error("Could not access ledgered sled config")]
    LedgeredSledConfig(#[source] InventoryError),

    #[error(transparent)]
    NestedDatasetMountError(#[from] NestedDatasetMountError),

    #[error(transparent)]
    NestedDatasetEnsureError(#[from] NestedDatasetEnsureError),

    #[error(transparent)]
    NestedDatasetDestroyError(#[from] NestedDatasetDestroyError),

    #[error(transparent)]
    NestedDatasetListError(#[from] NestedDatasetListError),
}

fn err_str(err: &dyn std::error::Error) -> String {
    InlineErrorChain::new(err).to_string()
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::HttpError(err) => err,
            Error::HashMismatch => {
                HttpError::for_bad_request(None, "Hash mismatch".to_string())
            }
            Error::DatasetNotFound => {
                HttpError::for_not_found(None, "Dataset not found".to_string())
            }
            Error::NotAFile => {
                HttpError::for_bad_request(None, "Not a file".to_string())
            }
            Error::Storage(err) => HttpError::from(ExternalError::from(err)),
            Error::Zip(err) => match err {
                ZipError::FileNotFound => HttpError::for_not_found(
                    None,
                    "Entry not found".to_string(),
                ),
                err => HttpError::for_internal_error(err_str(&err)),
            },
            err => HttpError::for_internal_error(err_str(&err)),
        }
    }
}

/// Abstracts the storage APIs for accessing datasets.
///
/// Allows support bundle storage to work on both simulated and non-simulated
/// sled agents.
#[async_trait]
pub trait LocalStorage: Sync {
    // These methods are all prefixed as "dyn_" to avoid duplicating the name
    // with the real implementations.
    //
    // Dispatch is a little silly; if I use the same name as the real
    // implementation, then a "missing function" dispatches to the trait instead
    // and results in infinite recursion.

    /// Returns all configured datasets
    async fn dyn_datasets_config_list(&self) -> Result<DatasetsConfig, Error>;

    /// Returns properties about a dataset
    async fn dyn_dataset_get(
        &self,
        dataset_name: &String,
    ) -> Result<DatasetProperties, Error>;

    /// Ensure a dataset is mounted
    async fn dyn_ensure_mounted_and_get_mountpoint(
        &self,
        dataset: NestedDatasetLocation,
    ) -> Result<Utf8PathBuf, Error>;

    /// Returns all nested datasets within an existing dataset
    async fn dyn_nested_dataset_list(
        &self,
        name: DatasetName,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error>;

    /// Ensures a nested dataset exists
    async fn dyn_nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<(), Error>;

    /// Destroys a nested dataset
    async fn dyn_nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<(), Error>;
}

/// This implementation is effectively a pass-through to the real methods
#[async_trait]
impl LocalStorage for ConfigReconcilerHandle {
    async fn dyn_datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        // TODO-cleanup This is super gross; add a better API (maybe fetch a
        // single dataset by ID, since that's what our caller wants?)
        let sled_config =
            self.ledgered_sled_config().map_err(Error::LedgeredSledConfig)?;
        let sled_config = match sled_config {
            Some(config) => config,
            None => return Ok(DatasetsConfig::default()),
        };
        Ok(DatasetsConfig {
            generation: sled_config.generation,
            datasets: sled_config
                .datasets
                .into_iter()
                .map(|d| (d.id, d))
                .collect(),
        })
    }

    async fn dyn_dataset_get(
        &self,
        dataset_name: &String,
    ) -> Result<DatasetProperties, Error> {
        let Some(dataset) = illumos_utils::zfs::Zfs::get_dataset_properties(
            &[dataset_name.clone()],
            illumos_utils::zfs::WhichDatasets::SelfOnly,
        )
        .await
        .map_err(|err| Error::DatasetLookup(err))?
        .pop() else {
            // This should not be possible, unless the "zfs get" command is
            // behaving unpredictably. We're only asking for a single dataset,
            // so on success, we should see the result of that dataset.
            return Err(Error::DatasetLookup(anyhow::anyhow!(
                "Zfs::get_dataset_properties returned an empty vec?"
            )));
        };

        Ok(dataset)
    }

    async fn dyn_ensure_mounted_and_get_mountpoint(
        &self,
        dataset: NestedDatasetLocation,
    ) -> Result<Utf8PathBuf, Error> {
        self.nested_dataset_ensure_mounted(dataset)
            .await
            .map_err(Error::from)?
            .map_err(Error::from)
    }

    async fn dyn_nested_dataset_list(
        &self,
        name: DatasetName,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error> {
        self.nested_dataset_list(name, options)
            .await
            .map_err(Error::from)?
            .map_err(Error::from)
    }

    async fn dyn_nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<(), Error> {
        self.nested_dataset_ensure(config)
            .await
            .map_err(Error::from)?
            .map_err(Error::from)
    }

    async fn dyn_nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<(), Error> {
        self.nested_dataset_destroy(name)
            .await
            .map_err(Error::from)?
            .map_err(Error::from)
    }
}

/// This implementation allows storage bundles to be stored on simulated storage
#[async_trait]
impl LocalStorage for crate::sim::Storage {
    async fn dyn_datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        self.lock().datasets_config_list().map_err(|err| err.into())
    }

    async fn dyn_dataset_get(
        &self,
        dataset_name: &String,
    ) -> Result<DatasetProperties, Error> {
        self.lock().dataset_get(dataset_name).map_err(|err| err.into())
    }

    async fn dyn_ensure_mounted_and_get_mountpoint(
        &self,
        dataset: NestedDatasetLocation,
    ) -> Result<Utf8PathBuf, Error> {
        let slf = self.lock();
        // Simulated storage treats all datasets as mounted.
        Ok(dataset.mountpoint(slf.root()))
    }

    async fn dyn_nested_dataset_list(
        &self,
        name: DatasetName,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error> {
        self.lock()
            .nested_dataset_list(
                NestedDatasetLocation { path: String::new(), root: name },
                options,
            )
            .map_err(|err| err.into())
    }

    async fn dyn_nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<(), Error> {
        self.lock().nested_dataset_ensure(config).map_err(|err| err.into())
    }

    async fn dyn_nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<(), Error> {
        self.lock().nested_dataset_destroy(name).map_err(|err| err.into())
    }
}

/// Describes the type of access to the support bundle
#[derive(Clone, Debug)]
pub(crate) enum SupportBundleQueryType {
    /// Access the whole support bundle
    Whole,
    /// Access the names of all files within the support bundle
    Index,
    /// Access a specific file within the support bundle
    Path { file_path: String },
}

// Implements "seeking" and "putting a capacity on a file" manually.
//
// TODO: When https://github.com/zip-rs/zip2/issues/231 is resolved,
// this method should be replaced by calling "seek" directly,
// via the "by_name_seek" method from the zip crate.
fn skip_and_limit(
    mut reader: impl std::io::Read,
    skip: usize,
    limit: usize,
) -> std::io::Result<impl std::io::Read> {
    const BUF_SIZE: usize = 4096;
    let mut buf = vec![0; BUF_SIZE];
    let mut skip_left = skip;

    while skip_left > 0 {
        let to_read = std::cmp::min(skip_left, BUF_SIZE);
        reader.read_exact(&mut buf[0..to_read])?;
        skip_left -= to_read;
    }

    Ok(reader.take(limit as u64))
}

fn stream_zip_entry_helper(
    tx: &tokio::sync::mpsc::Sender<Result<Vec<u8>, HttpError>>,
    mut archive: zip::ZipArchive<std::fs::File>,
    entry_path: String,
    range: Option<SingleRange>,
) -> Result<(), Error> {
    // TODO: When https://github.com/zip-rs/zip2/issues/231 is resolved,
    // this should call "by_name_seek" instead.
    let mut reader = archive.by_name(&entry_path)?;

    let reader: &mut dyn std::io::Read = match range {
        Some(range) => &mut skip_and_limit(
            reader,
            range.start() as usize,
            range.content_length().get() as usize,
        )?,
        None => &mut reader,
    };

    loop {
        let mut buf = vec![0; 4096];
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        buf.truncate(n);
        if let Err(_) = tx.blocking_send(Ok(buf)) {
            // If we cannot send anything, just bail out - we also won't be able
            // to send an appropriate error in this case, since we'd also be
            // sending it on this borked channel
            return Ok(());
        }
    }
}

struct ZipEntryStream {
    stream: tokio_stream::wrappers::ReceiverStream<Result<Vec<u8>, HttpError>>,
    range: Option<SingleRange>,
    size: u64,
}

// Possible responses from the success case of `stream_zip_entry`
enum ZipStreamOutput {
    // Returns the zip entry, as a byte stream
    Stream(ZipEntryStream),
    // Returns an HTTP response indicating the accepted ranges
    RangeResponse(http::Response<Body>),
}

// Returns a stream of bytes representing an entry within a zipfile.
//
// Q: Why does this spawn a task?
// A: Two reasons - first, the "zip" crate is synchronous, and secondly,
// it has strong opinions about the "archive" living as long as the "entry
// reader". Without a task, streaming an entry from the archive would require
// a self-referential struct, as described in:
// https://morestina.net/blog/1868/self-referential-types-for-fun-and-profit
fn stream_zip_entry(
    file: std::fs::File,
    entry_path: String,
    pr: Option<PotentialRange>,
) -> Result<ZipStreamOutput, Error> {
    let mut archive = zip::ZipArchive::new(file)?;
    let size = {
        // This is a little redundant -- we look up the same file entry within the
        // helper function we spawn below -- but the ZipFile is !Send and !Sync, so
        // we can't re-use it in the background task.
        let zipfile = archive.by_name(&entry_path)?;
        if !zipfile.is_file() {
            return Err(Error::NotAFile);
        }

        zipfile.size()
    };

    let range = if let Some(range) = pr {
        let range = match range.parse(size) {
            Ok(range) => range,
            Err(err) => return Ok(ZipStreamOutput::RangeResponse(err)),
        };
        Some(range)
    } else {
        None
    };

    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let r = range.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(err) = stream_zip_entry_helper(&tx, archive, entry_path, r) {
            let _ = tx.blocking_send(Err(err.into()));
        }
    });

    Ok(ZipStreamOutput::Stream(ZipEntryStream {
        stream: tokio_stream::wrappers::ReceiverStream::new(rx),
        range,
        size,
    }))
}

/// APIs to manage support bundle storage.
pub struct SupportBundleManager<'a> {
    log: &'a Logger,
    storage: &'a dyn LocalStorage,
}

impl<'a> SupportBundleManager<'a> {
    /// Creates a new SupportBundleManager, which provides access
    /// to support bundle CRUD APIs.
    pub fn new(
        log: &'a Logger,
        storage: &'a dyn LocalStorage,
    ) -> SupportBundleManager<'a> {
        Self { log, storage }
    }

    // Returns a dataset that the sled has been explicitly configured to use.
    //
    // In the context of Support Bundles, this is a "parent dataset", within
    // which the "nested support bundle" dataset will be stored.
    //
    // Returns an error if this dataset is not mounted.
    async fn get_mounted_dataset_config(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Result<DatasetConfig, Error> {
        let datasets_config = self.storage.dyn_datasets_config_list().await?;
        let dataset = datasets_config
            .datasets
            .get(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound)?;

        let dataset_props =
            self.storage.dyn_dataset_get(&dataset.name.full_name()).await?;
        if !dataset_props.mounted {
            return Err(Error::DatasetNotMounted {
                dataset: dataset.name.clone(),
            });
        }

        if dataset.id != dataset_id {
            return Err(Error::DatasetExistsBadConfig {
                wanted: dataset_id,
                actual: dataset.id,
            });
        }
        let actual = dataset.name.pool().id();
        if actual != zpool_id {
            return Err(Error::DatasetExistsOnWrongZpool {
                wanted: zpool_id,
                actual,
            });
        }
        Ok(dataset.clone())
    }

    /// Lists all support bundles on a particular dataset.
    pub async fn list(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Result<Vec<SupportBundleMetadata>, Error> {
        let root =
            self.get_mounted_dataset_config(zpool_id, dataset_id).await?.name;
        let datasets = self
            .storage
            .dyn_nested_dataset_list(
                root,
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await?;

        let mut bundles = Vec::with_capacity(datasets.len());
        for dataset in datasets {
            // We should be able to parse each dataset name as a support bundle UUID
            let Ok(support_bundle_id) =
                dataset.name.path.parse::<SupportBundleUuid>()
            else {
                warn!(self.log, "Dataset path not a UUID"; "path" => dataset.name.path);
                continue;
            };

            // The dataset for a support bundle exists.
            let support_bundle_path = self
                .storage
                .dyn_ensure_mounted_and_get_mountpoint(dataset.name)
                .await?
                .join(BUNDLE_FILE_NAME);

            // Identify whether or not the final "bundle" file exists.
            //
            // This is a signal that the support bundle has been fully written.
            let state = if tokio::fs::try_exists(&support_bundle_path).await? {
                SupportBundleState::Complete
            } else {
                SupportBundleState::Incomplete
            };

            let bundle = SupportBundleMetadata { support_bundle_id, state };
            bundles.push(bundle);
        }

        Ok(bundles)
    }

    /// Validates that the sha2 checksum of the file at `path` matches the
    /// expected value.
    async fn sha2_checksum_matches(
        path: &Utf8Path,
        expected: &ArtifactHash,
    ) -> Result<bool, Error> {
        let mut buf = vec![0u8; 65536];
        let mut file = tokio::fs::File::open(path).await?;
        let mut ctx = sha2::Sha256::new();
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            ctx.write_all(&buf[0..n])?;
        }

        let digest = ctx.finalize();
        return Ok(digest.as_slice() == expected.as_ref());
    }

    // A helper function which streams the contents of a bundle to a file.
    async fn stream_bundle(
        mut tmp_file: tokio::fs::File,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> Result<(), Error> {
        futures::pin_mut!(stream);

        // Write the body to the file
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            tmp_file.write_all(&chunk).await?;
        }
        Ok(())
    }

    /// Start creating a new support bundle on a dataset.
    pub async fn start_creation(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
    ) -> Result<SupportBundleMetadata, Error> {
        let log = self.log.new(o!(
            "operation" => "support_bundle_start_creation",
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => dataset_id.to_string(),
            "bundle_id" => support_bundle_id.to_string(),
        ));
        info!(log, "creating support bundle");

        // Access the parent dataset (presumably "crypt/debug")
        // where the support bundled will be mounted.
        let root =
            self.get_mounted_dataset_config(zpool_id, dataset_id).await?.name;
        let dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };

        // Ensure that the dataset exists.
        info!(log, "Ensuring dataset exists for bundle");
        self.storage
            .dyn_nested_dataset_ensure(NestedDatasetConfig {
                name: dataset.clone(),
                inner: SharedDatasetConfig {
                    compression: CompressionAlgorithm::On,
                    quota: None,
                    reservation: None,
                },
            })
            .await?;
        info!(log, "Dataset does exist for bundle");

        // The mounted root of the support bundle dataset
        let support_bundle_dir =
            self.storage.dyn_ensure_mounted_and_get_mountpoint(dataset).await?;
        let support_bundle_path = support_bundle_dir.join(BUNDLE_FILE_NAME);
        let support_bundle_path_tmp =
            support_bundle_dir.join(BUNDLE_TMP_FILE_NAME);

        // Exit early if the support bundle already exists
        if tokio::fs::try_exists(&support_bundle_path).await? {
            info!(log, "Support bundle already exists");
            let metadata = SupportBundleMetadata {
                support_bundle_id,
                state: SupportBundleState::Complete,
            };
            return Ok(metadata);
        }

        // Create the temporary file for access by subsequent transfer calls.
        //
        // Note that this truncates the tempfile if it already existed, for any
        // reason (e.g., incomplete transfer).
        info!(
            log,
            "Creating temp storage for support bundle";
            "path" => ?support_bundle_path_tmp,
        );
        let _ = tokio::fs::File::create(&support_bundle_path_tmp).await?;

        let metadata = SupportBundleMetadata {
            support_bundle_id,
            state: SupportBundleState::Incomplete,
        };
        Ok(metadata)
    }

    /// Transfer a new support bundle to a dataset
    pub async fn transfer(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        offset: u64,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> Result<SupportBundleMetadata, Error> {
        let log = self.log.new(o!(
            "operation" => "support_bundle_transfer",
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => dataset_id.to_string(),
            "bundle_id" => support_bundle_id.to_string(),
            "offset" => offset,
        ));
        info!(log, "transferring support bundle");

        // Access the parent dataset (presumably "crypt/debug")
        // where the support bundled will be mounted.
        let root =
            self.get_mounted_dataset_config(zpool_id, dataset_id).await?.name;
        let dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };

        // The mounted root of the support bundle dataset
        let support_bundle_dir =
            self.storage.dyn_ensure_mounted_and_get_mountpoint(dataset).await?;
        let support_bundle_path_tmp =
            support_bundle_dir.join(BUNDLE_TMP_FILE_NAME);

        // Stream the file into the dataset, first as a temporary file,
        // and then renaming to the final location.
        info!(
            log,
            "Streaming bundle to storage";
            "path" => ?support_bundle_path_tmp,
        );

        // Open the file which should have been created for us during "start
        // creation".
        let mut tmp_file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(&support_bundle_path_tmp)
            .await?;

        tmp_file
            .seek(tokio::io::SeekFrom::Current(i64::try_from(offset)?))
            .await?;

        if let Err(err) = Self::stream_bundle(tmp_file, stream).await {
            warn!(log, "Failed to write bundle to storage"; "error" => ?err);
            if let Err(unlink_err) =
                tokio::fs::remove_file(support_bundle_path_tmp).await
            {
                warn!(log, "Failed to unlink bundle after previous error"; "error" => ?unlink_err);
            }
            return Err(err);
        }

        info!(log, "Bundle written successfully");
        let metadata = SupportBundleMetadata {
            support_bundle_id,
            state: SupportBundleState::Complete,
        };
        Ok(metadata)
    }

    /// Finishes transferring a new support bundle to a dataset
    pub async fn finalize(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        expected_hash: ArtifactHash,
    ) -> Result<SupportBundleMetadata, Error> {
        let log = self.log.new(o!(
            "operation" => "support_bundle_finalize",
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => dataset_id.to_string(),
            "bundle_id" => support_bundle_id.to_string(),
        ));
        info!(log, "finalizing support bundle");

        // Access the parent dataset (presumably "crypt/debug")
        // where the support bundled will be mounted.
        let root =
            self.get_mounted_dataset_config(zpool_id, dataset_id).await?.name;
        let dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };

        // The mounted root of the support bundle dataset
        let support_bundle_dir =
            self.storage.dyn_ensure_mounted_and_get_mountpoint(dataset).await?;
        let support_bundle_path = support_bundle_dir.join(BUNDLE_FILE_NAME);
        let support_bundle_path_tmp =
            support_bundle_dir.join(BUNDLE_TMP_FILE_NAME);

        let metadata = SupportBundleMetadata {
            support_bundle_id,
            state: SupportBundleState::Complete,
        };

        // Deal with idempotency if the bundle has already been finalized.
        if tokio::fs::try_exists(&support_bundle_path).await? {
            if !Self::sha2_checksum_matches(
                &support_bundle_path_tmp,
                &expected_hash,
            )
            .await?
            {
                warn!(
                    log,
                    "Finalized support bundle exists, but the hash doesn't match"
                );
                return Err(Error::HashMismatch);
            }
            info!(log, "Support bundle already finalized");
            return Ok(metadata);
        }

        // Otherwise, finalize the "temporary" -> "permanent" bundle transfer.
        //
        // (This is the normal case)
        if !tokio::fs::try_exists(&support_bundle_path_tmp).await? {
            return Err(Error::BundleNotFound);
        }
        if !Self::sha2_checksum_matches(
            &support_bundle_path_tmp,
            &expected_hash,
        )
        .await?
        {
            warn!(
                log,
                "In-progress support bundle exists, but the hash doesn't match"
            );
            return Err(Error::HashMismatch);
        }

        // Finalize the transfer of the bundle
        tokio::fs::rename(support_bundle_path_tmp, support_bundle_path).await?;
        return Ok(metadata);
    }

    /// Destroys a support bundle that exists on a dataset.
    pub async fn delete(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
    ) -> Result<(), Error> {
        let log = self.log.new(o!(
            "operation" => "support_bundle_delete",
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => dataset_id.to_string(),
            "bundle_id" => support_bundle_id.to_string(),
        ));
        info!(log, "Destroying support bundle");
        let root =
            self.get_mounted_dataset_config(zpool_id, dataset_id).await?.name;
        self.storage
            .dyn_nested_dataset_destroy(NestedDatasetLocation {
                path: support_bundle_id.to_string(),
                root,
            })
            .await?;

        Ok(())
    }

    async fn support_bundle_get_file(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
    ) -> Result<tokio::fs::File, Error> {
        // Access the parent dataset where the support bundle is stored.
        let root =
            self.get_mounted_dataset_config(zpool_id, dataset_id).await?.name;
        let dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };

        // The mounted root of the support bundle dataset
        let support_bundle_dir =
            self.storage.dyn_ensure_mounted_and_get_mountpoint(dataset).await?;
        let path = support_bundle_dir.join(BUNDLE_FILE_NAME);

        let f = tokio::fs::File::open(&path).await?;
        Ok(f)
    }

    /// Streams a support bundle (or portion of a support bundle) from a
    /// dataset.
    pub async fn get(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        range: Option<PotentialRange>,
        query: SupportBundleQueryType,
    ) -> Result<http::Response<Body>, Error> {
        self.get_inner(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            query,
            false,
        )
        .await
    }

    /// Returns metadata about a support bundle.
    pub async fn head(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        range: Option<PotentialRange>,
        query: SupportBundleQueryType,
    ) -> Result<http::Response<Body>, Error> {
        self.get_inner(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            query,
            true,
        )
        .await
    }

    async fn get_inner(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        range: Option<PotentialRange>,
        query: SupportBundleQueryType,
        head_only: bool,
    ) -> Result<http::Response<Body>, Error> {
        // Regardless of the type of query, we first need to access the entire
        // bundle as a file.
        let mut file = self
            .support_bundle_get_file(zpool_id, dataset_id, support_bundle_id)
            .await?;

        match query {
            SupportBundleQueryType::Whole => {
                let len = file.metadata().await?.len();
                const CONTENT_TYPE: http::HeaderValue =
                    http::HeaderValue::from_static("application/zip");
                let content_type = Some(CONTENT_TYPE);

                if head_only {
                    return Ok(range_requests::make_head_response(
                        None,
                        len,
                        content_type,
                    )?);
                }

                if let Some(range) = range {
                    // If this has a range request, we need to validate the range
                    // and put bounds on the part of the file we're reading.
                    let range = match range.parse(len) {
                        Ok(range) => range,
                        Err(err_response) => return Ok(err_response),
                    };

                    info!(
                        &self.log,
                        "SupportBundle GET whole file (ranged)";
                        "bundle_id" => %support_bundle_id,
                        "start" => range.start(),
                        "limit" => range.content_length().get(),
                    );

                    file.seek(std::io::SeekFrom::Start(range.start())).await?;
                    let limit = range.content_length().get();
                    return Ok(range_requests::make_get_response(
                        Some(range),
                        len,
                        content_type,
                        ReaderStream::new(file.take(limit)),
                    )?);
                } else {
                    return Ok(range_requests::make_get_response(
                        None,
                        len,
                        content_type,
                        ReaderStream::new(file),
                    )?);
                };
            }
            SupportBundleQueryType::Index => {
                let file_std = file.into_std().await;
                let archive = zip::ZipArchive::new(file_std)?;
                let names: Vec<&str> = archive.file_names().collect();
                let all_names = names.join("\n");
                let all_names_bytes = all_names.as_bytes();
                let len = all_names_bytes.len() as u64;
                const CONTENT_TYPE: http::HeaderValue =
                    http::HeaderValue::from_static("text/plain");
                let content_type = Some(CONTENT_TYPE);

                if head_only {
                    return Ok(range_requests::make_head_response(
                        None,
                        len,
                        content_type,
                    )?);
                }

                let (range, bytes) = if let Some(range) = range {
                    let range = match range.parse(len) {
                        Ok(range) => range,
                        Err(err_response) => return Ok(err_response),
                    };

                    let section = &all_names_bytes[range.start() as usize
                        ..=range.end_inclusive() as usize];
                    (Some(range), section.to_owned())
                } else {
                    (None, all_names_bytes.to_owned())
                };

                let stream = futures::stream::once(async {
                    Ok::<_, std::convert::Infallible>(bytes)
                });
                return Ok(range_requests::make_get_response(
                    range,
                    len,
                    content_type,
                    stream,
                )?);
            }
            SupportBundleQueryType::Path { file_path } => {
                let file_std = file.into_std().await;

                let entry_stream =
                    match stream_zip_entry(file_std, file_path, range)? {
                        // We have a valid stream
                        ZipStreamOutput::Stream(entry_stream) => entry_stream,
                        // The entry exists, but the requested range is invalid --
                        // send it back as an http body.
                        ZipStreamOutput::RangeResponse(response) => {
                            return Ok(response);
                        }
                    };

                if head_only {
                    return Ok(range_requests::make_head_response(
                        None,
                        entry_stream.size,
                        None::<http::HeaderValue>,
                    )?);
                }

                return Ok(range_requests::make_get_response(
                    entry_stream.range,
                    entry_stream.size,
                    None::<http::HeaderValue>,
                    entry_stream.stream,
                )?);
            }
        };
    }
}

#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use super::*;

    use futures::stream;
    use http::status::StatusCode;
    use hyper::header::{
        ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE,
    };
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::DatasetsConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;
    use sha2::Sha256;
    use sled_storage::manager::StorageHandle;
    use sled_storage::manager_test_harness::StorageManagerTestHarness;
    use std::collections::BTreeMap;
    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

    // TODO-cleanup Should we rework these tests to not use StorageHandle (real
    // code now goes through `ConfigReconcilerHandle`)?
    #[async_trait]
    impl LocalStorage for StorageHandle {
        async fn dyn_datasets_config_list(
            &self,
        ) -> Result<DatasetsConfig, Error> {
            self.datasets_config_list().await.map_err(|err| err.into())
        }

        async fn dyn_dataset_get(
            &self,
            dataset_name: &String,
        ) -> Result<DatasetProperties, Error> {
            let Some(dataset) =
                illumos_utils::zfs::Zfs::get_dataset_properties(
                    &[dataset_name.clone()],
                    illumos_utils::zfs::WhichDatasets::SelfOnly,
                )
                .await
                .map_err(|err| Error::DatasetLookup(err))?
                .pop()
            else {
                // This should not be possible, unless the "zfs get" command is
                // behaving unpredictably. We're only asking for a single dataset,
                // so on success, we should see the result of that dataset.
                return Err(Error::DatasetLookup(anyhow::anyhow!(
                    "Zfs::get_dataset_properties returned an empty vec?"
                )));
            };

            Ok(dataset)
        }

        async fn dyn_ensure_mounted_and_get_mountpoint(
            &self,
            dataset: NestedDatasetLocation,
        ) -> Result<Utf8PathBuf, Error> {
            dataset
                .ensure_mounted_and_get_mountpoint(&self.mount_config().root)
                .await
                .map_err(|err| err.into())
        }

        async fn dyn_nested_dataset_list(
            &self,
            name: DatasetName,
            options: NestedDatasetListOptions,
        ) -> Result<Vec<NestedDatasetConfig>, Error> {
            self.nested_dataset_list(
                NestedDatasetLocation { path: String::new(), root: name },
                options,
            )
            .await
            .map_err(|err| err.into())
        }

        async fn dyn_nested_dataset_ensure(
            &self,
            config: NestedDatasetConfig,
        ) -> Result<(), Error> {
            self.nested_dataset_ensure(config).await.map_err(|err| err.into())
        }

        async fn dyn_nested_dataset_destroy(
            &self,
            name: NestedDatasetLocation,
        ) -> Result<(), Error> {
            self.nested_dataset_destroy(name).await.map_err(|err| err.into())
        }
    }

    struct SingleU2StorageHarness {
        storage_test_harness: StorageManagerTestHarness,
        zpool_id: ZpoolUuid,
    }

    impl SingleU2StorageHarness {
        async fn new(log: &Logger) -> Self {
            let mut harness = StorageManagerTestHarness::new(log).await;
            harness.handle().key_manager_ready().await;
            let _raw_internal_disks =
                harness.add_vdevs(&["m2_left.vdev", "m2_right.vdev"]).await;

            let raw_disks = harness.add_vdevs(&["u2_0.vdev"]).await;

            let config = harness.make_config(1, &raw_disks);
            let result = harness
                .handle()
                .omicron_physical_disks_ensure(config.clone())
                .await
                .expect("Failed to ensure disks");
            assert!(!result.has_error(), "{result:?}");

            let zpool_id = config.disks[0].pool_id;
            Self { storage_test_harness: harness, zpool_id }
        }

        async fn configure_dataset(
            &self,
            dataset_id: DatasetUuid,
            kind: DatasetKind,
        ) {
            let result = self
                .storage_test_harness
                .handle()
                .datasets_ensure(DatasetsConfig {
                    datasets: BTreeMap::from([(
                        dataset_id,
                        DatasetConfig {
                            id: dataset_id,
                            name: DatasetName::new(
                                ZpoolName::new_external(self.zpool_id),
                                kind,
                            ),
                            inner: Default::default(),
                        },
                    )]),
                    ..Default::default()
                })
                .await
                .expect("Failed to ensure datasets");
            assert!(!result.has_error(), "{result:?}");
        }

        async fn cleanup(mut self) {
            self.storage_test_harness.cleanup().await
        }
    }

    enum Data {
        File(&'static [u8]),
        Directory,
    }
    type NamedFile = (&'static str, Data);

    const GREET_PATH: &'static str = "greeting.txt";
    const GREET_DATA: &'static [u8] = b"Hello around the world!";
    const ARBITRARY_DIRECTORY: &'static str = "look-a-directory/";

    fn example_files() -> [NamedFile; 6] {
        [
            (ARBITRARY_DIRECTORY, Data::Directory),
            (GREET_PATH, Data::File(GREET_DATA)),
            ("english/", Data::Directory),
            ("english/hello.txt", Data::File(b"Hello world!")),
            ("spanish/", Data::Directory),
            ("spanish/hello.txt", Data::File(b"Hola mundo!")),
        ]
    }

    fn example_zipfile() -> Vec<u8> {
        let mut buf = vec![0u8; 65536];
        let len = {
            let mut zip = ZipWriter::new(std::io::Cursor::new(&mut buf[..]));
            let options = SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);

            for (name, data) in example_files() {
                match data {
                    Data::File(data) => {
                        zip.start_file(name, options).unwrap();
                        zip.write_all(data).unwrap();
                    }
                    Data::Directory => {
                        zip.add_directory(name, options).unwrap();
                    }
                }
            }
            zip.finish().unwrap().position()
        };
        buf.truncate(len as usize);
        buf
    }

    async fn read_body(response: &mut http::Response<Body>) -> Vec<u8> {
        use http_body_util::BodyExt;
        let mut data = vec![];
        while let Some(frame) = response.body_mut().frame().await {
            data.append(&mut frame.unwrap().into_data().unwrap().to_vec());
        }
        data
    }

    async fn start_transfer_and_finalize(
        mgr: &SupportBundleManager<'_>,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        hash: ArtifactHash,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> SupportBundleMetadata {
        mgr.start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have started creation");
        mgr.transfer(zpool_id, dataset_id, support_bundle_id, 0, stream)
            .await
            .expect("Should have transferred bundle");
        mgr.finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect("Should have finalized bundle")
    }

    async fn start_transfer_and_finalize_expect_finalize_err(
        mgr: &SupportBundleManager<'_>,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        hash: ArtifactHash,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> Error {
        mgr.start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have started creation");
        mgr.transfer(zpool_id, dataset_id, support_bundle_id, 0, stream)
            .await
            .expect("Should have transferred bundle");
        mgr.finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect_err("Should have failed to finalize bundle")
    }

    #[tokio::test]
    async fn basic_crud() {
        let logctx = test_setup_log("basic_crud");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Create a fake support bundle -- really, just a zipfile.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Create a new bundle
        let bundle = start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;
        assert_eq!(bundle.support_bundle_id, support_bundle_id);
        assert_eq!(bundle.state, SupportBundleState::Complete);

        // List the bundle we just created
        let bundles = mgr
            .list(harness.zpool_id, dataset_id)
            .await
            .expect("Should have been able to read bundles");
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].support_bundle_id, support_bundle_id);

        // HEAD the bundle we created - we can see it's a zipfile with the
        // expected length, even without reading anything.
        let mut response = mgr
            .head(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Whole,
            )
            .await
            .expect("Should have been able to HEAD bundle");
        assert_eq!(read_body(&mut response).await, Vec::<u8>::new());
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            zipfile_data.len().to_string()
        );
        assert_eq!(response.headers()[CONTENT_TYPE], "application/zip");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // GET the bundle we created, and observe the contents of the bundle
        let mut response = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Whole,
            )
            .await
            .expect("Should have been able to GET bundle");
        assert_eq!(read_body(&mut response).await, zipfile_data);
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            zipfile_data.len().to_string()
        );
        assert_eq!(response.headers()[CONTENT_TYPE], "application/zip");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // HEAD the index of the bundle - it should report the size of all
        // files.
        let mut response = mgr
            .head(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Index,
            )
            .await
            .expect("Should have been able to HEAD bundle index");
        assert_eq!(read_body(&mut response).await, Vec::<u8>::new());
        let expected_index = example_files()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<&str>>()
            .join("\n");
        let expected_len = expected_index.len().to_string();
        assert_eq!(response.headers().len(), 3);
        assert_eq!(response.headers()[CONTENT_LENGTH], expected_len);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // GET the index of the bundle.
        let mut response = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Index,
            )
            .await
            .expect("Should have been able to GET bundle index");
        assert_eq!(read_body(&mut response).await, expected_index.as_bytes());
        assert_eq!(response.headers().len(), 3);
        assert_eq!(response.headers()[CONTENT_LENGTH], expected_len);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // HEAD a single file from within the bundle.
        let mut response = mgr
            .head(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Path {
                    file_path: GREET_PATH.to_string(),
                },
            )
            .await
            .expect("Should have been able to HEAD single file");
        assert_eq!(read_body(&mut response).await, Vec::<u8>::new());
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            GREET_DATA.len().to_string()
        );
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "application/octet-stream"
        );
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // GET a single file within the bundle
        let mut response = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Path {
                    file_path: GREET_PATH.to_string(),
                },
            )
            .await
            .expect("Should have been able to GET single file");
        assert_eq!(read_body(&mut response).await, GREET_DATA);
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            GREET_DATA.len().to_string()
        );
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "application/octet-stream"
        );
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // Cannot GET nor HEAD a directory
        let err = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Path {
                    file_path: ARBITRARY_DIRECTORY.to_string(),
                },
            )
            .await
            .expect_err("Should not be able to GET directory");
        assert!(matches!(err, Error::NotAFile), "Unexpected error: {err:?}");

        let err = mgr
            .head(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Path {
                    file_path: ARBITRARY_DIRECTORY.to_string(),
                },
            )
            .await
            .expect_err("Should not be able to HEAD directory");
        assert!(matches!(err, Error::NotAFile), "Unexpected error: {err:?}");

        // delete the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn chunked_file_transfer() {
        let logctx = test_setup_log("chunked_file_transfer");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Create a fake support bundle -- really, just a zipfile.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let zpool_id = harness.zpool_id;

        mgr.start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have started creation");

        // Split the zipfile into halves, so we can transfer it in two chunks
        let len1 = zipfile_data.len() / 2;
        let stream1 = stream::once(async {
            Ok(Bytes::copy_from_slice(&zipfile_data.as_slice()[..len1]))
        });
        let stream2 = stream::once(async {
            Ok(Bytes::copy_from_slice(&zipfile_data.as_slice()[len1..]))
        });

        mgr.transfer(zpool_id, dataset_id, support_bundle_id, 0, stream1)
            .await
            .expect("Should have transferred bundle (part1)");
        mgr.transfer(
            zpool_id,
            dataset_id,
            support_bundle_id,
            len1 as u64,
            stream2,
        )
        .await
        .expect("Should have transferred bundle (part2)");
        let bundle = mgr
            .finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect("Should have finalized bundle");
        assert_eq!(bundle.support_bundle_id, support_bundle_id);
        assert_eq!(bundle.state, SupportBundleState::Complete);

        // GET the bundle we created, and observe the contents of the bundle
        let mut response = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Whole,
            )
            .await
            .expect("Should have been able to GET bundle");
        assert_eq!(read_body(&mut response).await, zipfile_data);
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            zipfile_data.len().to_string()
        );
        assert_eq!(response.headers()[CONTENT_TYPE], "application/zip");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // delete the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn chunked_file_transfer_restart() {
        let logctx = test_setup_log("chunked_file_transfer_restart");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Create a fake support bundle -- really, just a zipfile.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let zpool_id = harness.zpool_id;

        mgr.start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have started creation");

        let bad_stream =
            stream::once(async { Ok(Bytes::copy_from_slice(&[0, 0, 0, 0])) });

        // Write some "bad bytes" and finalize it.
        mgr.transfer(zpool_id, dataset_id, support_bundle_id, 0, bad_stream)
            .await
            .expect("Should have transferred bad bytes");
        let err = mgr
            .finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect_err("Should have failed to finalize");
        assert!(matches!(err, Error::HashMismatch), "Unexpected err: {err:?}");

        // We can restart the stream if we invoke "start_creation" again.
        mgr.start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have started creation");

        // Now transfer valid data
        mgr.transfer(
            zpool_id,
            dataset_id,
            support_bundle_id,
            0,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await
        .expect("Should have transferred bundle");
        let bundle = mgr
            .finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect("Should have finalized bundle");
        assert_eq!(bundle.support_bundle_id, support_bundle_id);
        assert_eq!(bundle.state, SupportBundleState::Complete);

        // GET the bundle we created, and observe the contents of the bundle
        let mut response = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Whole,
            )
            .await
            .expect("Should have been able to GET bundle");
        assert_eq!(read_body(&mut response).await, zipfile_data);
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            zipfile_data.len().to_string()
        );
        assert_eq!(response.headers()[CONTENT_TYPE], "application/zip");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // Delete the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn try_to_write_after_finalize() {
        let logctx = test_setup_log("try_to_write_after_finalize");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Create a fake support bundle -- really, just a zipfile.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let zpool_id = harness.zpool_id;

        mgr.start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have started creation");
        mgr.transfer(
            zpool_id,
            dataset_id,
            support_bundle_id,
            0,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await
        .expect("Should have transferred bundle");
        let bundle = mgr
            .finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect("Should have finalized bundle");
        assert_eq!(bundle.support_bundle_id, support_bundle_id);
        assert_eq!(bundle.state, SupportBundleState::Complete);

        // If we try to transfer again, we'll see a failure.
        let err = mgr
            .transfer(
                zpool_id,
                dataset_id,
                support_bundle_id,
                0,
                stream::once(async {
                    Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
                }),
            )
            .await
            .expect_err("Should have failed to transfer bundle");
        assert!(
            matches!(err, Error::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound),
            "Unexpected err: {err:?}"
        );

        // If we try to finalize again, we'll see a failure.
        let err = mgr
            .finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await
            .expect_err("Should have failed to finalize bundle");
        assert!(
            matches!(err, Error::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound),
            "Unexpected err: {err:?}"
        );

        // If we try to create again, we'll see "OK" - but the
        // bundle should already exist, so we can immediately "GET" it afterwards.
        let metadata = mgr
            .start_creation(zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Creation should have succeeded");
        assert_eq!(metadata.state, SupportBundleState::Complete);

        // GET the bundle we created, and observe the contents of the bundle
        let mut response = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Whole,
            )
            .await
            .expect("Should have been able to GET bundle");
        assert_eq!(read_body(&mut response).await, zipfile_data);
        assert_eq!(response.headers().len(), 3);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            zipfile_data.len().to_string()
        );
        assert_eq!(response.headers()[CONTENT_TYPE], "application/zip");
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");

        // Delete the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn creation_without_dataset() {
        let logctx = test_setup_log("creation_without_dataset");
        let log = &logctx.log;

        // Set up storage (zpool, but not dataset!)
        let harness = SingleU2StorageHarness::new(log).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Get a support bundle that we're ready to store.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Storing a bundle without a dataset should throw an error.
        let dataset_id = DatasetUuid::new_v4();
        let err = mgr
            .start_creation(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect_err("Bundle creation should fail without dataset");
        assert!(matches!(err, Error::Storage(_)), "Unexpected error: {err:?}");
        assert_eq!(HttpError::from(err).status_code, StatusCode::NOT_FOUND);

        // Configure the dataset now, so it'll exist for future requests.
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn creation_bad_hash() {
        let logctx = test_setup_log("creation_bad_hash");
        let log = &logctx.log;

        // Set up storage (zpool, but not dataset!)
        let harness = SingleU2StorageHarness::new(log).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Get a support bundle that we're ready to store.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Configure the dataset now, so it'll exist for future requests.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        let bad_hash = ArtifactHash(
            Sha256::digest(b"Hey, this ain't right")
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Creating the bundle with a bad hash should fail.
        let err = start_transfer_and_finalize_expect_finalize_err(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            bad_hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;
        assert!(
            matches!(err, Error::HashMismatch),
            "Unexpected error: {err:?}"
        );
        assert_eq!(HttpError::from(err).status_code, StatusCode::BAD_REQUEST);

        // As long as the dataset exists, we'll make storage for it, which means
        // the bundle will be visible, but incomplete.
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].support_bundle_id, support_bundle_id);
        assert_eq!(bundles[0].state, SupportBundleState::Incomplete);

        // Creating the bundle with bad data should fail
        let err = start_transfer_and_finalize_expect_finalize_err(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async { Ok(Bytes::from_static(b"Not a zipfile")) }),
        )
        .await;
        assert!(
            matches!(err, Error::HashMismatch),
            "Unexpected error: {err:?}"
        );
        assert_eq!(HttpError::from(err).status_code, StatusCode::BAD_REQUEST);

        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].support_bundle_id, support_bundle_id);
        assert_eq!(bundles[0].state, SupportBundleState::Incomplete);

        // Good hash + Good data -> creation should succeed
        start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;

        // The bundle should now appear "Complete"
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].support_bundle_id, support_bundle_id);
        assert_eq!(bundles[0].state, SupportBundleState::Complete);

        // We can delete the bundle, and it should no longer appear.
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 0);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn creation_bad_hash_still_deleteable() {
        let logctx = test_setup_log("creation_bad_hash_still_deleteable");
        let log = &logctx.log;

        // Set up storage (zpool, but not dataset!)
        let harness = SingleU2StorageHarness::new(log).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Get a support bundle that we're ready to store
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();

        // Configure the dataset now, so it'll exist for future requests.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        let bad_hash = ArtifactHash(
            Sha256::digest(b"Hey, this ain't right")
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Creating the bundle with a bad hash should fail.
        let err = start_transfer_and_finalize_expect_finalize_err(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            bad_hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;
        assert!(
            matches!(err, Error::HashMismatch),
            "Unexpected error: {err:?}"
        );
        assert_eq!(HttpError::from(err).status_code, StatusCode::BAD_REQUEST);

        // The bundle still appears to exist, as storage gets allocated after
        // the "create" call.
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].support_bundle_id, support_bundle_id);
        assert_eq!(bundles[0].state, SupportBundleState::Incomplete);

        // We can delete the bundle, and it should no longer appear.
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 0);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    async fn is_mounted(dataset: &str) -> bool {
        let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
        let cmd = command.args(&["list", "-Hpo", "mounted", dataset]);
        let output = cmd.output().await.unwrap();
        assert!(output.status.success(), "Failed to list dataset: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim() == "yes"
    }

    async fn unmount(dataset: &str) {
        let mut command = tokio::process::Command::new(illumos_utils::PFEXEC);
        let cmd =
            command.args(&[illumos_utils::zfs::ZFS, "unmount", "-f", dataset]);
        let output = cmd.output().await.unwrap();
        assert!(
            output.status.success(),
            "Failed to unmount dataset: {output:?}"
        );
    }

    #[tokio::test]
    async fn cannot_create_bundle_on_unmounted_parent() {
        let logctx = test_setup_log("cannot_create_bundle_on_unmounted_parent");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );
        let support_bundle_id = SupportBundleUuid::new_v4();

        // Before we actually create the bundle:
        //
        // Unmount the "parent dataset". This is equivalent to trying to create
        // a support bundle when the debug dataset exists, but has not been
        // mounted yet.
        let parent_dataset = mgr
            .get_mounted_dataset_config(harness.zpool_id, dataset_id)
            .await
            .expect("Could not get parent dataset from test harness")
            .name;
        let parent_dataset_name = parent_dataset.full_name();
        assert!(is_mounted(&parent_dataset_name).await);
        unmount(&parent_dataset_name).await;
        assert!(!is_mounted(&parent_dataset_name).await);

        // Create a new bundle
        let err = mgr
            .start_creation(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect_err("Should not have been able to create support bundle");
        let Error::DatasetNotMounted { dataset } = err else {
            panic!("Unexpected error: {err:?}");
        };
        assert_eq!(
            dataset, parent_dataset,
            "Unexpected 'parent dataset' in error message"
        );

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn listing_bundles_mounts_them() {
        let logctx = test_setup_log("listing_bundles_mounts_them");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Create a new bundle
        let _ = start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;

        // Peek under the hood: We should be able to observe the support
        // bundle as a nested dataset.
        let root = mgr
            .get_mounted_dataset_config(harness.zpool_id, dataset_id)
            .await
            .expect("Could not get parent dataset from test harness")
            .name;
        let nested_dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };
        let nested_dataset_name = nested_dataset.full_name();

        // The dataset was mounted after creation.
        assert!(is_mounted(&nested_dataset_name).await);

        // We can manually unmount this dataset.
        unmount(&nested_dataset_name).await;
        assert!(!is_mounted(&nested_dataset_name).await);

        // When we "list" this nested dataset, it'll be mounted once more.
        let _ = mgr
            .list(harness.zpool_id, dataset_id)
            .await
            .expect("Should have been able to list bundle");
        assert!(is_mounted(&nested_dataset_name).await);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn getting_bundles_mounts_them() {
        let logctx = test_setup_log("getting_bundles_mounts_them");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Create a new bundle
        let _ = start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;

        // Peek under the hood: We should be able to observe the support
        // bundle as a nested dataset.
        let root = mgr
            .get_mounted_dataset_config(harness.zpool_id, dataset_id)
            .await
            .expect("Could not get parent dataset from test harness")
            .name;
        let nested_dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };
        let nested_dataset_name = nested_dataset.full_name();

        // The dataset was mounted after creation.
        assert!(is_mounted(&nested_dataset_name).await);

        // We can manually unmount this dataset.
        unmount(&nested_dataset_name).await;
        assert!(!is_mounted(&nested_dataset_name).await);

        // When we "get" this nested dataset, it'll be mounted once more.
        let _ = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                None,
                SupportBundleQueryType::Whole,
            )
            .await
            .expect("Should have been able to GET bundle");
        assert!(is_mounted(&nested_dataset_name).await);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn creation_idempotency() {
        let logctx = test_setup_log("creation_idempotency");
        let log = &logctx.log;

        // Set up storage (zpool, but not dataset!)
        let harness = SingleU2StorageHarness::new(log).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Get a support bundle that we're ready to store.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Configure the dataset now, so it'll exist for future requests.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Create the bundle
        start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;

        // Creating the dataset again should work.
        let bundle = mgr
            .start_creation(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Support bundle should already exist");

        assert_eq!(bundle.state, SupportBundleState::Complete);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ranges() {
        let logctx = test_setup_log("ranges");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // For this test, we'll add a dataset that can contain our bundles.
        let dataset_id = DatasetUuid::new_v4();
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        // Access the Support Bundle API
        let mgr = SupportBundleManager::new(
            log,
            harness.storage_test_harness.handle(),
        );

        // Create a fake support bundle -- really, just a zipfile.
        let support_bundle_id = SupportBundleUuid::new_v4();
        let zipfile_data = example_zipfile();
        let hash = ArtifactHash(
            Sha256::digest(zipfile_data.as_slice())
                .as_slice()
                .try_into()
                .unwrap(),
        );

        // Create a new bundle
        let bundle = start_transfer_and_finalize(
            &mgr,
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await;
        assert_eq!(bundle.support_bundle_id, support_bundle_id);
        assert_eq!(bundle.state, SupportBundleState::Complete);

        // GET the bundle we created, and observe the contents of the bundle
        let ranges = [
            (0, 5),
            (5, 100),
            (0, 100),
            (1000, 1000),
            (1000, 1001),
            (1000, zipfile_data.len() - 1),
        ];

        for (first, last) in ranges.into_iter() {
            eprintln!("Trying whole-file range: {first}-{last}");
            let range =
                PotentialRange::new(format!("bytes={first}-{last}").as_bytes());
            let expected_data = &zipfile_data[first..=last];

            let mut response = mgr
                .get(
                    harness.zpool_id,
                    dataset_id,
                    support_bundle_id,
                    Some(range),
                    SupportBundleQueryType::Whole,
                )
                .await
                .expect("Should have been able to GET bundle");
            assert_eq!(read_body(&mut response).await, expected_data);
            assert_eq!(response.headers().len(), 4);
            assert_eq!(
                response.headers()[CONTENT_RANGE],
                format!("bytes {first}-{last}/{}", zipfile_data.len())
            );
            assert_eq!(
                response.headers()[CONTENT_LENGTH],
                ((last + 1) - first).to_string()
            );
            assert_eq!(response.headers()[CONTENT_TYPE], "application/zip");
            assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
        }

        // GET the index of the bundle.
        let expected_index_str = example_files()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<&str>>()
            .join("\n");
        let expected_index = expected_index_str.as_bytes();
        let ranges = [(0, 5), (5, 10), (10, expected_index.len() - 1)];

        for (first, last) in ranges.into_iter() {
            eprintln!("Trying index range: {first}-{last}");
            let range =
                PotentialRange::new(format!("bytes={first}-{last}").as_bytes());
            let expected_data = &expected_index[first..=last];
            let mut response = mgr
                .get(
                    harness.zpool_id,
                    dataset_id,
                    support_bundle_id,
                    Some(range),
                    SupportBundleQueryType::Index,
                )
                .await
                .expect("Should have been able to GET bundle index");
            assert_eq!(read_body(&mut response).await, expected_data);
            assert_eq!(response.headers().len(), 4);
            assert_eq!(
                response.headers()[CONTENT_RANGE],
                format!("bytes {first}-{last}/{}", expected_index.len())
            );
            assert_eq!(
                response.headers()[CONTENT_LENGTH],
                ((last + 1) - first).to_string(),
            );
            assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
            assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
        }

        // GET a single file within the bundle
        let ranges = [(0, 5), (5, 10), (5, GREET_DATA.len() - 1)];
        for (first, last) in ranges.into_iter() {
            eprintln!("Trying single file range: {first}-{last}");
            let range =
                PotentialRange::new(format!("bytes={first}-{last}").as_bytes());
            let expected_data = &GREET_DATA[first..=last];
            let mut response = mgr
                .get(
                    harness.zpool_id,
                    dataset_id,
                    support_bundle_id,
                    Some(range),
                    SupportBundleQueryType::Path {
                        file_path: GREET_PATH.to_string(),
                    },
                )
                .await
                .expect("Should have been able to GET single file");
            assert_eq!(read_body(&mut response).await, expected_data);
            assert_eq!(response.headers().len(), 4);
            assert_eq!(
                response.headers()[CONTENT_RANGE],
                format!("bytes {first}-{last}/{}", GREET_DATA.len())
            );
            assert_eq!(
                response.headers()[CONTENT_LENGTH],
                ((last + 1) - first).to_string(),
            );
            assert_eq!(
                response.headers()[CONTENT_TYPE],
                "application/octet-stream"
            );
            assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
        }

        // Cannot GET nor HEAD a directory, even with range requests
        let range = PotentialRange::new(b"bytes=0-1");
        let err = mgr
            .get(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                Some(range),
                SupportBundleQueryType::Path {
                    file_path: ARBITRARY_DIRECTORY.to_string(),
                },
            )
            .await
            .expect_err("Should not be able to GET directory");
        assert!(matches!(err, Error::NotAFile), "Unexpected error: {err:?}");

        let range = PotentialRange::new(b"bytes=0-1");
        let err = mgr
            .head(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                Some(range),
                SupportBundleQueryType::Path {
                    file_path: ARBITRARY_DIRECTORY.to_string(),
                },
            )
            .await
            .expect_err("Should not be able to HEAD directory");
        assert!(matches!(err, Error::NotAFile), "Unexpected error: {err:?}");

        // delete the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to delete bundle");

        harness.cleanup().await;
        logctx.cleanup_successful();
    }
}
