// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of and access to Support Bundles

use async_trait::async_trait;
use bytes::Bytes;
use camino::Utf8Path;
use dropshot::Body;
use dropshot::HttpError;
use futures::Stream;
use futures::StreamExt;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_common::update::ArtifactHash;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use range_requests::PotentialRange;
use range_requests::SingleRange;
use sha2::{Digest, Sha256};
use sled_agent_api::*;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::io::Write;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use zip::result::ZipError;

// The final name of the bundle, as it is stored within the dedicated
// datasets.
//
// The full path is of the form:
//
// /pool/ext/$(POOL_UUID)/crypt/$(DATASET_TYPE)/$(BUNDLE_UUID)/bundle.zip
//                              |               | This is a per-bundle nested dataset
//                              | This is a Debug dataset
const BUNDLE_FILE_NAME: &str = "bundle.zip";

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

    #[error(
        "Dataset exists, but has an invalid configuration: (wanted {wanted}, saw {actual})"
    )]
    DatasetExistsBadConfig { wanted: DatasetUuid, actual: DatasetUuid },

    #[error(
        "Dataset exists, but appears on the wrong zpool (wanted {wanted}, saw {actual})"
    )]
    DatasetExistsOnWrongZpool { wanted: ZpoolUuid, actual: ZpoolUuid },

    #[error(transparent)]
    Storage(#[from] sled_storage::error::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Range(#[from] range_requests::Error),

    #[error(transparent)]
    Zip(#[from] ZipError),
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

    /// Returns all nested datasets within an existing dataset
    async fn dyn_nested_dataset_list(
        &self,
        name: NestedDatasetLocation,
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

    /// Returns the root filesystem path where datasets are mounted.
    ///
    /// This is typically "/" in prod, but can be a temporary directory
    /// for tests to isolate storage that typically appears globally.
    fn zpool_mountpoint_root(&self) -> Cow<Utf8Path>;
}

/// This implementation is effectively a pass-through to the real methods
#[async_trait]
impl LocalStorage for StorageHandle {
    async fn dyn_datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        self.datasets_config_list().await.map_err(|err| err.into())
    }

    async fn dyn_nested_dataset_list(
        &self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error> {
        self.nested_dataset_list(name, options).await.map_err(|err| err.into())
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

    fn zpool_mountpoint_root(&self) -> Cow<Utf8Path> {
        Cow::Borrowed(illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT.into())
    }
}

/// This implementation allows storage bundles to be stored on simulated storage
#[async_trait]
impl LocalStorage for crate::sim::Storage {
    async fn dyn_datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        self.lock().datasets_config_list().map_err(|err| err.into())
    }

    async fn dyn_nested_dataset_list(
        &self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error> {
        self.lock().nested_dataset_list(name, options).map_err(|err| err.into())
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

    fn zpool_mountpoint_root(&self) -> Cow<Utf8Path> {
        Cow::Owned(self.lock().root().to_path_buf())
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
    async fn get_configured_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Result<DatasetConfig, Error> {
        let datasets_config = self.storage.dyn_datasets_config_list().await?;
        let dataset = datasets_config
            .datasets
            .get(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound)?;

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
            self.get_configured_dataset(zpool_id, dataset_id).await?.name;
        let dataset_location =
            NestedDatasetLocation { path: String::from(""), root };
        let datasets = self
            .storage
            .dyn_nested_dataset_list(
                dataset_location,
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
            let support_bundle_path = dataset
                .name
                .mountpoint(&self.storage.zpool_mountpoint_root())
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
    //
    // If at any point this function fails, the temporary file still exists,
    // and should be removed.
    async fn write_and_finalize_bundle(
        mut tmp_file: tokio::fs::File,
        from: &Utf8Path,
        to: &Utf8Path,
        expected_hash: ArtifactHash,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> Result<(), Error> {
        futures::pin_mut!(stream);

        // Write the body to the file
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            tmp_file.write_all(&chunk).await?;
        }
        let digest = hasher.finalize();
        if digest.as_slice() != expected_hash.as_ref() {
            return Err(Error::HashMismatch);
        }

        // Rename the file to indicate it's ready
        tokio::fs::rename(from, to).await?;
        Ok(())
    }

    /// Creates a new support bundle on a dataset.
    pub async fn create(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        expected_hash: ArtifactHash,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> Result<SupportBundleMetadata, Error> {
        let log = self.log.new(o!(
            "operation" => "support_bundle_create",
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => dataset_id.to_string(),
            "bundle_id" => support_bundle_id.to_string(),
        ));
        let root =
            self.get_configured_dataset(zpool_id, dataset_id).await?.name;
        let dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };
        // The mounted root of the support bundle dataset
        let support_bundle_dir =
            dataset.mountpoint(&self.storage.zpool_mountpoint_root());
        let support_bundle_path = support_bundle_dir.join(BUNDLE_FILE_NAME);
        let support_bundle_path_tmp = support_bundle_dir.join(format!(
            "bundle-{}.tmp",
            thread_rng()
                .sample_iter(Alphanumeric)
                .take(6)
                .map(char::from)
                .collect::<String>()
        ));

        // Ensure that the dataset exists.
        info!(log, "Ensuring dataset exists for bundle");
        self.storage
            .dyn_nested_dataset_ensure(NestedDatasetConfig {
                name: dataset,
                inner: SharedDatasetConfig {
                    compression: CompressionAlgorithm::On,
                    quota: None,
                    reservation: None,
                },
            })
            .await?;
        info!(log, "Dataset does exist for bundle");

        // Exit early if the support bundle already exists
        if tokio::fs::try_exists(&support_bundle_path).await? {
            if !Self::sha2_checksum_matches(
                &support_bundle_path,
                &expected_hash,
            )
            .await?
            {
                warn!(log, "Support bundle exists, but the hash doesn't match");
                return Err(Error::HashMismatch);
            }

            info!(log, "Support bundle already exists");
            let metadata = SupportBundleMetadata {
                support_bundle_id,
                state: SupportBundleState::Complete,
            };
            return Ok(metadata);
        }

        // Stream the file into the dataset, first as a temporary file,
        // and then renaming to the final location.
        info!(
            log,
            "Streaming bundle to storage";
            "path" => ?support_bundle_path_tmp,
        );
        let tmp_file =
            tokio::fs::File::create(&support_bundle_path_tmp).await?;

        if let Err(err) = Self::write_and_finalize_bundle(
            tmp_file,
            &support_bundle_path_tmp,
            &support_bundle_path,
            expected_hash,
            stream,
        )
        .await
        {
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
            self.get_configured_dataset(zpool_id, dataset_id).await?.name;
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
        let root =
            self.get_configured_dataset(zpool_id, dataset_id).await?.name;
        let dataset =
            NestedDatasetLocation { path: support_bundle_id.to_string(), root };
        // The mounted root of the support bundle dataset
        let support_bundle_dir =
            dataset.mountpoint(&self.storage.zpool_mountpoint_root());
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
    use sled_storage::manager_test_harness::StorageManagerTestHarness;
    use std::collections::BTreeMap;
    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

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
        let bundle = mgr
            .create(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                hash,
                stream::once(async {
                    Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
                }),
            )
            .await
            .expect("Should have created support bundle");
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

        // DELETE the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to DELETE bundle");

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
            .create(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                hash,
                stream::once(async {
                    Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
                }),
            )
            .await
            .expect_err("Bundle creation should fail without dataset");
        assert!(matches!(err, Error::Storage(_)), "Unexpected error: {err:?}");
        assert_eq!(HttpError::from(err).status_code, StatusCode::NOT_FOUND);

        // Configure the dataset now, so it'll exist for future requests.
        harness.configure_dataset(dataset_id, DatasetKind::Debug).await;

        mgr.create(
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await
        .expect("Should have created support bundle");

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
        let err = mgr
            .create(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                bad_hash,
                stream::once(async {
                    Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
                }),
            )
            .await
            .expect_err("Bundle creation should fail with bad hash");
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
        let err = mgr
            .create(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                hash,
                stream::once(async {
                    Ok(Bytes::from_static(b"Not a zipfile"))
                }),
            )
            .await
            .expect_err("Bundle creation should fail with bad hash");
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
        mgr.create(
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await
        .expect("Should have created support bundle");

        // The bundle should now appear "Complete"
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].support_bundle_id, support_bundle_id);
        assert_eq!(bundles[0].state, SupportBundleState::Complete);

        // We can delete the bundle, and it should no longer appear.
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to DELETE bundle");
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
        let err = mgr
            .create(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                bad_hash,
                stream::once(async {
                    Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
                }),
            )
            .await
            .expect_err("Bundle creation should fail with bad hash");
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
            .expect("Should have been able to DELETE bundle");
        let bundles = mgr.list(harness.zpool_id, dataset_id).await.unwrap();
        assert_eq!(bundles.len(), 0);

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
        mgr.create(
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await
        .expect("Should have created support bundle");

        // Creating the dataset again should work.
        mgr.create(
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
            }),
        )
        .await
        .expect("Support bundle should already exist");

        // This is an edge-case, but just to make sure the behavior
        // is codified: If we are creating a bundle that already exists,
        // we'll skip reading the body.
        mgr.create(
            harness.zpool_id,
            dataset_id,
            support_bundle_id,
            hash,
            stream::once(async {
                // NOTE: This is different from the call above.
                Ok(Bytes::from_static(b"Ignored"))
            }),
        )
        .await
        .expect("Support bundle should already exist");

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
        let bundle = mgr
            .create(
                harness.zpool_id,
                dataset_id,
                support_bundle_id,
                hash,
                stream::once(async {
                    Ok(Bytes::copy_from_slice(zipfile_data.as_slice()))
                }),
            )
            .await
            .expect("Should have created support bundle");
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

        // DELETE the bundle on the dataset
        mgr.delete(harness.zpool_id, dataset_id, support_bundle_id)
            .await
            .expect("Should have been able to DELETE bundle");

        harness.cleanup().await;
        logctx.cleanup_successful();
    }
}
