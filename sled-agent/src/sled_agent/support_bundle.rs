// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of and access to Support Bundles

use crate::sled_agent::SledAgent;
use camino::Utf8Path;
use dropshot::Body;
use dropshot::HttpError;
use dropshot::StreamingBody;
use futures::StreamExt;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_common::update::ArtifactHash;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use range_requests::PotentialRange;
use range_requests::SingleRange;
use sha2::{Digest, Sha256};
use sled_agent_api::*;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
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

    #[error(transparent)]
    Storage(#[from] sled_storage::error::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Range(#[from] range_requests::Error),

    #[error(transparent)]
    Zip(#[from] ZipError),
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::HttpError(err) => err,
            Error::HashMismatch => {
                HttpError::for_internal_error("Hash mismatch".to_string())
            }
            Error::DatasetNotFound => {
                HttpError::for_not_found(None, "Dataset not found".to_string())
            }
            Error::NotAFile => {
                HttpError::for_bad_request(None, "Not a file".to_string())
            }
            Error::Storage(err) => HttpError::from(ExternalError::from(err)),
            Error::Io(err) => HttpError::for_internal_error(err.to_string()),
            Error::Range(err) => HttpError::for_internal_error(err.to_string()),
            Error::Zip(err) => match err {
                ZipError::FileNotFound => HttpError::for_not_found(
                    None,
                    "Entry not found".to_string(),
                ),
                err => HttpError::for_internal_error(err.to_string()),
            },
        }
    }
}

fn stream_zip_entry_helper(
    tx: &tokio::sync::mpsc::Sender<Result<Vec<u8>, HttpError>>,
    mut archive: zip::ZipArchive<std::fs::File>,
    entry_path: String,
    range: Option<SingleRange>,
) -> Result<(), Error> {
    let mut reader = archive.by_name_seek(&entry_path)?;

    let mut reader: Box<dyn std::io::Read> = match range {
        Some(range) => {
            reader.seek(std::io::SeekFrom::Start(range.start()))?;
            Box::new(reader.take(range.content_length()))
        }
        None => Box::new(reader),
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
    // TODO: do we need to pass the size back? or are we good?
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

impl SledAgent {
    /// Returns a dataset that the sled has been explicitly configured to use.
    pub async fn get_configured_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Result<DatasetConfig, Error> {
        let datasets_config = self.storage().datasets_config_list().await?;
        let dataset = datasets_config
            .datasets
            .get(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound)?;

        if dataset.id != dataset_id || dataset.name.pool().id() != zpool_id {
            return Err(Error::DatasetNotFound);
        }
        Ok(dataset.clone())
    }

    pub async fn support_bundle_list(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Result<Vec<SupportBundleMetadata>, Error> {
        let root =
            self.get_configured_dataset(zpool_id, dataset_id).await?.name;
        let dataset_location =
            NestedDatasetLocation { path: String::from(""), root };
        let datasets = self
            .storage()
            .nested_dataset_list(
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
                .mountpoint(illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT.into())
                .join("bundle");

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

    /// Returns the hex, lowercase sha2 checksum of a file at `path`.
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
        body: StreamingBody,
    ) -> Result<(), Error> {
        let stream = body.into_stream();
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

    pub async fn support_bundle_create(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        expected_hash: ArtifactHash,
        body: StreamingBody,
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
        let support_bundle_dir = dataset
            .mountpoint(illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT.into());
        let support_bundle_path = support_bundle_dir.join("bundle");
        let support_bundle_path_tmp = support_bundle_dir.join("bundle.tmp");

        // Ensure that the dataset exists.
        info!(log, "Ensuring dataset exists for bundle");
        self.storage()
            .nested_dataset_ensure(NestedDatasetConfig {
                name: dataset,
                inner: SharedDatasetConfig {
                    compression: CompressionAlgorithm::On,
                    quota: None,
                    reservation: None,
                },
            })
            .await?;

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
        info!(log, "Streaming bundle to storage");
        let tmp_file =
            tokio::fs::File::create(&support_bundle_path_tmp).await?;
        if let Err(err) = Self::write_and_finalize_bundle(
            tmp_file,
            &support_bundle_path_tmp,
            &support_bundle_path,
            expected_hash,
            body,
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

    pub async fn support_bundle_delete(
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
        self.storage()
            .nested_dataset_destroy(NestedDatasetLocation {
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
        let support_bundle_dir = dataset
            .mountpoint(illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT.into());
        let path = support_bundle_dir.join("bundle");

        let f = tokio::fs::File::open(&path).await?;
        Ok(f)
    }

    pub async fn support_bundle_get(
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
                let content_type = Some("application/zip");

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

                    file.seek(std::io::SeekFrom::Start(range.start())).await?;
                    let limit = range.content_length() as usize;
                    return Ok(range_requests::make_get_response(
                        Some(range),
                        len,
                        content_type,
                        ReaderStream::new(file).take(limit),
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
                let content_type = Some("text/plain");

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
                            return Ok(response)
                        }
                    };

                if head_only {
                    return Ok(range_requests::make_head_response(
                        None,
                        entry_stream.size,
                        None,
                    )?);
                }

                return Ok(range_requests::make_get_response(
                    entry_stream.range,
                    entry_stream.size,
                    None,
                    entry_stream.stream,
                )?);
            }
        };
    }
}
