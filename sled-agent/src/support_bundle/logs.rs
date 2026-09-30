// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support Bundle interface to `sled-diagnostics` log collection.

use camino_tempfile::tempfile_in;
use dropshot::HttpError;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use range_requests::make_get_response;
use sled_agent_config_reconciler::AvailableDatasetsReceiver;
use sled_storage::dataset::U2_DEBUG_SCRATCH_DATASET_NAME;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use tokio::io::AsyncSeekExt;
use tokio_util::io::ReaderStream;

fn err_str(err: &dyn std::error::Error) -> String {
    InlineErrorChain::new(err).to_string()
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Failed to join tokio task: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Logs(#[from] sled_diagnostics::LogError),

    #[error("No storage found for temporary file storage")]
    MissingStorage,

    #[error(transparent)]
    Range(#[from] range_requests::Error),
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        HttpError::for_internal_error(err_str(&err))
    }
}

pub struct SupportBundleLogs<'a> {
    log: &'a Logger,
    available_datasets_rx: AvailableDatasetsReceiver,
}

impl<'a> SupportBundleLogs<'a> {
    pub fn new(
        log: &'a Logger,
        available_datasets_rx: AvailableDatasetsReceiver,
    ) -> Self {
        Self { log, available_datasets_rx }
    }

    /// Get a list of zones on a sled containing logs that we want to include in
    /// a support bundle.
    pub async fn zones_list(&self) -> Result<Vec<String>, Error> {
        tokio::task::spawn_blocking(move || {
            // We rely on sled-diagnostics to tell us about zones because other
            // methods within sled-agent usually do some sort of filtering and
            // we want all logs, even those in the global zone.
            sled_diagnostics::LogsHandle::get_zones()
        })
        .await
        .map_err(Error::Join)?
        .map_err(Error::Logs)
    }

    /// For a given zone and its services create a zip file of all logs
    /// found in that zone and stream it out via an `HttpResponse`.
    pub async fn get_logs_for_zone<Z>(
        &self,
        zone: Z,
        max_rotated: usize,
    ) -> Result<http::Response<dropshot::Body>, Error>
    where
        Z: Into<String>,
    {
        let dataset_path = self.dataset_for_temporary_storage().await?;
        let mut tempfile = tempfile_in(dataset_path)?;

        let log = self.log.clone();
        let zone = zone.into();

        let zip_file = {
            let handle = sled_diagnostics::LogsHandle::new(log);
            match handle.get_zone_logs(&zone, max_rotated, &mut tempfile).await
            {
                Ok(_) => Ok(tempfile),
                Err(e) => Err(e),
            }
        }
        .map_err(Error::Logs)?;

        // Since we are using a tempfile and the file path has already been
        // unlinked we need to convert our existing handle.
        let mut zip_file_async = tokio::fs::File::from_std(zip_file);
        // While we are at the end of a file seek by 0 to get its final length.
        let len = zip_file_async.seek(std::io::SeekFrom::Current(0)).await?;
        // After we have written to the zip file we need to seek back to the
        // start before streaming it out.
        zip_file_async.seek(std::io::SeekFrom::Start(0)).await?;

        const CONTENT_TYPE: http::HeaderValue =
            http::HeaderValue::from_static("application/zip");
        let content_type = Some(CONTENT_TYPE);

        // We don't actually support range requests directly because the zip
        // file is created on demand but the range-requests crate provides us
        // with a nice wrapper for streaming out the entire zip file.
        Ok(make_get_response(
            None,
            len,
            content_type,
            ReaderStream::new(zip_file_async),
        )?)
    }

    /// Attempt to find a U.2 device with the most available free space
    /// for temporary storage to assemble a zip file made up of all of the
    /// discovered zone's logs.
    ///
    /// Returns the debug scratch dataset on that device, or the debug dataset
    /// itself if the scratch dataset does not exist.
    async fn dataset_for_temporary_storage(
        &self,
    ) -> Result<camino::Utf8PathBuf, Error> {
        let mounted_debug_datasets =
            self.available_datasets_rx.all_mounted_debug_datasets();
        let storage_paths_to_size: Vec<_> = mounted_debug_datasets
            .into_iter()
            .map(|dataset_path| {
                let path = dataset_path.path;
                async move {
                    match illumos_utils::zfs::Zfs::get_value(
                        path.as_str(),
                        "available",
                    )
                    .await
                    {
                        Ok(size_str) => match size_str.parse::<usize>() {
                            Ok(size) => Some((path, size)),
                            Err(e) => {
                                warn!(
                                    &self.log,
                                    "failed to parse available size for the \
                                    dataset at path {path}";
                                    InlineErrorChain::new(&e),
                                );
                                None
                            }
                        },
                        Err(e) => {
                            warn!(
                                &self.log,
                                "failed to get available size for the  dataset \
                                at path {path}";
                                InlineErrorChain::new(&e)
                            );
                            None
                        }
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;

        let debug_path = storage_paths_to_size
            .into_iter()
            .flatten()
            .max_by_key(|(_, size)| *size)
            .map(|(dataset_path, _)| dataset_path)
            .ok_or(Error::MissingStorage)?;

        // Prefer the scratch dataset within the debug dataset. The zip file is
        // already compressed, and the debug dataset's gzip-9 compression would
        // spend CPU trying to compress it again.
        let scratch_path = debug_path.join(U2_DEBUG_SCRATCH_DATASET_NAME);
        match tokio::fs::try_exists(&scratch_path).await {
            Ok(true) => Ok(scratch_path),
            result => {
                warn!(
                    &self.log,
                    "debug scratch dataset unavailable, using the debug \
                    dataset for temporary storage";
                    "scratch_path" => %scratch_path,
                    "result" => ?result,
                );
                Ok(debug_path)
            }
        }
    }
}

#[cfg(all(target_os = "illumos", test))]
mod illumos_tests {
    use super::*;
    use illumos_utils::zfs::Zfs;
    use omicron_test_utils::dev::test_setup_log;
    use sled_storage::dataset::U2_DEBUG_DATASET;
    use zfs_test_harness::ZfsTestHarness;

    #[tokio::test]
    async fn temporary_storage_uses_debug_scratch_dataset() {
        let logctx =
            test_setup_log("temporary_storage_uses_debug_scratch_dataset");
        let log = &logctx.log;

        // Setting up a U.2 creates the scratch dataset within its debug
        // dataset, using lz4 compression.
        let mut harness = ZfsTestHarness::new(log.clone());
        harness.add_external_disks(1).await;
        let zpool = *harness.all_external_zpools().next().unwrap();
        let scratch_dataset =
            format!("{U2_DEBUG_DATASET}/{U2_DEBUG_SCRATCH_DATASET_NAME}");
        let compression = Zfs::get_value(
            &format!("{zpool}/{scratch_dataset}"),
            "compression",
        )
        .await
        .expect("Should have been able to read compression");
        assert_eq!(compression, "lz4");

        // Log collection picks the scratch dataset for its temporary files.
        let root = &harness.mount_config().root;
        let available_datasets_rx = AvailableDatasetsReceiver::fake_static(
            std::iter::once((zpool, zpool.dataset_mountpoint(root, ""))),
        );
        let logs = SupportBundleLogs::new(log, available_datasets_rx);
        let path = logs
            .dataset_for_temporary_storage()
            .await
            .expect("Should have found temporary storage");
        assert_eq!(path, zpool.dataset_mountpoint(root, &scratch_dataset));
        tempfile_in(&path).expect("Should be able to create a tempfile");

        harness.cleanup();
        logctx.cleanup_successful();
    }
}
