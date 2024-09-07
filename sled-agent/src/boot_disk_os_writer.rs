// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module provides `BootDiskOsWriter`, via which sled-agent can write new
//! OS images to its boot disks.

use async_trait::async_trait;
use bytes::Bytes;
use camino::Utf8PathBuf;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use futures::Stream;
use futures::TryStreamExt;
use installinator_common::RawDiskWriter;
use omicron_common::disk::M2Slot;
use sha3::Digest;
use sha3::Sha3_256;
use sled_agent_types::boot_disk::BootDiskOsWriteProgress;
use sled_agent_types::boot_disk::BootDiskOsWriteStatus;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::watch;
use uuid::Uuid;

fn to_boot_disk_status(
    update_id: Uuid,
    result: &Result<(), Arc<BootDiskOsWriteError>>,
) -> BootDiskOsWriteStatus {
    match result {
        Ok(()) => BootDiskOsWriteStatus::Complete { update_id },
        Err(err) => BootDiskOsWriteStatus::Failed {
            update_id,
            message: DisplayErrorChain::new(err).to_string(),
        },
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BootDiskOsWriteError {
    // This variant should be impossible in production, as we build with
    // panic=abort, but may be constructed in tests (e.g., during tokio runtime
    // shutdown).
    #[error("internal error (task panic)")]
    TaskPanic,
    #[error("an update is still running ({0})")]
    UpdateRunning(Uuid),
    #[error("a previous update completed ({0}); clear its status before starting a new update")]
    CannotStartWithoutClearingPreviousStatus(Uuid),
    #[error("failed to create temporary file")]
    FailedCreatingTempfile(#[source] io::Error),
    #[error("failed writing to temporary file")]
    FailedWritingTempfile(#[source] io::Error),
    #[error("failed downloading image from HTTP client")]
    FailedDownloadingImage(#[source] HttpError),
    #[error("hash mismatch in image from HTTP client: expected {expected} but got {got}")]
    UploadedImageHashMismatch { expected: String, got: String },
    #[error("failed to open disk for writing {path}")]
    FailedOpenDiskForWrite {
        #[source]
        error: io::Error,
        path: Utf8PathBuf,
    },
    #[error("image size ({image_size}) is not a multiple of disk block size ({disk_block_size})")]
    ImageSizeNotMultipleOfBlockSize {
        image_size: usize,
        disk_block_size: usize,
    },
    #[error("failed reading from temporary file")]
    FailedReadingTempfile(#[source] io::Error),
    #[error("failed writing to disk {path}")]
    FailedWritingDisk {
        #[source]
        error: io::Error,
        path: Utf8PathBuf,
    },
    #[error("failed to open disk for reading {path}")]
    FailedOpenDiskForRead {
        #[source]
        error: io::Error,
        path: Utf8PathBuf,
    },
    #[error("failed reading from disk {path}")]
    FailedReadingDisk {
        #[source]
        error: io::Error,
        path: Utf8PathBuf,
    },
    #[error("hash mismatch after writing disk {path}: expected {expected} but got {got}")]
    WrittenImageHashMismatch {
        path: Utf8PathBuf,
        expected: String,
        got: String,
    },
    #[error("unexpected update ID {0}: cannot clear status")]
    WrongUpdateIdClearingStatus(Uuid),
}

impl From<&BootDiskOsWriteError> for HttpError {
    fn from(error: &BootDiskOsWriteError) -> Self {
        let message = DisplayErrorChain::new(error).to_string();
        match error {
            BootDiskOsWriteError::UpdateRunning(_)
            | BootDiskOsWriteError::CannotStartWithoutClearingPreviousStatus(
                _,
            )
            | BootDiskOsWriteError::FailedDownloadingImage(_)
            | BootDiskOsWriteError::UploadedImageHashMismatch { .. }
            | BootDiskOsWriteError::ImageSizeNotMultipleOfBlockSize {
                ..
            }
            | BootDiskOsWriteError::WrongUpdateIdClearingStatus(_) => {
                HttpError::for_bad_request(None, message)
            }
            BootDiskOsWriteError::TaskPanic
            | BootDiskOsWriteError::FailedCreatingTempfile(_)
            | BootDiskOsWriteError::FailedWritingTempfile(_)
            | BootDiskOsWriteError::FailedReadingTempfile(_)
            | BootDiskOsWriteError::FailedOpenDiskForWrite { .. }
            | BootDiskOsWriteError::FailedOpenDiskForRead { .. }
            | BootDiskOsWriteError::FailedWritingDisk { .. }
            | BootDiskOsWriteError::FailedReadingDisk { .. }
            | BootDiskOsWriteError::WrittenImageHashMismatch { .. } => {
                HttpError {
                    status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                    error_code: None,
                    external_message: message.clone(),
                    internal_message: message,
                }
            }
        }
    }
}

// Note to future maintainers: `installinator` uses the `update_engine` crate to
// drive its process (which includes writing the boot disk). We could also use
// `update_engine` inside `BootDiskOsWriter`; instead, we've hand-rolled a state
// machine with manual progress reporting. The current implementation is
// _probably_ simple enough that this was a reasonable choice, but if it becomes
// more complex (or if additional work needs to be done that `update_engine`
// would make easier), consider switching it over.
#[derive(Debug)]
pub(crate) struct BootDiskOsWriter {
    // Note: We use a std Mutex here to avoid cancellation issues with tokio
    // Mutex. We never need to keep the lock held longer than necessary to copy
    // or replace the current writer state.
    states: Mutex<BTreeMap<M2Slot, WriterState>>,
    log: Logger,
}

impl BootDiskOsWriter {
    pub(crate) fn new(log: &Logger) -> Self {
        Self {
            states: Mutex::default(),
            log: log.new(slog::o!("component" => "BootDiskOsWriter")),
        }
    }

    /// Attempt to start a new update to the given disk (identified by both its
    /// slot and the path to its devfs device).
    ///
    /// This method will return after the `image_upload` stream has been saved
    /// to a local temporary file, but before the update has completed. Callers
    /// must poll `status()` to discover when the running update completes (or
    /// fails).
    ///
    /// # Errors
    ///
    /// This method will return an error and not start an update if any of the
    /// following are true:
    ///
    /// * A previously-started update of this same `boot_disk` is still running
    /// * A previously-completed update has not had its status cleared
    /// * The `image_upload` stream returns an error
    /// * The hash of the data provided by `image_upload` does not match
    ///   `sha3_256_digest`
    /// * Any of a variety of I/O errors occurs while copying from
    ///   `image_upload` to a temporary file
    ///
    /// In all but the first case, the error returned will also be saved and
    /// returned when `status()` is called (until another update is started).
    pub(crate) async fn start_update<S>(
        &self,
        boot_disk: M2Slot,
        disk_devfs_path: Utf8PathBuf,
        update_id: Uuid,
        sha3_256_digest: [u8; 32],
        image_upload: S,
    ) -> Result<(), Arc<BootDiskOsWriteError>>
    where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
    {
        self.start_update_impl(
            boot_disk,
            disk_devfs_path,
            update_id,
            sha3_256_digest,
            image_upload,
            RealDiskInterface {},
        )
        .await
    }

    async fn start_update_impl<S, Writer>(
        &self,
        boot_disk: M2Slot,
        disk_devfs_path: Utf8PathBuf,
        update_id: Uuid,
        sha3_256_digest: [u8; 32],
        image_upload: S,
        disk_writer: Writer,
    ) -> Result<(), Arc<BootDiskOsWriteError>>
    where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
        Writer: DiskInterface + Send + Sync + 'static,
    {
        // Construct a closure that will spawn a task to drive this update, but
        // don't actually start it yet: we only allow an update to start if
        // there's not currently an update running targetting the same slot, so
        // we'll spawn this after checking that below.
        let spawn_update_task = || {
            let (uploaded_image_tx, uploaded_image_rx) = oneshot::channel();
            let (progress_tx, progress_rx) = watch::channel(
                BootDiskOsWriteProgress::ReceivingUploadedImage {
                    bytes_received: 0,
                },
            );
            let (complete_tx, complete_rx) = oneshot::channel();
            let task = BootDiskOsWriteTask {
                log: self
                    .log
                    .new(slog::o!("update_id" => update_id.to_string())),
                disk_devfs_path,
                sha3_256_digest,
                progress_tx,
                disk_interface: disk_writer,
            };
            tokio::spawn(task.run(
                image_upload,
                uploaded_image_tx,
                complete_tx,
            ));
            (
                uploaded_image_rx,
                TaskRunningState { update_id, progress_rx, complete_rx },
            )
        };

        // Either call `spawn_update_task` and get back the handle to
        // `uploaded_image_rx`, or return an error (if another update for this
        // boot disk is still running).
        let uploaded_image_rx = {
            let mut states = self.states.lock().unwrap();
            match states.entry(boot_disk) {
                Entry::Vacant(slot) => {
                    let (uploaded_image_rx, running) = spawn_update_task();
                    slot.insert(WriterState::TaskRunning(running));
                    uploaded_image_rx
                }
                Entry::Occupied(mut slot) => match slot.get_mut() {
                    WriterState::TaskRunning(running) => {
                        // It's possible this task is actually complete and a
                        // result is sitting in the `running.complete_rx`
                        // oneshot, but for the purposes of starting a new
                        // update it doesn't matter either way: we'll refuse to
                        // start. Return the "another update running" error; the
                        // caller will have to check the `status()`, which will
                        // trigger a "see if it's actually done after all"
                        // check.
                        return Err(Arc::new(
                            BootDiskOsWriteError::UpdateRunning(
                                running.update_id,
                            ),
                        ));
                    }
                    WriterState::Complete(complete) => {
                        return Err(Arc::new(
                            BootDiskOsWriteError::CannotStartWithoutClearingPreviousStatus(
                                complete.update_id,
                        )));
                    }
                },
            }
        };

        // We've now spawned a task to drive the update, and we want to wait for
        // it to finish copying from `image_upload`.
        uploaded_image_rx.await.map_err(|_| BootDiskOsWriteError::TaskPanic)?
    }

    /// Clear the status of a finished or failed update with the given ID
    /// targetting `boot_disk`.
    ///
    /// If no update has ever been started for this `boot_disk`, returns
    /// `Ok(())`.
    ///
    /// # Errors
    ///
    /// Fails if an update to `boot_disk` is currently running; only terminal
    /// statuses can be cleared. Fails if the most recent terminal status
    /// targetting `boot_disk` had a different update ID.
    pub(crate) fn clear_terminal_status(
        &self,
        boot_disk: M2Slot,
        update_id: Uuid,
    ) -> Result<(), BootDiskOsWriteError> {
        let mut states = self.states.lock().unwrap();
        let mut slot = match states.entry(boot_disk) {
            // No status; nothing to clear.
            Entry::Vacant(_slot) => return Ok(()),
            Entry::Occupied(slot) => slot,
        };

        match slot.get_mut() {
            WriterState::Complete(complete) => {
                if complete.update_id == update_id {
                    slot.remove();
                    Ok(())
                } else {
                    Err(BootDiskOsWriteError::WrongUpdateIdClearingStatus(
                        complete.update_id,
                    ))
                }
            }
            WriterState::TaskRunning(running) => {
                // Check whether the task is _actually_ still running,
                // or whether it's done and just waiting for us to
                // realize it.
                match running.complete_rx.try_recv() {
                    Ok(result) => {
                        if running.update_id == update_id {
                            // This is a little surprising but legal: we've been
                            // asked to clear the terminal status of this
                            // update_id, even though we just realized it
                            // finished.
                            slot.remove();
                            Ok(())
                        } else {
                            let running_update_id = running.update_id;
                            // A different update just finished; store the
                            // result we got from the oneshot and don't remove
                            // the status.
                            slot.insert(WriterState::Complete(
                                TaskCompleteState {
                                    update_id: running_update_id,
                                    result,
                                },
                            ));
                            Err(BootDiskOsWriteError::WrongUpdateIdClearingStatus(
                                running_update_id
                            ))
                        }
                    }
                    Err(TryRecvError::Empty) => Err(
                        BootDiskOsWriteError::UpdateRunning(running.update_id),
                    ),
                    Err(TryRecvError::Closed) => {
                        Err(BootDiskOsWriteError::TaskPanic)
                    }
                }
            }
        }
    }

    /// Get the status of any update running that targets `boot_disk`.
    pub(crate) fn status(&self, boot_disk: M2Slot) -> BootDiskOsWriteStatus {
        let mut states = self.states.lock().unwrap();
        let mut slot = match states.entry(boot_disk) {
            Entry::Vacant(_) => return BootDiskOsWriteStatus::NoUpdateStarted,
            Entry::Occupied(slot) => slot,
        };

        match slot.get_mut() {
            WriterState::TaskRunning(running) => {
                // Is the task actually still running? Check and see if it's
                // sent us a result that we just haven't noticed yet.
                match running.complete_rx.try_recv() {
                    Ok(result) => {
                        let update_id = running.update_id;
                        let status = to_boot_disk_status(update_id, &result);
                        slot.insert(WriterState::Complete(TaskCompleteState {
                            update_id,
                            result,
                        }));
                        status
                    }
                    Err(TryRecvError::Empty) => {
                        let progress = *running.progress_rx.borrow_and_update();
                        BootDiskOsWriteStatus::InProgress {
                            update_id: running.update_id,
                            progress,
                        }
                    }
                    Err(TryRecvError::Closed) => {
                        let update_id = running.update_id;
                        let result =
                            Err(Arc::new(BootDiskOsWriteError::TaskPanic));
                        let status = to_boot_disk_status(update_id, &result);
                        slot.insert(WriterState::Complete(TaskCompleteState {
                            update_id,
                            result,
                        }));
                        status
                    }
                }
            }
            WriterState::Complete(complete) => {
                to_boot_disk_status(complete.update_id, &complete.result)
            }
        }
    }
}

#[derive(Debug)]
enum WriterState {
    /// A task is running to write a new image to a boot disk.
    TaskRunning(TaskRunningState),
    /// The result of the most recent write.
    Complete(TaskCompleteState),
}

#[derive(Debug)]
struct TaskRunningState {
    update_id: Uuid,
    progress_rx: watch::Receiver<BootDiskOsWriteProgress>,
    complete_rx: oneshot::Receiver<Result<(), Arc<BootDiskOsWriteError>>>,
}

#[derive(Debug)]
struct TaskCompleteState {
    update_id: Uuid,
    result: Result<(), Arc<BootDiskOsWriteError>>,
}

#[derive(Debug)]
struct BootDiskOsWriteTask<W> {
    log: Logger,
    sha3_256_digest: [u8; 32],
    disk_devfs_path: Utf8PathBuf,
    progress_tx: watch::Sender<BootDiskOsWriteProgress>,
    disk_interface: W,
}

impl<W: DiskInterface> BootDiskOsWriteTask<W> {
    async fn run<S>(
        self,
        image_upload: S,
        uploaded_image_tx: oneshot::Sender<
            Result<(), Arc<BootDiskOsWriteError>>,
        >,
        complete_tx: oneshot::Sender<Result<(), Arc<BootDiskOsWriteError>>>,
    ) where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
    {
        let result = self.run_impl(image_upload, uploaded_image_tx).await;

        // It's possible (albeit unlikely) our caller has discarded the receive
        // half of this channel; ignore any send error.
        _ = complete_tx.send(result);
    }

    async fn run_impl<S>(
        self,
        image_upload: S,
        uploaded_image_tx: oneshot::Sender<
            Result<(), Arc<BootDiskOsWriteError>>,
        >,
    ) -> Result<(), Arc<BootDiskOsWriteError>>
    where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
    {
        // Copy from `image_upload` into a tempfile, then report the result on
        // `uploaded_image_tx`. Our dropshot server will not respond to the
        // client that requested this update until we finish this step and send
        // a response on `uploaded_image_tx`, as `image_upload` is the
        // `StreamingBody` attached to their request.
        //
        // If this step fails, we will send the error to the client who sent the
        // request _and_ store a copy of the same error in our current update
        // state.
        let (image_tempfile, image_size) = match self
            .download_body_to_tempfile(image_upload)
            .await
            .map_err(Arc::new)
        {
            Ok(tempfile) => {
                _ = uploaded_image_tx.send(Ok(()));
                tempfile
            }
            Err(err) => {
                _ = uploaded_image_tx.send(Err(Arc::clone(&err)));
                return Err(err);
            }
        };

        let disk_block_size = self
            .copy_tempfile_to_disk(image_tempfile, image_size)
            .await
            .map_err(Arc::new)?;

        self.validate_written_image(image_size, disk_block_size)
            .await
            .map_err(Arc::new)?;

        Ok(())
    }

    async fn download_body_to_tempfile<S>(
        &self,
        image_upload: S,
    ) -> Result<(File, usize), BootDiskOsWriteError>
    where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
    {
        let tempfile = camino_tempfile::tempfile()
            .map_err(BootDiskOsWriteError::FailedCreatingTempfile)?;

        let mut tempfile =
            tokio::io::BufWriter::new(tokio::fs::File::from_std(tempfile));

        let mut image_upload = std::pin::pin!(image_upload);
        let mut hasher = Sha3_256::default();
        let mut bytes_received = 0;

        // Stream the uploaded image into our tempfile.
        while let Some(bytes) = image_upload
            .try_next()
            .await
            .map_err(BootDiskOsWriteError::FailedDownloadingImage)?
        {
            hasher.update(&bytes);
            tempfile
                .write_all(&bytes)
                .await
                .map_err(BootDiskOsWriteError::FailedWritingTempfile)?;
            bytes_received += bytes.len();
            self.progress_tx.send_modify(|progress| {
                *progress = BootDiskOsWriteProgress::ReceivingUploadedImage {
                    bytes_received,
                }
            });
        }

        // Flush any remaining buffered data.
        tempfile
            .flush()
            .await
            .map_err(BootDiskOsWriteError::FailedWritingTempfile)?;

        // Rewind the tempfile.
        let mut tempfile = tempfile.into_inner();
        tempfile
            .rewind()
            .await
            .map_err(BootDiskOsWriteError::FailedWritingTempfile)?;

        // Ensure the data the client sent us matches the hash they also sent
        // us. A failure here means either the client lied or something has gone
        // horribly wrong.
        let hash = hasher.finalize();
        let expected_hash_str = hex::encode(&self.sha3_256_digest);
        if hash == self.sha3_256_digest.into() {
            info!(
                self.log, "received uploaded image";
                "bytes_received" => bytes_received,
                "hash" => expected_hash_str,
            );

            Ok((tempfile, bytes_received))
        } else {
            let computed_hash_str = hex::encode(&hash);
            error!(
                self.log, "received uploaded image: incorrect hash";
                "bytes_received" => bytes_received,
                "computed_hash" => &computed_hash_str,
                "expected_hash" => &expected_hash_str,
            );

            Err(BootDiskOsWriteError::UploadedImageHashMismatch {
                expected: expected_hash_str,
                got: computed_hash_str,
            })
        }
    }

    /// Copy from `image_tempfile` to the disk device at `self.disk_devfs_path`.
    /// Returns the block size of that disk.
    async fn copy_tempfile_to_disk(
        &self,
        image_tempfile: File,
        image_size: usize,
    ) -> Result<usize, BootDiskOsWriteError> {
        let mut disk_writer = self
            .disk_interface
            .open_writer(self.disk_devfs_path.as_std_path())
            .await
            .map_err(|error| BootDiskOsWriteError::FailedOpenDiskForWrite {
                error,
                path: self.disk_devfs_path.clone(),
            })?;

        let disk_block_size = disk_writer.block_size();

        if image_size % disk_block_size != 0 {
            return Err(
                BootDiskOsWriteError::ImageSizeNotMultipleOfBlockSize {
                    image_size,
                    disk_block_size,
                },
            );
        }
        let num_blocks = image_size / disk_block_size;

        let mut buf = vec![0; disk_block_size];
        let mut image_tempfile = BufReader::new(image_tempfile);

        for block in 0..num_blocks {
            image_tempfile
                .read_exact(&mut buf)
                .await
                .map_err(BootDiskOsWriteError::FailedReadingTempfile)?;

            disk_writer.write_all(&buf).await.map_err(|error| {
                BootDiskOsWriteError::FailedWritingDisk {
                    error,
                    path: self.disk_devfs_path.clone(),
                }
            })?;

            self.progress_tx.send_modify(|progress| {
                *progress = BootDiskOsWriteProgress::WritingImageToDisk {
                    bytes_written: (block + 1) * buf.len(),
                }
            });
        }

        disk_writer.finalize().await.map_err(|error| {
            BootDiskOsWriteError::FailedWritingDisk {
                error,
                path: self.disk_devfs_path.clone(),
            }
        })?;

        info!(
            self.log, "copied OS image to disk";
            "path" => %self.disk_devfs_path,
            "bytes_written" => image_size,
        );

        Ok(disk_block_size)
    }

    async fn validate_written_image(
        self,
        image_size: usize,
        disk_block_size: usize,
    ) -> Result<(), BootDiskOsWriteError> {
        // We're reading the OS image back from disk and hashing it; this can
        // all be synchronous inside a spawn_blocking.
        tokio::task::spawn_blocking(move || {
            let mut f = self
                .disk_interface
                .open_reader(self.disk_devfs_path.as_std_path())
                .map_err(|error| {
                    BootDiskOsWriteError::FailedOpenDiskForRead {
                        error,
                        path: self.disk_devfs_path.clone(),
                    }
                })?;

            let mut buf = vec![0; disk_block_size];
            let mut hasher = Sha3_256::default();
            let mut bytes_read = 0;

            while bytes_read < image_size {
                // We already confirmed while writing the image that the image
                // size is an exact multiple of the disk block size, so we can
                // always read a full `buf` here.
                f.read_exact(&mut buf).map_err(|error| {
                    BootDiskOsWriteError::FailedReadingDisk {
                        error,
                        path: self.disk_devfs_path.clone(),
                    }
                })?;

                hasher.update(&buf);
                bytes_read += buf.len();
                self.progress_tx.send_modify(|progress| {
                    *progress =
                        BootDiskOsWriteProgress::ValidatingWrittenImage {
                            bytes_read,
                        };
                });
            }

            let expected_hash_str = hex::encode(&self.sha3_256_digest);
            let hash = hasher.finalize();
            if hash == self.sha3_256_digest.into() {
                info!(
                    self.log, "validated OS image written to disk";
                    "path" => %self.disk_devfs_path,
                    "hash" => expected_hash_str,
                );
                Ok(())
            } else {
                let computed_hash_str = hex::encode(&hash);
                error!(
                    self.log, "failed to validate written OS image";
                    "bytes_hashed" => image_size,
                    "computed_hash" => &computed_hash_str,
                    "expected_hash" => &expected_hash_str,
                );
                Err(BootDiskOsWriteError::WrittenImageHashMismatch {
                    path: self.disk_devfs_path,
                    expected: expected_hash_str,
                    got: computed_hash_str,
                })
            }
        })
        .await
        .expect("blocking task panicked")
    }
}

// Utility traits to allow injecting an in-memory "disk" for unit tests.
#[async_trait]
trait DiskWriter: AsyncWrite + Send + Sized + Unpin {
    fn block_size(&self) -> usize;
    async fn finalize(self) -> io::Result<()>;
}
#[async_trait]
trait DiskInterface: Send + Sync + 'static {
    type Writer: DiskWriter;
    type Reader: io::Read + Send;
    async fn open_writer(&self, path: &Path) -> io::Result<Self::Writer>;
    fn open_reader(&self, path: &Path) -> io::Result<Self::Reader>;
}

#[async_trait]
impl DiskWriter for RawDiskWriter {
    fn block_size(&self) -> usize {
        RawDiskWriter::block_size(self)
    }

    async fn finalize(self) -> io::Result<()> {
        RawDiskWriter::finalize(self).await
    }
}

struct RealDiskInterface {}

#[async_trait]
impl DiskInterface for RealDiskInterface {
    type Writer = RawDiskWriter;
    type Reader = std::fs::File;

    async fn open_writer(&self, path: &Path) -> io::Result<Self::Writer> {
        RawDiskWriter::open(path).await
    }

    fn open_reader(&self, path: &Path) -> io::Result<Self::Reader> {
        std::fs::File::open(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use futures::stream;
    use installinator_common::BlockSizeBufWriter;
    use omicron_test_utils::dev::test_setup_log;
    use rand::RngCore;
    use std::mem;
    use std::pin::Pin;
    use std::task::ready;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::sync::Semaphore;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_util::sync::PollSemaphore;

    // Most of the tests below end up looping while calling
    // `BootDiskOsWriter::status()` waiting for a specific status message to
    // arrive. If we get that wrong (or the code under test is wrong!), that
    // could end up looping forever, so we run all the relevant bits of the
    // tests under a tokio timeout. We expect all the tests to complete very
    // quickly in general (< 1 second), so we'll pick something
    // outrageously-long-enough that if we hit it, we're almost certainly
    // dealing with a hung test.
    const TEST_TIMEOUT: Duration = Duration::from_secs(30);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct InMemoryDiskContents {
        path: Utf8PathBuf,
        data: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    struct InMemoryDiskInterface {
        semaphore: Arc<Semaphore>,
        finalized_writes: Arc<Mutex<Vec<InMemoryDiskContents>>>,
    }

    impl InMemoryDiskInterface {
        const BLOCK_SIZE: usize = 16;

        fn new(semaphore: Semaphore) -> Self {
            Self {
                semaphore: Arc::new(semaphore),
                finalized_writes: Arc::default(),
            }
        }
    }

    #[async_trait]
    impl DiskInterface for InMemoryDiskInterface {
        type Writer = InMemoryDiskWriter;
        type Reader = io::Cursor<Vec<u8>>;

        async fn open_writer(&self, path: &Path) -> io::Result<Self::Writer> {
            Ok(InMemoryDiskWriter {
                opened_path: path
                    .to_owned()
                    .try_into()
                    .expect("non-utf8 test path"),
                data: BlockSizeBufWriter::with_block_size(
                    Self::BLOCK_SIZE,
                    Vec::new(),
                ),
                semaphore: PollSemaphore::new(Arc::clone(&self.semaphore)),
                finalized_writes: Arc::clone(&self.finalized_writes),
            })
        }

        fn open_reader(&self, path: &Path) -> io::Result<Self::Reader> {
            let written_files = self.finalized_writes.lock().unwrap();
            for contents in written_files.iter() {
                if contents.path == path {
                    return Ok(io::Cursor::new(contents.data.clone()));
                }
            }
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("no written file for {}", path.display()),
            ))
        }
    }

    struct InMemoryDiskWriter {
        opened_path: Utf8PathBuf,
        data: BlockSizeBufWriter<Vec<u8>>,
        semaphore: PollSemaphore,
        finalized_writes: Arc<Mutex<Vec<InMemoryDiskContents>>>,
    }

    #[async_trait]
    impl DiskWriter for InMemoryDiskWriter {
        fn block_size(&self) -> usize {
            self.data.block_size()
        }

        async fn finalize(mut self) -> io::Result<()> {
            self.data.flush().await?;

            let mut finalized = self.finalized_writes.lock().unwrap();
            finalized.push(InMemoryDiskContents {
                path: self.opened_path,
                data: self.data.into_inner(),
            });

            Ok(())
        }
    }

    impl AsyncWrite for InMemoryDiskWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let permit = match ready!(self.semaphore.poll_acquire(cx)) {
                Some(permit) => permit,
                None => panic!("test semaphore closed"),
            };
            let result = Pin::new(&mut self.data).poll_write(cx, buf);
            permit.forget();
            result
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.data).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.data).poll_shutdown(cx)
        }
    }

    fn expect_in_progress(
        status: BootDiskOsWriteStatus,
    ) -> BootDiskOsWriteProgress {
        let BootDiskOsWriteStatus::InProgress { progress, .. } = status else {
            panic!("expected Status::InProgress; got {status:?}");
        };
        progress
    }

    #[tokio::test]
    async fn boot_disk_os_writer_delivers_upload_progress_and_rejects_bad_hashes(
    ) {
        let logctx =
            test_setup_log("boot_disk_os_writer_delivers_upload_progress_and_rejects_bad_hashes");

        let writer = Arc::new(BootDiskOsWriter::new(&logctx.log));
        let boot_disk = M2Slot::A;

        // We'll give the writer an intentionally-wrong sha3 digest and confirm
        // it rejects the upload based on this.
        let claimed_sha3_digest = [0; 32];

        // Construct an in-memory stream around an mpsc channel as our client
        // upload.
        let (upload_tx, upload_rx) = mpsc::unbounded_channel();

        // Spawn the `start_update` onto a background task; this won't end until
        // we close (or send an error on) `upload_tx`.
        let start_update_task = {
            let writer = Arc::clone(&writer);
            tokio::spawn(async move {
                writer
                    .start_update(
                        boot_disk,
                        "/does-not-matter".into(),
                        Uuid::new_v4(),
                        claimed_sha3_digest,
                        UnboundedReceiverStream::new(upload_rx),
                    )
                    .await
            })
        };

        // As we stream data in, we'll compute the actual hash to check against
        // the error we expect to see.
        let mut actual_data_hasher = Sha3_256::new();

        // Run the rest of the test under a timeout to catch any incorrect
        // assumptions that result in a hang.
        tokio::time::timeout(TEST_TIMEOUT, async move {
            // We're racing `writer`'s spawning of the actual update task; spin
            // until we transition from "no update" to "receiving uploaded
            // image".
            loop {
                match writer.status(boot_disk) {
                    BootDiskOsWriteStatus::NoUpdateStarted => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    BootDiskOsWriteStatus::InProgress { progress, .. } => {
                        assert_eq!(
                            progress,
                            BootDiskOsWriteProgress::ReceivingUploadedImage {
                                bytes_received: 0
                            }
                        );
                        break;
                    }
                    status @ (BootDiskOsWriteStatus::Complete { .. }
                    | BootDiskOsWriteStatus::Failed { .. }) => {
                        panic!("unexpected status {status:?}")
                    }
                }
            }

            let mut prev_bytes_received = 0;

            // Send a few chunks of data. After each, we're racing with `writer`
            // which has to copy that data to a temp file before the status will
            // change, so loop until we see what we expect. Our TEST_TIMEOUT
            // ensures we don't stay here forever if something goes wrong.
            for i in 1..=10 {
                let data_len = i * 100;
                let chunk = vec![0; data_len];
                actual_data_hasher.update(&chunk);
                upload_tx.send(Ok(Bytes::from(chunk))).unwrap();

                loop {
                    let progress = expect_in_progress(writer.status(boot_disk));

                    // If we lost the race, the status is still what it was
                    // previously; sleep briefly and check again.
                    if progress
                        == (BootDiskOsWriteProgress::ReceivingUploadedImage {
                            bytes_received: prev_bytes_received,
                        })
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }

                    // It's not the old status; it should be exactly the new
                    // status. If it is, update our count and break out of this
                    // inner loop.
                    assert_eq!(
                        progress,
                        BootDiskOsWriteProgress::ReceivingUploadedImage {
                            bytes_received: prev_bytes_received + data_len
                        }
                    );
                    prev_bytes_received += data_len;
                    println!("chunk {i}: got {progress:?}");
                    break;
                }
            }

            // Close the channel; `writer` should recognize the upload is
            // complete, then realize there's a hash mismatch and fail the
            // request.
            mem::drop(upload_tx);

            // We expect to see an upload hash mismatch error with these hex
            // strings.
            let expected_hash = hex::encode(claimed_sha3_digest);
            let got_hash = hex::encode(actual_data_hasher.finalize());

            let start_update_result = start_update_task.await.unwrap();
            let error = start_update_result.unwrap_err();
            match &*error {
                BootDiskOsWriteError::UploadedImageHashMismatch {
                    expected,
                    got,
                } => {
                    assert_eq!(*got, got_hash);
                    assert_eq!(*expected, expected_hash);
                }
                _ => panic!("unexpected error {error:?}"),
            }

            // The same error should be present in the current update status.
            let expected_error =
                BootDiskOsWriteError::UploadedImageHashMismatch {
                    expected: expected_hash.clone(),
                    got: got_hash.clone(),
                };
            let status = writer.status(boot_disk);
            match status {
                BootDiskOsWriteStatus::Failed { message, .. } => {
                    assert_eq!(
                        message,
                        DisplayErrorChain::new(&expected_error).to_string()
                    );
                }
                BootDiskOsWriteStatus::NoUpdateStarted
                | BootDiskOsWriteStatus::InProgress { .. }
                | BootDiskOsWriteStatus::Complete { .. } => {
                    panic!("unexpected status {status:?}")
                }
            }
        })
        .await
        .unwrap();

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn boot_disk_os_writer_writes_data_to_disk() {
        let logctx = test_setup_log("boot_disk_os_writer_writes_data_to_disk");

        // generate a small, random "OS image" consisting of 10 "blocks"
        let num_data_blocks = 10;
        let data_len = num_data_blocks * InMemoryDiskInterface::BLOCK_SIZE;
        let mut data = vec![0; data_len];
        rand::thread_rng().fill_bytes(&mut data);
        let data_hash = Sha3_256::digest(&data);

        // generate a disk writer with a 0-permit semaphore; we'll inject
        // permits in the main loop below to force single-stepping through
        // writing the data
        let inject_disk_interface =
            InMemoryDiskInterface::new(Semaphore::new(0));
        let shared_semaphore = Arc::clone(&inject_disk_interface.semaphore);

        let writer = Arc::new(BootDiskOsWriter::new(&logctx.log));
        let boot_disk = M2Slot::A;
        let disk_devfs_path = "/unit-test/disk";

        writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                Uuid::new_v4(),
                data_hash.into(),
                stream::once(future::ready(Ok(Bytes::from(data.clone())))),
                inject_disk_interface,
            )
            .await
            .unwrap();

        // Run the rest of the test under a timeout to catch any incorrect
        // assumptions that result in a hang.
        tokio::time::timeout(TEST_TIMEOUT, async move {
            // Wait until `writer` has copied our data into a temp file
            loop {
                let progress = expect_in_progress(writer.status(boot_disk));
                match progress {
                    BootDiskOsWriteProgress::ReceivingUploadedImage {
                        bytes_received,
                    } => {
                        if bytes_received == data.len() {
                            break;
                        } else {
                            println!(
                                "got status with {} bytes received",
                                bytes_received
                            );
                        }
                    }
                    _ => panic!("unexpected progress {progress:?}"),
                }
            }

            for i in 0..num_data_blocks {
                // Add one permit to our shared semaphore, allowing one block of
                // data to be written to the "disk".
                shared_semaphore.add_permits(1);

                // Did we just release the write of the final block? If so,
                // break; we'll wait for completion below.
                if i + 1 == num_data_blocks {
                    break;
                }

                // Wait until we see the status we expect for a not-yet-last
                // block (i.e., that the disk is still being written).
                loop {
                    let progress = expect_in_progress(writer.status(boot_disk));
                    match progress {
                        BootDiskOsWriteProgress::WritingImageToDisk {
                            bytes_written,
                        } if (i + 1) * InMemoryDiskInterface::BLOCK_SIZE
                            == bytes_written =>
                        {
                            println!("saw expected progress for block {i}");
                            break;
                        }
                        _ => {
                            // This is not an error: we could still be in
                            // `ReceivingUploadedImage` or the previous
                            // block's `WritingImageToDisk`
                            println!("saw irrelevant progress {progress:?}");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    }
                }
            }

            // The last block is being or has been written, and after that the
            // writer will reread it to validate the hash. We won't bother
            // repeating the same machinery to check each step of that process;
            // we'll just wait for the eventual successful completion.
            loop {
                let status = writer.status(boot_disk);
                match status {
                    BootDiskOsWriteStatus::Complete { .. } => break,
                    BootDiskOsWriteStatus::InProgress { .. } => {
                        println!("saw irrelevant progress {status:?}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    BootDiskOsWriteStatus::NoUpdateStarted
                    | BootDiskOsWriteStatus::Failed { .. } => {
                        panic!("unexpected status {status:?}")
                    }
                }
            }
        })
        .await
        .unwrap();

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn boot_disk_os_writer_fails_if_reading_from_disk_doesnt_match() {
        let logctx = test_setup_log(
            "boot_disk_os_writer_fails_if_reading_from_disk_doesnt_match",
        );

        // generate a small, random "OS image" consisting of 10 "blocks"
        let num_data_blocks = 10;
        let data_len = num_data_blocks * InMemoryDiskInterface::BLOCK_SIZE;
        let mut data = vec![0; data_len];
        rand::thread_rng().fill_bytes(&mut data);
        let original_data_hash = Sha3_256::digest(&data);

        // generate a disk writer with (effectively) unlimited semaphore
        // permits, since we don't need to throttle the "disk writing"
        let inject_disk_interface =
            InMemoryDiskInterface::new(Semaphore::new(Semaphore::MAX_PERMITS));

        let writer = Arc::new(BootDiskOsWriter::new(&logctx.log));
        let boot_disk = M2Slot::A;
        let disk_devfs_path = "/unit-test/disk";

        // copy the data and corrupt it, then stage this in
        // `inject_disk_interface` so that it returns this corrupted data when
        // "reading" the disk
        let mut bad_data = data.clone();
        bad_data[0] ^= 1; // bit flip
        let bad_data_hash = Sha3_256::digest(&bad_data);
        inject_disk_interface.finalized_writes.lock().unwrap().push(
            InMemoryDiskContents {
                path: disk_devfs_path.into(),
                data: bad_data,
            },
        );

        writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                Uuid::new_v4(),
                original_data_hash.into(),
                stream::once(future::ready(Ok(Bytes::from(data.clone())))),
                inject_disk_interface,
            )
            .await
            .unwrap();

        // We expect the update to eventually fail; wait for it to do so.
        let failure_message = tokio::time::timeout(TEST_TIMEOUT, async move {
            loop {
                let status = writer.status(boot_disk);
                match status {
                    BootDiskOsWriteStatus::Failed { message, .. } => {
                        return message;
                    }
                    BootDiskOsWriteStatus::InProgress { .. } => {
                        println!("saw irrelevant status {status:?}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    BootDiskOsWriteStatus::Complete { .. }
                    | BootDiskOsWriteStatus::NoUpdateStarted => {
                        panic!("unexpected status {status:?}");
                    }
                }
            }
        })
        .await
        .unwrap();

        // Confirm that the update fails for the reason we expect: when
        // re-reading what had been written to disk, it got our corrupt data
        // (which hashes to `bad_data_hash`) instead of the expected
        // `original_data_hash`.
        let expected_error = BootDiskOsWriteError::WrittenImageHashMismatch {
            path: disk_devfs_path.into(),
            expected: hex::encode(&original_data_hash),
            got: hex::encode(&bad_data_hash),
        };

        assert_eq!(
            failure_message,
            DisplayErrorChain::new(&expected_error).to_string()
        );

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn boot_disk_os_writer_can_update_both_slots_simultaneously() {
        let logctx = test_setup_log(
            "boot_disk_os_writer_can_update_both_slots_simultaneously",
        );

        // generate two small, random "OS image"s consisting of 10 "blocks" each
        let num_data_blocks = 10;
        let data_len = num_data_blocks * InMemoryDiskInterface::BLOCK_SIZE;
        let mut data_a = vec![0; data_len];
        let mut data_b = vec![0; data_len];
        rand::thread_rng().fill_bytes(&mut data_a);
        rand::thread_rng().fill_bytes(&mut data_b);
        let data_hash_a = Sha3_256::digest(&data_a);
        let data_hash_b = Sha3_256::digest(&data_b);

        // generate a disk writer with no semaphore permits so the updates block
        // until we get a chance to start both of them
        let inject_disk_interface =
            InMemoryDiskInterface::new(Semaphore::new(0));
        let shared_semaphore = Arc::clone(&inject_disk_interface.semaphore);

        let writer = Arc::new(BootDiskOsWriter::new(&logctx.log));
        let disk_devfs_path_a = "/unit-test/disk/a";
        let disk_devfs_path_b = "/unit-test/disk/b";

        let update_id_a = Uuid::new_v4();
        let update_id_b = Uuid::new_v4();

        writer
            .start_update_impl(
                M2Slot::A,
                disk_devfs_path_a.into(),
                update_id_a,
                data_hash_a.into(),
                stream::once(future::ready(Ok(Bytes::from(data_a.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap();

        writer
            .start_update_impl(
                M2Slot::B,
                disk_devfs_path_b.into(),
                update_id_b,
                data_hash_b.into(),
                stream::once(future::ready(Ok(Bytes::from(data_b.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap();

        // Both updates have successfully started; unblock the "disks".
        shared_semaphore.add_permits(Semaphore::MAX_PERMITS);

        // Wait for both updates to complete successfully.
        for boot_disk in [M2Slot::A, M2Slot::B] {
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    let status = writer.status(boot_disk);
                    match status {
                        BootDiskOsWriteStatus::InProgress { .. } => {
                            println!("saw irrelevant status {status:?}");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        BootDiskOsWriteStatus::Complete { update_id } => {
                            match boot_disk {
                                M2Slot::A => assert_eq!(update_id, update_id_a),
                                M2Slot::B => assert_eq!(update_id, update_id_b),
                            }
                            break;
                        }
                        BootDiskOsWriteStatus::Failed { .. }
                        | BootDiskOsWriteStatus::NoUpdateStarted => {
                            panic!("unexpected status {status:?}");
                        }
                    }
                }
            })
            .await
            .unwrap();
        }

        // Ensure each "disk" saw the expected contents.
        let expected_disks = [
            InMemoryDiskContents {
                path: disk_devfs_path_a.into(),
                data: data_a,
            },
            InMemoryDiskContents {
                path: disk_devfs_path_b.into(),
                data: data_b,
            },
        ];
        let written_disks =
            inject_disk_interface.finalized_writes.lock().unwrap();
        assert_eq!(written_disks.len(), expected_disks.len());
        for expected in expected_disks {
            assert!(
                written_disks.contains(&expected),
                "written disks missing expected contents for {}",
                expected.path,
            );
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn boot_disk_os_writer_rejects_new_updates_while_old_running() {
        let logctx = test_setup_log(
            "boot_disk_os_writer_rejects_new_updates_while_old_running",
        );

        // generate two small, random "OS image"s consisting of 10 "blocks" each
        let num_data_blocks = 10;
        let data_len = num_data_blocks * InMemoryDiskInterface::BLOCK_SIZE;
        let mut data_a = vec![0; data_len];
        let mut data_b = vec![0; data_len];
        rand::thread_rng().fill_bytes(&mut data_a);
        rand::thread_rng().fill_bytes(&mut data_b);
        let data_hash_a = Sha3_256::digest(&data_a);
        let data_hash_b = Sha3_256::digest(&data_b);

        // generate a disk writer with no semaphore permits so the updates block
        // until we get a chance to (try to) start both of them
        let inject_disk_interface =
            InMemoryDiskInterface::new(Semaphore::new(0));
        let shared_semaphore = Arc::clone(&inject_disk_interface.semaphore);

        let writer = Arc::new(BootDiskOsWriter::new(&logctx.log));
        let disk_devfs_path = "/unit-test/disk";
        let boot_disk = M2Slot::A;

        let update_id_a = Uuid::new_v4();
        let update_id_b = Uuid::new_v4();

        writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                update_id_a,
                data_hash_a.into(),
                stream::once(future::ready(Ok(Bytes::from(data_a.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap();

        let error = writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                update_id_b,
                data_hash_b.into(),
                stream::once(future::ready(Ok(Bytes::from(data_b.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap_err();
        match &*error {
            BootDiskOsWriteError::UpdateRunning(running_id) => {
                assert_eq!(*running_id, update_id_a);
            }
            _ => panic!("unexpected error {error}"),
        }

        // Both update attempts started; unblock the "disk".
        shared_semaphore.add_permits(Semaphore::MAX_PERMITS);

        // Wait for the first update to complete successfully.
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let status = writer.status(boot_disk);
                match status {
                    BootDiskOsWriteStatus::InProgress { .. } => {
                        println!("saw irrelevant status {status:?}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    BootDiskOsWriteStatus::Complete { update_id } => {
                        assert_eq!(update_id, update_id_a);
                        break;
                    }
                    BootDiskOsWriteStatus::Failed { .. }
                    | BootDiskOsWriteStatus::NoUpdateStarted => {
                        panic!("unexpected status {status:?}");
                    }
                }
            }
        })
        .await
        .unwrap();

        // Ensure we wrote the contents of the first update.
        let expected_disks = [InMemoryDiskContents {
            path: disk_devfs_path.into(),
            data: data_a,
        }];
        let written_disks =
            inject_disk_interface.finalized_writes.lock().unwrap();
        assert_eq!(*written_disks, expected_disks);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn boot_disk_os_writer_rejects_new_updates_while_old_completed() {
        let logctx = test_setup_log(
            "boot_disk_os_writer_rejects_new_updates_while_old_completed",
        );

        // generate two small, random "OS image"s consisting of 10 "blocks" each
        let num_data_blocks = 10;
        let data_len = num_data_blocks * InMemoryDiskInterface::BLOCK_SIZE;
        let mut data_a = vec![0; data_len];
        let mut data_b = vec![0; data_len];
        rand::thread_rng().fill_bytes(&mut data_a);
        rand::thread_rng().fill_bytes(&mut data_b);
        let data_hash_a = Sha3_256::digest(&data_a);
        let data_hash_b = Sha3_256::digest(&data_b);

        // generate a disk writer with effectively infinite semaphore permits
        let inject_disk_interface =
            InMemoryDiskInterface::new(Semaphore::new(Semaphore::MAX_PERMITS));

        let writer = Arc::new(BootDiskOsWriter::new(&logctx.log));
        let disk_devfs_path = "/unit-test/disk";
        let boot_disk = M2Slot::A;

        let update_id_a = Uuid::new_v4();
        let update_id_b = Uuid::new_v4();

        writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                update_id_a,
                data_hash_a.into(),
                stream::once(future::ready(Ok(Bytes::from(data_a.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap();

        // Wait for the first update to complete successfully.
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let status = writer.status(boot_disk);
                match status {
                    BootDiskOsWriteStatus::InProgress { update_id, .. } => {
                        assert_eq!(update_id, update_id_a);
                        println!("saw irrelevant status {status:?}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    BootDiskOsWriteStatus::Complete { update_id } => {
                        assert_eq!(update_id, update_id_a);
                        break;
                    }
                    BootDiskOsWriteStatus::Failed { .. }
                    | BootDiskOsWriteStatus::NoUpdateStarted => {
                        panic!("unexpected status {status:?}");
                    }
                }
            }
        })
        .await
        .unwrap();

        // Ensure we wrote the contents of the first update.
        let expected_disks = [InMemoryDiskContents {
            path: disk_devfs_path.into(),
            data: data_a,
        }];
        {
            let mut written_disks =
                inject_disk_interface.finalized_writes.lock().unwrap();
            assert_eq!(*written_disks, expected_disks);
            written_disks.clear();
        }

        // Check that we get the expected error when attempting to start another
        // update to this same disk.
        let expected_error =
            BootDiskOsWriteError::CannotStartWithoutClearingPreviousStatus(
                update_id_a,
            );
        let error = writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                update_id_b,
                data_hash_b.into(),
                stream::once(future::ready(Ok(Bytes::from(data_b.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), expected_error.to_string());

        // We should not be able to clear the status with an incorrect update
        // ID.
        let expected_error =
            BootDiskOsWriteError::WrongUpdateIdClearingStatus(update_id_a);
        let error =
            writer.clear_terminal_status(boot_disk, update_id_b).unwrap_err();
        assert_eq!(error.to_string(), expected_error.to_string());

        // We should be able to clear the status with the correct update ID, and
        // then start the new one.
        writer.clear_terminal_status(boot_disk, update_id_a).unwrap();
        writer
            .start_update_impl(
                boot_disk,
                disk_devfs_path.into(),
                update_id_b,
                data_hash_b.into(),
                stream::once(future::ready(Ok(Bytes::from(data_b.clone())))),
                inject_disk_interface.clone(),
            )
            .await
            .unwrap();

        // Wait for the second update to complete successfully.
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let status = writer.status(boot_disk);
                match status {
                    BootDiskOsWriteStatus::InProgress { update_id, .. } => {
                        assert_eq!(update_id, update_id_b);
                        println!("saw irrelevant status {status:?}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    BootDiskOsWriteStatus::Complete { update_id } => {
                        assert_eq!(update_id, update_id_b);
                        break;
                    }
                    BootDiskOsWriteStatus::Failed { .. }
                    | BootDiskOsWriteStatus::NoUpdateStarted => {
                        panic!("unexpected status {status:?}");
                    }
                }
            }
        })
        .await
        .unwrap();

        // Ensure we wrote the contents of the second update.
        let expected_disks = [InMemoryDiskContents {
            path: disk_devfs_path.into(),
            data: data_b,
        }];
        {
            let mut written_disks =
                inject_disk_interface.finalized_writes.lock().unwrap();
            assert_eq!(*written_disks, expected_disks);
            written_disks.clear();
        }

        logctx.cleanup_successful();
    }
}
