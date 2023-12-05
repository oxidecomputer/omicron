// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO FIXME

use crate::http_entrypoints::BootDiskOsWriteProgress;
use crate::http_entrypoints::BootDiskOsWriteStatus;
use async_trait::async_trait;
use bytes::Bytes;
use camino::Utf8PathBuf;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use futures::Stream;
use futures::TryStreamExt;
use installinator_common::M2Slot;
use installinator_common::RawDiskWriter;
use sha3::Digest;
use sha3::Sha3_256;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io;
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

impl BootDiskOsWriteStatus {
    fn from_result(
        update_id: Uuid,
        result: &Result<(), Arc<BootDiskOsWriteError>>,
    ) -> Self {
        match result {
            Ok(()) => Self::Complete { update_id },
            Err(err) => Self::Failed {
                update_id,
                message: DisplayErrorChain::new(err).to_string(),
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BootDiskOsWriteError {
    // This variant should be impossible in production, as we build with
    // panic=abort, but may be constructed in tests (e.g., during tokio runtime
    // shutdown).
    #[error("internal error (task panic)")]
    TaskPanic,
    #[error("another update is still running ({0})")]
    AnotherUpdateRunning(Uuid),
    #[error("failed to create temporary file")]
    FailedCreatingTempfile(#[source] io::Error),
    #[error("failed writing to temporary file")]
    FailedWritingTempfile(#[source] io::Error),
    #[error("failed downloading image from HTTP client")]
    FailedDownloadingImage(#[source] HttpError),
    #[error("hash mismatch in image from HTTP client: expected {expected} but got {got}")]
    UploadedImageHashMismatch { expected: String, got: String },
    #[error("failed to open disk for writing {path}")]
    FailedOpenDisk {
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
}

impl From<&BootDiskOsWriteError> for HttpError {
    fn from(error: &BootDiskOsWriteError) -> Self {
        let message = DisplayErrorChain::new(error).to_string();
        match error {
            BootDiskOsWriteError::AnotherUpdateRunning(_)
            | BootDiskOsWriteError::FailedDownloadingImage(_)
            | BootDiskOsWriteError::UploadedImageHashMismatch { .. }
            | BootDiskOsWriteError::ImageSizeNotMultipleOfBlockSize {
                ..
            } => HttpError::for_bad_request(None, message),
            BootDiskOsWriteError::TaskPanic
            | BootDiskOsWriteError::FailedCreatingTempfile(_)
            | BootDiskOsWriteError::FailedWritingTempfile(_)
            | BootDiskOsWriteError::FailedReadingTempfile(_)
            | BootDiskOsWriteError::FailedOpenDisk { .. }
            | BootDiskOsWriteError::FailedWritingDisk { .. } => HttpError {
                status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                external_message: message.clone(),
                internal_message: message,
            },
        }
    }
}

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
            InjectRawDiskWriter,
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
        Writer: InjectDiskWriter + Send + Sync + 'static,
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
                complete_tx,
                disk_writer,
            };
            tokio::spawn(task.run(image_upload, uploaded_image_tx));
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
                        // Check whether the task is _actually_ still running,
                        // or whether it's done and just waiting for us to
                        // realize it.
                        match running.complete_rx.try_recv() {
                            Ok(_prev_result) => {
                                // A previous write is done, but we're
                                // immedately starting a new one, so discard the
                                // previous result.
                                let (uploaded_image_rx, running) =
                                    spawn_update_task();
                                slot.insert(WriterState::TaskRunning(running));
                                uploaded_image_rx
                            }
                            Err(TryRecvError::Empty) => {
                                return Err(Arc::new(
                                    BootDiskOsWriteError::AnotherUpdateRunning(
                                        running.update_id,
                                    ),
                                ));
                            }
                            Err(TryRecvError::Closed) => {
                                return Err(Arc::new(
                                    BootDiskOsWriteError::TaskPanic,
                                ));
                            }
                        }
                    }
                    WriterState::Complete(_) => {
                        let (uploaded_image_rx, running) = spawn_update_task();
                        slot.insert(WriterState::TaskRunning(running));
                        uploaded_image_rx
                    }
                },
            }
        };

        // We've now spawned a task to drive the update, and we want to wait for
        // it to finish copying from `image_upload`.
        uploaded_image_rx.await.map_err(|_| BootDiskOsWriteError::TaskPanic)?
    }

    pub(crate) fn status(&self, boot_disk: M2Slot) -> BootDiskOsWriteStatus {
        let mut states = self.states.lock().unwrap();
        let mut slot = match states.entry(boot_disk) {
            Entry::Vacant(_) => return BootDiskOsWriteStatus::NoUpdateRunning,
            Entry::Occupied(slot) => slot,
        };

        match slot.get_mut() {
            WriterState::TaskRunning(running) => {
                match running.complete_rx.try_recv() {
                    Ok(result) => {
                        let update_id = running.update_id;
                        let status = BootDiskOsWriteStatus::from_result(
                            update_id, &result,
                        );
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
                        let status = BootDiskOsWriteStatus::from_result(
                            update_id, &result,
                        );
                        slot.insert(WriterState::Complete(TaskCompleteState {
                            update_id,
                            result,
                        }));
                        status
                    }
                }
            }
            WriterState::Complete(complete) => {
                BootDiskOsWriteStatus::from_result(
                    complete.update_id,
                    &complete.result,
                )
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
    complete_tx: oneshot::Sender<Result<(), Arc<BootDiskOsWriteError>>>,
    disk_writer: W,
}

impl<W: InjectDiskWriter> BootDiskOsWriteTask<W> {
    async fn run<S>(
        self,
        image_upload: S,
        uploaded_image_tx: oneshot::Sender<
            Result<(), Arc<BootDiskOsWriteError>>,
        >,
    ) where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
    {
        let result = self.run_impl(image_upload, uploaded_image_tx).await;

        // It's possible (albeit unlikely) our caller has discarded the receive
        // half of this channel; ignore any send error.
        _ = self.complete_tx.send(result);
    }

    async fn run_impl<S>(
        &self,
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
        // request _and_ a copy of the same error in our current update state.
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

        let mut image_upload = std::pin::pin!(image_upload.into_stream());
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
        let hash: [u8; 32] = hasher.finalize().into();
        let expected_hash_str = hex::encode(&self.sha3_256_digest);
        if hash == self.sha3_256_digest {
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
        let disk_writer = self
            .disk_writer
            .open(self.disk_devfs_path.as_std_path())
            .await
            .map_err(|error| BootDiskOsWriteError::FailedOpenDisk {
                error,
                path: self.disk_devfs_path.clone(),
            })?;
        tokio::pin!(disk_writer);

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

        Ok(disk_block_size)
    }

    async fn validate_written_image(
        &self,
        image_size: usize,
        disk_block_size: usize,
    ) -> Result<(), BootDiskOsWriteError> {
        // TODO
        Ok(())
    }
}

// Utility traits to allow injecting an in-memory "disk" for unit tests.
#[async_trait]
trait DiskWriter: AsyncWrite + Send + Sized {
    fn block_size(&self) -> usize;
    async fn finalize(self) -> io::Result<()>;
}
#[async_trait]
trait InjectDiskWriter {
    type Writer: DiskWriter;
    async fn open(&self, path: &Path) -> io::Result<Self::Writer>;
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

struct InjectRawDiskWriter;

#[async_trait]
impl InjectDiskWriter for InjectRawDiskWriter {
    type Writer = RawDiskWriter;

    async fn open(&self, path: &Path) -> io::Result<Self::Writer> {
        RawDiskWriter::open(path).await
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
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::task::ready;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::sync::Semaphore;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_util::sync::PollSemaphore;

    // TODO DOCUMENT AND BUMP TO 30
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    struct InMemoryDiskContents {
        path: PathBuf,
        data: Vec<u8>,
    }

    struct InjectInMemoryDiskWriter {
        semaphore: Arc<Semaphore>,
        finalized_writes: Arc<Mutex<Vec<InMemoryDiskContents>>>,
    }

    impl InjectInMemoryDiskWriter {
        const BLOCK_SIZE: usize = 16;

        fn new(semaphore: Semaphore) -> Self {
            Self {
                semaphore: Arc::new(semaphore),
                finalized_writes: Arc::default(),
            }
        }
    }

    #[async_trait]
    impl InjectDiskWriter for InjectInMemoryDiskWriter {
        type Writer = InMemoryDiskWriter;

        async fn open(&self, path: &Path) -> io::Result<Self::Writer> {
            Ok(InMemoryDiskWriter {
                opened_path: path.into(),
                data: BlockSizeBufWriter::with_block_size(
                    Self::BLOCK_SIZE,
                    Vec::new(),
                ),
                semaphore: PollSemaphore::new(Arc::clone(&self.semaphore)),
                finalized_writes: Arc::clone(&self.finalized_writes),
            })
        }
    }

    struct InMemoryDiskWriter {
        opened_path: PathBuf,
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
                    BootDiskOsWriteStatus::NoUpdateRunning => {
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

            let start_update_result = start_update_task.await.unwrap();
            let error = start_update_result.unwrap_err();
            match &*error {
                BootDiskOsWriteError::UploadedImageHashMismatch {
                    expected,
                    got,
                } => {
                    assert_eq!(
                        *got,
                        hex::encode(actual_data_hasher.finalize())
                    );
                    assert_eq!(*expected, hex::encode(claimed_sha3_digest));
                }
                _ => panic!("unexpected error {error:?}"),
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
        let data_len = num_data_blocks * InjectInMemoryDiskWriter::BLOCK_SIZE;
        let mut data = vec![0; data_len];
        rand::thread_rng().fill_bytes(&mut data);
        let data_hash = Sha3_256::digest(&data);

        // generate a disk writer with a 0-permit semaphore; we'll inject
        // permits in the main loop below to force single-stepping through
        // writing the data
        let inject_disk_writer =
            InjectInMemoryDiskWriter::new(Semaphore::new(0));
        let shared_semaphore = Arc::clone(&inject_disk_writer.semaphore);

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
                inject_disk_writer,
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

                // Wait until we see the status we expect
                loop {
                    let status = writer.status(boot_disk);
                    if i + 1 < num_data_blocks {
                        // not the last block - we should see progress that
                        // matches the amount of data being copied
                        let progress = expect_in_progress(status);
                        match progress {
                            BootDiskOsWriteProgress::WritingImageToDisk {
                                bytes_written,
                            } if (i + 1)
                                * InjectInMemoryDiskWriter::BLOCK_SIZE
                                == bytes_written =>
                            {
                                println!("saw expected progress for block {i}");
                                break;
                            }
                            _ => {
                                // This is not an error: we could still be in
                                // `ReceivingUploadedImage` or the previous
                                // block's `WritingImageToDisk`
                                println!(
                                    "saw irrelevant progress {progress:?}"
                                );
                                tokio::time::sleep(Duration::from_millis(50))
                                    .await;
                                continue;
                            }
                        }
                    } else {
                        // On the last block, we may see an "in progress" with
                        // all data written, or we may skip straight to
                        // "complete". Either is fine and signals all data has
                        // been written.
                        match status {
                            BootDiskOsWriteStatus::Complete { .. } => break,
                            BootDiskOsWriteStatus::InProgress {
                                progress: BootDiskOsWriteProgress::WritingImageToDisk {
                                    bytes_written,
                                },
                                ..
                            } if bytes_written == data_len => break,
                            BootDiskOsWriteStatus::InProgress {
                                progress, ..
                            } => {
                                println!(
                                    "saw irrelevant progress {progress:?}"
                                );
                                tokio::time::sleep(Duration::from_millis(50))
                                    .await;
                                continue;
                            }
                            BootDiskOsWriteStatus::NoUpdateRunning
                            | BootDiskOsWriteStatus::Failed {..} => {
                                panic!("unexpected status {status:?}");
                            }
                        }
                    }
                }
            }
        })
        .await
        .unwrap();

        logctx.cleanup_successful();
    }
}
