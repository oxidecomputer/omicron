// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO FIXME

use crate::http_entrypoints::BootDiskOsWriteProgress;
use crate::http_entrypoints::BootDiskOsWriteStatus;
use bytes::Bytes;
use display_error_chain::DisplayErrorChain;
use dropshot::HttpError;
use futures::Stream;
use futures::TryStreamExt;
use installinator_common::M2Slot;
use sha3::Digest;
use sha3::Sha3_256;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
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
    FailedCreatingTempfile(io::Error),
    #[error("failed writing to temporary file")]
    FailedWritingTempfile(io::Error),
    #[error("failed downloading image from HTTP client")]
    FailedDownloadingImage(HttpError),
    #[error("hash mismatch in image from HTTP client: expected {expected} but got {got}")]
    UploadedImageHashMismatch { expected: String, got: String },
}

impl From<&BootDiskOsWriteError> for HttpError {
    fn from(error: &BootDiskOsWriteError) -> Self {
        let message = DisplayErrorChain::new(error).to_string();
        match error {
            BootDiskOsWriteError::AnotherUpdateRunning(_)
            | BootDiskOsWriteError::FailedDownloadingImage(_)
            | BootDiskOsWriteError::UploadedImageHashMismatch { .. } => {
                HttpError::for_bad_request(None, message)
            }
            BootDiskOsWriteError::TaskPanic
            | BootDiskOsWriteError::FailedCreatingTempfile(_)
            | BootDiskOsWriteError::FailedWritingTempfile(_) => HttpError {
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
        update_id: Uuid,
        sha3_256_digest: [u8; 32],
        image_upload: S,
    ) -> Result<(), Arc<BootDiskOsWriteError>>
    where
        S: Stream<Item = Result<Bytes, HttpError>> + Send + 'static,
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
                sha3_256_digest,
                progress_tx,
                complete_tx,
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
struct BootDiskOsWriteTask {
    log: Logger,
    sha3_256_digest: [u8; 32],
    progress_tx: watch::Sender<BootDiskOsWriteProgress>,
    complete_tx: oneshot::Sender<Result<(), Arc<BootDiskOsWriteError>>>,
}

impl BootDiskOsWriteTask {
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
        let image_tempfile = match self
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

        warn!(
            self.log,
            "update implementation incomplete - \
             abandoning after copying image to a local tempfile"
        );
        _ = image_tempfile;

        Ok(())
    }

    async fn download_body_to_tempfile<S>(
        &self,
        image_upload: S,
    ) -> Result<File, BootDiskOsWriteError>
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

        // Rewind the tempfile.
        let mut tempfile = tempfile.into_inner();
        tempfile
            .seek(io::SeekFrom::Start(0))
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

            Ok(tempfile)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;
    use std::mem;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    // TODO DOCUMENT AND BUMP TO 30
    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

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
                        println!("got {progress:?}");
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
}
