// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use buf_list::BufList;
use bytes::Buf;
use camino::{Utf8Path, Utf8PathBuf};
use installinator_common::{CompletionEventKind, ProgressEventKind};
use omicron_common::update::ArtifactHashId;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::mpsc,
    time::Instant,
};

use crate::reporter::ReportEvent;

#[derive(Clone, Debug)]
pub(crate) struct WriteDestination {
    pub(crate) host_phase_2: Utf8PathBuf,
    pub(crate) control_plane: Utf8PathBuf,
}

impl WriteDestination {
    pub(crate) fn in_directory(dir: &Utf8Path) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("error creating directories at {dir}"))?;
        Ok(Self {
            host_phase_2: dir.join("host_phase_2.bin"),
            control_plane: dir.join("control_plane.bin"),
        })
    }
}

pub(crate) async fn write_artifact(
    log: &slog::Logger,
    artifact_id: &ArtifactHashId,
    artifact: BufList,
    destination: &Utf8Path,
    event_sender: &mpsc::Sender<ReportEvent>,
) {
    let mut transport = FileTransport { destination: destination.to_owned() };
    write_artifact_with_transport(
        log,
        artifact_id,
        artifact,
        &mut transport,
        event_sender,
    )
    .await;
}

async fn write_artifact_with_transport(
    log: &slog::Logger,
    artifact_id: &ArtifactHashId,
    artifact: BufList,
    transport: &mut impl WriteTransport,
    event_sender: &mpsc::Sender<ReportEvent>,
) {
    let mut attempt = 0;

    loop {
        attempt += 1;
        slog::info!(
            log,
            "writing artifact ({} bytes) to {} (attempt {attempt})",
            artifact.num_bytes(),
            transport.destination();
            "artifact_id" => ?artifact_id
        );
        match write_artifact_impl(
            attempt,
            artifact_id,
            artifact.clone(),
            transport,
            event_sender,
        )
        .await
        {
            Ok(()) => {
                slog::info!(
                    log,
                    "wrote artifact ({} bytes) to {} in {attempt} attempts",
                    artifact.num_bytes(),
                    transport.destination();
                    "artifact_id" => ?artifact_id,
                );
                break;
            }
            Err(error) => {
                slog::info!(log, "{error:?}"; "artifact_id" => ?artifact_id);
                // Give it a short break, then keep trying.
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// Used in tests to test against file failures.
#[async_trait]
trait WriteTransport: fmt::Debug {
    type W: AsyncWrite + Unpin;

    async fn make_writer(&mut self) -> Result<Self::W>;
    fn destination(&self) -> &Utf8Path;
}

#[derive(Debug)]
struct FileTransport {
    destination: Utf8PathBuf,
}

#[async_trait]
impl WriteTransport for FileTransport {
    type W = tokio::fs::File;

    async fn make_writer(&mut self) -> Result<Self::W> {
        Ok(tokio::fs::OpenOptions::new()
            // TODO: do we want create = true? Maybe only if writing to a file and not an M.2.
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.destination)
            .await
            .with_context(|| {
                format!(
                    "failed to open destination `{}` for writing",
                    self.destination
                )
            })?)
    }

    fn destination(&self) -> &Utf8Path {
        &self.destination
    }
}

async fn write_artifact_impl(
    attempt: usize,
    artifact_id: &ArtifactHashId,
    mut artifact: BufList,
    transport: &mut impl WriteTransport,
    event_sender: &mpsc::Sender<ReportEvent>,
) -> Result<()> {
    let mut writer = transport.make_writer().await?;
    let destination = transport.destination();

    let total_bytes = artifact.num_bytes() as u64;
    let mut written_bytes = 0u64;

    let start = Instant::now();

    while artifact.has_remaining() {
        match writer.write_buf(&mut artifact).await {
            Ok(n) => {
                written_bytes += n as u64;
                let _ = event_sender
                    .send(ReportEvent::Progress(
                        ProgressEventKind::WriteProgress {
                            attempt,
                            kind: artifact_id.kind.clone(),
                            destination: destination.to_owned(),
                            written_bytes,
                            total_bytes,
                            elapsed: start.elapsed(),
                        },
                    ))
                    .await;
            }
            Err(error) => {
                let _ = event_sender
                    .send(ReportEvent::Completion(
                        CompletionEventKind::WriteFailed {
                            attempt,
                            kind: artifact_id.kind.clone(),
                            destination: destination.to_owned(),
                            written_bytes,
                            total_bytes,
                            elapsed: start.elapsed(),
                            message: error.to_string(),
                        },
                    ))
                    .await;
                return Err(error).with_context(|| {
                    format!(
                        "failed to write artifact {artifact_id:?} \
                         ({total_bytes} bytes) to destination `{destination}`"
                    )
                });
            }
        }
    }

    match writer.flush().await {
        Ok(()) => {}
        Err(error) => {
            let _ = event_sender
                .send(ReportEvent::Completion(
                    CompletionEventKind::WriteFailed {
                        attempt,
                        kind: artifact_id.kind.clone(),
                        destination: destination.to_owned(),
                        written_bytes,
                        total_bytes,
                        elapsed: start.elapsed(),
                        message: format!("flush failed: {error}"),
                    },
                ))
                .await;
            return Err(error).with_context(|| {
                format!(
                    "failed to flush artifact {artifact_id:?} \
                     ({total_bytes} bytes) to destination `{destination}`"
                )
            });
        }
    };

    let _ = event_sender
        .send(ReportEvent::Completion(CompletionEventKind::WriteCompleted {
            attempt,
            kind: artifact_id.kind.clone(),
            destination: destination.to_owned(),
            artifact_size: total_bytes,
            elapsed: start.elapsed(),
        }))
        .await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::test_helpers::{dummy_artifact_hash_id, with_test_runtime};

    use anyhow::Result;
    use bytes::{Buf, Bytes};
    use camino::Utf8Path;
    use futures::StreamExt;
    use omicron_test_utils::dev::test_setup_log;
    use partial_io::{
        proptest_types::{
            interrupted_would_block_strategy, partial_op_strategy,
        },
        PartialAsyncWrite, PartialOp,
    };
    use proptest::prelude::*;
    use tempfile::tempdir;
    use test_strategy::proptest;
    use tokio::io::AsyncReadExt;
    use tokio_stream::wrappers::ReceiverStream;

    #[proptest(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
    fn proptest_write_artifact(
        #[strategy(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8192), 0..16))]
        data: Vec<Vec<u8>>,
        #[strategy(op_strategy())] partial_ops: VecDeque<Vec<PartialOp>>,
    ) {
        with_test_runtime(move || async move {
            proptest_write_artifact_impl(data, partial_ops)
                .await
                .expect("test failed");
        })
    }

    fn op_strategy() -> impl Strategy<Value = VecDeque<Vec<PartialOp>>> {
        // XXX: Ideally we'd be able to figure out how many bytes get written by
        // merely inspecting the list of operations, but partial-io is a bit
        // broken:
        // https://github.com/sunshowers-code/partial-io/issues/32
        //
        // For now, always error out on earlier attempts and have one successful
        // attempt at the end. Revisit this after fixing upstream.
        let success_strategy = prop::collection::vec(
            partial_op_strategy(interrupted_would_block_strategy(), 1024),
            0..16,
        );

        (0..16u32, success_strategy).prop_map(|(failure_count, success)| {
            (0..failure_count)
                .map(|_| vec![PartialOp::Err(std::io::ErrorKind::Other)])
                .chain(std::iter::once(success))
                .collect()
        })
    }

    async fn proptest_write_artifact_impl(
        data: Vec<Vec<u8>>,
        partial_ops: VecDeque<Vec<PartialOp>>,
    ) -> Result<()> {
        let logctx = test_setup_log("test_write_artifact");
        let tempdir = tempdir()?;
        let tempdir_path: &Utf8Path = tempdir.path().try_into()?;
        let temp_destination = tempdir_path.join("test.bin");
        let mut artifact: BufList = data.into_iter().map(Bytes::from).collect();

        // Which attempt is this going to first succeed at? For now, the last
        // attempt is successful (see the XXX in op_strategy for more.)
        let expected_attempt = partial_ops.len();

        let mut transport = PartialIoTransport {
            file_transport: FileTransport {
                destination: temp_destination.clone(),
            },
            partial_ops,
        };

        let artifact_id = dummy_artifact_hash_id();

        let (event_sender, event_receiver) = mpsc::channel(512);

        let receiver_handle = tokio::spawn(async move {
            ReceiverStream::new(event_receiver).collect::<Vec<_>>().await
        });

        write_artifact_with_transport(
            &logctx.log,
            &artifact_id,
            artifact.clone(),
            &mut transport,
            &event_sender,
        )
        .await;

        std::mem::drop(event_sender);

        let events = receiver_handle.await?;

        let mut seen_completion = false;
        let mut current_attempt = 1;
        let mut last_written_bytes = 0;

        for event in events {
            match event {
                ReportEvent::Progress(ProgressEventKind::WriteProgress {
                    attempt,
                    kind,
                    destination,
                    written_bytes,
                    total_bytes,
                    ..
                }) => {
                    assert!(
                        !seen_completion,
                        "no more progress events after completion"
                    );
                    assert_eq!(attempt, current_attempt);
                    assert_eq!(kind, artifact_id.kind);
                    assert_eq!(destination, temp_destination);
                    assert_eq!(total_bytes, artifact.num_bytes() as u64);
                    assert!(
                        written_bytes > 0,
                        "non-zero number of bytes should be written"
                    );
                    assert!(
                        written_bytes > last_written_bytes,
                        "progress made with written bytes {written_bytes} > {last_written_bytes}"
                    );
                    last_written_bytes = written_bytes;
                }
                ReportEvent::Completion(
                    CompletionEventKind::WriteCompleted {
                        attempt,
                        kind,
                        destination,
                        artifact_size,
                        ..
                    },
                ) => {
                    assert!(
                        !seen_completion,
                        "only one WriteCompleted event seen"
                    );
                    seen_completion = true;
                    assert_eq!(
                        attempt, expected_attempt,
                        "succeeded on expected attempt"
                    );
                    assert_eq!(kind, artifact_id.kind);
                    assert_eq!(destination, temp_destination);
                    assert_eq!(artifact_size, artifact.num_bytes() as u64);
                }
                ReportEvent::Completion(CompletionEventKind::WriteFailed {
                    attempt,
                    kind,
                    destination,
                    written_bytes,
                    total_bytes,
                    ..
                }) => {
                    assert!(
                        !seen_completion,
                        "no more failure events after completion"
                    );
                    assert_eq!(attempt, current_attempt, "attempt matches");
                    assert_eq!(kind, artifact_id.kind);
                    assert_eq!(destination, temp_destination);
                    assert_eq!(written_bytes, last_written_bytes);
                    assert_eq!(total_bytes, artifact.num_bytes() as u64);

                    // Reset the counter of last written bytes since we're starting afresh.
                    last_written_bytes = 0;
                    current_attempt += 1;
                }
                other => {
                    panic!("unexpected event: {other:?}");
                }
            }
        }

        assert!(seen_completion, "seen a WriteCompleted event");

        // Read the artifact from disk and ensure it is correct.
        let mut file = tokio::fs::File::open(&temp_destination)
            .await
            .with_context(|| {
                format!("failed to open {temp_destination} to verify contents")
            })?;
        let mut buf = Vec::with_capacity(artifact.num_bytes());
        let read_num_bytes =
            file.read_to_end(&mut buf).await.with_context(|| {
                format!("failed to read {temp_destination} into memory")
            })?;
        assert_eq!(
            read_num_bytes,
            artifact.num_bytes(),
            "read num_bytes matches"
        );

        let bytes = artifact.copy_to_bytes(artifact.num_bytes());
        assert_eq!(buf, bytes, "bytes written to disk match");

        logctx.cleanup_successful();
        Ok(())
    }

    #[derive(Debug)]
    struct PartialIoTransport {
        file_transport: FileTransport,
        partial_ops: VecDeque<Vec<PartialOp>>,
    }

    #[async_trait]
    impl WriteTransport for PartialIoTransport {
        type W = PartialAsyncWrite<tokio::fs::File>;

        async fn make_writer(&mut self) -> Result<Self::W> {
            let f = self.file_transport.make_writer().await?;
            // This is the next series of operations.
            let these_ops =
                self.partial_ops.pop_front().unwrap_or_else(Vec::new);
            Ok(PartialAsyncWrite::new(f, these_ops))
        }

        fn destination(&self) -> &Utf8Path {
            self.file_transport.destination()
        }
    }
}
