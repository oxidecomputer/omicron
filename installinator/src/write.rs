// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result};
use buf_list::BufList;
use bytes::Buf;
use camino::{Utf8Path, Utf8PathBuf};
use installinator_common::{CompletionEventKind, ProgressEventKind};
use omicron_common::update::ArtifactHashId;
use tokio::{io::AsyncWriteExt, sync::mpsc, time::Instant};

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
    let mut attempt = 0;

    loop {
        attempt += 1;
        slog::info!(
            log,
            "writing artifact ({} bytes) to {destination} (attempt {attempt})",
            artifact.num_bytes();
            "artifact_id" => ?artifact_id
        );
        match write_artifact_impl(
            attempt,
            artifact_id,
            artifact.clone(),
            destination,
            event_sender,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                slog::info!(log, "{error:?}"; "artifact_id" => ?artifact_id);
                // Give it a short break, then keep trying.
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn write_artifact_impl(
    attempt: usize,
    artifact_id: &ArtifactHashId,
    artifact: BufList,
    destination: &Utf8Path,
    event_sender: &mpsc::Sender<ReportEvent>,
) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        // TODO: do we want create = true? Maybe only if writing to a file and not an M.2.
        .create(true)
        .write(true)
        .truncate(true)
        .open(destination)
        .await
        .with_context(|| {
            format!("failed to open destination `{destination}` for writing")
        })?;

    let total_bytes = artifact.num_bytes() as u64;
    let mut written_bytes = 0u64;

    let start = Instant::now();

    while artifact.has_remaining() {
        // BufWriter shouldn't be necessary because we've downloaded, and are
        // writing, data in typical buffer-sized chunks. Besides,
        // tokio::fs::File already uses a buffer internally as of tokio 1.25.0.
        match file.write(artifact.chunk()).await {
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

    match file.flush().await {
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
