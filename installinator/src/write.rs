// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt,
    time::Duration,
};

use anyhow::{anyhow, ensure, Context, Result};
use async_trait::async_trait;
use buf_list::BufList;
use bytes::Buf;
use camino::{Utf8Path, Utf8PathBuf};
use installinator_common::{CompletionEventKind, ProgressEventKind};
use omicron_common::update::ArtifactHashId;
use slog::{info, warn, Logger};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::mpsc,
    time::Instant,
};

use crate::{hardware::Hardware, reporter::ReportEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum M2Slot {
    A,
    B,
}

impl TryFrom<i64> for M2Slot {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> std::result::Result<Self, Self::Error> {
        match value {
            // Gimlet should have 2 M.2 drives: drive A is assigned slot 17, and
            // drive B is assigned slot 18.
            17 => Ok(Self::A),
            18 => Ok(Self::B),
            _ => Err(anyhow!("unexpected M.2 slot {value}")),
        }
    }
}

#[derive(Clone, Debug)]
struct ArtifactDestination {
    // On real gimlets, we expect to write the host phase 2 to an
    // already-existing device (the appropriate M.2 slice). But for tests or
    // runs on non-gimlets, we want to write the host phase 2 to a new file we
    // create.
    create_host_phase_2: bool,
    host_phase_2: Utf8PathBuf,

    // TODO-completeness This SHOULD NOT be optional, but at the time of this
    // writing we don't know how to write the control plane artifacts on a real
    // gimlet, so we leave it optional for now. This should be fixed very soon!
    control_plane: Option<Utf8PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct WriteDestination {
    drives: BTreeMap<M2Slot, ArtifactDestination>,
}

impl WriteDestination {
    pub(crate) fn in_directory(dir: &Utf8Path) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("error creating directories at {dir}"))?;

        // `in_directory()` is only used for testing (e.g., on
        // not-really-gimlets); pretend we're only writing to M.2 A.
        let mut drives = BTreeMap::new();
        drives.insert(
            M2Slot::A,
            ArtifactDestination {
                create_host_phase_2: true,
                host_phase_2: dir.join("host_phase_2.bin"),
                control_plane: Some(dir.join("control_plane.bin")),
            },
        );

        Ok(Self { drives })
    }

    pub(crate) fn from_hardware(log: &Logger) -> Result<Self> {
        let hardware = Hardware::scan(log)?;

        // We want the `,raw`-suffixed path to the boot image partition, as that
        // allows us file-like access via the character device.
        let raw_devfs_path = true;

        let mut drives = BTreeMap::new();

        for disk in hardware.m2_disks() {
            let Ok(slot) = M2Slot::try_from(disk.slot()) else {
                warn!(
                    log, "skipping M.2 drive with unexpected slot number";
                    "slot" => disk.slot(),
                );
                continue;
            };

            match disk.boot_image_devfs_path(raw_devfs_path) {
                Ok(path) => {
                    info!(
                        log, "found target M.2 disk";
                        "identity" => ?disk.identity(),
                        "path" => disk.devfs_path().display(),
                        "slot" => disk.slot(),
                        "boot_image_path" => path.display(),
                        "zpool" => %disk.zpool_name(),
                    );

                    match drives.entry(slot) {
                        Entry::Vacant(entry) => {
                            entry.insert(ArtifactDestination {
                                create_host_phase_2: false,
                                host_phase_2: Utf8PathBuf::try_from(path)
                                    .context("non-UTF8 drive path")?,
                                // TODO-completeness Fix this once we know how
                                // to write the control plane image to this
                                // disk's zpool.
                                control_plane: None,
                            });
                        }
                        Entry::Occupied(_) => {
                            warn!(
                                log, "skipping duplicate M.2 drive entry";
                                "identity" => ?disk.identity(),
                                "path" => disk.devfs_path().display(),
                                "slot" => disk.slot(),
                                "boot_image_path" => path.display(),
                                "zpool" => %disk.zpool_name(),
                            );
                            continue;
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        log, "found M.2 disk but failed to find boot image path";
                        "identity" => ?disk.identity(),
                        "path" => disk.devfs_path().display(),
                        "slot" => disk.slot(),
                        "boot_image_path_err" => %err,
                        "zpool" => %disk.zpool_name(),
                    );
                }
            }
        }

        ensure!(!drives.is_empty(), "no valid M.2 target drives found");

        Ok(Self { drives })
    }
}

/// State machine for our progress writing to one of the M.2 drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveWriteProgress {
    /// We have not yet attempted any writes to the drive.
    Unstarted,
    /// We've tried and failed to write the host phase 2 image `attempts` times.
    HostPhase2Failed { attempts: usize },
    /// We succeeded in writing the host phase 2 image, but failed to write the
    /// control plane `attempts` times.
    ControlPlaneFailed { attempts: usize },
    /// We succeeded in writing both the host phase 2 image and the control
    /// plane image.
    Done,
}

pub(crate) struct ArtifactWriter<'a> {
    drives: BTreeMap<M2Slot, (ArtifactDestination, DriveWriteProgress)>,
    host_phase_2_id: &'a ArtifactHashId,
    host_phase_2_data: &'a BufList,
    control_plane_id: &'a ArtifactHashId,
    control_plane_data: &'a BufList,
}

impl<'a> ArtifactWriter<'a> {
    pub(crate) fn new(
        host_phase_2_id: &'a ArtifactHashId,
        host_phase_2_data: &'a BufList,
        control_plane_id: &'a ArtifactHashId,
        control_plane_data: &'a BufList,
        destination: WriteDestination,
    ) -> Self {
        let drives = destination
            .drives
            .into_iter()
            .map(|(key, value)| (key, (value, DriveWriteProgress::Unstarted)))
            .collect();
        Self {
            drives,
            host_phase_2_id,
            host_phase_2_data,
            control_plane_id,
            control_plane_data,
        }
    }

    pub(crate) async fn write(
        &mut self,
        log: &Logger,
        event_sender: &mpsc::Sender<ReportEvent>,
    ) -> Vec<M2Slot> {
        let mut transport = FileTransport;
        self.write_with_transport(log, &mut transport, event_sender).await
    }

    async fn write_with_transport(
        &mut self,
        log: &Logger,
        transport: &mut impl WriteTransport,
        event_sender: &mpsc::Sender<ReportEvent>,
    ) -> Vec<M2Slot> {
        let mut done_drives = Vec::new();

        loop {
            // How many drives did we finish writing this iteration?
            let mut success_this_iter = 0;

            // How many drives did we finish writing on a previous iteration?
            let mut success_prev_iter = 0;

            for (drive, (destinations, progress)) in self.drives.iter_mut() {
                *progress = match progress {
                    DriveWriteProgress::Unstarted => {
                        let new_progress = write_starting_with_host_phase_2(
                            log,
                            1,
                            destinations,
                            self.host_phase_2_id,
                            self.host_phase_2_data,
                            self.control_plane_id,
                            self.control_plane_data,
                            transport,
                            event_sender,
                        )
                        .await;

                        if new_progress == DriveWriteProgress::Done {
                            done_drives.push(*drive);
                            success_this_iter += 1;
                        }

                        new_progress
                    }
                    DriveWriteProgress::HostPhase2Failed { attempts } => {
                        let new_progress = write_starting_with_host_phase_2(
                            log,
                            *attempts + 1,
                            destinations,
                            self.host_phase_2_id,
                            self.host_phase_2_data,
                            self.control_plane_id,
                            self.control_plane_data,
                            transport,
                            event_sender,
                        )
                        .await;

                        if new_progress == DriveWriteProgress::Done {
                            done_drives.push(*drive);
                            success_this_iter += 1;
                        }

                        new_progress
                    }
                    DriveWriteProgress::ControlPlaneFailed { attempts } => {
                        let new_progress = write_starting_with_control_plane(
                            log,
                            *attempts + 1,
                            destinations,
                            self.control_plane_id,
                            self.control_plane_data,
                            transport,
                            event_sender,
                        )
                        .await;

                        if new_progress == DriveWriteProgress::Done {
                            done_drives.push(*drive);
                            success_this_iter += 1;
                        }

                        new_progress
                    }
                    DriveWriteProgress::Done => {
                        success_prev_iter += 1;
                        DriveWriteProgress::Done
                    }
                };
            }

            // Stop if either:
            // 1. All drives have successfully written
            // 2. At least one drive was successfully written on a previous
            //    iteration, which implies all other drives got to retry during
            //    this iteration.
            if success_this_iter == self.drives.len() || success_prev_iter > 0 {
                break;
            }

            // Give it a short break, then keep trying.
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        done_drives.sort();

        done_drives
    }
}

// Attempt to write the host phase 2 and then, if successful, the control plane
// image.
async fn write_starting_with_host_phase_2(
    log: &Logger,
    host_phase_2_attempt: usize,
    destinations: &ArtifactDestination,
    host_phase_2_id: &ArtifactHashId,
    host_phase_2_data: &BufList,
    control_plane_id: &ArtifactHashId,
    control_plane_data: &BufList,
    transport: &mut impl WriteTransport,
    event_sender: &mpsc::Sender<ReportEvent>,
) -> DriveWriteProgress {
    if let Err(error) = write_artifact_impl(
        host_phase_2_attempt,
        host_phase_2_id,
        host_phase_2_data.clone(),
        &destinations.host_phase_2,
        destinations.create_host_phase_2,
        transport,
        event_sender,
    )
    .await
    {
        info!(log, "{error:?}"; "artifact_id" => ?host_phase_2_id);
        return DriveWriteProgress::HostPhase2Failed {
            attempts: host_phase_2_attempt,
        };
    }

    write_starting_with_control_plane(
        log,
        1,
        destinations,
        control_plane_id,
        control_plane_data,
        transport,
        event_sender,
    )
    .await
}

// Attempt to write the control plane image, assuming the host phase 2 has
// already been written.
async fn write_starting_with_control_plane(
    log: &Logger,
    control_plane_attempt: usize,
    destinations: &ArtifactDestination,
    control_plane_id: &ArtifactHashId,
    control_plane_data: &BufList,
    transport: &mut impl WriteTransport,
    event_sender: &mpsc::Sender<ReportEvent>,
) -> DriveWriteProgress {
    // Temporary workaround while we may not know how to write the control plane
    // image: if we don't know where to put it, we're done.
    let Some(control_plane_dest) = destinations.control_plane.as_ref() else {
        return DriveWriteProgress::Done;
    };

    if let Err(error) = write_artifact_impl(
        control_plane_attempt,
        control_plane_id,
        control_plane_data.clone(),
        control_plane_dest,
        true,
        transport,
        event_sender,
    )
    .await
    {
        info!(log, "{error:?}"; "artifact_id" => ?control_plane_id);
        return DriveWriteProgress::ControlPlaneFailed {
            attempts: control_plane_attempt,
        };
    }

    DriveWriteProgress::Done
}

// Used in tests to test against file failures.
#[async_trait]
trait WriteTransport: fmt::Debug {
    type W: AsyncWrite + Unpin;

    async fn make_writer(
        &mut self,
        destination: &Utf8Path,
        create: bool,
    ) -> Result<Self::W>;
}

#[derive(Debug)]
struct FileTransport;

#[async_trait]
impl WriteTransport for FileTransport {
    type W = tokio::fs::File;

    async fn make_writer(
        &mut self,
        destination: &Utf8Path,
        create: bool,
    ) -> Result<Self::W> {
        Ok(tokio::fs::OpenOptions::new()
            .create(create)
            .write(true)
            .truncate(true)
            .open(destination)
            .await
            .with_context(|| {
                format!(
                    "failed to open destination `{destination}` for writing",
                )
            })?)
    }
}

async fn write_artifact_impl(
    attempt: usize,
    artifact_id: &ArtifactHashId,
    mut artifact: BufList,
    destination: &Utf8Path,
    create: bool,
    transport: &mut impl WriteTransport,
    event_sender: &mpsc::Sender<ReportEvent>,
) -> Result<()> {
    let mut writer = transport.make_writer(destination, create).await?;

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
    use omicron_common::api::internal::nexus::KnownArtifactKind;
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
        data1: Vec<Vec<u8>>,
        #[strategy(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8192), 0..16))]
        data2: Vec<Vec<u8>>,
        #[strategy(op_strategy())] partial_ops: VecDeque<Vec<PartialOp>>,
    ) {
        with_test_runtime(move || async move {
            proptest_write_artifact_impl(data1, data2, partial_ops)
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
        //
        // Because we will write the host image and then the control plane
        // image, we return two concatenated lists of "fails then one success".
        let success_strategy_host = prop::collection::vec(
            partial_op_strategy(interrupted_would_block_strategy(), 1024),
            0..16,
        );
        let success_strategy_control_plane = prop::collection::vec(
            partial_op_strategy(interrupted_would_block_strategy(), 1024),
            0..16,
        );

        (
            0..16u32,
            success_strategy_host,
            0..16u32,
            success_strategy_control_plane,
        )
            .prop_map(
                |(failure_count1, success1, failure_count2, success2)| {
                    let failure1 = (0..failure_count1).map(|_| {
                        vec![PartialOp::Err(std::io::ErrorKind::Other)]
                    });
                    let failure2 = (0..failure_count2).map(|_| {
                        vec![PartialOp::Err(std::io::ErrorKind::Other)]
                    });

                    failure1
                        .chain(std::iter::once(success1))
                        .chain(failure2)
                        .chain(std::iter::once(success2))
                        .collect()
                },
            )
    }

    async fn proptest_write_artifact_impl(
        data1: Vec<Vec<u8>>,
        data2: Vec<Vec<u8>>,
        partial_ops: VecDeque<Vec<PartialOp>>,
    ) -> Result<()> {
        let logctx = test_setup_log("test_write_artifact");
        let tempdir = tempdir()?;
        let tempdir_path: &Utf8Path = tempdir.path().try_into()?;

        let destination_host = tempdir_path.join("test-host.bin");
        let destination_control_plane =
            tempdir_path.join("test-control-plane.bin");

        let mut artifact_host: BufList =
            data1.into_iter().map(Bytes::from).collect();
        let mut artifact_control_plane: BufList =
            data2.into_iter().map(Bytes::from).collect();

        let host_id = dummy_artifact_hash_id(KnownArtifactKind::Host);
        let control_plane_id =
            dummy_artifact_hash_id(KnownArtifactKind::ControlPlane);

        // Which attempt is this going to first succeed at? For now, we expect a
        // sequence of: [failures, success, failures, success]; the host will
        // succeed on the first success and the control plane on the second, but
        // its attempt count is just its sequence of failures. Scan
        // `partial_ops` to produce these counts.
        let mut expected_attempt_host = None;
        let mut expected_attempt_control_plane = None;
        for (i, op) in partial_ops.iter().enumerate() {
            if !matches!(op[..], [PartialOp::Err(std::io::ErrorKind::Other)]) {
                match expected_attempt_host {
                    Some(attempt) => {
                        expected_attempt_control_plane = Some(i + 1 - attempt);
                        break;
                    }
                    None => {
                        expected_attempt_host = Some(i + 1);
                    }
                }
            }
        }
        let expected_attempt_host = expected_attempt_host
            .expect("did not find first success in partial_ops");
        let expected_attempt_control_plane = expected_attempt_control_plane
            .expect("did not find second success in partial_ops");

        let mut transport =
            PartialIoTransport { file_transport: FileTransport, partial_ops };

        let (event_sender, event_receiver) = mpsc::channel(512);

        let receiver_handle = tokio::spawn(async move {
            ReceiverStream::new(event_receiver).collect::<Vec<_>>().await
        });

        // Create a `WriteDestination` that points to our tempdir paths.
        let mut drives = BTreeMap::new();

        // TODO This only tests writing to a single drive; we should expand this
        // test (or maybe write a different one, given how long this one already
        // is?) that checks writing to both drives.
        drives.insert(
            M2Slot::A,
            ArtifactDestination {
                create_host_phase_2: true,
                host_phase_2: destination_host.clone(),
                control_plane: Some(destination_control_plane.clone()),
            },
        );
        let destination = WriteDestination { drives };

        let mut writer = ArtifactWriter::new(
            &host_id,
            &artifact_host,
            &control_plane_id,
            &artifact_control_plane,
            destination,
        );

        writer
            .write_with_transport(&logctx.log, &mut transport, &event_sender)
            .await;

        std::mem::drop(event_sender);

        let events = receiver_handle.await?;

        let mut seen_completion_host = false;
        let mut seen_completion_control_plane = false;
        let mut current_attempt_host = 1;
        let mut current_attempt_control_plane = 1;
        let mut last_written_bytes_host = 0;
        let mut last_written_bytes_control_plane = 0;

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
                    if kind == KnownArtifactKind::Host.into() {
                        assert!(
                            !seen_completion_host,
                            "no more progress events after completion"
                        );
                        assert!(
                            !seen_completion_control_plane,
                            "host should be written before control plane"
                        );
                        assert_eq!(attempt, current_attempt_host);
                        assert_eq!(destination, destination_host);
                        assert_eq!(
                            total_bytes,
                            artifact_host.num_bytes() as u64
                        );
                        assert!(
                            written_bytes > 0,
                            "non-zero number of bytes should be written"
                        );
                        assert!(
                            written_bytes > last_written_bytes_host,
                            "progress made with written bytes {written_bytes} > {last_written_bytes_host}"
                        );
                        last_written_bytes_host = written_bytes;
                    } else if kind == KnownArtifactKind::ControlPlane.into() {
                        assert!(
                            seen_completion_host,
                            "control plane should only be written after host completes"
                        );
                        assert!(
                            !seen_completion_control_plane,
                            "no more progress events after completion"
                        );
                        assert_eq!(attempt, current_attempt_control_plane);
                        assert_eq!(destination, destination_control_plane);
                        assert_eq!(
                            total_bytes,
                            artifact_control_plane.num_bytes() as u64
                        );
                        assert!(
                            written_bytes > 0,
                            "non-zero number of bytes should be written"
                        );
                        assert!(
                            written_bytes > last_written_bytes_control_plane,
                            "progress made with written bytes {written_bytes} > {last_written_bytes_control_plane}"
                        );
                        last_written_bytes_control_plane = written_bytes;
                    } else {
                        panic!("unexpected kind {kind:?}");
                    }
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
                    if kind == KnownArtifactKind::Host.into() {
                        assert!(
                            !seen_completion_host,
                            "only one WriteCompleted event seen"
                        );
                        assert!(
                            !seen_completion_control_plane,
                            "host should be written before control plane"
                        );
                        seen_completion_host = true;
                        assert_eq!(
                            attempt, expected_attempt_host,
                            "succeeded on expected attempt"
                        );
                        assert_eq!(destination, destination_host);
                        assert_eq!(
                            artifact_size,
                            artifact_host.num_bytes() as u64
                        );
                    } else if kind == KnownArtifactKind::ControlPlane.into() {
                        assert!(
                            seen_completion_host,
                            "host should be written before control plane"
                        );
                        assert!(
                            !seen_completion_control_plane,
                            "only one WriteCompleted event seen"
                        );
                        seen_completion_control_plane = true;
                        assert_eq!(
                            attempt, expected_attempt_control_plane,
                            "succeeded on expected attempt"
                        );
                        assert_eq!(destination, destination_control_plane);
                        assert_eq!(
                            artifact_size,
                            artifact_control_plane.num_bytes() as u64
                        );
                    } else {
                        panic!("unexpected kind {kind:?}");
                    }
                }
                ReportEvent::Completion(CompletionEventKind::WriteFailed {
                    attempt,
                    kind,
                    destination,
                    written_bytes,
                    total_bytes,
                    ..
                }) => {
                    if kind == KnownArtifactKind::Host.into() {
                        assert!(
                            !seen_completion_host,
                            "no more failure events after completion"
                        );
                        assert!(
                            !seen_completion_control_plane,
                            "host should be written before control plane"
                        );
                        assert_eq!(
                            attempt, current_attempt_host,
                            "attempt matches"
                        );
                        assert_eq!(destination, destination_host);
                        assert_eq!(written_bytes, last_written_bytes_host);
                        assert_eq!(
                            total_bytes,
                            artifact_host.num_bytes() as u64
                        );

                        // Reset the counter of last written bytes since we're
                        // starting afresh.
                        last_written_bytes_host = 0;
                        current_attempt_host += 1;
                    } else if kind == KnownArtifactKind::ControlPlane.into() {
                        assert!(
                            seen_completion_host,
                            "host should be written before control plane"
                        );
                        assert!(
                            !seen_completion_control_plane,
                            "no more failure events after completion"
                        );
                        assert_eq!(
                            attempt, current_attempt_control_plane,
                            "attempt matches"
                        );
                        assert_eq!(destination, destination_control_plane);
                        assert_eq!(
                            written_bytes,
                            last_written_bytes_control_plane
                        );
                        assert_eq!(
                            total_bytes,
                            artifact_control_plane.num_bytes() as u64
                        );

                        // Reset the counter of last written bytes since we're
                        // starting afresh.
                        last_written_bytes_control_plane = 0;
                        current_attempt_control_plane += 1;
                    } else {
                        panic!("unexpected kind {kind:?}");
                    }
                }
                other => {
                    panic!("unexpected event: {other:?}");
                }
            }
        }

        assert!(seen_completion_host, "seen a WriteCompleted event for host");

        // Read the host artifact from disk and ensure it is correct.
        let mut file = tokio::fs::File::open(&destination_host)
            .await
            .with_context(|| {
                format!("failed to open {destination_host} to verify contents")
            })?;
        let mut buf = Vec::with_capacity(artifact_host.num_bytes());
        let read_num_bytes =
            file.read_to_end(&mut buf).await.with_context(|| {
                format!("failed to read {destination_host} into memory")
            })?;
        assert_eq!(
            read_num_bytes,
            artifact_host.num_bytes(),
            "read num_bytes matches"
        );

        let bytes = artifact_host.copy_to_bytes(artifact_host.num_bytes());
        assert_eq!(buf, bytes, "bytes written to disk match");

        assert!(
            seen_completion_control_plane,
            "seen a WriteCompleted event for control_plane"
        );

        // Read the control_plane artifact from disk and ensure it is correct.
        let mut file = tokio::fs::File::open(&destination_control_plane)
            .await
            .with_context(|| {
                format!("failed to open {destination_control_plane} to verify contents")
            })?;
        let mut buf = Vec::with_capacity(artifact_control_plane.num_bytes());
        let read_num_bytes =
            file.read_to_end(&mut buf).await.with_context(|| {
                format!(
                    "failed to read {destination_control_plane} into memory"
                )
            })?;
        assert_eq!(
            read_num_bytes,
            artifact_control_plane.num_bytes(),
            "read num_bytes matches"
        );

        let bytes = artifact_control_plane
            .copy_to_bytes(artifact_control_plane.num_bytes());
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

        async fn make_writer(
            &mut self,
            destination: &Utf8Path,
            create: bool,
        ) -> Result<Self::W> {
            let f =
                self.file_transport.make_writer(destination, create).await?;
            // This is the next series of operations.
            let these_ops =
                self.partial_ops.pop_front().unwrap_or_else(Vec::new);
            Ok(PartialAsyncWrite::new(f, these_ops))
        }
    }
}
