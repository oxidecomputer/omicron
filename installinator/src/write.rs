// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt, io,
    os::fd::AsRawFd,
    time::Duration,
};

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use buf_list::BufList;
use bytes::Buf;
use camino::{Utf8Path, Utf8PathBuf};
use illumos_utils::dkio::MediaInfoExtended;
use installinator_common::{
    M2Slot, StepContext, StepProgress, StepResult, UpdateEngine,
    WriteComponent, WriteError, WriteOutput, WriteSpec, WriteStepId,
};
use omicron_common::update::ArtifactHashId;
use slog::{info, warn, Logger};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{block_size_writer::BlockSizeBufWriter, hardware::Hardware};

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
    is_host_phase_2_block_device: bool,
}

/// The name of the host phase 2 image written to disk.
///
/// Exposed for testing.
pub static HOST_PHASE_2_FILE_NAME: &str = "host_phase_2.bin";

/// The name of the control plane image written to disk.
///
/// Exposed for testing.
pub static CONTROL_PLANE_FILE_NAME: &str = "control_plane.bin";

impl WriteDestination {
    pub(crate) fn in_directory(dir: &Utf8Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("error creating directories at {dir}"))?;

        // `in_directory()` is only used for testing (e.g., on
        // not-really-gimlets); pretend we're only writing to M.2 A.
        let mut drives = BTreeMap::new();
        drives.insert(
            M2Slot::A,
            ArtifactDestination {
                create_host_phase_2: true,
                host_phase_2: dir.join(HOST_PHASE_2_FILE_NAME),
                control_plane: Some(dir.join(CONTROL_PLANE_FILE_NAME)),
            },
        );

        Ok(Self { drives, is_host_phase_2_block_device: false })
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
                    let path = Utf8PathBuf::try_from(path)
                        .context("non-UTF8 drive path")?;
                    info!(
                        log, "found target M.2 disk";
                        "identity" => ?disk.identity(),
                        "path" => disk.devfs_path().display(),
                        "slot" => disk.slot(),
                        "boot_image_path" => %path,
                        "zpool" => %disk.zpool_name(),
                    );

                    match drives.entry(slot) {
                        Entry::Vacant(entry) => {
                            entry.insert(ArtifactDestination {
                                create_host_phase_2: false,
                                host_phase_2: path,
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
                                "boot_image_path" => %path,
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

        Ok(Self { drives, is_host_phase_2_block_device: true })
    }
}

/// State machine for our progress writing to one of the M.2 drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveWriteProgress {
    /// We have not yet attempted any writes to the drive.
    Unstarted,
    /// We've tried and failed to write the host phase 2 image.
    HostPhase2Failed,
    /// We succeeded in writing the host phase 2 image, but failed to write the
    /// control plane `attempts` times.
    ControlPlaneFailed,
    /// We succeeded in writing both the host phase 2 image and the control
    /// plane image.
    Done,
}

pub(crate) struct ArtifactWriter<'a> {
    drives: BTreeMap<M2Slot, (ArtifactDestination, DriveWriteProgress)>,
    is_host_phase_2_block_device: bool,
    artifacts: ArtifactsToWrite<'a>,
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
            is_host_phase_2_block_device: destination
                .is_host_phase_2_block_device,
            artifacts: ArtifactsToWrite {
                host_phase_2_id,
                host_phase_2_data,
                control_plane_id,
                control_plane_data,
            },
        }
    }

    pub(crate) async fn write(
        &mut self,
        cx: &StepContext,
        log: &Logger,
    ) -> WriteOutput {
        let mut control_plane_transport = FileTransport;
        if self.is_host_phase_2_block_device {
            let mut host_transport = BlockDeviceTransport;
            self.write_with_transport(
                cx,
                log,
                &mut host_transport,
                &mut control_plane_transport,
            )
            .await
        } else {
            let mut host_transport = FileTransport;
            self.write_with_transport(
                cx,
                log,
                &mut host_transport,
                &mut control_plane_transport,
            )
            .await
        }
    }

    async fn write_with_transport(
        &mut self,
        cx: &StepContext,
        log: &Logger,
        host_phase_2_transport: &mut impl WriteTransport,
        control_plane_transport: &mut impl WriteTransport,
    ) -> WriteOutput {
        let mut done_drives = BTreeSet::new();

        // How many drives did we finish writing during the previous iteration?
        let mut success_prev_iter = 0;

        loop {
            // How many drives did we finish writing during this iteration?
            // Includes drives that were written during a previous iteration.
            let mut success_this_iter = 0;

            for (drive, (destinations, progress)) in self.drives.iter_mut() {
                // Register a separate nested engine for each drive, since we
                // want each drive to track success and failure independently.
                let write_cx = SlotWriteContext {
                    log: log.clone(),
                    artifacts: self.artifacts,
                    slot: *drive,
                    destinations,
                    progress: *progress,
                };
                let res = cx
                    .with_nested_engine(|engine| {
                        write_cx.register_steps(
                            engine,
                            host_phase_2_transport,
                            control_plane_transport,
                        );
                        Ok(())
                    })
                    .await;

                match res {
                    Ok(_) => {
                        // This drive succeeded in this iteration. This can be
                        // either:
                        // * the drive was written this time, or
                        // * the drive was successfully written during a
                        //   previous attempt.
                        *progress = DriveWriteProgress::Done;
                        done_drives.insert(*drive);
                        success_this_iter += 1;
                    }
                    Err(error) => match error.component {
                        WriteComponent::HostPhase2 => {
                            *progress = DriveWriteProgress::HostPhase2Failed;
                        }
                        WriteComponent::ControlPlane => {
                            *progress = DriveWriteProgress::ControlPlaneFailed;
                        }
                        WriteComponent::Unknown => {
                            unreachable!(
                                "we should never generate an unknown component"
                            )
                        }
                    },
                }
            }

            // Stop if either:
            // 1. All drives have successfully written
            // 2. At least one drive was successfully written on a previous
            //    iteration, which implies all other drives got to retry during
            //    this iteration.
            if success_this_iter == self.drives.len() || success_prev_iter > 0 {
                break;
            }

            cx.send_progress(StepProgress::retry(format!(
                "{}/{} slots succeeded",
                success_this_iter,
                self.drives.len()
            )))
            .await;

            // Give it a short break, then keep trying.
            tokio::time::sleep(Duration::from_secs(5)).await;

            success_prev_iter = success_this_iter;
        }

        WriteOutput {
            slots_attempted: self.drives.keys().copied().collect(),
            slots_written: done_drives.into_iter().collect(),
        }
    }
}

struct SlotWriteContext<'a> {
    log: Logger,
    artifacts: ArtifactsToWrite<'a>,
    slot: M2Slot,
    destinations: &'a ArtifactDestination,
    progress: DriveWriteProgress,
}

impl<'a> SlotWriteContext<'a> {
    fn register_steps<'b>(
        &'b self,
        engine: &UpdateEngine<'b, WriteSpec>,
        host_phase_2_transport: &'b mut impl WriteTransport,
        control_plane_transport: &'b mut impl WriteTransport,
    ) {
        match self.progress {
            DriveWriteProgress::Unstarted
            | DriveWriteProgress::HostPhase2Failed => {
                self.register_host_phase_2_step(engine, host_phase_2_transport);
                self.register_control_plane_step(
                    engine,
                    control_plane_transport,
                );
            }
            DriveWriteProgress::ControlPlaneFailed => {
                self.register_control_plane_step(
                    engine,
                    control_plane_transport,
                );
            }
            DriveWriteProgress::Done => {
                // Don't register any steps -- this is done.
            }
        }
    }

    fn register_host_phase_2_step<'b, WT: WriteTransport>(
        &'b self,
        engine: &UpdateEngine<'b, WriteSpec>,
        transport: &'b mut WT,
    ) {
        engine
            .new_step(
                WriteComponent::HostPhase2,
                WriteStepId::Writing { slot: self.slot },
                format!("Writing host phase 2 to slot {}", self.slot),
                move |cx2| async move {
                    self.artifacts
                        .write_host_phase_2(
                            &self.log,
                            self.slot,
                            self.destinations,
                            transport,
                            &cx2,
                        )
                        .await
                },
            )
            .register();
    }

    fn register_control_plane_step<'b, WT: WriteTransport>(
        &'b self,
        engine: &UpdateEngine<'b, WriteSpec>,
        transport: &'b mut WT,
    ) {
        engine
            .new_step(
                WriteComponent::ControlPlane,
                WriteStepId::Writing { slot: self.slot },
                format!("Writing control plane to slot {}", self.slot),
                move |cx2| async move {
                    self.artifacts
                        .write_control_plane(
                            &self.log,
                            self.slot,
                            self.destinations,
                            transport,
                            &cx2,
                        )
                        .await
                },
            )
            .register();
    }
}

#[derive(Copy, Clone)]
struct ArtifactsToWrite<'a> {
    host_phase_2_id: &'a ArtifactHashId,
    host_phase_2_data: &'a BufList,
    control_plane_id: &'a ArtifactHashId,
    control_plane_data: &'a BufList,
}

impl ArtifactsToWrite<'_> {
    /// Attempt to write the host phase 2 image.
    async fn write_host_phase_2<'b, WT: WriteTransport>(
        &self,
        log: &Logger,
        slot: M2Slot,
        destinations: &ArtifactDestination,
        transport: &'b mut WT,
        cx: &StepContext<WriteSpec>,
    ) -> Result<StepResult<&'b mut WT, WriteSpec>, WriteError> {
        write_artifact_impl(
            WriteComponent::HostPhase2,
            slot,
            self.host_phase_2_data.clone(),
            &destinations.host_phase_2,
            destinations.create_host_phase_2,
            transport,
            cx,
        )
        .await
        .map_err(|error| {
            info!(log, "{error:?}"; "artifact_id" => ?self.host_phase_2_id);
            error
        })?;

        StepResult::success(transport, ())
    }

    // Attempt to write the control plane image.
    async fn write_control_plane(
        &self,
        log: &Logger,
        slot: M2Slot,
        destinations: &ArtifactDestination,
        transport: &mut impl WriteTransport,
        cx: &StepContext<WriteSpec>,
    ) -> Result<StepResult<(), WriteSpec>, WriteError> {
        // Temporary workaround while we may not know how to write the control
        // plane image: if we don't know where to put it, we're done.
        let Some(control_plane_dest) = destinations.control_plane.as_ref() else
        {
            return StepResult::skipped((), (), "We don't yet know how to write the control plane");
        };

        write_artifact_impl(
            WriteComponent::ControlPlane,
            slot,
            self.control_plane_data.clone(),
            control_plane_dest,
            true,
            transport,
            cx,
        )
        .await
        .map_err(|error| {
            info!(log, "{error:?}"; "artifact_id" => ?self.control_plane_id);
            error
        })?;

        StepResult::success((), ())
    }
}

// Used in tests to test against file failures.
#[async_trait]
trait WriteTransport: fmt::Debug + Send {
    type W: AsyncWrite + Send + Unpin;

    async fn make_writer(
        &mut self,
        component: WriteComponent,
        slot: M2Slot,
        destination: &Utf8Path,
        total_bytes: u64,
        create: bool,
    ) -> Result<Self::W, WriteError>;
}

#[derive(Debug)]
struct FileTransport;

#[async_trait]
impl WriteTransport for FileTransport {
    type W = tokio::fs::File;

    async fn make_writer(
        &mut self,
        component: WriteComponent,
        slot: M2Slot,
        destination: &Utf8Path,
        total_bytes: u64,
        create: bool,
    ) -> Result<Self::W, WriteError> {
        tokio::fs::OpenOptions::new()
            .create(create)
            .write(true)
            .truncate(create)
            .open(destination)
            .await
            .map_err(|error| WriteError {
                component,
                slot,
                written_bytes: 0,
                total_bytes,
                error,
            })
    }
}

#[derive(Debug)]
struct BlockDeviceTransport;

#[async_trait]
impl WriteTransport for BlockDeviceTransport {
    type W = BlockSizeBufWriter<tokio::fs::File>;

    async fn make_writer(
        &mut self,
        component: WriteComponent,
        slot: M2Slot,
        destination: &Utf8Path,
        total_bytes: u64,
        create: bool,
    ) -> Result<Self::W, WriteError> {
        let f = tokio::fs::OpenOptions::new()
            .create(create)
            .write(true)
            .truncate(create)
            .open(destination)
            .await
            .map_err(|error| WriteError {
                component,
                slot,
                written_bytes: 0,
                total_bytes,
                error,
            })?;

        let media_info =
            MediaInfoExtended::from_fd(f.as_raw_fd()).map_err(|error| {
                WriteError {
                    component,
                    slot,
                    written_bytes: 0,
                    total_bytes,
                    error,
                }
            })?;

        let block_size = u64::from(media_info.logical_block_size);

        // When writing to a block device, we must write a multiple of the block
        // size. We can assume the image we're given should be
        // appropriately-sized: return an error here if it is not.
        if total_bytes % block_size != 0 {
            return Err(WriteError {
                component,
                slot,
                written_bytes: 0,
                total_bytes,
                error: io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("file size ({total_bytes}) is not a multiple of target device block size ({block_size})")
                ),
            });
        }

        Ok(BlockSizeBufWriter::with_block_size(block_size as usize, f))
    }
}

async fn write_artifact_impl(
    component: WriteComponent,
    slot: M2Slot,
    mut artifact: BufList,
    destination: &Utf8Path,
    create: bool,
    transport: &mut impl WriteTransport,
    cx: &StepContext<WriteSpec>,
) -> Result<(), WriteError> {
    let mut writer = transport
        .make_writer(
            component,
            slot,
            destination,
            artifact.num_bytes() as u64,
            create,
        )
        .await?;

    let total_bytes = artifact.num_bytes() as u64;
    let mut written_bytes = 0u64;

    while artifact.has_remaining() {
        match writer.write_buf(&mut artifact).await {
            Ok(n) => {
                written_bytes += n as u64;
                cx.send_progress(StepProgress::with_current_and_total(
                    written_bytes,
                    total_bytes,
                    (),
                ))
                .await;
            }
            Err(error) => {
                return Err(WriteError {
                    component,
                    slot,
                    written_bytes,
                    total_bytes,
                    error,
                });
            }
        }
    }

    match writer.flush().await {
        Ok(()) => {}
        Err(error) => {
            return Err(WriteError {
                component,
                slot,
                written_bytes,
                total_bytes,
                error,
            });
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use super::*;
    use crate::test_helpers::{dummy_artifact_hash_id, with_test_runtime};

    use anyhow::Result;
    use bytes::{Buf, Bytes};
    use camino::Utf8Path;
    use futures::StreamExt;
    use installinator_common::{
        Event, InstallinatorCompletionMetadata, InstallinatorComponent,
        InstallinatorStepId, StepEventKind, StepOutcome,
    };
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
    use tokio::sync::Mutex;
    use tokio_stream::wrappers::ReceiverStream;

    #[proptest(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
    fn proptest_write_artifact(
        #[strategy(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8192), 0..16))]
        data1: Vec<Vec<u8>>,
        #[strategy(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8192), 0..16))]
        data2: Vec<Vec<u8>>,
        #[strategy(WriteOps::strategy())] write_ops: WriteOps,
    ) {
        with_test_runtime(move || async move {
            proptest_write_artifact_impl(data1, data2, write_ops)
                .await
                .expect("test failed");
        })
    }

    #[derive(Debug)]
    struct WriteOps {
        ops: VecDeque<Vec<PartialOp>>,
        host_failure_count: usize,
        control_plane_failure_count: usize,
    }

    impl WriteOps {
        fn strategy() -> impl Strategy<Value = Self> {
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
                0..16usize,
                success_strategy_host,
                0..16usize,
                success_strategy_control_plane,
            )
                .prop_map(
                    |(
                        host_failure_count,
                        success1,
                        control_plane_failure_count,
                        success2,
                    )| {
                        let failure1 = (0..host_failure_count).map(|_| {
                            vec![PartialOp::Err(std::io::ErrorKind::Other)]
                        });
                        let failure2 =
                            (0..control_plane_failure_count).map(|_| {
                                vec![PartialOp::Err(std::io::ErrorKind::Other)]
                            });

                        let ops = failure1
                            .chain(std::iter::once(success1))
                            .chain(failure2)
                            .chain(std::iter::once(success2))
                            .collect();

                        Self {
                            ops,
                            host_failure_count,
                            control_plane_failure_count,
                        }
                    },
                )
        }

        fn total_attempts(&self) -> usize {
            // +1 because if there are no failures, we start counting at 1.
            self.host_failure_count + self.control_plane_failure_count + 1
        }
    }

    async fn proptest_write_artifact_impl(
        data1: Vec<Vec<u8>>,
        data2: Vec<Vec<u8>>,
        write_ops: WriteOps,
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

        // XXX: note we don't assert on the number of attempts it took to write
        // just the host image at the moment.
        let expected_total_attempts = write_ops.total_attempts();

        let (mut host_transport, mut control_plane_transport) = {
            let transport = PartialIoTransport {
                file_transport: FileTransport,
                partial_ops: write_ops.ops,
            };
            let inner = Arc::new(Mutex::new(transport));
            (SharedTransport(Arc::clone(&inner)), SharedTransport(inner))
        };

        let (event_sender, event_receiver) = tokio::sync::mpsc::channel(512);

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
        let destination =
            WriteDestination { drives, is_host_phase_2_block_device: false };

        let mut writer = ArtifactWriter::new(
            &host_id,
            &artifact_host,
            &control_plane_id,
            &artifact_control_plane,
            destination,
        );

        let engine = UpdateEngine::new(&logctx.log, event_sender);
        let log = logctx.log.clone();
        engine
            .new_step(
                InstallinatorComponent::Both,
                InstallinatorStepId::Write,
                "Writing",
                |cx| async move {
                    let write_output = writer
                        .write_with_transport(
                            &cx,
                            &log,
                            &mut host_transport,
                            &mut control_plane_transport,
                        )
                        .await;
                    StepResult::success(
                        (),
                        InstallinatorCompletionMetadata::Write {
                            output: write_output,
                        },
                    )
                },
            )
            .register();

        engine.execute().await.expect("we keep retrying until success");

        let events = receiver_handle.await?;

        // For now, just ensure that we receive an execution completed event
        // with the right number of attempts.
        //
        // TODO: expand this in the future.
        let last_event = events.last().expect("at least one event present");
        match last_event {
            Event::Step(event) => match &event.kind {
                StepEventKind::ExecutionCompleted {
                    last_attempt,
                    last_outcome,
                    ..
                } => {
                    assert_eq!(
                        *last_attempt, expected_total_attempts,
                        "last attempt matches expected"
                    );
                    match last_outcome {
                        StepOutcome::Success {
                            metadata:
                                InstallinatorCompletionMetadata::Write { output },
                        } => {
                            assert_eq!(
                                &output
                                    .slots_written
                                    .iter()
                                    .copied()
                                    .collect::<Vec<_>>(),
                                &vec![M2Slot::A],
                                "correct slots written"
                            );
                        }
                        other => {
                            panic!("unexpected last_outcome: {other:?}")
                        }
                    }
                }
                other => {
                    panic!("unexpected step event: {other:?}");
                }
            },
            other => {
                panic!("unexpected event: {other:?}");
            }
        }

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
    struct SharedTransport(Arc<Mutex<PartialIoTransport>>);

    #[async_trait]
    impl WriteTransport for SharedTransport {
        type W = PartialAsyncWrite<tokio::fs::File>;

        async fn make_writer(
            &mut self,
            component: WriteComponent,
            slot: M2Slot,
            destination: &Utf8Path,
            total_bytes: u64,
            create: bool,
        ) -> Result<Self::W, WriteError> {
            self.0
                .lock()
                .await
                .make_writer(component, slot, destination, total_bytes, create)
                .await
        }
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
            component: WriteComponent,
            slot: M2Slot,
            destination: &Utf8Path,
            total_bytes: u64,
            create: bool,
        ) -> Result<Self::W, WriteError> {
            let f = self
                .file_transport
                .make_writer(component, slot, destination, total_bytes, create)
                .await?;
            // This is the next series of operations.
            let these_ops =
                self.partial_ops.pop_front().unwrap_or_else(Vec::new);
            Ok(PartialAsyncWrite::new(f, these_ops))
        }
    }
}
