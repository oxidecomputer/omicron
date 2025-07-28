// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt,
    io::{self, Read},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use buf_list::BufList;
use bytes::{Buf, Bytes};
use camino::{Utf8Path, Utf8PathBuf};
use iddqd::IdOrdMap;
use illumos_utils::zpool::{Zpool, ZpoolName};
use installinator_common::{
    ControlPlaneZonesSpec, ControlPlaneZonesStepId, RawDiskWriter, StepContext,
    StepProgress, StepResult, StepSuccess, UpdateEngine, WriteComponent,
    WriteError, WriteOutput, WriteSpec, WriteStepId,
};
use omicron_common::{
    disk::M2Slot,
    update::{
        MupdateOverrideInfo, OmicronZoneFileMetadata, OmicronZoneManifest,
        OmicronZoneManifestSource,
    },
};
use omicron_uuid_kinds::{MupdateOverrideUuid, MupdateUuid};
use sha2::{Digest, Sha256};
use slog::{Logger, info, warn};
use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt},
    task::JoinSet,
};
use tufaceous_artifact::{ArtifactHash, ArtifactHashId};
use tufaceous_lib::ControlPlaneZoneImages;
use update_engine::{
    StepSpec, errors::NestedEngineError, events::ProgressUnits,
};

use crate::{async_temp_file::AsyncNamedTempFile, hardware::Hardware};

#[derive(Clone, Debug)]
struct ArtifactDestination {
    // Path to write the host image; either a raw device corresponding to an M.2
    // partition (real gimlet) or a file (test).
    host_phase_2: Utf8PathBuf,

    // On real gimlets, we remove any files currently in the control plane
    // destination directory. For tests and non-gimlets, we don't want to go
    // around removing files arbitrarily.
    clean_control_plane_dir: bool,

    // Directory in which we unpack the control plane zone images.
    control_plane_dir: Utf8PathBuf,

    // Zpool containing `control_plane_dir`, if we're on a real gimlet.
    control_plane_zpool: Option<ZpoolName>,
}

impl ArtifactDestination {
    fn from_directory(dir: &Utf8Path) -> Result<Self> {
        // The install dataset goes into a directory called "install".
        let control_plane_dir = dir.join("install");
        std::fs::create_dir_all(&control_plane_dir)
            .with_context(|| format!("error creating directories at {dir}"))?;

        Ok(Self {
            host_phase_2: dir.join(HOST_PHASE_2_FILE_NAME),
            clean_control_plane_dir: false,
            control_plane_dir,
            control_plane_zpool: None,
        })
    }
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

impl WriteDestination {
    pub(crate) fn in_directories(
        a_dir: &Utf8Path,
        b_dir: Option<&Utf8Path>,
    ) -> Result<Self> {
        let mut drives = BTreeMap::new();

        drives.insert(M2Slot::A, ArtifactDestination::from_directory(a_dir)?);
        if let Some(dir) = b_dir {
            // b_dir can fail after we insert a_dir into drives, but that's okay
            // because we drop drives.
            drives.insert(M2Slot::B, ArtifactDestination::from_directory(dir)?);
        }

        Ok(Self { drives, is_host_phase_2_block_device: false })
    }

    pub(crate) async fn from_hardware(log: &Logger) -> Result<Self> {
        let hardware = Hardware::scan(log).await?;

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
                        "path" => disk.devfs_path().as_str(),
                        "slot" => disk.slot(),
                        "boot_image_path" => path.as_str(),
                        "zpool" => %disk.zpool_name(),
                    );

                    let zpool_name = *disk.zpool_name();
                    let control_plane_dir = zpool_name.dataset_mountpoint(
                        illumos_utils::zpool::ZPOOL_MOUNTPOINT_ROOT.into(),
                        sled_storage::dataset::INSTALL_DATASET,
                    );

                    match drives.entry(slot) {
                        Entry::Vacant(entry) => {
                            entry.insert(ArtifactDestination {
                                host_phase_2: path,
                                clean_control_plane_dir: true,
                                control_plane_dir,
                                control_plane_zpool: Some(zpool_name),
                            });
                        }
                        Entry::Occupied(_) => {
                            warn!(
                                log, "skipping duplicate M.2 drive entry";
                                "identity" => ?disk.identity(),
                                "path" => disk.devfs_path().as_str(),
                                "slot" => disk.slot(),
                                "boot_image_path" => path.as_str(),
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
                        "path" => disk.devfs_path().as_str(),
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

    pub(crate) fn num_target_disks(&self) -> usize {
        self.drives.len()
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
        mupdate_id: MupdateUuid,
        host_phase_2_id: &'a ArtifactHashId,
        host_phase_2_data: &'a BufList,
        control_plane_id: &'a ArtifactHashId,
        control_plane_zones: &'a ControlPlaneZoneImages,
        destination: WriteDestination,
    ) -> Self {
        let drives = destination
            .drives
            .into_iter()
            .map(|(key, value)| (key, (value, DriveWriteProgress::Unstarted)))
            .collect();

        // The mupdate override UUID is freshly generated within installinator.
        // At the moment, there's no reason to have it be passed in via Wicket
        // or some other means, though there are conceivably reasons to do so in
        // the future.
        let mupdate_override_uuid = MupdateOverrideUuid::new_v4();

        Self {
            drives,
            is_host_phase_2_block_device: destination
                .is_host_phase_2_block_device,
            artifacts: ArtifactsToWrite {
                host_phase_2_id,
                host_phase_2_data,
                control_plane_id,
                control_plane_zones,
                mupdate_id,
                mupdate_override_uuid,
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
                    Err(error) => match error {
                        NestedEngineError::Creation { .. } => {
                            unreachable!("nested engine creation is infallible")
                        }
                        NestedEngineError::StepFailed { component, .. }
                        | NestedEngineError::Aborted { component, .. } => {
                            match component {
                                WriteComponent::HostPhase2 => {
                                    *progress =
                                        DriveWriteProgress::HostPhase2Failed;
                                }
                                WriteComponent::ControlPlane => {
                                    *progress =
                                        DriveWriteProgress::ControlPlaneFailed;
                                }
                                WriteComponent::Unknown => {
                                    unreachable!(
                                        "we should never generate an unknown component"
                                    )
                                }
                            }
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

impl SlotWriteContext<'_> {
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
        let block_size_handle = engine
            .new_step(
                WriteComponent::HostPhase2,
                WriteStepId::Writing { slot: self.slot },
                format!("Writing host phase 2 to slot {}", self.slot),
                async move |ctx| {
                    self.artifacts
                        .write_host_phase_2(
                            &self.log,
                            self.slot,
                            self.destinations,
                            transport,
                            &ctx,
                        )
                        .await
                },
            )
            .register();

        engine
            .new_step(
                WriteComponent::HostPhase2,
                WriteStepId::Writing { slot: self.slot },
                format!(
                    "Validating checksum of host phase 2 in slot {}",
                    self.slot
                ),
                async move |ctx| {
                    let block_size =
                        block_size_handle.into_value(&ctx.token()).await;
                    self.validate_written_host_phase_2_hash(block_size).await
                },
            )
            .register();
    }

    async fn validate_written_host_phase_2_hash(
        &self,
        block_size: Option<usize>,
    ) -> Result<StepResult<(), WriteSpec>, WriteError> {
        let slot = self.slot;
        let mut remaining = self.artifacts.host_phase_2_data.num_bytes();
        let destination = self.destinations.host_phase_2.clone();

        // If we don't need a specific block size, default to hashing 1 MiB at a
        // time.
        let block_size = block_size.unwrap_or(1 << 20);

        // We definitely want to compute a large sha256 inside a
        // `spawn_blocking`, so we'll also use regular old std::fs::File. We
        // have to be a little careful to read in `block_size` chunks.
        let computed_hash = tokio::task::spawn_blocking(move || {
            let mut f =
                std::fs::File::open(&destination).with_context(|| {
                    format!("failed to open {destination} for reading")
                })?;

            let mut buf = vec![0; block_size];
            let mut hasher = Sha256::new();
            let mut offset = 0;

            while remaining > 0 {
                let buf = &mut buf[..usize::min(block_size, remaining)];
                f.read_exact(buf).with_context(|| {
                    format!(
                        "I/O error reading {destination} at offset {offset}"
                    )
                })?;

                hasher.update(&buf);

                offset += buf.len();
                remaining -= buf.len();
            }

            Ok(ArtifactHash(hasher.finalize().into()))
        })
        .await
        .unwrap()
        .map_err(WriteError::ChecksumValidationError)?;

        if computed_hash == self.artifacts.host_phase_2_id.hash {
            StepSuccess::new(())
                .with_message(format!(
                    "validated hash {computed_hash} \
                     for host phase 2 written to {slot:?}"
                ))
                .into()
        } else {
            Err(WriteError::ChecksumValidationError(anyhow!(
                "expected {} but computed {computed_hash} \
                 for host phase 2 written to {slot:?}",
                self.artifacts.host_phase_2_id.hash
            )))
        }
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
                async move |cx2| {
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
    control_plane_zones: &'a ControlPlaneZoneImages,
    mupdate_id: MupdateUuid,
    mupdate_override_uuid: MupdateOverrideUuid,
}

impl ArtifactsToWrite<'_> {
    /// Attempt to write the host phase 2 image.
    async fn write_host_phase_2<WT: WriteTransport>(
        &self,
        log: &Logger,
        slot: M2Slot,
        destinations: &ArtifactDestination,
        transport: &mut WT,
        cx: &StepContext<WriteSpec>,
    ) -> Result<StepResult<Option<usize>, WriteSpec>, WriteError> {
        let block_size = write_artifact_impl(
            WriteComponent::HostPhase2,
            slot,
            self.host_phase_2_data.clone(),
            &destinations.host_phase_2,
            transport,
            cx,
        )
        .await
        .map_err(|error| {
            info!(log, "{error:?}"; "artifact_id" => ?self.host_phase_2_id);
            error
        })?;

        StepSuccess::new(block_size).into()
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
        // Register a nested engine to write the set of zones, each zone as its
        // own step.
        let inner_cx = &ControlPlaneZoneWriteContext {
            slot,
            clean_output_directory: destinations.clean_control_plane_dir,
            output_directory: &destinations.control_plane_dir,
            zones: self.control_plane_zones,
            host_phase_2_id: self.host_phase_2_id,
            control_plane_id: self.control_plane_id,
            mupdate_id: self.mupdate_id,
            mupdate_override_uuid: self.mupdate_override_uuid,
        };
        cx.with_nested_engine(|engine| {
            inner_cx.register_steps(
                engine,
                transport,
                destinations.control_plane_zpool.as_ref(),
            );
            Ok(())
        })
        .await
        .map_err(|error| {
            warn!(
                log, "{error:?}";
                "artifact_id" => ?self.control_plane_id,
            );
            error
        })?;

        info!(
            log,
            "finished writing {} control plane zones",
            self.control_plane_zones.zones.len()
        );

        StepSuccess::new(()).into()
    }
}

struct ControlPlaneZoneWriteContext<'a> {
    slot: M2Slot,
    clean_output_directory: bool,
    output_directory: &'a Utf8Path,
    zones: &'a ControlPlaneZoneImages,
    host_phase_2_id: &'a ArtifactHashId,
    control_plane_id: &'a ArtifactHashId,
    mupdate_id: MupdateUuid,
    mupdate_override_uuid: MupdateOverrideUuid,
}

impl ControlPlaneZoneWriteContext<'_> {
    fn register_steps<'b>(
        &'b self,
        engine: &UpdateEngine<'b, ControlPlaneZonesSpec>,
        transport: &'b mut impl WriteTransport,
        zpool: Option<&'b ZpoolName>,
    ) {
        use update_engine::StepHandle;

        let slot = self.slot;
        let mupdate_override_uuid = self.mupdate_override_uuid;

        // If we're on a gimlet, remove any files in the control plane
        // destination directory.
        if self.clean_output_directory {
            let output_directory = self.output_directory.to_path_buf();
            engine
                .new_step(
                    WriteComponent::ControlPlane,
                    ControlPlaneZonesStepId::CleanTargetDirectory {
                        path: output_directory.clone(),
                    },
                    format!("Removing files in {}", output_directory),
                    async move |_cx| {
                        let path = output_directory.clone();
                        tokio::task::spawn_blocking(move || {
                            remove_contents_of(&output_directory)
                        })
                        .await
                        .unwrap()
                        .map_err(|error| {
                            WriteError::RemoveFilesError { path, error }
                        })?;

                        StepSuccess::new(()).into()
                    },
                )
                .register();
        }

        // Dealing with the `&mut impl WriteTransport` is tricky. Every step in
        // the loop below needs access to it, but we can't move it into every
        // closure. Instead, we put it into a `StepHandle`, and have each step
        // return it on completion. This way each step passes it forward to its
        // successor.
        let mut transport = StepHandle::ready(transport);

        // Write out a file to indicate that installinator has updated the
        // dataset. Do this at the very beginning to ensure that installinator's
        // presence is recorded even if something goes wrong after this step.
        transport = engine
            .new_step(
                WriteComponent::ControlPlane,
                ControlPlaneZonesStepId::MupdateOverride,
                "Writing MUPdate override file",
                async move |cx| {
                    let transport = transport.into_value(cx.token()).await;
                    let mupdate_json =
                        self.mupdate_override_artifact(mupdate_override_uuid);

                    let out_path = self
                        .output_directory
                        .join(MupdateOverrideInfo::FILE_NAME);

                    write_artifact_impl(
                        WriteComponent::ControlPlane,
                        slot,
                        mupdate_json,
                        &out_path,
                        transport,
                        &cx,
                    )
                    .await?;

                    StepSuccess::new(transport)
                        .with_message(format!(
                            "{out_path} written with mupdate override UUID: \
                             {mupdate_override_uuid}",
                        ))
                        .into()
                },
            )
            .register();

        transport = engine
            .new_step(
                WriteComponent::ControlPlane,
                ControlPlaneZonesStepId::ZoneManifest,
                "Writing zone manifest",
                async move |cx| {
                    let transport = transport.into_value(cx.token()).await;
                    let zone_manifest_json =
                        self.omicron_zone_manifest_artifact().await;

                    let out_path = self
                        .output_directory
                        .join(OmicronZoneManifest::FILE_NAME);

                    write_artifact_impl(
                        WriteComponent::ControlPlane,
                        slot,
                        zone_manifest_json,
                        &out_path,
                        transport,
                        &cx,
                    )
                    .await?;

                    StepSuccess::new(transport)
                        .with_message(format!(
                            "{out_path} written with mupdate UUID: {}",
                            self.mupdate_id,
                        ))
                        .into()
                },
            )
            .register();

        for (name, data) in &self.zones.zones {
            let out_path = self.output_directory.join(name);
            transport = engine
                .new_step(
                    WriteComponent::ControlPlane,
                    ControlPlaneZonesStepId::Zone { name: name.clone() },
                    format!("Writing zone {name}"),
                    async move |cx| {
                        let transport = transport.into_value(cx.token()).await;
                        write_artifact_impl(
                            WriteComponent::ControlPlane,
                            slot,
                            data.clone().into(),
                            &out_path,
                            transport,
                            &cx,
                        )
                        .await?;

                        StepSuccess::new(transport).into()
                    },
                )
                .register();
        }

        // `fsync()` the directory to ensure the directory entries for all the
        // files we just created are written to disk.
        let output_directory = self.output_directory.to_path_buf();
        engine
            .new_step(
                WriteComponent::ControlPlane,
                ControlPlaneZonesStepId::Fsync,
                "Syncing writes to disk",
                async move |_cx| {
                    let output_directory =
                        File::open(&output_directory).await.map_err(
                            |error| WriteError::SyncOutputDirError { error },
                        )?;
                    output_directory.sync_all().await.map_err(|error| {
                        WriteError::SyncOutputDirError { error }
                    })?;

                    // Drop `output_directory` to close it so we can export the
                    // zpool.
                    std::mem::drop(output_directory);

                    if let Some(zpool) = zpool {
                        Zpool::export(zpool).await?;
                    }

                    StepSuccess::new(()).into()
                },
            )
            .register();
    }

    fn mupdate_override_artifact(
        &self,
        mupdate_override_uuid: MupdateOverrideUuid,
    ) -> BufList {
        let hash_ids =
            [self.host_phase_2_id.clone(), self.control_plane_id.clone()]
                .into_iter()
                .collect();

        let mupdate_override = MupdateOverrideInfo {
            mupdate_uuid: mupdate_override_uuid,
            hash_ids,
        };
        let json_bytes = serde_json::to_vec(&mupdate_override)
            .expect("this serialization is infallible");
        BufList::from(json_bytes)
    }

    async fn omicron_zone_manifest_artifact(&self) -> BufList {
        let zones = compute_zone_hashes(&self.zones).await;

        let omicron_zone_manifest = OmicronZoneManifest {
            source: OmicronZoneManifestSource::Installinator {
                mupdate_id: self.mupdate_id,
            },
            zones,
        };
        let json_bytes = serde_json::to_vec(&omicron_zone_manifest)
            .expect("this serialization is infallible");
        BufList::from(json_bytes)
    }
}

/// Computes the zone hash IDs.
///
/// Hash computation is done in parallel on blocking tasks.
///
/// # Panics
///
/// Panics if the runtime shuts down causing a task abort, or a task panics.
async fn compute_zone_hashes(
    images: &ControlPlaneZoneImages,
) -> IdOrdMap<OmicronZoneFileMetadata> {
    let mut tasks = JoinSet::new();
    for (file_name, data) in &images.zones {
        let file_name = file_name.clone();
        // data is a Bytes so is cheap to clone.
        let data: Bytes = data.clone();
        // Compute hashes in parallel.
        tasks.spawn_blocking(move || {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let hash = hasher.finalize();
            OmicronZoneFileMetadata {
                file_name,
                file_size: u64::try_from(data.len()).unwrap(),
                hash: ArtifactHash(hash.into()),
            }
        });
    }

    let mut output = IdOrdMap::new();
    while let Some(res) = tasks.join_next().await {
        // Propagate panics across tasks—this is the standard pattern we follow
        // in installinator.
        output
            .insert_unique(res.expect("task panicked"))
            .expect("filenames are unique");
    }
    output
}

fn remove_contents_of(path: &Utf8Path) -> io::Result<()> {
    use std::fs;

    // We can't use `std::fs::remove_dir_all()` because we want to keep `path`
    // itself. Instead, walk through it and remove any files/directories we
    // find.
    let dir = fs::read_dir(path)?;

    for entry in dir {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
        } else {
            fs::remove_dir_all(entry.path())?;
        }
    }

    Ok(())
}

// Used in tests to test against file failures.
#[async_trait]
trait WriteTransport: fmt::Debug + Send {
    type W: WriteTransportWriter;

    async fn make_writer(
        &mut self,
        component: WriteComponent,
        slot: M2Slot,
        destination: &Utf8Path,
        total_bytes: u64,
    ) -> Result<Self::W, WriteError>;
}

#[async_trait]
trait WriteTransportWriter: AsyncWrite + Send + Unpin {
    fn block_size(&self) -> Option<usize>;
    async fn finalize(self) -> io::Result<()>;
}

#[async_trait]
impl WriteTransportWriter for AsyncNamedTempFile {
    fn block_size(&self) -> Option<usize> {
        None
    }

    async fn finalize(self) -> io::Result<()> {
        self.sync_all().await?;
        self.persist().await?;
        Ok(())
    }
}

#[async_trait]
impl WriteTransportWriter for RawDiskWriter {
    fn block_size(&self) -> Option<usize> {
        Some(RawDiskWriter::block_size(self))
    }

    async fn finalize(self) -> io::Result<()> {
        RawDiskWriter::finalize(self).await
    }
}

#[derive(Debug)]
struct FileTransport;

#[async_trait]
impl WriteTransport for FileTransport {
    type W = AsyncNamedTempFile;

    async fn make_writer(
        &mut self,
        component: WriteComponent,
        slot: M2Slot,
        destination: &Utf8Path,
        total_bytes: u64,
    ) -> Result<Self::W, WriteError> {
        AsyncNamedTempFile::with_destination(destination).await.map_err(
            |error| WriteError::WriteError {
                component,
                slot,
                written_bytes: 0,
                total_bytes,
                error,
            },
        )
    }
}

#[derive(Debug)]
struct BlockDeviceTransport;

#[async_trait]
impl WriteTransport for BlockDeviceTransport {
    type W = RawDiskWriter;

    async fn make_writer(
        &mut self,
        component: WriteComponent,
        slot: M2Slot,
        destination: &Utf8Path,
        total_bytes: u64,
    ) -> Result<Self::W, WriteError> {
        let writer = RawDiskWriter::open(destination.as_std_path())
            .await
            .map_err(|error| WriteError::WriteError {
                component,
                slot,
                written_bytes: 0,
                total_bytes,
                error,
            })?;

        let block_size = writer.block_size() as u64;

        // When writing to a block device, we must write a multiple of the block
        // size. We can assume the image we're given should be
        // appropriately-sized: return an error here if it is not.
        if total_bytes % block_size != 0 {
            return Err(WriteError::WriteError {
                component,
                slot,
                written_bytes: 0,
                total_bytes,
                error: io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "file size ({total_bytes}) is not a multiple of \
                         target device block size ({block_size})"
                    ),
                ),
            });
        }

        Ok(writer)
    }
}

/// On success, returns the block size required when interacting with
/// `destination`.
async fn write_artifact_impl<S: StepSpec<ProgressMetadata = ()>>(
    component: WriteComponent,
    slot: M2Slot,
    mut artifact: BufList,
    destination: &Utf8Path,
    transport: &mut impl WriteTransport,
    cx: &StepContext<S>,
) -> Result<Option<usize>, WriteError> {
    let mut writer = transport
        .make_writer(component, slot, destination, artifact.num_bytes() as u64)
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
                    ProgressUnits::BYTES,
                    (),
                ))
                .await;
            }
            Err(error) => {
                return Err(WriteError::WriteError {
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
            return Err(WriteError::WriteError {
                component,
                slot,
                written_bytes,
                total_bytes,
                error,
            });
        }
    };

    let block_size = writer.block_size();

    match writer.finalize().await {
        Ok(()) => {}
        Err(error) => {
            return Err(WriteError::WriteError {
                component,
                slot,
                written_bytes,
                total_bytes,
                error,
            });
        }
    };

    Ok(block_size)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use super::*;
    use crate::test_helpers::{dummy_artifact_hash_id, with_test_runtime};

    use anyhow::Result;
    use bytes::{Buf, Bytes};
    use camino::Utf8Path;
    use camino_tempfile::tempdir;
    use futures::StreamExt;
    use installinator_common::{
        Event, InstallinatorCompletionMetadata, InstallinatorComponent,
        InstallinatorStepId, StepEventKind, StepOutcome,
    };
    use omicron_test_utils::dev::test_setup_log;
    use partial_io::{
        PartialAsyncWrite, PartialOp,
        proptest_types::{
            interrupted_would_block_strategy, partial_op_strategy,
        },
    };
    use proptest::prelude::*;
    use test_strategy::proptest;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Mutex;
    use tokio_stream::wrappers::ReceiverStream;
    use tufaceous_artifact::{ArtifactKind, KnownArtifactKind};

    #[proptest(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
    fn proptest_write_artifact(
        #[strategy(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8192), 0..16))]
        data1: Vec<Vec<u8>>,
        #[strategy(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8192), 0..16))]
        data2: Vec<Vec<u8>>,
        #[strategy(WriteOps::strategy())] write_ops: WriteOps,
    ) {
        with_test_runtime(async move {
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
        let tempdir_path = tempdir.path();

        let destination_host = tempdir_path.join("test-host.bin");
        let destination_control_plane =
            tempdir_path.join("test-control-plane.bin");

        let mut artifact_host: BufList =
            data1.into_iter().map(Bytes::from).collect();
        let mut artifact_control_plane: BufList =
            data2.into_iter().map(Bytes::from).collect();

        let host_id = ArtifactHashId {
            kind: ArtifactKind::HOST_PHASE_2,
            hash: {
                // The `validate_written_host_phase_2_hash()` will fail unless
                // we give the actual hash of the host phase 2 data, so compute
                // it here.
                //
                // We currently don't have any equivalent check on the control
                // plane, so it can use `dummy_artifact_hash_id` instead.
                let mut hasher = Sha256::new();
                for chunk in artifact_host.iter() {
                    hasher.update(chunk);
                }
                ArtifactHash(hasher.finalize().into())
            },
        };
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

        let (event_sender, event_receiver) = update_engine::channel();

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
                host_phase_2: destination_host.clone(),
                clean_control_plane_dir: false,
                control_plane_dir: tempdir_path.into(),
                control_plane_zpool: None,
            },
        );
        let destination =
            WriteDestination { drives, is_host_phase_2_block_device: false };

        // Assemble our one control plane artifact into a 1-long list of zone
        // images.
        let control_plane_zone_images = ControlPlaneZoneImages {
            zones: vec![(
                destination_control_plane.file_name().unwrap().to_string(),
                artifact_control_plane.iter().flatten().copied().collect(),
            )],
        };

        let mut writer = ArtifactWriter::new(
            MupdateUuid::new_v4(),
            &host_id,
            &artifact_host,
            &control_plane_id,
            &control_plane_zone_images,
            destination,
        );

        let engine = UpdateEngine::new(&logctx.log, event_sender);
        let log = logctx.log.clone();
        engine
            .new_step(
                InstallinatorComponent::Both,
                InstallinatorStepId::Write,
                "Writing",
                async move |cx| {
                    let write_output = writer
                        .write_with_transport(
                            &cx,
                            &log,
                            &mut host_transport,
                            &mut control_plane_transport,
                        )
                        .await;
                    StepSuccess::new(())
                        .with_metadata(InstallinatorCompletionMetadata::Write {
                            output: write_output,
                        })
                        .into()
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
                                Some(InstallinatorCompletionMetadata::Write {
                                    output,
                                }),
                            ..
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
        type W = PartialAsyncWrite<AsyncNamedTempFile>;

        async fn make_writer(
            &mut self,
            component: WriteComponent,
            slot: M2Slot,
            destination: &Utf8Path,
            total_bytes: u64,
        ) -> Result<Self::W, WriteError> {
            self.0
                .lock()
                .await
                .make_writer(component, slot, destination, total_bytes)
                .await
        }
    }

    #[async_trait]
    impl WriteTransportWriter for PartialAsyncWrite<AsyncNamedTempFile> {
        fn block_size(&self) -> Option<usize> {
            None
        }

        async fn finalize(self) -> io::Result<()> {
            self.into_inner().finalize().await
        }
    }

    #[derive(Debug)]
    struct PartialIoTransport {
        file_transport: FileTransport,
        partial_ops: VecDeque<Vec<PartialOp>>,
    }

    #[async_trait]
    impl WriteTransport for PartialIoTransport {
        type W = PartialAsyncWrite<AsyncNamedTempFile>;

        async fn make_writer(
            &mut self,
            component: WriteComponent,
            slot: M2Slot,
            destination: &Utf8Path,
            total_bytes: u64,
        ) -> Result<Self::W, WriteError> {
            let f = self
                .file_transport
                .make_writer(component, slot, destination, total_bytes)
                .await?;
            // This is the next series of operations.
            let these_ops =
                self.partial_ops.pop_front().unwrap_or_else(Vec::new);
            Ok(PartialAsyncWrite::new(f, these_ops))
        }
    }
}
