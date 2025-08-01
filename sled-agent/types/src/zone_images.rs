// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, fs::FileType, io, sync::Arc};

use camino::Utf8PathBuf;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use nexus_sled_agent_shared::inventory::MupdateOverrideBootInventory;
use nexus_sled_agent_shared::inventory::MupdateOverrideInventory;
use nexus_sled_agent_shared::inventory::MupdateOverrideNonBootInventory;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::RemoveMupdateOverrideBootSuccessInventory;
use nexus_sled_agent_shared::inventory::RemoveMupdateOverrideInventory;
use nexus_sled_agent_shared::inventory::ZoneArtifactInventory;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_sled_agent_shared::inventory::ZoneManifestBootInventory;
use nexus_sled_agent_shared::inventory::ZoneManifestInventory;
use nexus_sled_agent_shared::inventory::ZoneManifestNonBootInventory;
use omicron_common::update::{
    MupdateOverrideInfo, OmicronZoneManifest, OmicronZoneManifestSource,
};
use omicron_common::zone_images::ZoneImageFileSource;
use omicron_uuid_kinds::InternalZpoolUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use slog::{error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use swrite::{SWrite, swriteln};
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;

/// The location to look for images shipped with the RAM disk.
pub const RAMDISK_IMAGE_PATH: &str = "/opt/oxide";

/// Current status of the zone image resolver.
#[derive(Clone, Debug)]
pub struct ResolverStatus {
    /// The zone manifest status.
    pub zone_manifest: ZoneManifestStatus,

    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideStatus,

    /// The image directory override, if any.
    ///
    /// This is injected by tests.
    pub image_directory_override: Option<Utf8PathBuf>,
}

impl ResolverStatus {
    /// Convert this status to the inventory format.
    pub fn to_inventory(&self) -> ZoneImageResolverInventory {
        ZoneImageResolverInventory {
            zone_manifest: self.zone_manifest.to_inventory(),
            mupdate_override: self.mupdate_override.to_inventory(),
        }
    }
}

/// Describes the current state of zone manifests.
#[derive(Clone, Debug)]
pub struct ZoneManifestStatus {
    /// The path to the zone manifest JSON on the boot disk.
    pub boot_disk_path: Utf8PathBuf,

    /// Status of the boot disk.
    pub boot_disk_result:
        Result<ZoneManifestArtifactsResult, ZoneManifestReadError>,

    /// Status of the non-boot disks. This results in warnings in case of a
    /// mismatch.
    pub non_boot_disk_metadata: IdOrdMap<ZoneManifestNonBootInfo>,
}

impl ZoneManifestStatus {
    /// Convert this status to the inventory format.
    pub fn to_inventory(&self) -> ZoneManifestInventory {
        let boot_inventory = match &self.boot_disk_result {
            Ok(artifacts_result) => Ok(artifacts_result.to_boot_inventory()),
            Err(error) => Err(InlineErrorChain::new(error).to_string()),
        };

        let non_boot_status = self
            .non_boot_disk_metadata
            .iter()
            .map(|info| ZoneManifestNonBootInventory {
                zpool_id: info.zpool_id,
                path: info.path.clone(),
                is_valid: info.result.is_valid(),
                message: info.result.display().to_string(),
            })
            .collect();

        ZoneManifestInventory {
            boot_disk_path: self.boot_disk_path.clone(),
            boot_inventory,
            non_boot_status,
        }
    }

    /// Return the validated artifact hash for a given [`ZoneKind`].
    ///
    /// Only considers [`Self::boot_disk_result`].
    pub fn zone_hash(
        &self,
        kind: ZoneKind,
    ) -> Result<ArtifactHash, ZoneManifestZoneHashError> {
        let artifacts_result =
            self.boot_disk_result.as_ref().map_err(|err| {
                ZoneManifestZoneHashError::ReadBootDisk(err.clone())
            })?;

        let file_name = kind.artifact_in_install_dataset();
        let artifact = &artifacts_result
            .data
            .get(file_name)
            .ok_or(ZoneManifestZoneHashError::NoArtifactForZoneKind(kind))?;

        match &artifact.status {
            ArtifactReadResult::Valid => Ok(artifact.expected_hash),
            ArtifactReadResult::Mismatch { actual_size, actual_hash } => {
                Err(ZoneManifestZoneHashError::SizeHashMismatch {
                    expected_size: artifact.expected_size,
                    expected_hash: artifact.expected_hash,
                    actual_size: *actual_size,
                    actual_hash: *actual_hash,
                })
            }
            ArtifactReadResult::Error(err) => {
                Err(ZoneManifestZoneHashError::ReadArtifact(err.clone()))
            }
        }
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum ZoneManifestZoneHashError {
    #[error("error reading boot disk")]
    ReadBootDisk(#[source] ZoneManifestReadError),
    #[error("no artifact found for zone kind {0:?}")]
    NoArtifactForZoneKind(ZoneKind),
    #[error(
        "size/hash mismatch: expected {} bytes/{}, got {} bytes/{}",
        .expected_size,
        .expected_hash,
        .actual_size,
        .actual_hash,
    )]
    SizeHashMismatch {
        expected_size: u64,
        expected_hash: ArtifactHash,
        actual_size: u64,
        actual_hash: ArtifactHash,
    },
    #[error("error reading artifact")]
    ReadArtifact(#[source] ArcIoError),
}

/// The result of reading artifacts from an install dataset.
///
/// This may or may not be valid, depending on the status of the artifacts. See
/// [`Self::is_valid`].
#[derive(Clone, Debug, PartialEq)]
pub struct ZoneManifestArtifactsResult {
    pub manifest: OmicronZoneManifest,
    pub data: IdOrdMap<ZoneManifestArtifactResult>,
}

impl ZoneManifestArtifactsResult {
    /// Returns true if all artifacts are valid.
    pub fn is_valid(&self) -> bool {
        self.data.iter().all(|artifact| artifact.is_valid())
    }

    /// Returns a displayable representation of the artifacts.
    pub fn display(&self) -> ZoneManifestArtifactsDisplay<'_> {
        ZoneManifestArtifactsDisplay {
            source: &self.manifest.source,
            artifacts: &self.data,
        }
    }

    /// Converts this result to the inventory format, used for the boot disk.
    pub fn to_boot_inventory(&self) -> ZoneManifestBootInventory {
        let artifacts =
            self.data.iter().map(|artifact| artifact.to_inventory()).collect();

        ZoneManifestBootInventory { source: self.manifest.source, artifacts }
    }
}

pub struct ZoneManifestArtifactsDisplay<'a> {
    source: &'a OmicronZoneManifestSource,
    artifacts: &'a IdOrdMap<ZoneManifestArtifactResult>,
}

impl fmt::Display for ZoneManifestArtifactsDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // First, display a summary of the artifacts.
        let (valid, mismatch, error) = self.artifacts.iter().fold(
            (0, 0, 0),
            |(valid, mismatch, error), artifact| match &artifact.status {
                ArtifactReadResult::Valid => (valid + 1, mismatch, error),
                ArtifactReadResult::Mismatch { .. } => {
                    (valid, mismatch + 1, error)
                }
                ArtifactReadResult::Error { .. } => {
                    (valid, mismatch, error + 1)
                }
            },
        );
        writeln!(
            f,
            "{} artifacts in manifest generated by {}: {valid} valid, \
             {mismatch} mismatched, {error} errors:",
            self.artifacts.len(),
            self.source,
        )?;

        for artifact in self.artifacts {
            writeln!(f, "  - {}", artifact.display())?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ZoneManifestArtifactResult {
    /// The filename.
    pub file_name: String,

    /// The full path to the file.
    pub path: Utf8PathBuf,

    /// The expected size.
    pub expected_size: u64,

    /// The expected hash.
    pub expected_hash: ArtifactHash,

    /// The status on disk.
    pub status: ArtifactReadResult,
}

impl ZoneManifestArtifactResult {
    pub fn is_valid(&self) -> bool {
        matches!(self.status, ArtifactReadResult::Valid)
    }

    pub fn display(&self) -> ZoneManifestArtifactDisplay<'_> {
        ZoneManifestArtifactDisplay { artifact: self }
    }

    /// Convert this result to inventory format.
    pub fn to_inventory(&self) -> ZoneArtifactInventory {
        let status = match &self.status {
            ArtifactReadResult::Valid => Ok(()),
            ArtifactReadResult::Mismatch { actual_size, actual_hash } => {
                Err(format!(
                    "size/hash mismatch: expected {} bytes/{}, got {} bytes/{}",
                    self.expected_size,
                    self.expected_hash,
                    actual_size,
                    actual_hash
                ))
            }
            ArtifactReadResult::Error(error) => {
                Err(InlineErrorChain::new(error).to_string())
            }
        };

        ZoneArtifactInventory {
            file_name: self.file_name.clone(),
            path: self.path.clone(),
            expected_size: self.expected_size,
            expected_hash: self.expected_hash,
            status,
        }
    }
}

impl IdOrdItem for ZoneManifestArtifactResult {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
}

pub struct ZoneManifestArtifactDisplay<'a> {
    artifact: &'a ZoneManifestArtifactResult,
}

impl fmt::Display for ZoneManifestArtifactDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.artifact.status {
            ArtifactReadResult::Valid => {
                write!(
                    f,
                    "{}: valid ({} bytes, {})",
                    self.artifact.file_name,
                    self.artifact.expected_size,
                    self.artifact.expected_hash
                )
            }
            ArtifactReadResult::Mismatch { actual_size, actual_hash } => {
                write!(
                    f,
                    "{}: mismatch (expected {} bytes, {}; \
                     found {} bytes, {})",
                    self.artifact.file_name,
                    self.artifact.expected_size,
                    self.artifact.expected_hash,
                    actual_size,
                    actual_hash
                )
            }
            ArtifactReadResult::Error(error) => {
                write!(
                    f,
                    "{}: error ({})",
                    self.artifact.file_name,
                    InlineErrorChain::new(error),
                )
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ZoneManifestNonBootInfo {
    /// The ID of the zpool.
    pub zpool_id: InternalZpoolUuid,

    /// The dataset directory.
    pub dataset_dir: Utf8PathBuf,

    /// The zone manifest path.
    pub path: Utf8PathBuf,

    /// The result of performing the read operation.
    pub result: ZoneManifestNonBootResult,
}

impl ZoneManifestNonBootInfo {
    pub fn log_to(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "non_boot_zpool" => self.zpool_id.to_string(),
            "non_boot_path" => self.path.to_string(),
        ));
        self.result.log_to(&log);
    }
}

impl IdOrdItem for ZoneManifestNonBootInfo {
    type Key<'a> = InternalZpoolUuid;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq)]
pub enum ZoneManifestNonBootResult {
    /// The manifest is present and matches the value on the boot disk.
    ///
    /// This does not necessarily mean that the zone tarballs on the non-boot
    /// disk match the manifest. Information about that is stored in the
    /// `ZoneManifestArtifactsResult`.
    Matches(ZoneManifestArtifactsResult),

    /// A mismatch between the boot disk and the other disk was detected.
    Mismatch(ZoneManifestNonBootMismatch),

    /// An error occurred while reading the zone manifest on this disk.
    ReadError(ZoneManifestReadError),
}

impl ZoneManifestNonBootResult {
    /// Returns true if the status is valid.
    ///
    /// The necessary conditions for validity are:
    ///
    /// 1. `Self::Matches` being true
    /// 2. The result inside is valid.
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Matches(result) => result.is_valid(),
            Self::Mismatch(_) | Self::ReadError(_) => false,
        }
    }

    /// Returns a displayable representation of this result.
    pub fn display(&self) -> ZoneManifestNonBootDisplay<'_> {
        ZoneManifestNonBootDisplay { result: self }
    }

    fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::Matches(result) => {
                if result.is_valid() {
                    info!(
                        log,
                        "found valid, matching zone manifest for non-boot disk";
                        "non_boot_disk_result" => %result.display(),
                    );
                } else {
                    warn!(
                        log,
                        "zone manifest for non-boot disk is invalid";
                        "non_boot_disk_result" => %result.display(),
                    );
                }
            }
            Self::Mismatch(mismatch) => match mismatch {
                ZoneManifestNonBootMismatch::ValueMismatch {
                    non_boot_disk_result,
                } => {
                    warn!(
                        log,
                        "zone manifest contents differ between boot disk and non-boot disk";
                        "non_boot_disk_result" => %non_boot_disk_result.display(),
                    );
                }
                ZoneManifestNonBootMismatch::BootDiskReadError {
                    non_boot_disk_result,
                } => {
                    warn!(
                        log,
                        "unable to verify zone manifest consistency between \
                         boot disk and non-boot disk due to boot disk read error";
                        "non_boot_disk_result" => %non_boot_disk_result.display(),
                    );
                }
            },
            Self::ReadError(error) => {
                warn!(
                    log,
                    "error reading zone manifest on non-boot disk";
                    "error" => InlineErrorChain::new(error),
                );
            }
        }
    }
}

pub struct ZoneManifestNonBootDisplay<'a> {
    result: &'a ZoneManifestNonBootResult,
}

impl fmt::Display for ZoneManifestNonBootDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.result {
            ZoneManifestNonBootResult::Matches(result) => {
                if result.is_valid() {
                    write!(f, "valid zone manifest: {}", result.display())
                } else {
                    write!(f, "invalid zone manifest: {}", result.display())
                }
            }
            ZoneManifestNonBootResult::Mismatch(mismatch) => match mismatch {
                ZoneManifestNonBootMismatch::ValueMismatch {
                    non_boot_disk_result,
                } => {
                    write!(
                        f,
                        "contents differ from boot disk: {}",
                        non_boot_disk_result.display()
                    )
                }
                ZoneManifestNonBootMismatch::BootDiskReadError {
                    non_boot_disk_result,
                } => {
                    write!(
                        f,
                        "boot disk read error, non-boot disk: {}",
                        non_boot_disk_result.display()
                    )
                }
            },
            ZoneManifestNonBootResult::ReadError(error) => {
                write!(f, "read error: {}", error)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ZoneManifestNonBootMismatch {
    /// The file's contents differ between the boot disk and the other disk.
    ValueMismatch { non_boot_disk_result: ZoneManifestArtifactsResult },

    /// There was a read error on the boot disk, so we were unable to verify
    /// consistency.
    BootDiskReadError {
        /// The value as found on this disk. This value is logged but not used.
        non_boot_disk_result: ZoneManifestArtifactsResult,
    },
}

/// Describes the current state of mupdate overrides.
#[derive(Clone, Debug)]
pub struct MupdateOverrideStatus {
    /// The path to the mupdate override JSON on the boot disk.
    pub boot_disk_path: Utf8PathBuf,

    /// Status of the boot disk.
    pub boot_disk_override:
        Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError>,

    /// Status of the non-boot disks. This results in warnings in case of a
    /// mismatch.
    pub non_boot_disk_overrides: IdOrdMap<MupdateOverrideNonBootInfo>,
}

impl MupdateOverrideStatus {
    /// Converts this status to the inventory format.
    pub fn to_inventory(&self) -> MupdateOverrideInventory {
        let boot_override = match &self.boot_disk_override {
            Ok(Some(override_info)) => Ok(Some(MupdateOverrideBootInventory {
                mupdate_override_id: override_info.mupdate_uuid,
            })),
            Ok(None) => Ok(None),
            Err(error) => Err(InlineErrorChain::new(error).to_string()),
        };

        let non_boot_status = self
            .non_boot_disk_overrides
            .iter()
            .map(|info| MupdateOverrideNonBootInventory {
                zpool_id: info.zpool_id,
                path: info.path.clone(),
                is_valid: info.result.is_valid(),
                message: info.result.display().to_string(),
            })
            .collect();

        MupdateOverrideInventory {
            boot_disk_path: self.boot_disk_path.clone(),
            boot_override,
            non_boot_status,
        }
    }
}

/// Describes the result of reading a mupdate override file from a non-boot disk.
#[derive(Clone, Debug, PartialEq)]
pub struct MupdateOverrideNonBootInfo {
    /// The ID of the zpool.
    pub zpool_id: InternalZpoolUuid,

    /// The path to the mupdate override file.
    pub path: Utf8PathBuf,

    /// The result of reading the mupdate override file.
    pub result: MupdateOverrideNonBootResult,
}

impl MupdateOverrideNonBootInfo {
    pub fn log_to(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "non_boot_zpool_id" => self.zpool_id.to_string(),
            "non_boot_path" => self.path.to_string(),
        ));

        match &self.result {
            MupdateOverrideNonBootResult::MatchesPresent => {
                info!(
                    log,
                    "mupdate override for non-boot disk matches boot disk (present)"
                );
            }
            MupdateOverrideNonBootResult::MatchesAbsent => {
                info!(
                    log,
                    "mupdate override for non-boot disk matches boot disk (absent)"
                );
            }
            MupdateOverrideNonBootResult::Mismatch(mismatch) => {
                warn!(
                    log,
                    "mupdate override for non-boot disk does not match boot disk";
                    "mismatch" => ?mismatch,
                );
            }
            MupdateOverrideNonBootResult::ReadError(error) => {
                warn!(
                    log,
                    "error reading mupdate override for non-boot disk";
                    "error" => InlineErrorChain::new(error),
                );
            }
        }
    }
}

impl IdOrdItem for MupdateOverrideNonBootInfo {
    type Key<'a> = InternalZpoolUuid;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }

    id_upcast!();
}

/// The result of reading a mupdate override file from a non-boot disk.
#[derive(Clone, Debug, PartialEq)]
pub enum MupdateOverrideNonBootResult {
    /// The non-boot disk matches the boot disk (both present).
    MatchesPresent,

    /// The non-boot disk matches the boot disk (both absent).
    MatchesAbsent,

    /// The non-boot disk does not match the boot disk.
    Mismatch(MupdateOverrideNonBootMismatch),

    /// There was an error reading the mupdate override file from the non-boot disk.
    ReadError(MupdateOverrideReadError),
}

impl MupdateOverrideNonBootResult {
    /// Returns true if the status is considered to be valid.
    pub fn is_valid(&self) -> bool {
        match self {
            MupdateOverrideNonBootResult::MatchesPresent
            | MupdateOverrideNonBootResult::MatchesAbsent => true,
            MupdateOverrideNonBootResult::Mismatch(_)
            | MupdateOverrideNonBootResult::ReadError(_) => false,
        }
    }

    /// Returns a displayable representation of this result.
    pub fn display(&self) -> MupdateOverrideNonBootDisplay<'_> {
        MupdateOverrideNonBootDisplay { result: self }
    }
}

pub struct MupdateOverrideNonBootDisplay<'a> {
    result: &'a MupdateOverrideNonBootResult,
}

impl fmt::Display for MupdateOverrideNonBootDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.result {
            MupdateOverrideNonBootResult::MatchesPresent => {
                write!(f, "matches boot disk (present)")
            }
            MupdateOverrideNonBootResult::MatchesAbsent => {
                write!(f, "matches boot disk (absent)")
            }
            MupdateOverrideNonBootResult::Mismatch(mismatch) => match mismatch {
                MupdateOverrideNonBootMismatch::BootPresentOtherAbsent => {
                    write!(
                        f,
                        "boot disk has override but non-boot disk does not"
                    )
                }
                MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info,
                } => {
                    write!(
                        f,
                        "non-boot disk has override ({:?}) but boot disk does not",
                        non_boot_disk_info
                    )
                }
                MupdateOverrideNonBootMismatch::ValueMismatch {
                    non_boot_disk_info,
                } => {
                    write!(
                        f,
                        "boot disk and non-boot disk have different overrides (non-boot: {:?})",
                        non_boot_disk_info
                    )
                }
                MupdateOverrideNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info,
                } => {
                    write!(
                        f,
                        "error reading boot disk, non-boot disk override: {:?}",
                        non_boot_disk_info
                    )
                }
            },
            MupdateOverrideNonBootResult::ReadError(err) => {
                write!(f, "read error: {}", err)
            }
        }
    }
}

/// Describes a mismatch between the boot disk and a non-boot disk.
#[derive(Clone, Debug, PartialEq)]
pub enum MupdateOverrideNonBootMismatch {
    /// The boot disk is present but the non-boot disk is absent.
    BootPresentOtherAbsent,

    /// The boot disk is absent but the non-boot disk is present.
    BootAbsentOtherPresent { non_boot_disk_info: MupdateOverrideInfo },

    /// Both disks are present but have different values.
    ValueMismatch { non_boot_disk_info: MupdateOverrideInfo },

    /// There was an error reading the boot disk.
    BootDiskReadError { non_boot_disk_info: Option<MupdateOverrideInfo> },
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum ZoneManifestReadError {
    #[error("error reading install metadata")]
    InstallMetadata(#[from] InstallMetadataReadError),
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum MupdateOverrideReadError {
    #[error("install metadata read error")]
    InstallMetadata(#[from] InstallMetadataReadError),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ArtifactReadResult {
    /// The artifact was read successfully and matches.
    Valid,

    /// The artifact was read successfully but does not match.
    Mismatch {
        /// The actual file size.
        actual_size: u64,

        /// The actual hash.
        actual_hash: ArtifactHash,
    },

    /// An error occurred while reading the artifact.
    Error(ArcIoError),
}

/// The result of an operation to remove MUPdate overrides on a sled's boot
/// disk.
#[derive(Clone, Debug)]
pub struct RemoveMupdateOverrideResult {
    /// The path to the override on the boot disk.
    pub boot_disk_path: Utf8PathBuf,

    /// The result of removing the mupdate override on the boot disk.
    pub boot_disk_result: Result<
        RemoveMupdateOverrideBootSuccess,
        RemoveMupdateOverrideBootError,
    >,

    /// The result of removing the mupdate override on non-boot disks.
    pub non_boot_disk_info: IdOrdMap<RemoveMupdateOverrideNonBootInfo>,
}

impl RemoveMupdateOverrideResult {
    pub fn to_inventory(&self) -> RemoveMupdateOverrideInventory {
        let boot_disk_result = match &self.boot_disk_result {
            Ok(RemoveMupdateOverrideBootSuccess::Removed(_)) => {
                Ok(RemoveMupdateOverrideBootSuccessInventory::Removed)
            }
            Ok(RemoveMupdateOverrideBootSuccess::NoOverride) => {
                Ok(RemoveMupdateOverrideBootSuccessInventory::NoOverride)
            }
            Err(error) => Err(InlineErrorChain::new(error).to_string()),
        };

        let mut non_boot_message = String::new();
        for info in &self.non_boot_disk_info {
            swriteln!(
                non_boot_message,
                "- for non-boot disk {}: {}",
                info.zpool_id,
                info.result.display(),
            );
        }

        RemoveMupdateOverrideInventory { boot_disk_result, non_boot_message }
    }

    pub fn log_to(&self, log: &slog::Logger) {
        let log =
            log.new(o!("boot_disk_path" => self.boot_disk_path.to_string()));
        match &self.boot_disk_result {
            Ok(info) => {
                info!(
                    log,
                    "removed mupdate override on boot disk";
                    "prev_info" => ?info,
                );
            }
            Err(error) => {
                error!(
                    log,
                    "failed to remove mupdate override on boot disk";
                    "error" => InlineErrorChain::new(error),
                );
            }
        }

        for info in &self.non_boot_disk_info {
            info.log_to(&log);
        }
    }
}

/// A success condition removing the mupdate override on a boot disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoveMupdateOverrideBootSuccess {
    /// The mupdate override was matched up and successfully removed.
    Removed(MupdateOverrideInfo),

    /// No mupdate override was found.
    ///
    /// This is considered a success condition for idempotency reasons.
    NoOverride,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum RemoveMupdateOverrideBootError {
    #[error("boot disk not found in internal disks")]
    BootDiskMissing,
    #[error(
        "mismatch between override ID on boot disk ({actual}) \
         and provided ID ({provided})"
    )]
    IdMismatch {
        /// The actual override ID on the boot disk.
        actual: MupdateOverrideUuid,

        /// The override ID provided to the `remove_mupdate_override` method.
        provided: MupdateOverrideUuid,
    },
    #[error("error removing mupdate override file at `{path}`")]
    RemoveError {
        /// The path to the mupdate override file that could not be removed.
        path: Utf8PathBuf,

        /// The underlying error.
        #[source]
        error: ArcIoError,
    },
    #[error(
        "mupdate override at `{path}` not removed \
         since there was an error reading it"
    )]
    ReadError {
        /// The path to the mupdate override file that could not be read.
        path: Utf8PathBuf,

        /// The underlying error.
        #[source]
        error: MupdateOverrideReadError,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveMupdateOverrideNonBootInfo {
    /// The zpool ID of the non-boot disk.
    pub zpool_id: InternalZpoolUuid,

    /// The path to the mupdate override file on the disk, or None if no status
    /// was available.
    pub path: Option<Utf8PathBuf>,

    /// The result of removing the mupdate override on the non-boot disk.
    pub result: RemoveMupdateOverrideNonBootResult,
}

impl RemoveMupdateOverrideNonBootInfo {
    pub fn log_to(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "non_boot_zpool_id" => self.zpool_id.to_string(),
            "non_boot_path" => self.path.as_ref().map_or_else(
                || "(none)".to_owned(),
                |path| path.to_string()
            ),
        ));

        self.result.log_to(&log);
    }
}

impl IdOrdItem for RemoveMupdateOverrideNonBootInfo {
    type Key<'a> = InternalZpoolUuid;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq)]
pub enum RemoveMupdateOverrideNonBootResult {
    /// The mupdate override was present and was removed successfully.
    Removed {
        /// The previous mupdate override result that was removed. This could
        /// potentially be an invalid override.
        prev_result: MupdateOverrideNonBootResult,
    },

    /// No mupdate override was found on the non-boot disk.
    NoOverride,

    /// There was an error removing the mupdate override on the boot disk, so
    /// the non-boot disk was not altered.
    BootDiskError,

    /// An error occurred while removing the mupdate override on the non-boot
    /// disk.
    RemoveError {
        /// The path to the MUPdate override file that could not be removed.
        path: Utf8PathBuf,

        /// The error that occurred.
        error: ArcIoError,
    },

    /// An error occurred while reading the mupdate override on the non-boot
    /// disk.
    ReadError {
        /// The path to the MUPdate override file that could not be read.
        path: Utf8PathBuf,

        /// The error that occurred.
        error: MupdateOverrideReadError,
    },

    /// No status was found for the non-boot disk, possibly indicating the
    /// non-boot disk being missing at the time Sled Agent was started.
    NoStatus,

    /// The disk was missing from the latest InternalDisksWithBootDisk but was
    /// present at startup. The on-disk data was not altered.
    DiskMissing,
}

impl RemoveMupdateOverrideNonBootResult {
    pub fn display(&self) -> RemoveMupdateOverrideNonBootDisplay<'_> {
        RemoveMupdateOverrideNonBootDisplay { result: self }
    }

    fn log_to(&self, log: &slog::Logger) {
        match self {
            RemoveMupdateOverrideNonBootResult::Removed { prev_result } => {
                info!(
                    log,
                    "removed mupdate override on non-boot disk";
                    "prev_result" => %prev_result.display(),
                );
            }
            RemoveMupdateOverrideNonBootResult::BootDiskError => {
                warn!(
                    log,
                    "mupdate override on non-boot disk not removed due to \
                     boot disk error"
                );
            }
            RemoveMupdateOverrideNonBootResult::RemoveError { path, error } => {
                warn!(
                    log,
                    "error removing mupdate override file on non-boot disk";
                    "path" => %path,
                    "error" => InlineErrorChain::new(error),
                );
            }
            RemoveMupdateOverrideNonBootResult::ReadError { path, error } => {
                warn!(
                    log,
                    "error reading mupdate override file on non-boot disk";
                    "path" => %path,
                    "error" => InlineErrorChain::new(error),
                );
            }
            RemoveMupdateOverrideNonBootResult::NoStatus => {
                warn!(
                    log,
                    "no status available for non-boot disk when sled-agent \
                     started, mupdate override not removed"
                );
            }
            RemoveMupdateOverrideNonBootResult::DiskMissing => {
                warn!(
                    log,
                    "non-boot disk missing from latest InternalDisks, \
                     mupdate override not removed"
                );
            }
            RemoveMupdateOverrideNonBootResult::NoOverride => {
                warn!(
                    log,
                    "no mupdate override found on non-boot disk to remove"
                );
            }
        }
    }
}

pub struct RemoveMupdateOverrideNonBootDisplay<'a> {
    result: &'a RemoveMupdateOverrideNonBootResult,
}

impl fmt::Display for RemoveMupdateOverrideNonBootDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.result {
            RemoveMupdateOverrideNonBootResult::Removed { prev_result } => {
                write!(f, "removed (previous: {})", prev_result.display())
            }
            RemoveMupdateOverrideNonBootResult::BootDiskError => {
                write!(f, "not removed due to boot disk error")
            }
            RemoveMupdateOverrideNonBootResult::RemoveError { path, error } => {
                write!(
                    f,
                    "error removing file at `{path}`: {}",
                    InlineErrorChain::new(error),
                )
            }
            RemoveMupdateOverrideNonBootResult::ReadError { path, error } => {
                write!(
                    f,
                    "error reading file at `{path}`: {}",
                    InlineErrorChain::new(error),
                )
            }
            RemoveMupdateOverrideNonBootResult::NoStatus => {
                write!(
                    f,
                    "no status was available when sled-agent was started, \
                     so mupdate override not removed"
                )
            }
            RemoveMupdateOverrideNonBootResult::DiskMissing => {
                write!(
                    f,
                    "non-boot disk missing from latest InternalDisks, \
                     mupdate override not removed"
                )
            }
            RemoveMupdateOverrideNonBootResult::NoOverride => {
                write!(f, "no override to remove")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Error)]
pub enum InstallMetadataReadError {
    #[error(
        "error retrieving metadata for install dataset directory \
         `{dataset_dir}`"
    )]
    DatasetDirMetadata {
        dataset_dir: Utf8PathBuf,
        #[source]
        error: ArcIoError,
    },

    #[error(
        "expected install dataset `{dataset_dir}` to be a directory, \
         found {file_type:?}"
    )]
    DatasetNotDirectory { dataset_dir: Utf8PathBuf, file_type: FileType },

    #[error("error reading metadata file from `{path}`")]
    Read {
        path: Utf8PathBuf,
        #[source]
        error: ArcIoError,
    },

    #[error("error deserializing `{path}`, contents: {contents:?}")]
    Deserialize {
        path: Utf8PathBuf,
        contents: String,
        #[source]
        error: ArcSerdeJsonError,
    },
    #[error("error reading entries from install dataset dir {dataset_dir}")]
    ReadDir {
        dataset_dir: Utf8PathBuf,
        #[source]
        error: ArcIoError,
    },
    #[error("error reading file type for {path}")]
    ReadFileType {
        path: Utf8PathBuf,
        #[source]
        error: ArcIoError,
    },
    #[error("error reading file {path}")]
    ReadFile {
        path: Utf8PathBuf,
        #[source]
        error: ArcIoError,
    },
}

/// An Omicron zone that is ready to be started.
///
/// This consists of the zone's configuration, as well as an image source for
/// the zone.
#[derive(Clone, Debug)]
pub struct PreparedOmicronZone<'a> {
    /// The zone's configuration.
    config: &'a OmicronZoneConfig,

    /// The file source of the zone.
    file_source: OmicronZoneFileSource,
}

impl<'a> PreparedOmicronZone<'a> {
    /// Creates a new `PreparedOmicronZone` from the given configuration and
    /// file source.
    pub fn new(
        config: &'a OmicronZoneConfig,
        file_source: OmicronZoneFileSource,
    ) -> Self {
        Self { config, file_source }
    }

    /// Returns the zone's configuration.
    pub fn config(&self) -> &'a OmicronZoneConfig {
        self.config
    }

    /// Returns the file source of the zone.
    pub fn file_source(&self) -> &OmicronZoneFileSource {
        &self.file_source
    }
}

/// Contains information about the location of an Omicron zone image file after
/// being resolved by a `ZoneImageSourceResolver`.
#[derive(Clone, Debug, PartialEq)]
pub struct OmicronZoneFileSource {
    /// The actual source from which the zone image was resolved.
    ///
    /// This is usually derived from the provided `OmicronZoneImageSource`, but
    /// it may be a different source if a mupdate override is active.
    pub location: OmicronZoneImageLocation,

    /// The file name and search locations.
    pub file_source: ZoneImageFileSource,
}

/// The location of an Omicron zone image after mupdate overrides have been
/// considered, along with the hash corresponding to the zone.
///
/// Part of [`OmicronZoneFileSource`].
#[derive(Clone, Debug, PartialEq)]
pub enum OmicronZoneImageLocation {
    /// The zone was looked up from the artifact store.
    Artifact {
        /// The hash of the zone image as provided, or an error reading the
        /// mupdate override file.
        ///
        /// If the mupdate override file couldn't be read, we don't know whether
        /// to start the zone from the artifact or the install dataset -- and
        /// out of caution, we will refuse to start the zone entirely.
        hash: Result<ArtifactHash, MupdateOverrideReadError>,
    },

    /// We attempted to look the zone up from the install dataset.
    InstallDataset {
        /// The hash of the zone image as found in the zone manifest, or an
        /// error that occurred while looking up the zone from the install
        /// dataset.
        hash: Result<ArtifactHash, ZoneImageLocationError>,
    },
}

impl OmicronZoneImageLocation {
    /// Returns a [`RunningZoneImageLocation`], or `None` if it is impossible
    /// to start the zone.
    pub fn to_running(
        &self,
    ) -> Result<RunningZoneImageLocation, MupdateOverrideReadError> {
        match self {
            OmicronZoneImageLocation::Artifact { hash: Ok(hash) } => {
                Ok(RunningZoneImageLocation::Artifact { hash: *hash })
            }
            OmicronZoneImageLocation::Artifact { hash: Err(error) } => {
                // In this case, it's impossible to start the zone.
                Err(error.clone())
            }
            OmicronZoneImageLocation::InstallDataset { hash: Ok(hash) } => {
                Ok(RunningZoneImageLocation::InstallDataset { hash: *hash })
            }
            OmicronZoneImageLocation::InstallDataset { hash: Err(_) } => {
                // In this case, if we can start the zone at all, it must be
                // from the RAM disk.
                Ok(RunningZoneImageLocation::Ramdisk)
            }
        }
    }
}

/// The location of a running Omicron zone.
///
/// This is a stripped-down variant of [`OmicronZoneImageLocation`], with only
/// success variants reported.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunningZoneImageLocation {
    /// The zone was an Omicron zone, and was looked up from the artifact store.
    Artifact {
        /// The hash of the zone image.
        hash: ArtifactHash,
    },

    /// The zone was run from the install dataset.
    InstallDataset {
        /// The hash of the zone image as found in the zone manifest.
        hash: ArtifactHash,
    },

    /// The zone was run from the RAM disk.
    ///
    /// This can only happen with install-dataset zones in test scenarios such
    /// as a4x2. In production, this should never happen.
    Ramdisk,
}

/// An error that occurred while looking up a zone image from the install
/// dataset.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum ZoneImageLocationError {
    /// An error occurred while looking up the zone hash.
    #[error("error looking up zone hash from zone manifest")]
    ZoneHash(#[source] ZoneManifestZoneHashError),

    /// The boot disk is unavailable.
    #[error("boot disk missing")]
    BootDiskMissing,
}

/// An `io::Error` wrapper that implements `Clone` and `PartialEq`.
#[derive(Clone, Debug, Error)]
#[error(transparent)]
pub struct ArcIoError(pub Arc<io::Error>);

impl ArcIoError {
    pub fn new(error: io::Error) -> Self {
        Self(Arc::new(error))
    }
}

/// Testing aid.
impl PartialEq for ArcIoError {
    fn eq(&self, other: &Self) -> bool {
        // Simply comparing io::ErrorKind is good enough for tests.
        self.0.kind() == other.0.kind()
    }
}

/// A `serde_json::Error` that implements `Clone` and `PartialEq`.
#[derive(Clone, Debug, Error)]
#[error(transparent)]
pub struct ArcSerdeJsonError(pub Arc<serde_json::Error>);

impl ArcSerdeJsonError {
    pub fn new(error: serde_json::Error) -> Self {
        Self(Arc::new(error))
    }
}

/// Testing aid.
impl PartialEq for ArcSerdeJsonError {
    fn eq(&self, other: &Self) -> bool {
        // Simply comparing line/column/category is good enough for tests.
        self.0.line() == other.0.line()
            && self.0.column() == other.0.column()
            && self.0.classify() == other.0.classify()
    }
}
