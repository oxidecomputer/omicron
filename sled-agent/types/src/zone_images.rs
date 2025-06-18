// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, fs::FileType, io, sync::Arc};

use camino::Utf8PathBuf;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_common::update::{MupdateOverrideInfo, OmicronZoneManifest};
use omicron_uuid_kinds::InternalZpoolUuid;
use slog::{info, o, warn};
use slog_error_chain::InlineErrorChain;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;

/// Current status of the zone image resolver.
#[derive(Clone, Debug)]
pub struct ResolverStatus {
    /// The zone manifest status.
    pub zone_manifest: ZoneManifestStatus,

    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideStatus,
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
        ZoneManifestArtifactsDisplay { artifacts: &self.data }
    }
}

pub struct ZoneManifestArtifactsDisplay<'a> {
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
            "{} artifacts in manifest: {valid} valid, {mismatch} mismatched, {error} errors:",
            self.artifacts.len(),
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
                    InlineErrorChain::new(error)
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
