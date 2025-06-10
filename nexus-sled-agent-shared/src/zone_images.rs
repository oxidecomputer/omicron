// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt;

use camino::Utf8PathBuf;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use illumos_utils::zpool::ZpoolName;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_common::update::MupdateOverrideInfo;
use omicron_common::update::OmicronZoneManifest;
use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use schemars::SchemaGenerator;
use schemars::schema::Schema;
use schemars::schema::SchemaObject;
use serde::Deserialize;
use serde::Serialize;
use slog::info;
use slog::o;
use slog::warn;
use tufaceous_artifact::ArtifactHash;

/// Current status of the zone image resolver.
///
/// The zone resolver is the component responsible for looking up zone images
/// from the install dataset and artifact store.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ZoneImageResolverStatus {
    /// The zone manifest status.
    pub zone_manifest: ZoneManifestStatus,

    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideStatus,
}

impl ZoneImageResolverStatus {
    /// Creates a new, fake status for use in testing.
    pub fn new_fake() -> Self {
        Self {
            zone_manifest: ZoneManifestStatus {
                boot_disk_path: Utf8PathBuf::from("/fake/path"),
                boot_disk_result: Ok(ZoneManifestArtifactsResult {
                    manifest: OmicronZoneManifest {
                        mupdate_id: MupdateUuid::nil(),
                        zones: IdOrdMap::new(),
                    },
                    data: IdOrdMap::new(),
                }),
                non_boot_disk_metadata: IdOrdMap::new(),
            },
            mupdate_override: MupdateOverrideStatus {
                boot_disk_path: Utf8PathBuf::from("/fake/path"),
                boot_disk_override: Ok(None),
                non_boot_disk_overrides: IdOrdMap::new(),
            },
        }
    }
}

/// Describes the current state of zone manifests.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ZoneManifestStatus {
    /// The path to the zone manifest JSON on the boot disk.
    #[schemars(schema_with = "path_schema")]
    pub boot_disk_path: Utf8PathBuf,

    /// Status of the boot disk.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<ZoneManifestArtifactsResult, String>::json_schema"
    )]
    pub boot_disk_result: Result<ZoneManifestArtifactsResult, String>,

    /// Status of the non-boot disks. This results in warnings in case of a
    /// mismatch.
    pub non_boot_disk_metadata: IdOrdMap<ZoneManifestNonBootInfo>,
}

/// The result of reading artifacts from an install dataset.
///
/// This may or may not be valid, depending on the status of the artifacts. See
/// [`Self::is_valid`].
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
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
                ZoneManifestArtifactStatus::Valid => {
                    (valid + 1, mismatch, error)
                }
                ZoneManifestArtifactStatus::Mismatch { .. } => {
                    (valid, mismatch + 1, error)
                }
                ZoneManifestArtifactStatus::Error { .. } => {
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ZoneManifestArtifactResult {
    /// The filename.
    pub file_name: String,

    /// The full path to the file.
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,

    /// The expected size.
    pub expected_size: u64,

    /// The expected hash.
    pub expected_hash: ArtifactHash,

    /// The status on disk.
    pub status: ZoneManifestArtifactStatus,
}

impl ZoneManifestArtifactResult {
    pub fn is_valid(&self) -> bool {
        matches!(self.status, ZoneManifestArtifactStatus::Valid)
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ZoneManifestArtifactStatus {
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
    // TODO: we return the error as a string here (and in other places below) so
    // we can serialize it, but that's pretty unfortunate because tests have to
    // do somewhat janky string equality comparisons.
    Error { message: String },
}

pub struct ZoneManifestArtifactDisplay<'a> {
    artifact: &'a ZoneManifestArtifactResult,
}

impl fmt::Display for ZoneManifestArtifactDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.artifact.status {
            ZoneManifestArtifactStatus::Valid => {
                write!(
                    f,
                    "{}: valid ({} bytes, {})",
                    self.artifact.file_name,
                    self.artifact.expected_size,
                    self.artifact.expected_hash
                )
            }
            ZoneManifestArtifactStatus::Mismatch {
                actual_size,
                actual_hash,
            } => {
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
            ZoneManifestArtifactStatus::Error { message } => {
                write!(f, "{}: error ({})", self.artifact.file_name, message)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ZoneManifestNonBootInfo {
    /// The name of the zpool.
    pub zpool_name: ZpoolName,

    /// The dataset directory.
    #[schemars(schema_with = "path_schema")]
    pub dataset_dir: Utf8PathBuf,

    /// The zone manifest path.
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,

    /// The result of performing the read operation.
    pub result: ZoneManifestNonBootResult,
}

impl ZoneManifestNonBootInfo {
    pub fn log_to(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "non_boot_zpool" => self.zpool_name.to_string(),
            "non_boot_path" => self.path.to_string(),
        ));
        self.result.log_to(&log);
    }
}

impl IdOrdItem for ZoneManifestNonBootInfo {
    type Key<'a> = ZpoolName;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_name
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ZoneManifestNonBootResult {
    /// The manifest is present and matches the value on the boot disk.
    ///
    /// This does not necessarily mean that the zone tarballs on the non-boot
    /// disk match the manifest. Information about that is stored in the
    /// `ZoneManifestArtifactsResult`.
    Matches { artifacts_result: ZoneManifestArtifactsResult },

    /// A mismatch between the boot disk and the other disk was detected.
    Mismatch { reason: ZoneManifestNonBootMismatch },

    /// An error occurred while reading the zone manifest on this disk.
    ReadError { message: String },
}

impl ZoneManifestNonBootResult {
    /// Returns true if the result is valid.
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Matches { artifacts_result } => artifacts_result.is_valid(),
            Self::Mismatch { .. } => false,
            Self::ReadError { .. } => false,
        }
    }

    /// Logs the result to the given logger.
    pub fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::Matches { artifacts_result } => {
                if artifacts_result.is_valid() {
                    info!(
                        log,
                        "found valid, matching zone manifest for non-boot disk";
                        "non_boot_disk_result" => %artifacts_result.display(),
                    );
                } else {
                    warn!(
                        log,
                        "zone manifest for non-boot disk is invalid";
                        "non_boot_disk_result" => %artifacts_result.display(),
                    );
                }
            }
            Self::Mismatch { reason } => match reason {
                ZoneManifestNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_result,
                } => {
                    warn!(
                        log,
                        "zone manifest absent on boot disk but present on non-boot disk";
                        "non_boot_disk_result" => %non_boot_disk_result.display(),
                    );
                }
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
            Self::ReadError { message } => {
                warn!(
                    log,
                    "error reading zone manifest on non-boot disk";
                    "message" => message,
                );
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ZoneManifestNonBootMismatch {
    /// The file is absent on the boot disk but present on the other disk.
    BootAbsentOtherPresent {
        /// The result of reading the file on the other disk.
        non_boot_disk_result: ZoneManifestArtifactsResult,
    },

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
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct MupdateOverrideStatus {
    /// The path to the mupdate override JSON on the boot disk.
    #[schemars(schema_with = "path_schema")]
    pub boot_disk_path: Utf8PathBuf,

    /// Status of the boot disk.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<Option<MupdateOverrideInfo>, String>::json_schema"
    )]
    pub boot_disk_override: Result<Option<MupdateOverrideInfo>, String>,

    /// Status of the non-boot disks. This results in warnings in case of a
    /// mismatch.
    pub non_boot_disk_overrides: IdOrdMap<MupdateOverrideNonBootInfo>,
}

/// Describes the result of reading a mupdate override file from a non-boot disk.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct MupdateOverrideNonBootInfo {
    /// The zpool name.
    pub zpool_name: ZpoolName,

    /// The path to the mupdate override file.
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,

    /// The result of reading the mupdate override file.
    pub result: MupdateOverrideNonBootResult,
}

impl MupdateOverrideNonBootInfo {
    pub fn log_to(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "non_boot_zpool" => self.zpool_name.to_string(),
            "non_boot_path" => self.path.to_string(),
        ));
        self.result.log_to(&log);
    }
}

impl IdOrdItem for MupdateOverrideNonBootInfo {
    type Key<'a> = ZpoolName;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_name
    }

    id_upcast!();
}

/// The result of reading a mupdate override file from a non-boot disk.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum MupdateOverrideNonBootResult {
    /// The non-boot disk matches the boot disk (both present).
    MatchesPresent,

    /// The non-boot disk matches the boot disk (both absent).
    MatchesAbsent,

    /// The non-boot disk does not match the boot disk.
    Mismatch { reason: MupdateOverrideNonBootMismatch },

    /// There was an error reading the mupdate override file from the non-boot
    /// disk.
    ReadError { message: String },
}

impl MupdateOverrideNonBootResult {
    pub fn log_to(&self, log: &slog::Logger) {
        match &self {
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
            MupdateOverrideNonBootResult::Mismatch { reason } => {
                warn!(
                    log,
                    "mupdate override for non-boot disk does not match boot disk";
                    "reason" => ?reason,
                );
            }
            MupdateOverrideNonBootResult::ReadError { message } => {
                warn!(
                    log,
                    "error reading mupdate override for non-boot disk";
                    "message" => message,
                );
            }
        }
    }
}

/// Describes a mismatch between the boot disk and a non-boot disk.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "reason", rename_all = "snake_case")]
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

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
