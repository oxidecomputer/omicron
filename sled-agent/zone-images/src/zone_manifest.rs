// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::{Utf8Path, Utf8PathBuf};
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use illumos_utils::zpool::ZpoolName;
use omicron_common::update::{OmicronZoneFileMetadata, OmicronZoneManifest};
use rayon::iter::{ParallelBridge, ParallelIterator};
use sha2::{Digest, Sha256};
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
use slog::{error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::{
    fmt,
    fs::File,
    io::{self, Read},
};
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;

use crate::{
    AllInstallMetadataFiles, ArcIoError, InstallMetadataNonBootInfo,
    InstallMetadataNonBootMismatch, InstallMetadataNonBootResult,
    InstallMetadataReadError,
};

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

#[derive(Debug)]
pub(crate) struct AllZoneManifests {
    boot_zpool: ZpoolName,
    boot_disk_path: Utf8PathBuf,
    boot_disk_result:
        Result<ZoneManifestArtifactsResult, ZoneManifestReadError>,
    non_boot_disk_metadata: IdOrdMap<ZoneManifestNonBootInfo>,
}

impl AllZoneManifests {
    /// Attempt to find zone manifests.
    pub(crate) fn read_all(
        log: &slog::Logger,
        internal_disks: &InternalDisksWithBootDisk,
    ) -> Self {
        // First read all the files.
        let files = AllInstallMetadataFiles::read_all(
            log,
            OmicronZoneManifest::FILE_NAME,
            internal_disks,
        );

        // Validate files on the boot disk.
        let boot_disk_result = match files.boot_disk_metadata {
            Ok(Some(manifest)) => Ok(ZoneManifestArtifactsResult::new(
                &files.boot_dataset_dir,
                manifest,
            )),
            Ok(None) => {
                // The file is missing -- this is an error.
                Err(ZoneManifestReadError::NotFound(
                    files.boot_disk_path.clone(),
                ))
            }
            Err(error) => Err(ZoneManifestReadError::InstallMetadata(error)),
        };

        // Validate files on non-boot disks (non-fatal, will produce warnings if
        // errors or mismatches are encountered).
        let non_boot_disk_metadata = files
            .non_boot_disk_metadata
            .into_iter()
            .map(|info| ZoneManifestNonBootInfo::new(info))
            .collect::<IdOrdMap<_>>();

        let ret = Self {
            boot_zpool: files.boot_zpool,
            boot_disk_path: files.boot_disk_path,
            boot_disk_result,
            non_boot_disk_metadata,
        };

        ret.log_results(&log);
        ret
    }

    pub(crate) fn status(&self) -> ZoneManifestStatus {
        ZoneManifestStatus {
            boot_disk_path: self.boot_disk_path.clone(),
            boot_disk_result: self.boot_disk_result.clone(),
            non_boot_disk_metadata: self.non_boot_disk_metadata.clone(),
        }
    }

    fn log_results(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "component" => "zone_manifest",
            "boot_zpool" => self.boot_zpool.to_string(),
            "boot_disk_path" => self.boot_disk_path.to_string(),
        ));

        match &self.boot_disk_result {
            Ok(result) => {
                if result.is_valid() {
                    info!(
                        log,
                        "found zone manifest for boot disk";
                        "boot_disk_result" => %result.display(),
                    );
                } else {
                    error!(
                        log,
                        "zone manifest for boot disk is invalid, \
                         will not bring up zones that mismatch";
                        "boot_disk_result" => %result.display(),
                    );
                }
            }
            Err(error) => {
                // This error most likely requires operator intervention -- if
                // it happens, we'll continue to bring sled-agent up but reject
                // all Omicron zone image lookups.
                error!(
                    log,
                    "error reading zone manifest for boot disk, \
                     will not bring up Omicron zones";
                    "error" => InlineErrorChain::new(error),
                );
            }
        }

        if self.non_boot_disk_metadata.is_empty() {
            warn!(
                log,
                "no non-boot zpools found, unable to verify consistency -- \
                 this may be a hardware issue with the non-boot M.2"
            );
        }

        for info in &self.non_boot_disk_metadata {
            info.log_to(&log);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ZoneManifestNonBootInfo {
    /// The name of the zpool.
    pub zpool_name: ZpoolName,

    /// The dataset directory.
    pub dataset_dir: Utf8PathBuf,

    /// The zone manifest path.
    pub path: Utf8PathBuf,

    /// The result of performing the read operation.
    pub result: ZoneManifestNonBootResult,
}

impl ZoneManifestNonBootInfo {
    pub(crate) fn new(
        info: InstallMetadataNonBootInfo<OmicronZoneManifest>,
    ) -> Self {
        let result = ZoneManifestNonBootResult::new(
            &info.dataset_dir,
            &info.path,
            info.result,
        );
        Self {
            zpool_name: info.zpool_name,
            dataset_dir: info.dataset_dir,
            path: info.path,
            result,
        }
    }

    pub(crate) fn log_to(&self, log: &slog::Logger) {
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
    fn new(
        dataset_dir: &Utf8Path,
        path: &Utf8Path,
        result: InstallMetadataNonBootResult<OmicronZoneManifest>,
    ) -> Self {
        match result {
            InstallMetadataNonBootResult::MatchesPresent(
                non_boot_disk_metadata,
            ) => Self::Matches(ZoneManifestArtifactsResult::new(
                dataset_dir,
                non_boot_disk_metadata,
            )),
            InstallMetadataNonBootResult::MatchesAbsent => {
                // Error case.
                Self::ReadError(ZoneManifestReadError::NotFound(
                    path.to_owned(),
                ))
            }
            InstallMetadataNonBootResult::Mismatch(mismatch) => match mismatch {
                InstallMetadataNonBootMismatch::BootPresentOtherAbsent => {
                    // Error case.
                    Self::ReadError(ZoneManifestReadError::NotFound(
                        path.to_owned(),
                    ))
                }
                InstallMetadataNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info,
                } => Self::Mismatch(
                    ZoneManifestNonBootMismatch::BootAbsentOtherPresent {
                        non_boot_disk_result: ZoneManifestArtifactsResult::new(
                            dataset_dir,
                            non_boot_disk_info,
                        ),
                    },
                ),
                InstallMetadataNonBootMismatch::ValueMismatch {
                    non_boot_disk_info,
                } => {
                    Self::Mismatch(ZoneManifestNonBootMismatch::ValueMismatch {
                        non_boot_disk_result: ZoneManifestArtifactsResult::new(
                            dataset_dir,
                            non_boot_disk_info,
                        ),
                    })
                }
                InstallMetadataNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info: Some(info),
                } => Self::Mismatch(
                    ZoneManifestNonBootMismatch::BootDiskReadError {
                        non_boot_disk_result: ZoneManifestArtifactsResult::new(
                            dataset_dir,
                            info,
                        ),
                    },
                ),
                InstallMetadataNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info: None,
                } => Self::ReadError(ZoneManifestReadError::NotFound(
                    path.to_owned(),
                )),
            },
            InstallMetadataNonBootResult::ReadError(error) => {
                Self::ReadError(error.into())
            }
        }
    }

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
    /// Makes a new `ZoneManifestArtifactsResult` by reading artifacts from the
    /// given directory.
    fn new(dir: &Utf8Path, manifest: OmicronZoneManifest) -> Self {
        let artifacts: Vec<_> = manifest
            .zones
            .iter()
            // Parallelize artifact reading to speed it up.
            .par_bridge()
            .map(|zone| {
                let artifact_path = dir.join(&zone.file_name);
                let status = validate_one(&artifact_path, &zone);

                ZoneManifestArtifactResult {
                    file_name: zone.file_name.clone(),
                    path: artifact_path,
                    expected_size: zone.file_size,
                    expected_hash: zone.hash,
                    status,
                }
            })
            .collect();

        Self { manifest, data: artifacts.into_iter().collect() }
    }

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
    pub(crate) fn is_valid(&self) -> bool {
        matches!(self.status, ArtifactReadResult::Valid)
    }

    pub(crate) fn display(&self) -> ZoneManifestArtifactDisplay<'_> {
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

pub(crate) struct ZoneManifestArtifactDisplay<'a> {
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

#[derive(Clone, Debug, Error, PartialEq)]
pub enum ZoneManifestReadError {
    #[error("error reading install metadata")]
    InstallMetadata(#[from] InstallMetadataReadError),
    #[error("zone manifest not found at `{0}`")]
    NotFound(Utf8PathBuf),
}

fn validate_one(
    artifact_path: &Utf8Path,
    zone: &OmicronZoneFileMetadata,
) -> ArtifactReadResult {
    let mut f = match File::open(artifact_path) {
        Ok(f) => f,
        Err(error) => {
            return ArtifactReadResult::Error(ArcIoError::new(error));
        }
    };

    match compute_size_and_hash(&mut f) {
        Ok((actual_size, actual_hash)) => {
            if zone.file_size == actual_size && zone.hash == actual_hash {
                ArtifactReadResult::Valid
            } else {
                ArtifactReadResult::Mismatch { actual_size, actual_hash }
            }
        }
        Err(error) => ArtifactReadResult::Error(ArcIoError::new(error)),
    }
}

fn compute_size_and_hash(
    f: &mut File,
) -> Result<(u64, ArtifactHash), io::Error> {
    let mut hasher = Sha256::new();
    // Zone artifacts are pretty big, so we read them in chunks.
    let mut buffer = [0u8; 8192];
    let mut total_bytes_read = 0;
    loop {
        let bytes_read = f.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
        total_bytes_read += bytes_read;
    }
    Ok((total_bytes_read as u64, ArtifactHash(hasher.finalize().into())))
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{
        BOOT_PATHS, BOOT_ZPOOL, NON_BOOT_2_PATHS, NON_BOOT_2_ZPOOL,
        NON_BOOT_3_PATHS, NON_BOOT_3_ZPOOL, NON_BOOT_PATHS, NON_BOOT_ZPOOL,
        WriteInstallDatasetContext, deserialize_error, make_internal_disks,
    };

    use camino_tempfile_ext::prelude::*;
    use dropshot::{ConfigLogging, ConfigLoggingLevel, test_util::LogContext};
    use expectorate::assert_contents;
    use iddqd::id_ord_map;
    use pretty_assertions::assert_eq;

    // Much of the logic in this module is shared with mupdate_override.rs, and
    // tested there.

    /// Success case: zone manifest JSON present on boot and non-boot disk and matches.
    #[test]
    fn read_success() {
        let logctx = LogContext::new(
            "zone_manifest_read_success",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();

        // Write the valid manifest to both boot and non-boot disks.
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks =
            make_internal_disks(dir.path(), BOOT_ZPOOL, &[NON_BOOT_ZPOOL]);
        let manifests =
            AllZoneManifests::read_all(&logctx.log, &internal_disks);

        // Boot disk should be valid.
        assert_eq!(
            manifests.boot_disk_result.as_ref().unwrap(),
            &cx.expected_result(&dir.path().join(&BOOT_PATHS.install_dataset))
        );

        // Non-boot disk should match boot disk.
        assert_eq!(
            manifests.non_boot_disk_metadata,
            id_ord_map! {
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::Matches(
                        cx.expected_result(
                            &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                        )
                    )
                }
            }
        );

        logctx.cleanup_successful();
    }

    /// Error case: zone manifest JSON missing from boot disk.
    #[test]
    fn read_boot_disk_missing() {
        let logctx = LogContext::new(
            "zone_manifest_read_boot_disk_missing",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();

        // Write the valid manifest to the non-boot disk.
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();
        // Create the install dataset directory, but not the manifest, on the
        // boot disk.
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let internal_disks =
            make_internal_disks(dir.path(), BOOT_ZPOOL, &[NON_BOOT_ZPOOL]);
        let manifests =
            AllZoneManifests::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            manifests.boot_disk_result.as_ref().unwrap_err(),
            &ZoneManifestReadError::NotFound(
                dir.path().join(&BOOT_PATHS.zones_json)
            ),
        );

        assert_eq!(
            manifests.non_boot_disk_metadata,
            id_ord_map! {
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::Mismatch(
                        ZoneManifestNonBootMismatch::BootAbsentOtherPresent {
                            non_boot_disk_result: cx.expected_result(
                                &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                            ),
                        }
                    )
                }
            }
        );
    }

    /// Error case: zone manifest JSON on boot disk has a read error.
    #[test]
    fn read_boot_disk_read_error() {
        let logctx = LogContext::new(
            "zone_manifest_read_boot_disk_read_error",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();

        // Write the valid manifest to the non-boot disk.
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();
        // Create an empty file on the boot disk (will cause a read error).
        dir.child(&BOOT_PATHS.zones_json).touch().unwrap();

        let internal_disks =
            make_internal_disks(dir.path(), BOOT_ZPOOL, &[NON_BOOT_ZPOOL]);
        let manifests =
            AllZoneManifests::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            manifests.boot_disk_result.as_ref().unwrap_err(),
            &deserialize_error(dir.path(), &BOOT_PATHS.zones_json, "").into(),
        );

        assert_eq!(
            manifests.non_boot_disk_metadata,
            id_ord_map! {
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::Mismatch(
                        ZoneManifestNonBootMismatch::BootDiskReadError {
                            non_boot_disk_result: cx.expected_result(
                                &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                            ),
                        }
                    )
                }
            }
        );

        logctx.cleanup_successful();
    }

    /// Error case: zones don't match expected ones on boot disk.
    #[test]
    fn read_boot_disk_zone_mismatch() {
        let logctx = LogContext::new(
            "zone_manifest_read_boot_disk_zone_mismatch",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let mut invalid_cx = cx.clone();
        invalid_cx.make_error_cases();

        // Write the valid manifest to the non-boot disk.
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();
        // Write the invalid manifest to the boot disk.
        invalid_cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks =
            make_internal_disks(dir.path(), BOOT_ZPOOL, &[NON_BOOT_ZPOOL]);
        let manifests =
            AllZoneManifests::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            manifests.boot_disk_result.as_ref().unwrap(),
            &invalid_cx
                .expected_result(&dir.path().join(&BOOT_PATHS.install_dataset)),
        );

        assert_eq!(
            manifests.non_boot_disk_metadata,
            id_ord_map! {
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::Mismatch(
                        // The boot disk was read successfully but the zones on
                        // the boot disk didn't match what was on disk. We could
                        // treat this as either a ValueMismatch or a
                        // BootDiskReadError -- currently, we treat it as a
                        // ValueMismatch for convenience.
                        ZoneManifestNonBootMismatch::ValueMismatch {
                            non_boot_disk_result: cx.expected_result(
                                &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                            ),
                        }
                    )
                }
            },
        );

        logctx.cleanup_successful();
    }

    /// Warning case: zones don't match expected ones on non-boot
    /// disk/error/absent.
    #[test]
    fn read_non_boot_disk_zone_mismatch() {
        let logctx = LogContext::new(
            "zone_manifest_read_non_boot_disk_zone_mismatch",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let mut invalid_cx = cx.clone();
        invalid_cx.make_error_cases();

        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        invalid_cx
            .write_to(&dir.child(&NON_BOOT_PATHS.install_dataset))
            .unwrap();
        // Zone manifest file that's absent.
        dir.child(&NON_BOOT_2_PATHS.install_dataset).create_dir_all().unwrap();
        // Read error (empty file).
        dir.child(&NON_BOOT_3_PATHS.zones_json).touch().unwrap();

        let internal_disks = make_internal_disks(
            dir.path(),
            BOOT_ZPOOL,
            &[NON_BOOT_ZPOOL, NON_BOOT_2_ZPOOL, NON_BOOT_3_ZPOOL],
        );
        let manifests =
            AllZoneManifests::read_all(&logctx.log, &internal_disks);
        // The boot disk is valid.
        let boot_disk_result = manifests.boot_disk_result.as_ref().unwrap();
        assert_eq!(
            boot_disk_result,
            &cx.expected_result(&dir.path().join(&BOOT_PATHS.install_dataset))
        );

        // The non-boot disks have various error cases.
        let non_boot_disk_result = invalid_cx.expected_result(
            dir.child(&NON_BOOT_PATHS.install_dataset).as_path(),
        );
        assert_eq!(
            manifests.non_boot_disk_metadata,
            id_ord_map! {
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::Mismatch(
                        ZoneManifestNonBootMismatch::ValueMismatch {
                            non_boot_disk_result: non_boot_disk_result.clone(),
                        }
                    )
                },
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_2_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_2_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_2_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::ReadError(
                        ZoneManifestReadError::NotFound(
                            dir.path().join(&NON_BOOT_2_PATHS.zones_json)
                        ),
                    )
                },
                ZoneManifestNonBootInfo {
                    zpool_name: NON_BOOT_3_ZPOOL,
                    dataset_dir: dir.path().join(&NON_BOOT_3_PATHS.install_dataset),
                    path: dir.path().join(&NON_BOOT_3_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::ReadError(
                        deserialize_error(
                            dir.path(),
                            &NON_BOOT_3_PATHS.zones_json,
                            "",
                        )
                        .into()
                    )
                }
            },
        );

        // Also use the opportunity to test display output.
        assert_contents(
            "tests/output/zone_manifest_match_result.txt",
            &boot_disk_result.display().to_string(),
        );
        assert_contents(
            "tests/output/zone_manifest_mismatch_result.txt",
            &non_boot_disk_result.display().to_string(),
        );

        logctx.cleanup_successful();
    }
}
