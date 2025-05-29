// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Track MUPdate overrides within sled-agent.
//!
//! For more about commingling MUPdate and update, see RFD 556.

use std::fmt;
use std::fs;
use std::fs::File;
use std::fs::FileType;
use std::io;
use std::io::Read;
use std::sync::Arc;

use crate::ZoneImageZpools;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use illumos_utils::zpool::ZpoolName;
use omicron_common::update::MupdateOverrideInfo;
use omicron_common::update::MupdateOverrideZone;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use sha2::Digest;
use sha2::Sha256;
use sled_storage::dataset::INSTALL_DATASET;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;

/// Describes the current state of mupdate overrides.
#[derive(Clone, Debug)]
pub struct MupdateOverrideStatus {
    /// The boot zpool.
    pub boot_zpool: ZpoolName,

    /// The boot disk path.
    pub boot_disk_path: Utf8PathBuf,

    /// Status of the boot disk.
    pub boot_disk_override:
        Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError>,

    /// Status of the non-boot disks. This results in warnings.
    pub non_boot_disk_overrides: IdOrdMap<MupdateOverrideNonBootInfo>,
}

#[derive(Debug)]
pub(crate) struct AllMupdateOverrides {
    // This is internal-only, and currently a duplicate of MupdateOverrideStatus
    // in case we need to store more information here in the future.
    boot_zpool: ZpoolName,
    boot_disk_path: Utf8PathBuf,
    boot_disk_override:
        Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError>,
    non_boot_disk_overrides: IdOrdMap<MupdateOverrideNonBootInfo>,
}

impl AllMupdateOverrides {
    /// Attempt to find MUPdate override files. If present, this file will cause
    /// install-dataset artifacts to be used even if the image source is Artifact.
    ///
    /// For now we treat the boot disk as authoritative, since install-dataset
    /// artifacts are always served from the boot disk. There is a class of issues
    /// here related to transient failures on one of the M.2s that we're
    /// acknowledging but not tackling for now.
    ///
    /// In general, this API follows an interpreter pattern: first read all the
    /// results and put them in a map, then make decisions based on them in a
    /// separate step. This enables better testing and ensures that changes to
    /// behavior are described in the type system.
    ///
    /// For more about commingling MUPdate and update, see RFD 556.
    ///
    /// TODO: This is somewhat complex error handling logic that's similar to,
    /// but different from, `Ledgerable` (for example, it only does an equality
    /// check, not an ordering check, and it always considers the boot disk to
    /// be authoritative). Consider extracting this out into something generic.
    pub(crate) fn read_all(
        log: &slog::Logger,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: &ZpoolName,
    ) -> Self {
        let dataset =
            boot_zpool.dataset_mountpoint(zpools.root, INSTALL_DATASET);

        let (boot_disk_path, boot_disk_res) =
            read_mupdate_override(log, &dataset);

        // Now read the file from all other disks. We attempt to make sure they
        // match up and will log a warning if they don't, though (until we have
        // a better story on transient failures) it's not fatal.
        let non_boot_zpools = zpools
            .all_m2_zpools
            .iter()
            .filter(|&zpool_name| zpool_name != boot_zpool);
        let non_boot_disk_overrides = non_boot_zpools
            .map(|zpool_name| {
                let dataset =
                    zpool_name.dataset_mountpoint(zpools.root, INSTALL_DATASET);

                let (path, res) = read_mupdate_override(log, &dataset);
                MupdateOverrideNonBootInfo {
                    zpool_name: *zpool_name,
                    path,
                    result: MupdateOverrideNonBootResult::new(
                        res,
                        &boot_disk_res,
                    ),
                }
            })
            .collect();

        let ret = Self {
            boot_zpool: *boot_zpool,
            boot_disk_path,
            boot_disk_override: boot_disk_res,
            non_boot_disk_overrides,
        };

        ret.log_results(&log);
        ret
    }

    pub(crate) fn status(&self) -> MupdateOverrideStatus {
        MupdateOverrideStatus {
            boot_zpool: self.boot_zpool,
            boot_disk_path: self.boot_disk_path.clone(),
            boot_disk_override: self.boot_disk_override.clone(),
            non_boot_disk_overrides: self.non_boot_disk_overrides.clone(),
        }
    }

    fn log_results(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "boot_zpool" => self.boot_zpool.to_string(),
            "boot_disk_path" => self.boot_disk_path.to_string(),
        ));

        match &self.boot_disk_override {
            Ok(Some(mupdate_override)) => {
                info!(
                    log,
                    "found mupdate override for boot disk";
                    "data" => ?mupdate_override,
                );
            }
            Ok(None) => {
                info!(log, "no mupdate override for boot disk");
            }
            Err(error) => {
                // This error most likely requires operator intervention -- if
                // it happens, we'll continue to bring sled-agent up but reject
                // all zone image lookups.
                error!(
                    log,
                    "error reading mupdate override for boot disk, \
                     will not bring up zones";
                    "error" => InlineErrorChain::new(error),
                );
            }
        }

        if self.non_boot_disk_overrides.is_empty() {
            warn!(
                log,
                "no non-boot zpools found, unable to verify consistency -- \
                 this may be a hardware issue with the non-boot M.2"
            );
        }

        for info in &self.non_boot_disk_overrides {
            info.log_result(&log);
        }
    }
}

fn read_mupdate_override(
    log: &slog::Logger,
    dataset_dir: &Utf8Path,
) -> (Utf8PathBuf, Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError>)
{
    let override_path = dataset_dir.join(MupdateOverrideInfo::FILE_NAME);

    fn inner(
        log: &slog::Logger,
        dataset_dir: &Utf8Path,
        override_path: &Utf8Path,
    ) -> Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError> {
        // First check that the dataset directory exists. This distinguishes the
        // two cases:
        //
        // 1. The install dataset is missing (an error).
        // 2. The install dataset is present, but the override file is missing
        //    (expected).
        //
        // It would be nice if we could use openat-style APIs to read the file
        // from the opened directory, but:
        //
        // * those don't exist in rust std
        // * it's not crucial -- we don't expect TOCTTOU races much in this code
        //   path, and we're not generally resilient to them anyway.
        //
        // We use symlink_metadata (lstat) rather than metadata (stat) because
        // there really shouldn't be any symlinks involved.
        let dir_metadata =
            fs::symlink_metadata(dataset_dir).map_err(|error| {
                MupdateOverrideReadError::DatasetDirMetadata {
                    dataset_dir: dataset_dir.to_owned(),
                    error: ArcIoError::new(error),
                }
            })?;
        if !dir_metadata.is_dir() {
            return Err(MupdateOverrideReadError::DatasetNotDirectory {
                dataset_dir: dataset_dir.to_owned(),
                file_type: dir_metadata.file_type(),
            });
        }

        let mupdate_override = match std::fs::read_to_string(&override_path) {
            Ok(data) => {
                let data = serde_json::from_str::<MupdateOverrideInfo>(&data)
                    .map_err(|error| {
                    MupdateOverrideReadError::Deserialize {
                        path: override_path.to_owned(),
                        error: ArcSerdeJsonError::new(error),
                        contents: data,
                    }
                })?;
                let artifacts =
                    MupdateOverrideArtifactsResult::new(dataset_dir, data);
                if artifacts.is_valid() {
                    // If there are errors, return them as appropriate.
                    Some(artifacts.info)
                } else {
                    // At least one artifact was invalid: return an error.
                    //
                    // XXX: Should we be more fine-grained than this, handle
                    // errors on a per-artifact basis? Seems excessive.
                    return Err(MupdateOverrideReadError::ArtifactRead {
                        dataset_dir: dataset_dir.to_owned(),
                        artifacts,
                    });
                }
            }
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    debug!(
                        log,
                        "mupdate override file not found, treating as absent";
                        "path" => %override_path
                    );
                    None
                } else {
                    return Err(MupdateOverrideReadError::Read {
                        path: override_path.to_owned(),
                        error: ArcIoError::new(error),
                    });
                }
            }
        };

        Ok(mupdate_override)
    }

    let res = inner(log, dataset_dir, &override_path);
    (override_path, res)
}

#[derive(Clone, Debug, PartialEq)]
pub struct MupdateOverrideNonBootInfo {
    /// The name of the zpool.
    pub zpool_name: ZpoolName,

    /// The path that was read from.
    pub path: Utf8PathBuf,

    /// The result of performing the read operation.
    pub result: MupdateOverrideNonBootResult,
}

impl MupdateOverrideNonBootInfo {
    fn log_result(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "non_boot_zpool_name" => self.zpool_name.to_string(),
            "non_boot_path" => self.path.to_string(),
        ));

        match &self.result {
            MupdateOverrideNonBootResult::MatchesAbsent => {
                info!(
                    log,
                    "mupdate override absent on this non-boot \
                     disk, matches absence on boot disk",
                );
            }
            MupdateOverrideNonBootResult::MatchesPresent => {
                info!(
                    log,
                    "mupdate override present on this non-boot \
                     disk, matches presence on boot disk",
                );
            }
            MupdateOverrideNonBootResult::ReadError(error) => {
                warn!(
                    log,
                    "failed to read mupdate override from other disk";
                    "error" => InlineErrorChain::new(error),
                );
            }
            MupdateOverrideNonBootResult::Mismatch(mismatch) => {
                mismatch.log_to(&log)
            }
        }
    }
}

impl IdOrdItem for MupdateOverrideNonBootInfo {
    type Key<'a> = ZpoolName;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_name
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq)]
pub enum MupdateOverrideNonBootResult {
    /// The override is present and matches the value on the boot disk.
    MatchesPresent,

    /// The override is absent and is also absent on the boot disk.
    MatchesAbsent,

    /// A mismatch between the boot disk and the other disk was detected.
    Mismatch(MupdateOverrideNonBootMismatch),

    /// An error occurred while reading the mupdate override info on this disk.
    ReadError(MupdateOverrideReadError),
}

impl MupdateOverrideNonBootResult {
    fn new(
        res: Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError>,
        boot_disk_res: &Result<
            Option<MupdateOverrideInfo>,
            MupdateOverrideReadError,
        >,
    ) -> Self {
        match (res, boot_disk_res) {
            (Ok(Some(non_boot_disk_info)), Ok(Some(boot_disk_info))) => {
                if boot_disk_info == &non_boot_disk_info {
                    Self::MatchesPresent
                } else {
                    Self::Mismatch(
                        MupdateOverrideNonBootMismatch::ValueMismatch {
                            non_boot_disk_info,
                        },
                    )
                }
            }
            (Ok(Some(non_boot_disk_info)), Ok(None)) => Self::Mismatch(
                MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info,
                },
            ),
            (Ok(None), Ok(Some(_))) => Self::Mismatch(
                MupdateOverrideNonBootMismatch::BootPresentOtherAbsent,
            ),
            (Ok(None), Ok(None)) => Self::MatchesAbsent,
            (Ok(non_boot_disk_info), Err(_)) => Self::Mismatch(
                MupdateOverrideNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info,
                },
            ),
            (Err(error), _) => Self::ReadError(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MupdateOverrideNonBootMismatch {
    /// The override is present on the boot disk but absent on the other disk.
    BootPresentOtherAbsent,

    /// The override is absent on the boot disk but present on the other disk.
    BootAbsentOtherPresent {
        /// The information found on the other disk.
        non_boot_disk_info: MupdateOverrideInfo,
    },

    /// The override value differs between the boot disk and the other disk.
    ValueMismatch { non_boot_disk_info: MupdateOverrideInfo },

    /// There was a read error on the boot disk, so we were unable to verify
    /// consistency.
    BootDiskReadError {
        /// The value as found on this disk. This value is logged but not used.
        non_boot_disk_info: Option<MupdateOverrideInfo>,
    },
}

impl MupdateOverrideNonBootMismatch {
    // This function assumes that `log` has already been provided context about
    // the zpool name and path.
    fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::BootPresentOtherAbsent => {
                warn!(
                    log,
                    "mupdate override absent on this non-boot disk but \
                     present on boot disk, treating file on boot disk as \
                     authoritative"
                )
            }
            Self::BootAbsentOtherPresent { non_boot_disk_info } => {
                warn!(
                    log,
                    "mupdate override present on this non-boot disk but \
                     absent on boot disk, treating the absence on boot disk \
                     as authoritative";
                    "non_boot_disk_info" => ?non_boot_disk_info,
                )
            }
            Self::ValueMismatch { non_boot_disk_info } => {
                warn!(
                    log,
                    "mupdate override value present on both this non-boot \
                     disk and the boot disk, but different across disks, \
                     treating boot disk as authoritative";
                    "non_boot_disk_info" => ?non_boot_disk_info,
                )
            }
            Self::BootDiskReadError { non_boot_disk_info } => {
                warn!(
                    log,
                    "mupdate override read error on boot disk, unable \
                     to verify consistency across disks";
                    "non_boot_disk_info" => ?non_boot_disk_info,
                )
            }
        }
    }
}

/// The result of reading artifacts from an install dataset.
///
/// This may or may not be valid, depending on the status of the artifacts. See
/// [`Self::is_valid`].
#[derive(Clone, Debug, PartialEq)]
pub struct MupdateOverrideArtifactsResult {
    pub info: MupdateOverrideInfo,
    pub data: IdOrdMap<MupdateOverrideArtifactResult>,
}

impl MupdateOverrideArtifactsResult {
    /// Makes a new `MupdateOverrideArtifacts` by reading artifacts from the
    /// given directory.
    fn new(dir: &Utf8Path, info: MupdateOverrideInfo) -> Self {
        let artifacts: Vec<_> = info
            .zones
            .iter()
            // Parallelize artifact reading to speed it up.
            .par_bridge()
            .map(|zone| {
                let artifact_path = dir.join(&zone.file_name);
                let status = validate_one(&artifact_path, &zone);

                MupdateOverrideArtifactResult {
                    file_name: zone.file_name.clone(),
                    path: artifact_path,
                    expected_size: zone.file_size,
                    expected_hash: zone.hash,
                    status,
                }
            })
            .collect();

        Self { info, data: artifacts.into_iter().collect() }
    }

    /// Returns true if all artifacts are valid.
    pub fn is_valid(&self) -> bool {
        self.data.iter().all(|artifact| artifact.is_valid())
    }

    /// Returns a displayable representation of the artifacts.
    pub fn display(&self) -> MupdateOverrideArtifactsDisplay<'_> {
        MupdateOverrideArtifactsDisplay { artifacts: &self.data }
    }
}

pub struct MupdateOverrideArtifactsDisplay<'a> {
    artifacts: &'a IdOrdMap<MupdateOverrideArtifactResult>,
}

impl fmt::Display for MupdateOverrideArtifactsDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for artifact in self.artifacts {
            match &artifact.status {
                ArtifactReadResult::Valid => {
                    writeln!(
                        f,
                        "  {}: ok ({} bytes, {})",
                        artifact.file_name,
                        artifact.expected_size,
                        artifact.expected_hash
                    )?;
                }
                ArtifactReadResult::Mismatch { actual_size, actual_hash } => {
                    writeln!(
                        f,
                        "  {}: mismatch (expected {} bytes, {}; \
                         found {} bytes, {})",
                        artifact.file_name,
                        artifact.expected_size,
                        artifact.expected_hash,
                        actual_size,
                        actual_hash
                    )?;
                }
                ArtifactReadResult::Error(error) => {
                    writeln!(f, "  {}: error ({})", artifact.file_name, error)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MupdateOverrideArtifactResult {
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

impl MupdateOverrideArtifactResult {
    fn is_valid(&self) -> bool {
        matches!(self.status, ArtifactReadResult::Valid)
    }
}

impl IdOrdItem for MupdateOverrideArtifactResult {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
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

fn validate_one(
    artifact_path: &Utf8Path,
    zone: &MupdateOverrideZone,
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

#[derive(Clone, Debug, PartialEq, Error)]
pub enum MupdateOverrideReadError {
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

    #[error("error reading mupdate override from `{path}`")]
    Read {
        path: Utf8PathBuf,
        #[source]
        error: ArcIoError,
    },

    #[error(
        "error deserializing `{path}` into MupdateOverrideInfo, \
         contents: {contents:?}"
    )]
    Deserialize {
        path: Utf8PathBuf,
        contents: String,
        #[source]
        error: ArcSerdeJsonError,
    },

    #[error("error reading artifacts in `{dataset_dir}:\n{}`", artifacts.display())]
    ArtifactRead {
        dataset_dir: Utf8PathBuf,
        artifacts: MupdateOverrideArtifactsResult,
    },
}

/// An `io::Error` wrapper that implements `Clone` and `PartialEq`.
#[derive(Clone, Debug, Error)]
#[error(transparent)]
pub struct ArcIoError(Arc<io::Error>);

impl ArcIoError {
    fn new(error: io::Error) -> Self {
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
pub struct ArcSerdeJsonError(Arc<serde_json::Error>);

impl ArcSerdeJsonError {
    fn new(error: serde_json::Error) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use camino_tempfile_ext::fixture::ChildPath;
    use camino_tempfile_ext::fixture::FixtureError;
    use camino_tempfile_ext::fixture::FixtureKind;
    use camino_tempfile_ext::prelude::*;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use dropshot::test_util::LogContext;
    use omicron_uuid_kinds::MupdateOverrideUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;
    use std::io;
    use std::sync::LazyLock;

    struct OverridePaths {
        install_dataset: Utf8PathBuf,
        override_json: Utf8PathBuf,
    }

    impl OverridePaths {
        fn for_uuid(uuid: ZpoolUuid) -> Self {
            let install_dataset =
                Utf8PathBuf::from(format!("pool/int/{uuid}/install"));
            let mupdate_override_json =
                install_dataset.join("mupdate-override.json");
            Self { install_dataset, override_json: mupdate_override_json }
        }
    }

    const BOOT_UUID: ZpoolUuid =
        ZpoolUuid::from_u128(0xd3e7205d_4efe_493b_ac5e_9175584907cd);
    const BOOT_ZPOOL: ZpoolName = ZpoolName::new_internal(BOOT_UUID);
    static BOOT_PATHS: LazyLock<OverridePaths> =
        LazyLock::new(|| OverridePaths::for_uuid(BOOT_UUID));

    const NON_BOOT_UUID: ZpoolUuid =
        ZpoolUuid::from_u128(0x4854189f_b290_47cd_b076_374d0e1748ec);
    const NON_BOOT_ZPOOL: ZpoolName = ZpoolName::new_internal(NON_BOOT_UUID);
    static NON_BOOT_PATHS: LazyLock<OverridePaths> =
        LazyLock::new(|| OverridePaths::for_uuid(NON_BOOT_UUID));

    const NON_BOOT_2_UUID: ZpoolUuid =
        ZpoolUuid::from_u128(0x72201e1e_9fee_4231_81cd_4e2d514cb632);
    const NON_BOOT_2_ZPOOL: ZpoolName =
        ZpoolName::new_internal(NON_BOOT_2_UUID);
    static NON_BOOT_2_PATHS: LazyLock<OverridePaths> =
        LazyLock::new(|| OverridePaths::for_uuid(NON_BOOT_2_UUID));

    const NON_BOOT_3_UUID: ZpoolUuid =
        ZpoolUuid::from_u128(0xd0d04947_93c5_40fd_97ab_4648b8cc28d6);
    const NON_BOOT_3_ZPOOL: ZpoolName =
        ZpoolName::new_internal(NON_BOOT_3_UUID);
    static NON_BOOT_3_PATHS: LazyLock<OverridePaths> =
        LazyLock::new(|| OverridePaths::for_uuid(NON_BOOT_3_UUID));

    /// Boot disk present / no other disks. (This produces a warning, but is
    /// otherwise okay.)
    #[test]
    fn read_solo_boot_disk() {
        let logctx = LogContext::new(
            "mupdate_override_read_other_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info =
            cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );
        assert_eq!(overrides.non_boot_disk_overrides, IdOrdMap::new());

        logctx.cleanup_successful();
    }

    /// Matching case: boot disk present / other disk present.
    #[test]
    fn read_both_present() {
        let logctx = LogContext::new(
            "mupdate_override_read_both_present",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info =
            cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        let info2 =
            cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();
        assert_eq!(info, info2, "the same contents must have been written out");

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::MatchesPresent,
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Matching case: boot disk absent / other disk absent.
    #[test]
    fn read_both_absent() {
        let logctx = LogContext::new(
            "mupdate_override_read_both_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );

        let dir = Utf8TempDir::new().unwrap();

        // Create the directories but not the override JSONs within them.
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();
        dir.child(&NON_BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            None,
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::MatchesAbsent,
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Mismatch case: Boot disk present / other disk absent.
    #[test]
    fn read_boot_present_other_absent() {
        let logctx = LogContext::new(
            "mupdate_override_read_boot_present_other_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info =
            cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create the directory, but not the override JSON within it.
        dir.child(&NON_BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::Mismatch(
                    MupdateOverrideNonBootMismatch::BootPresentOtherAbsent,
                ),
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Mismatch case: Boot disk absent / other disk present.
    #[test]
    fn read_boot_absent_other_present() {
        let logctx = LogContext::new(
            "mupdate_override_read_boot_absent_other_present",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();

        // Create the directory, but not the override JSON within it.
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let info =
            cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            None,
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::Mismatch(
                    MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                        non_boot_disk_info: info.clone()
                    },
                ),
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Mismatch case: present on both disks but values differ.
    #[test]
    fn read_different_values() {
        let logctx = LogContext::new(
            "mupdate_override_read_different_values",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );

        let dir = Utf8TempDir::new().unwrap();

        // Make two different contexts. Each will have a different mupdate_uuid
        // so will not match.
        let cx = WriteInstallDatasetContext::new_basic();
        let info =
            cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        let cx2 = WriteInstallDatasetContext::new_basic();
        let info2 =
            cx2.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info),
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::Mismatch(
                    MupdateOverrideNonBootMismatch::ValueMismatch {
                        non_boot_disk_info: info2,
                    }
                ),
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Error case: boot and other install datasets don't exist (possibly not
    /// mounted? This is a strange situation.)
    #[test]
    fn read_boot_install_dataset_missing() {
        let logctx = LogContext::new(
            "mupdate_override_read_boot_install_dataset_missing",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();

        // Create the parent directory but not the install dataset directory.
        dir.child(&BOOT_PATHS.install_dataset.parent().unwrap())
            .create_dir_all()
            .unwrap();
        dir.child(&NON_BOOT_PATHS.install_dataset.parent().unwrap())
            .create_dir_all()
            .unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap_err(),
            &dataset_missing_error(
                &dir.path().join(&BOOT_PATHS.install_dataset)
            )
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::ReadError(
                    dataset_missing_error(
                        &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                    ),
                )
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Error case: boot and other install datasets are not directories
    #[test]
    fn read_boot_install_dataset_not_dir() {
        let logctx = LogContext::new(
            "mupdate_override_read_boot_install_dataset_missing",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();

        // Make the install directory paths files -- fun!
        dir.child(&BOOT_PATHS.install_dataset).touch().unwrap();
        dir.child(&NON_BOOT_PATHS.install_dataset).touch().unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap_err(),
            &dataset_not_dir_error(
                &dir.path().join(&BOOT_PATHS.install_dataset)
            )
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::ReadError(
                    dataset_not_dir_error(
                        &dir.path().join(&NON_BOOT_PATHS.install_dataset),
                    ),
                ),
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Error case: Boot read error / other present/absent/deserialize error.
    #[test]
    fn read_boot_read_error() {
        let logctx = LogContext::new(
            "mupdate_override_read_boot_read_error",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();

        // Create an empty file: this won't deserialize correctly.
        dir.child(&BOOT_PATHS.override_json).touch().unwrap();
        // File with the correct contents.
        let info =
            cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();
        // File that's absent.
        dir.child(&NON_BOOT_2_PATHS.install_dataset).create_dir_all().unwrap();
        // Read error (empty file).
        dir.child(&NON_BOOT_3_PATHS.override_json).touch().unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![
                BOOT_ZPOOL,
                NON_BOOT_ZPOOL,
                NON_BOOT_2_ZPOOL,
                NON_BOOT_3_ZPOOL,
            ],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap_err(),
            &deserialize_error(dir.path(), &BOOT_PATHS.override_json, "",),
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootDiskReadError {
                            non_boot_disk_info: Some(info),
                        },
                    ),
                },
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_2_ZPOOL,
                    path: dir.path().join(&NON_BOOT_2_PATHS.override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootDiskReadError {
                            non_boot_disk_info: None,
                        },
                    ),
                },
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_3_ZPOOL,
                    path: dir.path().join(&NON_BOOT_3_PATHS.override_json),
                    result: MupdateOverrideNonBootResult::ReadError(
                        deserialize_error(
                            dir.path(),
                            &NON_BOOT_3_PATHS.override_json,
                            "",
                        ),
                    ),
                },
            ]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Error case: zones don't match expected ones on boot disk.
    #[test]
    fn read_boot_disk_zone_mismatch() {
        let logctx = LogContext::new(
            "mupdate_override_read_boot_disk_zone_mismatch",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let mut invalid_cx = cx.clone();
        invalid_cx.make_error_cases();

        invalid_cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        let valid_info =
            cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap_err(),
            &MupdateOverrideReadError::ArtifactRead {
                dataset_dir: dir
                    .child(&BOOT_PATHS.install_dataset)
                    .as_path()
                    .to_path_buf(),
                artifacts: invalid_cx.expected_result(
                    dir.child(&BOOT_PATHS.install_dataset).as_path()
                ),
            }
        );

        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::Mismatch(
                    MupdateOverrideNonBootMismatch::BootDiskReadError {
                        non_boot_disk_info: Some(valid_info),
                    }
                )
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Warning case: zones don't match expected ones on non-boot disk.
    #[test]
    fn read_non_boot_disk_zone_mismatch() {
        let logctx = LogContext::new(
            "mupdate_override_read_non_boot_disk_zone_mismatch",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let mut invalid_cx = cx.clone();
        invalid_cx.make_error_cases();

        let info =
            cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        invalid_cx
            .write_to(&dir.child(&NON_BOOT_PATHS.install_dataset))
            .unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        // The boot disk is valid.
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );

        // The non-boot disk has an error.
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::ReadError(
                    MupdateOverrideReadError::ArtifactRead {
                        dataset_dir: dir
                            .path()
                            .join(&NON_BOOT_PATHS.install_dataset),
                        artifacts: invalid_cx.expected_result(
                            &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                        ),
                    },
                ),
            }]
            .into_iter()
            .collect(),
        );

        logctx.cleanup_successful();
    }

    /// Context for writing out fake zones to install dataset directories.
    ///
    /// The tests in this module ensure that the override JSON's list of zones
    /// matches the zone files on disk.
    #[derive(Clone, Debug)]
    struct WriteInstallDatasetContext {
        zones: IdOrdMap<ZoneContents>,
        mupdate_uuid: MupdateOverrideUuid,
    }

    impl WriteInstallDatasetContext {
        /// Initializes a new context with a couple of zones and no known
        /// errors.
        fn new_basic() -> Self {
            Self {
                zones: [
                    ZoneContents::new("zone1.tar.gz", b"zone1"),
                    ZoneContents::new("zone2.tar.gz", b"zone2"),
                    ZoneContents::new("zone3.tar.gz", b"zone3"),
                    ZoneContents::new("zone4.tar.gz", b"zone4"),
                    ZoneContents::new("zone5.tar.gz", b"zone5"),
                ]
                .into_iter()
                .collect(),
                mupdate_uuid: MupdateOverrideUuid::new_v4(),
            }
        }

        /// Makes a number of error cases for testing.
        fn make_error_cases(&mut self) {
            // zone1.tar.gz is valid.
            // For zone2.tar.gz, change the size.
            self.zones.get_mut("zone2.tar.gz").unwrap().json_size = 1024;
            // For zone3.tar.gz, change the hash.
            self.zones.get_mut("zone3.tar.gz").unwrap().json_hash =
                ArtifactHash([0; 32]);
            // Don't write out zone4 but include it in the JSON.
            self.zones.get_mut("zone4.tar.gz").unwrap().write_to_disk = false;
            // Write out zone5 but don't include it in the JSON.
            self.zones.get_mut("zone5.tar.gz").unwrap().include_in_json = false;
        }

        fn override_info(&self) -> MupdateOverrideInfo {
            MupdateOverrideInfo {
                mupdate_uuid: self.mupdate_uuid,
                // The hash IDs are not used for validation, so leave this
                // empty.
                hash_ids: BTreeSet::new(),
                zones: self
                    .zones
                    .iter()
                    .filter_map(|zone| {
                        zone.include_in_json.then(|| MupdateOverrideZone {
                            file_name: zone.file_name.clone(),
                            file_size: zone.json_size,
                            hash: zone.json_hash,
                        })
                    })
                    .collect(),
            }
        }

        /// Returns the expected result of the override, taking into account
        /// mismatches, etc.
        fn expected_result(
            &self,
            dir: &Utf8Path,
        ) -> MupdateOverrideArtifactsResult {
            let info = self.override_info();
            let data = self
                .zones
                .iter()
                .filter_map(|zone| {
                    // Currently, zone files not present in the JSON aren't
                    // reported at all.
                    //
                    // XXX: should they be?
                    zone.include_in_json.then(|| zone.expected_result(dir))
                })
                .collect();
            MupdateOverrideArtifactsResult { info, data }
        }

        /// Writes the context to a directory, returning the JSON that was
        /// written out.
        fn write_to(
            &self,
            dir: &ChildPath,
        ) -> Result<MupdateOverrideInfo, FixtureError> {
            for zone in &self.zones {
                if zone.write_to_disk {
                    dir.child(&zone.file_name).write_binary(&zone.contents)?;
                }
            }

            let info = self.override_info();
            let json = serde_json::to_string(&info).map_err(|e| {
                FixtureError::new(FixtureKind::WriteFile).with_source(e)
            })?;
            // No need to create intermediate directories with
            // camino-tempfile-ext.
            dir.child(MupdateOverrideInfo::FILE_NAME).write_str(&json)?;

            Ok(info)
        }
    }

    #[derive(Clone, Debug)]
    struct ZoneContents {
        file_name: String,
        contents: Vec<u8>,
        // json_size and json_hash are stored separately, so tests can tweak
        // them before writing out the override info.
        json_size: u64,
        json_hash: ArtifactHash,
        write_to_disk: bool,
        include_in_json: bool,
    }

    impl ZoneContents {
        fn new(file_name: &str, contents: &[u8]) -> Self {
            let size = contents.len() as u64;
            let hash = compute_hash(contents);
            Self {
                file_name: file_name.to_string(),
                contents: contents.to_vec(),
                json_size: size,
                json_hash: hash,
                write_to_disk: true,
                include_in_json: true,
            }
        }

        fn expected_result(
            &self,
            dir: &Utf8Path,
        ) -> MupdateOverrideArtifactResult {
            let status = if !self.write_to_disk {
                // Missing from the disk
                ArtifactReadResult::Error(ArcIoError::new(io::Error::new(
                    io::ErrorKind::NotFound,
                    "file not found",
                )))
            } else {
                let actual_size = self.contents.len() as u64;
                let actual_hash = compute_hash(&self.contents);
                if self.json_size != actual_size
                    || self.json_hash != actual_hash
                {
                    ArtifactReadResult::Mismatch { actual_size, actual_hash }
                } else {
                    ArtifactReadResult::Valid
                }
            };

            MupdateOverrideArtifactResult {
                file_name: self.file_name.clone(),
                path: dir.join(&self.file_name),
                expected_size: self.json_size,
                expected_hash: self.json_hash,
                status,
            }
        }
    }

    impl IdOrdItem for ZoneContents {
        type Key<'a> = &'a str;

        fn key(&self) -> Self::Key<'_> {
            &self.file_name
        }

        id_upcast!();
    }

    fn compute_hash(contents: &[u8]) -> ArtifactHash {
        let hash = Sha256::digest(contents);
        ArtifactHash(hash.into())
    }

    fn dataset_missing_error(dir_path: &Utf8Path) -> MupdateOverrideReadError {
        MupdateOverrideReadError::DatasetDirMetadata {
            dataset_dir: dir_path.to_owned(),
            error: ArcIoError(Arc::new(io::Error::from(
                io::ErrorKind::NotFound,
            ))),
        }
    }

    fn dataset_not_dir_error(dir_path: &Utf8Path) -> MupdateOverrideReadError {
        // A `FileType` must unfortunately be retrieved from disk -- can't
        // create a new one in-memory. We assume that `dir.path()` passed in
        // actually has the described error condition.
        MupdateOverrideReadError::DatasetNotDirectory {
            dataset_dir: dir_path.to_owned(),
            file_type: fs::symlink_metadata(dir_path)
                .expect("lstat on dir.path() succeeded")
                .file_type(),
        }
    }

    fn deserialize_error(
        dir_path: &Utf8Path,
        json_path: &Utf8Path,
        contents: &str,
    ) -> MupdateOverrideReadError {
        MupdateOverrideReadError::Deserialize {
            path: dir_path.join(json_path),
            contents: contents.to_owned(),
            error: ArcSerdeJsonError(Arc::new(
                serde_json::from_str::<MupdateOverrideInfo>(contents)
                    .unwrap_err(),
            )),
        }
    }
}
