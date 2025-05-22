// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Track MUPdate overrides within sled-agent.
//!
//! For more about commingling MUPdate and update, see RFD 556.

use std::fs;
use std::fs::FileType;
use std::io;
use std::sync::Arc;

use crate::ZoneImageZpools;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::zpool::ZpoolName;
use omicron_common::update::MupdateOverrideInfo;
use sled_storage::dataset::INSTALL_DATASET;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct AllMupdateOverrides {
    boot_zpool: ZpoolName,
    boot_disk_path: Utf8PathBuf,
    boot_disk_override:
        Result<Option<MupdateOverrideInfo>, MupdateOverrideReadError>,
    non_boot_disk_overrides: IdMap<MupdateOverrideNonBootInfo>,
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
        let non_boot_disks_overrides = non_boot_zpools
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
            non_boot_disk_overrides: non_boot_disks_overrides,
        };

        ret.log_results(&log);
        ret
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
                Some(data)
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
struct MupdateOverrideNonBootInfo {
    /// The name of the zpool.
    zpool_name: ZpoolName,

    /// The path that was read from.
    path: Utf8PathBuf,

    /// The result of performing the read operation.
    result: MupdateOverrideNonBootResult,
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

impl IdMappable for MupdateOverrideNonBootInfo {
    type Id = ZpoolName;

    fn id(&self) -> Self::Id {
        self.zpool_name
    }
}

#[derive(Clone, Debug, PartialEq)]
enum MupdateOverrideNonBootResult {
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
enum MupdateOverrideNonBootMismatch {
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

#[derive(Clone, Debug, Error, PartialEq)]
enum MupdateOverrideReadError {
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
}

/// An `io::Error` wrapper that implements `Clone` and `PartialEq`.
#[derive(Clone, Debug, Error)]
#[error(transparent)]
struct ArcIoError(Arc<io::Error>);

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
struct ArcSerdeJsonError(Arc<serde_json::Error>);

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

    static OVERRIDE_UUID: MupdateOverrideUuid =
        MupdateOverrideUuid::from_u128(0x70b965c2_fc95_4843_a34d_a2c7246788a8);
    static OVERRIDE_2_UUID: MupdateOverrideUuid =
        MupdateOverrideUuid::from_u128(0x20588f8f_c680_4101_afc7_820226d03ada);

    /// Boot disk present / no other disks. (This produces a warning, but is
    /// otherwise okay.)
    #[test]
    fn read_solo_boot_disk() {
        let logctx = LogContext::new(
            "mupdate_override_read_other_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let override_info = override_info();
        let dir = Utf8TempDir::new().unwrap();

        dir.child(&BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&override_info)
        );
        assert_eq!(overrides.non_boot_disk_overrides, IdMap::new());

        logctx.cleanup_successful();
    }

    /// Matching case: boot disk present / other disk present.
    #[test]
    fn read_both_present() {
        let logctx = LogContext::new(
            "mupdate_override_read_both_present",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let override_info = override_info();
        let dir = Utf8TempDir::new().unwrap();

        dir.child(&BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .unwrap();
        dir.child(&NON_BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&override_info)
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
        let override_info = override_info();
        let dir = Utf8TempDir::new().unwrap();

        dir.child(&BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .unwrap();
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
            Some(&override_info)
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
        let override_info = override_info();
        let dir = Utf8TempDir::new().unwrap();

        // Create the directory, but not the override JSON within it.
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        dir.child(&NON_BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .unwrap();

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
                        non_boot_disk_info: override_info.clone()
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
        let override_info = override_info();
        let override_info_2 = override_info_2();
        let dir = Utf8TempDir::new().unwrap();

        dir.child(&BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .expect("failed to write override json");
        dir.child(&NON_BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info_2).unwrap())
            .expect("failed to write override json");

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL, NON_BOOT_ZPOOL],
        };
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &zpools, &BOOT_ZPOOL);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&override_info),
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            [MupdateOverrideNonBootInfo {
                zpool_name: NON_BOOT_ZPOOL,
                path: dir.path().join(&NON_BOOT_PATHS.override_json),
                result: MupdateOverrideNonBootResult::Mismatch(
                    MupdateOverrideNonBootMismatch::ValueMismatch {
                        non_boot_disk_info: override_info_2,
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
        let override_info = override_info();
        let dir = Utf8TempDir::new().unwrap();

        // Create an empty file: this won't deserialize correctly.
        dir.child(&BOOT_PATHS.override_json).touch().unwrap();
        // File with the correct contents.
        dir.child(&NON_BOOT_PATHS.override_json)
            .write_str(&serde_json::to_string(&override_info).unwrap())
            .unwrap();
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
                            non_boot_disk_info: Some(override_info),
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

    fn override_info() -> MupdateOverrideInfo {
        MupdateOverrideInfo {
            mupdate_uuid: OVERRIDE_UUID,
            hash_ids: BTreeSet::new(),
        }
    }

    fn override_info_2() -> MupdateOverrideInfo {
        MupdateOverrideInfo {
            mupdate_uuid: OVERRIDE_2_UUID,
            hash_ids: BTreeSet::new(),
        }
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
