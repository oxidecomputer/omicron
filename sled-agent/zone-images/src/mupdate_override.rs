// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Track MUPdate overrides within sled-agent.
//!
//! For more about commingling MUPdate and update, see RFD 556.

use crate::AllInstallMetadataFiles;
use crate::InstallMetadataNonBootInfo;
use crate::InstallMetadataNonBootMismatch;
use crate::InstallMetadataNonBootResult;
use crate::InstallMetadataReadError;
use crate::ZoneImageZpools;
use camino::Utf8PathBuf;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use illumos_utils::zpool::ZpoolName;
use omicron_common::update::MupdateOverrideInfo;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use thiserror::Error;

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

#[derive(Debug)]
pub(crate) struct AllMupdateOverrides {
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
    /// For more about commingling MUPdate and update, see RFD 556.
    pub(crate) fn read_all(
        log: &slog::Logger,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: &ZpoolName,
    ) -> Self {
        let files = AllInstallMetadataFiles::<MupdateOverrideInfo>::read_all(
            log,
            MupdateOverrideInfo::FILE_NAME,
            zpools,
            boot_zpool,
        );

        let boot_disk_override = files
            .boot_disk_metadata
            .map_err(MupdateOverrideReadError::InstallMetadata);
        let non_boot_disk_overrides = files
            .non_boot_disk_metadata
            .into_iter()
            .map(MupdateOverrideNonBootInfo::new)
            .collect();

        let ret = Self {
            boot_zpool: files.boot_zpool,
            boot_disk_path: files.boot_disk_path,
            boot_disk_override,
            non_boot_disk_overrides,
        };

        ret.log_results(&log);
        ret
    }

    pub(crate) fn status(&self) -> MupdateOverrideStatus {
        MupdateOverrideStatus {
            boot_disk_path: self.boot_disk_path.clone(),
            boot_disk_override: self.boot_disk_override.clone(),
            non_boot_disk_overrides: self.non_boot_disk_overrides.clone(),
        }
    }

    fn log_results(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "component" => "mupdate_override",
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

/// Describes the result of reading a mupdate override file from a non-boot disk.
#[derive(Clone, Debug, PartialEq)]
pub struct MupdateOverrideNonBootInfo {
    /// The zpool name.
    pub zpool_name: ZpoolName,

    /// The path to the mupdate override file.
    pub path: Utf8PathBuf,

    /// The result of reading the mupdate override file.
    pub result: MupdateOverrideNonBootResult,
}

impl MupdateOverrideNonBootInfo {
    pub fn new(info: InstallMetadataNonBootInfo<MupdateOverrideInfo>) -> Self {
        let result = match info.result {
            InstallMetadataNonBootResult::MatchesPresent(_) => {
                MupdateOverrideNonBootResult::MatchesPresent
            }
            InstallMetadataNonBootResult::MatchesAbsent => {
                MupdateOverrideNonBootResult::MatchesAbsent
            }
            InstallMetadataNonBootResult::Mismatch(mismatch) => {
                let mupdate_mismatch = match mismatch {
                    InstallMetadataNonBootMismatch::BootPresentOtherAbsent => {
                        MupdateOverrideNonBootMismatch::BootPresentOtherAbsent
                    }
                    InstallMetadataNonBootMismatch::BootAbsentOtherPresent { non_boot_disk_info } => {
                        MupdateOverrideNonBootMismatch::BootAbsentOtherPresent { non_boot_disk_info }
                    }
                    InstallMetadataNonBootMismatch::ValueMismatch { non_boot_disk_info } => {
                        MupdateOverrideNonBootMismatch::ValueMismatch { non_boot_disk_info }
                    }
                    InstallMetadataNonBootMismatch::BootDiskReadError { non_boot_disk_info } => {
                        MupdateOverrideNonBootMismatch::BootDiskReadError { non_boot_disk_info }
                    }
                };
                MupdateOverrideNonBootResult::Mismatch(mupdate_mismatch)
            }
            InstallMetadataNonBootResult::ReadError(error) => {
                MupdateOverrideNonBootResult::ReadError(
                    MupdateOverrideReadError::InstallMetadata(error),
                )
            }
        };

        Self { zpool_name: info.zpool_name, path: info.path, result }
    }

    fn log_result(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "zpool_name" => self.zpool_name.to_string(),
            "path" => self.path.to_string(),
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
    type Key<'a> = ZpoolName;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_name
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
pub enum MupdateOverrideReadError {
    #[error("install metadata read error")]
    InstallMetadata(#[from] InstallMetadataReadError),
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::BOOT_PATHS;
    use crate::test_utils::BOOT_ZPOOL;
    use crate::test_utils::NON_BOOT_2_PATHS;
    use crate::test_utils::NON_BOOT_2_ZPOOL;
    use crate::test_utils::NON_BOOT_3_PATHS;
    use crate::test_utils::NON_BOOT_3_ZPOOL;
    use crate::test_utils::NON_BOOT_PATHS;
    use crate::test_utils::NON_BOOT_ZPOOL;
    use crate::test_utils::WriteInstallDatasetContext;
    use crate::test_utils::dataset_missing_error;
    use crate::test_utils::dataset_not_dir_error;
    use crate::test_utils::deserialize_error;

    use camino_tempfile_ext::prelude::*;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use dropshot::test_util::LogContext;
    use iddqd::id_ord_map;
    use pretty_assertions::assert_eq;

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
        let info = cx.override_info();
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
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

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
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::MatchesPresent,
                }
            },
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
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::MatchesAbsent,
                }
            },
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
        let info = cx.override_info();
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
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootPresentOtherAbsent,
                    ),
                }
            },
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
        let info = cx.override_info();

        // Create the directory, but not the override JSON within it.
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();

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
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                            non_boot_disk_info: info.clone()
                        },
                    ),
                }
            },
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
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        let cx2 = WriteInstallDatasetContext::new_basic();
        let info2 = cx2.override_info();
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
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::ValueMismatch {
                            non_boot_disk_info: info2,
                        }
                    ),
                }
            },
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
            .into()
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::ReadError(
                        dataset_missing_error(
                            &dir.path().join(&NON_BOOT_PATHS.install_dataset)
                        )
                        .into(),
                    )
                }
            },
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
            .into()
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::ReadError(
                        dataset_not_dir_error(
                            &dir.path().join(&NON_BOOT_PATHS.install_dataset),
                        )
                        .into(),
                    ),
                }
            },
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
        let info = cx.override_info();

        // Create an empty file: this won't deserialize correctly.
        dir.child(&BOOT_PATHS.mupdate_override_json).touch().unwrap();
        // File with the correct contents.
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();
        // File that's absent.
        dir.child(&NON_BOOT_2_PATHS.install_dataset).create_dir_all().unwrap();
        // Read error (empty file).
        dir.child(&NON_BOOT_3_PATHS.mupdate_override_json).touch().unwrap();

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
            &deserialize_error(
                dir.path(),
                &BOOT_PATHS.mupdate_override_json,
                ""
            )
            .into(),
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_ZPOOL,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootDiskReadError {
                            non_boot_disk_info: Some(info),
                        },
                    ),
                },
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_2_ZPOOL,
                    path: dir.path().join(&NON_BOOT_2_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootDiskReadError {
                            non_boot_disk_info: None,
                        },
                    ),
                },
                MupdateOverrideNonBootInfo {
                    zpool_name: NON_BOOT_3_ZPOOL,
                    path: dir.path().join(&NON_BOOT_3_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::ReadError(
                        deserialize_error(
                            dir.path(),
                            &NON_BOOT_3_PATHS.mupdate_override_json,
                            "",
                        )
                        .into(),
                    ),
                },
            },
        );

        logctx.cleanup_successful();
    }
}
