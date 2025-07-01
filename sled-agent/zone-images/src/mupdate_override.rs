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
use camino::Utf8PathBuf;
use iddqd::IdOrdMap;
use omicron_common::update::MupdateOverrideInfo;
use omicron_uuid_kinds::InternalZpoolUuid;
use sled_agent_types::internal_disks::InternalDisksWithBootDisk;
use sled_agent_types::zone_images::MupdateOverrideNonBootInfo;
use sled_agent_types::zone_images::MupdateOverrideNonBootMismatch;
use sled_agent_types::zone_images::MupdateOverrideNonBootResult;
use sled_agent_types::zone_images::MupdateOverrideReadError;
use sled_agent_types::zone_images::MupdateOverrideStatus;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;

#[derive(Debug)]
pub(crate) struct AllMupdateOverrides {
    boot_zpool: InternalZpoolUuid,
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
        internal_disks: &InternalDisksWithBootDisk,
    ) -> Self {
        let files = AllInstallMetadataFiles::<MupdateOverrideInfo>::read_all(
            log,
            MupdateOverrideInfo::FILE_NAME,
            internal_disks,
            // For mupdate overrides there is no default value.
            |_| Ok(None),
        );

        let boot_disk_override = match files.boot_disk_metadata {
            // There is no default value provided for mupdate overrides, so we
            // don't need to care about the InstallMetadata wrapper.
            Ok(Some(metadata)) => Ok(Some(metadata.value)),
            Ok(None) => Ok(None),
            Err(error) => Err(MupdateOverrideReadError::InstallMetadata(error)),
        };
        let non_boot_disk_overrides = files
            .non_boot_disk_metadata
            .into_iter()
            .map(make_non_boot_info)
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
            info.log_to(&log);
        }
    }
}

fn make_non_boot_info(
    info: InstallMetadataNonBootInfo<MupdateOverrideInfo>,
) -> MupdateOverrideNonBootInfo {
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
                InstallMetadataNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info,
                } => {
                    // Here and below, we don't return a default value while
                    // constructing the set, so we can get rid of the
                    // InstallMetadata wrapper.
                    MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                        non_boot_disk_info: non_boot_disk_info.value,
                    }
                }
                InstallMetadataNonBootMismatch::ValueMismatch {
                    non_boot_disk_info,
                } => MupdateOverrideNonBootMismatch::ValueMismatch {
                    non_boot_disk_info: non_boot_disk_info.value,
                },
                InstallMetadataNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info,
                } => MupdateOverrideNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info: non_boot_disk_info.map(|v| v.value),
                },
            };
            MupdateOverrideNonBootResult::Mismatch(mupdate_mismatch)
        }
        InstallMetadataNonBootResult::ReadError(error) => {
            MupdateOverrideNonBootResult::ReadError(
                MupdateOverrideReadError::InstallMetadata(error),
            )
        }
    };

    MupdateOverrideNonBootInfo {
        zpool_id: info.zpool_id,
        path: info.path,
        result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::make_internal_disks_rx;

    use camino_tempfile_ext::prelude::*;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingLevel;
    use dropshot::test_util::LogContext;
    use iddqd::id_ord_map;
    use pretty_assertions::assert_eq;
    use sled_agent_zone_images_examples::BOOT_PATHS;
    use sled_agent_zone_images_examples::BOOT_UUID;
    use sled_agent_zone_images_examples::NON_BOOT_2_PATHS;
    use sled_agent_zone_images_examples::NON_BOOT_2_UUID;
    use sled_agent_zone_images_examples::NON_BOOT_3_PATHS;
    use sled_agent_zone_images_examples::NON_BOOT_3_UUID;
    use sled_agent_zone_images_examples::NON_BOOT_PATHS;
    use sled_agent_zone_images_examples::NON_BOOT_UUID;
    use sled_agent_zone_images_examples::WriteInstallDatasetContext;
    use sled_agent_zone_images_examples::dataset_missing_error;
    use sled_agent_zone_images_examples::dataset_not_dir_error;
    use sled_agent_zone_images_examples::deserialize_error;

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

        let internal_disks = make_internal_disks_rx(dir.path(), BOOT_UUID, &[])
            .current_with_boot_disk();
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            None,
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();

        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            None,
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info),
        );
        assert_eq!(
            overrides.non_boot_disk_overrides,
            id_ord_map! {
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
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
                    zpool_id: NON_BOOT_UUID,
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

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
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
                    zpool_id: NON_BOOT_UUID,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::ReadError(
                        dataset_not_dir_error(
                            &dir.path().join(&NON_BOOT_PATHS.install_dataset),
                        )
                        .into(),
                    ),
                },
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

        let internal_disks = make_internal_disks_rx(
            dir.path(),
            BOOT_UUID,
            &[NON_BOOT_UUID, NON_BOOT_2_UUID, NON_BOOT_3_UUID],
        )
        .current_with_boot_disk();
        let overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);
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
                    zpool_id: NON_BOOT_UUID,
                    path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootDiskReadError {
                            non_boot_disk_info: Some(info),
                        },
                    ),
                },
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_2_UUID,
                    path: dir.path().join(&NON_BOOT_2_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootDiskReadError {
                            non_boot_disk_info: None,
                        },
                    ),
                },
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_3_UUID,
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
