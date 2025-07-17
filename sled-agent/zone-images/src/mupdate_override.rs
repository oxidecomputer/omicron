// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Track MUPdate overrides within sled-agent.
//!
//! For more about commingling MUPdate and update, see RFD 556.

use std::fs;

use crate::AllInstallMetadataFiles;
use crate::InstallMetadataNonBootInfo;
use crate::InstallMetadataNonBootMismatch;
use crate::InstallMetadataNonBootResult;
use camino::Utf8PathBuf;
use iddqd::IdOrdMap;
use omicron_common::update::MupdateOverrideInfo;
use omicron_uuid_kinds::InternalZpoolUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
use sled_agent_types::zone_images::ArcIoError;
use sled_agent_types::zone_images::ClearMupdateOverrideBootError;
use sled_agent_types::zone_images::ClearMupdateOverrideBootSuccess;
use sled_agent_types::zone_images::ClearMupdateOverrideNonBootInfo;
use sled_agent_types::zone_images::ClearMupdateOverrideNonBootResult;
use sled_agent_types::zone_images::ClearMupdateOverrideResult;
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

    pub(crate) fn clear_override(
        &mut self,
        override_id: MupdateOverrideUuid,
        internal_disks: InternalDisksWithBootDisk,
    ) -> ClearMupdateOverrideResult {
        let boot_disk_result = match self.boot_disk_override.clone() {
            Ok(Some(info)) if info.mupdate_uuid == override_id => {
                // Remove the override from the boot disk (which is fallible)
                // before clearing it in-memory (which is infallible).
                match fs::remove_file(&self.boot_disk_path) {
                    Ok(()) => {
                        // Remove the in-memory override.
                        self.boot_disk_override = Ok(None);
                        Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
                    }
                    Err(error) => {
                        Err(ClearMupdateOverrideBootError::RemoveError {
                            path: self.boot_disk_path.clone(),
                            error: ArcIoError::new(error),
                        })
                    }
                }
            }
            Ok(Some(info)) => {
                // The override ID does not match the boot disk override, so we
                // shouldn't clear it.
                Err(ClearMupdateOverrideBootError::IdMismatch {
                    actual: info.mupdate_uuid,
                    provided: override_id,
                })
            }
            Ok(None) => {
                // There is no override on the boot disk, which indicates that
                // the override was cleared in a prior attempt. We accept this
                // for idempotency reasons.
                Ok(ClearMupdateOverrideBootSuccess::NoOverride)
            }
            Err(error) => {
                // If the mupdate override couldn't be read in the first place,
                // we don't have enough information to determine if it should be
                // cleared. Don't clear it.
                Err(ClearMupdateOverrideBootError::ReadError {
                    path: self.boot_disk_path.clone(),
                    error,
                })
            }
        };

        // Iterate over non-boot disks, clearing overrides if they exist.
        let mut non_boot_disk_info = IdOrdMap::new();
        for zpool_id in internal_disks.non_boot_disk_zpool_ids() {
            let (path, clear_result) =
                match self.non_boot_disk_overrides.get_mut(&zpool_id) {
                    Some(mut info) => {
                        let (new_result, clear_result) =
                            clear_non_boot_disk(&boot_disk_result, &info);
                        info.result = new_result;
                        (Some(info.path.clone()), clear_result)
                    }
                    None => {
                        // No status was found on the non-boot disk.
                        (None, ClearMupdateOverrideNonBootResult::NoStatus)
                    }
                };
            non_boot_disk_info
                .insert_unique(ClearMupdateOverrideNonBootInfo {
                    zpool_id,
                    path,
                    result: clear_result,
                })
                .expect("non-boot zpool IDs should be unique");
        }

        // Are there any non-boot disks that were originally read at startup but
        // are missing from InternalDisksWithBootDisk?
        for mut non_boot_disk_override in &mut self.non_boot_disk_overrides {
            if !non_boot_disk_info
                .contains_key(&non_boot_disk_override.zpool_id)
            {
                // If the boot disk was successfully cleared, we may have
                // introduced a mismatch.
                if let Ok(ClearMupdateOverrideBootSuccess::Cleared(
                    boot_disk_info,
                )) = &boot_disk_result
                {
                    let new_result = match &non_boot_disk_override.result {
                        MupdateOverrideNonBootResult::MatchesPresent => {
                            MupdateOverrideNonBootResult::Mismatch(
                                MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                                    non_boot_disk_info: boot_disk_info.clone(),
                                },
                            )
                        }
                        MupdateOverrideNonBootResult::MatchesAbsent => {
                            unreachable!(
                                "boot disk absent means that \
                                 boot_disk_result is always an error"
                            )
                        }
                        MupdateOverrideNonBootResult::Mismatch(
                            MupdateOverrideNonBootMismatch::BootPresentOtherAbsent
                        ) => {
                            // The mupdate override file is now absent from both
                            // the boot and the non-boot disk, so this goes from
                            // mismatch to MatchesAbsent.
                            MupdateOverrideNonBootResult::MatchesAbsent
                        }
                        MupdateOverrideNonBootResult::Mismatch(
                            MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                                ..
                            },
                        ) => {
                            unreachable!(
                                "boot disk absent means that \
                                 boot_disk_result is always an error"
                            )
                        }
                        MupdateOverrideNonBootResult::Mismatch(
                            MupdateOverrideNonBootMismatch::ValueMismatch { non_boot_disk_info },
                        ) => {
                            // Goes to BootAbsentOtherPresent.
                            MupdateOverrideNonBootResult::Mismatch(
                                MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                                    non_boot_disk_info: non_boot_disk_info.clone(),
                                }
                            )
                        }
                        MupdateOverrideNonBootResult::Mismatch(
                            MupdateOverrideNonBootMismatch::BootDiskReadError { .. },
                        ) => {
                            unreachable!(
                                "boot disk read error means that \
                                 boot_disk_result is always an error"
                            )
                        }
                        MupdateOverrideNonBootResult::ReadError(_)
                         => {
                             non_boot_disk_override.result.clone()
                        }
                    };

                    non_boot_disk_override.result = new_result;
                }
                non_boot_disk_info
                    .insert_unique(ClearMupdateOverrideNonBootInfo {
                        zpool_id: non_boot_disk_override.zpool_id,
                        path: Some(non_boot_disk_override.path.clone()),
                        result: ClearMupdateOverrideNonBootResult::DiskMissing,
                    })
                    .expect("non-boot zpool IDs should be unique");
            }
        }

        ClearMupdateOverrideResult {
            boot_disk_path: self.boot_disk_path.clone(),
            boot_disk_result,
            non_boot_disk_info,
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

fn clear_non_boot_disk(
    boot_disk_result: &Result<
        ClearMupdateOverrideBootSuccess,
        ClearMupdateOverrideBootError,
    >,
    info: &MupdateOverrideNonBootInfo,
) -> (MupdateOverrideNonBootResult, ClearMupdateOverrideNonBootResult) {
    match &info.result {
        MupdateOverrideNonBootResult::MatchesPresent => {
            if boot_disk_result.is_ok() {
                // Try removing the file.
                remove_non_boot_file(info)
            } else {
                // There was an error removing the boot disk, so don't alter the
                // non-boot disk.
                (
                    info.result.clone(),
                    ClearMupdateOverrideNonBootResult::BootDiskError,
                )
            }
        }
        MupdateOverrideNonBootResult::MatchesAbsent => {
            // No action needed -- the status stays the same.
            (info.result.clone(), ClearMupdateOverrideNonBootResult::NoOverride)
        }
        MupdateOverrideNonBootResult::Mismatch(
            MupdateOverrideNonBootMismatch::BootPresentOtherAbsent,
        ) => {
            // The file doesn't exist on disk, so the clear result is always
            // NoOverride. The new result depends on whether the override was
            // cleared on the boot disk.
            let new_result = if boot_disk_result.is_ok() {
                MupdateOverrideNonBootResult::MatchesAbsent
            } else {
                info.result.clone()
            };
            let clear_result = ClearMupdateOverrideNonBootResult::NoOverride;
            (new_result, clear_result)
        }
        MupdateOverrideNonBootResult::Mismatch(
            MupdateOverrideNonBootMismatch::BootAbsentOtherPresent { .. },
        ) => {
            let success = boot_disk_result.as_ref().expect(
                "boot disk override being absent is a success condition",
            );
            assert!(
                matches!(success, ClearMupdateOverrideBootSuccess::NoOverride),
                "success condition should be NoOverride, \
                 but instead was {success:?}"
            );

            // Always remove the mupdate override on the non-boot disk. Since
            // the boot disk is always authoritative, this is a chance for us to
            // bring the non-boot disk into alignment.
            remove_non_boot_file(info)
        }
        MupdateOverrideNonBootResult::Mismatch(
            MupdateOverrideNonBootMismatch::ValueMismatch { .. },
        ) => {
            if boot_disk_result.is_ok() {
                // Clear out the mupdate override.
                remove_non_boot_file(info)
            } else {
                // There was an error removing the boot disk, so don't alter the
                // non-boot disk.
                (
                    info.result.clone(),
                    ClearMupdateOverrideNonBootResult::BootDiskError,
                )
            }
        }
        MupdateOverrideNonBootResult::Mismatch(
            MupdateOverrideNonBootMismatch::BootDiskReadError { .. },
        ) => {
            // There was an error reading the boot disk's mupdate override. The
            // safest thing to do is to not alter the non-boot disk.
            (
                info.result.clone(),
                ClearMupdateOverrideNonBootResult::BootDiskError,
            )
        }
        MupdateOverrideNonBootResult::ReadError(error) => {
            // Don't alter the non-boot disk on error.
            (
                info.result.clone(),
                ClearMupdateOverrideNonBootResult::ReadError {
                    path: info.path.clone(),
                    error: error.clone(),
                },
            )
        }
    }
}

/// Removes a non-boot mupdate override file, assuming that the boot mupdate
/// override file was successfully removed and that the non-boot file currently
/// exists.
fn remove_non_boot_file(
    info: &MupdateOverrideNonBootInfo,
) -> (MupdateOverrideNonBootResult, ClearMupdateOverrideNonBootResult) {
    match fs::remove_file(&info.path) {
        Ok(()) => {
            // The new status is now MatchesAbsent.
            let new_result = MupdateOverrideNonBootResult::MatchesAbsent;
            let clear_result = ClearMupdateOverrideNonBootResult::Cleared {
                prev_result: info.result.clone(),
            };
            (new_result, clear_result)
        }
        Err(error) => {
            let new_result = info.result.clone();
            let clear_result = ClearMupdateOverrideNonBootResult::RemoveError {
                path: info.path.clone(),
                error: ArcIoError::new(error),
            };
            (new_result, clear_result)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    use crate::test_utils::make_internal_disks_rx;

    use camino::Utf8Path;
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

    /// Test clearing the override with the boot disk being successful and the
    /// non-boot disk being MatchesPresent (the most common case).
    #[test]
    fn clear_boot_success_non_boot_matches_present() {
        let logctx = LogContext::new(
            "mupdate_override_clear_boot_success_non_boot_matches_present",
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
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        let result =
            overrides.clear_override(info.mupdate_uuid, internal_disks);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
        );

        // The non-boot disk should be cleared too.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::Cleared {
                        prev_result: MupdateOverrideNonBootResult::MatchesPresent,
                    },
                }
            }
        );

        // Verify that both files were removed.
        assert!(!dir.child(&BOOT_PATHS.mupdate_override_json).exists());
        assert!(!dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with an ID mismatch on the boot disk and the
    /// non-boot disk being MatchesPresent.
    #[test]
    fn clear_boot_mismatch_non_boot_matches_present() {
        let logctx = LogContext::new(
            "mupdate_override_clear_boot_mismatch_non_boot_matches_present",
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
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // Use a different UUID to cause an error clearing the override.
        let other_uuid = MupdateOverrideUuid::new_v4();
        let result = overrides.clear_override(other_uuid, internal_disks);

        // The boot disk should get an ID mismatch error.
        assert_eq!(
            result.boot_disk_result,
            Err(ClearMupdateOverrideBootError::IdMismatch {
                actual: info.mupdate_uuid,
                provided: other_uuid,
            })
        );

        // The non-boot disk should not be altered because clearing the ID
        // failed on the boot disk.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::BootDiskError,
                }
            }
        );

        // Verify that both files still exist.
        assert!(dir.child(&BOOT_PATHS.mupdate_override_json).exists());
        assert!(dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with non-boot disk being MatchesAbsent.
    #[test]
    fn clear_non_boot_matches_absent() {
        let logctx = LogContext::new(
            "mupdate_override_clear_non_boot_matches_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create the non-boot directory, but do not create an override file.
        dir.child(&NON_BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        let result =
            overrides.clear_override(info.mupdate_uuid, internal_disks);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
        );

        // The non-boot disk should remain MatchesAbsent with a NoOverride
        // result.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::NoOverride,
                }
            }
        );

        logctx.cleanup_successful();
    }

    /// Test clearing the override with the boot disk being successful, and the
    /// non-boot disk being BootPresentOtherAbsent.
    #[test]
    fn clear_boot_success_non_boot_boot_present_other_absent() {
        let logctx = LogContext::new(
            "mupdate_override_clear_boot_success_non_boot_boot_present_other_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create non-boot directory but no override file.
        dir.child(&NON_BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // First, verify we have the expected mismatch state.
        assert_eq!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::Mismatch(
                MupdateOverrideNonBootMismatch::BootPresentOtherAbsent
            )
        );

        let result =
            overrides.clear_override(info.mupdate_uuid, internal_disks);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
        );

        // The non-boot disk should transition to MatchesAbsent.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::NoOverride,
                }
            }
        );

        // Verify that the non-boot disk state was updated to MatchesAbsent.
        assert_eq!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::MatchesAbsent
        );

        logctx.cleanup_successful();
    }

    /// Test clearing the override with an ID mismatch on the boot disk, and
    /// non-boot disk being BootPresentOtherAbsent.
    #[test]
    fn clear_boot_mismatch_non_boot_boot_present_other_absent() {
        let logctx = LogContext::new(
            "mupdate_override_clear_boot_mismatch_non_boot_boot_present_other_absent",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create non-boot directory but no override file.
        dir.child(&NON_BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // Use a different UUID to cause a mismatch.
        let other_uuid = MupdateOverrideUuid::new_v4();
        let result = overrides.clear_override(other_uuid, internal_disks);

        // The boot disk should get an ID mismatch error.
        assert_eq!(
            result.boot_disk_result,
            Err(ClearMupdateOverrideBootError::IdMismatch {
                actual: info.mupdate_uuid,
                provided: other_uuid,
            })
        );

        // The non-boot disk should remain in the mismatched state.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::NoOverride,
                }
            }
        );

        // Verify that the non-boot disk state remained unchanged.
        assert_eq!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::Mismatch(
                MupdateOverrideNonBootMismatch::BootPresentOtherAbsent
            )
        );

        logctx.cleanup_successful();
    }

    /// Test clearing the override with a BootAbsentOtherPresent mismatch
    /// (always clears the non-boot disk).
    #[test]
    fn clear_non_boot_boot_absent_other_present() {
        let logctx = LogContext::new(
            "mupdate_override_clear_non_boot_boot_absent_other_present",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();

        // Create the boot directory without an override file.
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // First, verify we have the expected mismatch state.
        assert_eq!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::Mismatch(
                MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info: info.clone()
                }
            )
        );

        let some_uuid = MupdateOverrideUuid::new_v4();
        let result = overrides.clear_override(some_uuid, internal_disks);

        // The boot disk should return NoOverride.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::NoOverride)
        );

        // The non-boot disk should always be cleared in this case, even though
        // there's an ID mismatch.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::Cleared {
                        prev_result: MupdateOverrideNonBootResult::Mismatch(
                            MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                                non_boot_disk_info: info
                            }
                        ),
                    },
                }
            }
        );

        // Verify that the non-boot file was removed.
        assert!(!dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with the boot disk being successful and the
    /// non-boot disk being ValueMismatch.
    #[test]
    fn clear_boot_success_non_boot_value_mismatch() {
        let logctx = LogContext::new(
            "mupdate_override_clear_boot_success_non_boot_value_mismatch",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create another context for the non-boot disk.
        let cx2 = WriteInstallDatasetContext::new_basic();
        let info2 = cx2.override_info();
        cx2.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // First, verify we have the expected mismatch state.
        assert_eq!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::Mismatch(
                MupdateOverrideNonBootMismatch::ValueMismatch {
                    non_boot_disk_info: info2.clone()
                }
            )
        );

        let result =
            overrides.clear_override(info.mupdate_uuid, internal_disks);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
        );

        // The non-boot disk should be cleared too.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::Cleared {
                        prev_result: MupdateOverrideNonBootResult::Mismatch(
                            MupdateOverrideNonBootMismatch::ValueMismatch {
                                non_boot_disk_info: info2
                            }
                        ),
                    },
                }
            }
        );

        // Verify that both files were removed.
        assert!(!dir.child(&BOOT_PATHS.mupdate_override_json).exists());
        assert!(!dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with an ID mismatch on the boot disk, and a
    /// ValueMismatch on the non-boot disk.
    #[test]
    fn clear_boot_error_non_boot_value_mismatch() {
        let logctx = LogContext::new(
            "mupdate_override_clear_boot_error_non_boot_value_mismatch",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create another context for the non-boot disk.
        let cx2 = WriteInstallDatasetContext::new_basic();
        cx2.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // Use a different UUID to cause a mismatch on the boot disk.
        let other_uuid = MupdateOverrideUuid::new_v4();
        let result = overrides.clear_override(other_uuid, internal_disks);

        // The boot disk should get an ID mismatch error.
        assert_eq!(
            result.boot_disk_result,
            Err(ClearMupdateOverrideBootError::IdMismatch {
                actual: info.mupdate_uuid,
                provided: other_uuid,
            })
        );

        // The non-boot disk should not be altered due to the boot disk
        // mismatch.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::BootDiskError,
                }
            }
        );

        // Verify that both files still exist.
        assert!(dir.child(&BOOT_PATHS.mupdate_override_json).exists());
        assert!(dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with a boot disk read error.
    #[test]
    fn clear_non_boot_boot_disk_read_error() {
        let logctx = LogContext::new(
            "mupdate_override_clear_non_boot_boot_disk_read_error",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();

        // Create an empty boot file (read error) and a valid non-boot file.
        dir.child(&BOOT_PATHS.mupdate_override_json).touch().unwrap();
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // First, verify we have the expected mismatch state.
        assert!(matches!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::Mismatch(
                MupdateOverrideNonBootMismatch::BootDiskReadError { .. }
            )
        ));

        let other_uuid = MupdateOverrideUuid::new_v4();
        let result = overrides.clear_override(other_uuid, internal_disks);

        // The boot disk should return a read error.
        assert!(matches!(
            result.boot_disk_result,
            Err(ClearMupdateOverrideBootError::ReadError { .. })
        ));

        // The non-boot disk should not be altered due to the boot disk read
        // error.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::BootDiskError,
                }
            }
        );

        // Verify that the non-boot file still exists.
        assert!(dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with a non-boot disk read error.
    #[test]
    fn clear_non_boot_read_error() {
        let logctx = LogContext::new(
            "mupdate_override_clear_non_boot_read_error",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Create an empty non-boot file (read error).
        dir.child(&NON_BOOT_PATHS.mupdate_override_json).touch().unwrap();

        let internal_disks =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // First, verify we have the expected read error state.
        assert!(matches!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::ReadError(_)
        ));

        let result =
            overrides.clear_override(info.mupdate_uuid, internal_disks);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
        );

        // The non-boot disk should not be altered due to a read error on the
        // disk.
        let expected_error = deserialize_error(
            dir.path(),
            &NON_BOOT_PATHS.mupdate_override_json,
            "",
        )
        .into();
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::ReadError {
                        path: dir.path().join(&NON_BOOT_PATHS.mupdate_override_json),
                        error: expected_error,
                    },
                }
            }
        );

        // Verify that the boot file was removed but that the non-boot file
        // still exists.
        assert!(!dir.child(&BOOT_PATHS.mupdate_override_json).exists());
        assert!(dir.child(&NON_BOOT_PATHS.mupdate_override_json).exists());

        logctx.cleanup_successful();
    }

    /// Test clearing the override with the non-boot disk missing from
    /// InternalDisksWithBootDisk.
    #[test]
    fn clear_non_boot_disk_missing() {
        let logctx = LogContext::new(
            "mupdate_override_clear_non_boot_disk_missing",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        cx.write_to(&dir.child(&NON_BOOT_PATHS.install_dataset)).unwrap();

        // Build the `AllMupdateOverrides` with both disks present.
        let internal_disks_with_both =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let mut overrides = AllMupdateOverrides::read_all(
            &logctx.log,
            &internal_disks_with_both,
        );

        // Clear the override with only the boot disk present.
        let internal_disks_boot_only =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[])
                .current_with_boot_disk();
        let result = overrides
            .clear_override(info.mupdate_uuid, internal_disks_boot_only);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info.clone()))
        );

        // The non-boot disk should return a DiskMissing result.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: Some(dir.path().join(&NON_BOOT_PATHS.mupdate_override_json)),
                    result: ClearMupdateOverrideNonBootResult::DiskMissing,
                }
            }
        );

        // Verify that the non-boot disk override was updated accordingly.
        assert_eq!(
            overrides
                .non_boot_disk_overrides
                .get(&NON_BOOT_UUID)
                .unwrap()
                .result,
            MupdateOverrideNonBootResult::Mismatch(
                MupdateOverrideNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info: info
                }
            )
        );

        logctx.cleanup_successful();
    }

    /// Test clearing the override with no status for non-boot disk.
    #[test]
    fn clear_no_status() {
        let logctx = LogContext::new(
            "mupdate_override_clear_no_status",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        // Build the `AllMupdateOverrides` with just the boot disk present.
        let internal_disks_boot_only =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[])
                .current_with_boot_disk();
        let mut overrides = AllMupdateOverrides::read_all(
            &logctx.log,
            &internal_disks_boot_only,
        );

        // Clear the override with the non-boot disk now present.
        let internal_disks_with_both =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[NON_BOOT_UUID])
                .current_with_boot_disk();
        let result = overrides
            .clear_override(info.mupdate_uuid, internal_disks_with_both);

        // The boot disk should be cleared.
        assert_eq!(
            result.boot_disk_result,
            Ok(ClearMupdateOverrideBootSuccess::Cleared(info))
        );

        // The non-boot disk should return a NoStatus result.
        assert_eq!(
            result.non_boot_disk_info,
            id_ord_map! {
                ClearMupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: None,
                    result: ClearMupdateOverrideNonBootResult::NoStatus,
                }
            }
        );

        logctx.cleanup_successful();
    }

    /// Test clearing the override with an IO error removing the file.
    #[test]
    fn clear_file_io_error() {
        let logctx = LogContext::new(
            "mupdate_override_clear_file_removal_error",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let info = cx.override_info();
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks = make_internal_disks_rx(dir.path(), BOOT_UUID, &[])
            .current_with_boot_disk();
        let mut overrides =
            AllMupdateOverrides::read_all(&logctx.log, &internal_disks);

        // Make the parent directory read-only to prevent removing files from
        // inside it.
        let mut guard =
            ReadOnlyDirDropGuard::new(&dir.child(&BOOT_PATHS.install_dataset))
                .unwrap();

        let result =
            overrides.clear_override(info.mupdate_uuid, internal_disks);

        // The boot disk should return a RemoveError.
        assert!(matches!(
            result.boot_disk_result,
            Err(ClearMupdateOverrideBootError::RemoveError { .. })
        ));
        assert_eq!(result.non_boot_disk_info, IdOrdMap::new());

        // Verify in-memory state was not updated.
        assert_eq!(
            overrides.boot_disk_override.as_ref().unwrap().as_ref(),
            Some(&info)
        );

        guard.finish().unwrap();

        logctx.cleanup_successful();
    }

    /// A drop guard to help set up read-only permissions for a directory, and
    /// to clean them up regardless of whether the test panics.
    #[derive(Debug)]
    struct ReadOnlyDirDropGuard {
        path: Utf8PathBuf,
        finished: bool,
    }

    impl ReadOnlyDirDropGuard {
        fn new(path: &Utf8Path) -> io::Result<Self> {
            fs::set_permissions(path, fs::Permissions::from_mode(0o555))?;

            Ok(Self { path: path.to_owned(), finished: false })
        }

        fn finish(&mut self) -> io::Result<()> {
            if self.finished {
                return Ok(());
            }
            self.finished = true;
            fs::set_permissions(&self.path, fs::Permissions::from_mode(0o755))
        }
    }

    impl Drop for ReadOnlyDirDropGuard {
        fn drop(&mut self) {
            // Avoid a double-panic here -- instead, just log a failure.
            if let Err(error) = self.finish() {
                eprintln!(
                    "failed to clean up permissions for {}: {error}",
                    self.path,
                );
            }
        }
    }
}
