// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::mupdate_override::AllMupdateOverrides;
use crate::zone_manifest::AllZoneManifests;
use camino::Utf8PathBuf;
use omicron_uuid_kinds::MupdateOverrideUuid;
use sled_agent_config_reconciler::InternalDisks;
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
use sled_agent_types::zone_images::RemoveMupdateOverrideResult;
use sled_agent_types::zone_images::ResolverStatus;
use sled_agent_types_migrations::latest::inventory::OmicronZoneImageSource;
use slog::o;
use std::sync::Arc;
use std::sync::Mutex;

/// A zone image source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZoneImageSource {
    /// An Omicron zone.
    Omicron(OmicronZoneImageSource),

    /// A RAM disk-based zone.
    Ramdisk,
}

/// Resolves [`OmicronZoneImageSource`] instances into file names and search
/// paths.
///
/// This is cheaply cloneable.
#[derive(Clone)]
pub struct ZoneImageSourceResolver {
    // Inner state, guarded by a mutex.
    inner: Arc<Mutex<ResolverInner>>,
}

impl ZoneImageSourceResolver {
    /// Creates a new `ZoneImageSourceResolver`.
    pub fn new(
        log: &slog::Logger,
        internal_disks: InternalDisksWithBootDisk,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ResolverInner::new(
                log,
                internal_disks,
            ))),
        }
    }

    /// Returns current information about resolver status and health.
    pub fn status(&self) -> ResolverStatus {
        let inner = self.inner.lock().unwrap();
        let zone_manifest = inner.zone_manifests.status();
        let mupdate_override = inner.mupdate_overrides.status();
        let image_directory_override = inner.image_directory_override.clone();

        ResolverStatus {
            mupdate_override,
            zone_manifest,
            image_directory_override,
        }
    }

    /// Removes the mupdate override field and files on disk.
    pub fn remove_mupdate_override(
        &self,
        override_id: MupdateOverrideUuid,
        internal_disks: &InternalDisks,
    ) -> RemoveMupdateOverrideResult {
        let mut inner = self.inner.lock().unwrap();
        let ret =
            inner.mupdate_overrides.clear_override(override_id, internal_disks);

        ret.log_to(&inner.log);
        ret
    }
}

#[derive(Debug)]
struct ResolverInner {
    log: slog::Logger,
    image_directory_override: Option<Utf8PathBuf>,
    // Store all collected information for zones -- we're going to need to
    // report this via inventory.
    zone_manifests: AllZoneManifests,
    // Store all collected information for mupdate overrides -- we're going to
    // need to report this via inventory.
    mupdate_overrides: AllMupdateOverrides,
}

impl ResolverInner {
    fn new(
        log: &slog::Logger,
        internal_disks: InternalDisksWithBootDisk,
    ) -> Self {
        let log = log.new(o!("component" => "ZoneImageSourceResolver"));

        let zone_manifests = AllZoneManifests::read_all(&log, &internal_disks);
        let mupdate_overrides =
            AllMupdateOverrides::read_all(&log, &internal_disks);

        Self {
            log,
            image_directory_override: None,
            zone_manifests,
            mupdate_overrides,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::make_internal_disks_rx;

    use camino_tempfile_ext::prelude::*;
    use dropshot::{ConfigLogging, ConfigLoggingLevel, test_util::LogContext};
    use omicron_common::zone_images::ZoneImageFileSource;
    use sled_agent_config_reconciler::{
        HostPhase2PreparedContents, ResolverStatusExt,
    };
    use sled_agent_types::zone_images::{
        MupdateOverrideReadError, OmicronZoneFileSource,
        OmicronZoneImageLocation, RAMDISK_IMAGE_PATH, ZoneImageLocationError,
        ZoneManifestReadError, ZoneManifestZoneHashError,
    };
    use sled_agent_types_migrations::latest::inventory::{
        HostPhase2DesiredContents, ZoneKind,
    };
    use sled_agent_zone_images_examples::{
        BOOT_PATHS, BOOT_UUID, WriteInstallDatasetContext, deserialize_error,
    };
    use tufaceous_artifact::ArtifactHash;

    /// Test source resolver behavior when the zone manifest is missing.
    #[test]
    fn file_source_zone_manifest_invalid() {
        let logctx = LogContext::new(
            "source_resolver_file_source_zone_manifest_missing",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();

        // Create an empty non-boot file (read error).
        dir.child(&BOOT_PATHS.zones_json).touch().unwrap();

        let internal_disks_rx =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[]);
        let resolver = ZoneImageSourceResolver::new(
            &logctx.log,
            internal_disks_rx.current_with_boot_disk(),
        );
        let status = resolver.status();

        let file_source = status.omicron_file_source(
            &logctx.log,
            ZoneKind::CockroachDb,
            &OmicronZoneImageSource::InstallDataset,
            &internal_disks_rx.current(),
        );

        // Because the zone manifest is invalid, the file source should not
        // return the install dataset.
        assert_eq!(
            file_source,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    hash: Err(ZoneImageLocationError::ZoneHash(
                        ZoneManifestZoneHashError::ReadBootDisk(
                            ZoneManifestReadError::InstallMetadata(
                                deserialize_error(
                                    dir.path(),
                                    &BOOT_PATHS.zones_json,
                                    "",
                                ),
                            ),
                        ),
                    )),
                },
                file_source: ZoneImageFileSource {
                    file_name: ZoneKind::CockroachDb
                        .artifact_in_install_dataset()
                        .to_owned(),
                    search_paths: vec![Utf8PathBuf::from(RAMDISK_IMAGE_PATH)],
                },
            }
        );

        logctx.cleanup_successful();
    }

    /// Test source resolver behavior when the zone manifest detects errors.
    #[test]
    fn file_source_with_errors() {
        let logctx = LogContext::new(
            "source_resolver_file_source_with_errors",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        let mut cx = WriteInstallDatasetContext::new_basic();
        let errors = cx.make_error_cases();

        let manifest = cx.zone_manifest();

        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks_rx =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[]);
        let resolver = ZoneImageSourceResolver::new(
            &logctx.log,
            internal_disks_rx.current_with_boot_disk(),
        );

        let status = resolver.status();

        // The cockroach zone is valid.
        let file_source = status.omicron_file_source(
            &logctx.log,
            ZoneKind::CockroachDb,
            &OmicronZoneImageSource::InstallDataset,
            &internal_disks_rx.current(),
        );
        assert_eq!(
            file_source,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    hash: Ok(manifest
                        .zones
                        .get(
                            ZoneKind::CockroachDb.artifact_in_install_dataset()
                        )
                        .unwrap()
                        .hash)
                },
                file_source: ZoneImageFileSource {
                    file_name: ZoneKind::CockroachDb
                        .artifact_in_install_dataset()
                        .to_string(),
                    search_paths: vec![
                        dir.path().join(&BOOT_PATHS.install_dataset),
                        Utf8PathBuf::from(RAMDISK_IMAGE_PATH),
                    ]
                },
            },
        );

        // Clickhouse, Crucible, InternalDns and Nexus aren't valid, and none of
        // them will return the install dataset path.
        for zone_kind in [
            ZoneKind::Clickhouse,
            ZoneKind::Crucible,
            ZoneKind::InternalDns,
            ZoneKind::Nexus,
        ] {
            let error = errors.get(&zone_kind).unwrap();
            let file_source = status.omicron_file_source(
                &logctx.log,
                zone_kind,
                &OmicronZoneImageSource::InstallDataset,
                &internal_disks_rx.current(),
            );
            assert_eq!(
                file_source,
                OmicronZoneFileSource {
                    location: OmicronZoneImageLocation::InstallDataset {
                        hash: Err(ZoneImageLocationError::ZoneHash(
                            error.error.clone()
                        )),
                    },
                    file_source: ZoneImageFileSource {
                        file_name: zone_kind
                            .artifact_in_install_dataset()
                            .to_owned(),
                        search_paths: vec![Utf8PathBuf::from(
                            RAMDISK_IMAGE_PATH
                        )]
                    },
                }
            );
        }

        logctx.cleanup_successful();
    }

    /// Test file source and prepare behavior when there's a mupdate override in
    /// place.
    #[test]
    fn lookup_with_mupdate_override() {
        let logctx = LogContext::new(
            "lookup_with_mupdate_override",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );

        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let manifest = cx.zone_manifest();

        // Write the install dataset.
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks_rx =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[]);
        let resolver = ZoneImageSourceResolver::new(
            &logctx.log,
            internal_disks_rx.current_with_boot_disk(),
        );

        let status = resolver.status();

        // Artifact should be overridden since a mupdate override is present.
        let artifact_source = OmicronZoneImageSource::Artifact {
            // The hash isn't important here.
            hash: ArtifactHash([0; 32]),
        };

        // Look up the file source.
        let file_source = status.omicron_file_source(
            &logctx.log,
            ZoneKind::CockroachDb,
            &artifact_source,
            &internal_disks_rx.current(),
        );

        assert_eq!(
            file_source,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    // The hash should be looked up from the zone manifest.
                    hash: Ok(manifest
                        .zones
                        .get(
                            ZoneKind::CockroachDb.artifact_in_install_dataset()
                        )
                        .unwrap()
                        .hash),
                },
                file_source: ZoneImageFileSource {
                    file_name: ZoneKind::CockroachDb
                        .artifact_in_install_dataset()
                        .to_owned(),
                    search_paths: vec![
                        dir.path().join(&BOOT_PATHS.install_dataset),
                    ],
                },
            }
        );

        // Look up host phase 2 contents.
        let prepared_contents = status.prepare_host_phase_2_contents(
            &logctx.log,
            &HostPhase2DesiredContents::CurrentContents,
        );
        assert_eq!(
            prepared_contents,
            HostPhase2PreparedContents::WithMupdateOverride
        );
        assert_eq!(
            prepared_contents.desired_contents(),
            &HostPhase2DesiredContents::CurrentContents
        );

        let desired =
            HostPhase2DesiredContents::Artifact { hash: ArtifactHash([2; 32]) };
        let prepared_contents =
            status.prepare_host_phase_2_contents(&logctx.log, &desired);
        assert_eq!(
            prepared_contents,
            HostPhase2PreparedContents::WithMupdateOverride,
        );
        assert_eq!(
            prepared_contents.desired_contents(),
            &HostPhase2DesiredContents::CurrentContents
        );

        logctx.cleanup_successful();
    }

    /// Test source resolver behavior when an error occurred while obtaining the
    /// mupdate override.
    #[test]
    fn lookup_with_mupdate_override_error() {
        let logctx = LogContext::new(
            "lookup_with_mupdate_override_error",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );

        let dir = Utf8TempDir::new().unwrap();
        let cx = WriteInstallDatasetContext::new_basic();
        let manifest = cx.zone_manifest();

        // Write the install dataset.
        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();
        // Introduce an error with the mupdate override JSON (this is a
        // deserialization error).
        dir.child(&BOOT_PATHS.mupdate_override_json).write_str("").unwrap();

        let internal_disks_rx =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[]);
        let resolver = ZoneImageSourceResolver::new(
            &logctx.log,
            internal_disks_rx.current_with_boot_disk(),
        );

        let status = resolver.status();

        // Artifact image sources should return an error.
        let artifact_hash = ArtifactHash([0; 32]);
        let artifact_source =
            OmicronZoneImageSource::Artifact { hash: artifact_hash };
        let file_source = status.omicron_file_source(
            &logctx.log,
            ZoneKind::CockroachDb,
            &artifact_source,
            &internal_disks_rx.current(),
        );

        assert_eq!(
            file_source,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::Artifact {
                    // The hash should be looked up from the zone manifest.
                    hash: Err(MupdateOverrideReadError::InstallMetadata(
                        deserialize_error(
                            dir.path(),
                            &BOOT_PATHS.mupdate_override_json,
                            "",
                        )
                    )),
                },
                file_source: ZoneImageFileSource {
                    file_name: artifact_hash.to_string(),
                    // The search paths will be empty, causing zone startup to
                    // fail.
                    search_paths: vec![],
                },
            }
        );

        // InstallDataset zones should not return an error because they don't
        // consider the mupdate override.
        let file_source = status.omicron_file_source(
            &logctx.log,
            ZoneKind::CockroachDb,
            &OmicronZoneImageSource::InstallDataset,
            &internal_disks_rx.current(),
        );

        assert_eq!(
            file_source,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    // The hash should be looked up from the zone manifest.
                    hash: Ok(manifest
                        .zones
                        .get(
                            ZoneKind::CockroachDb.artifact_in_install_dataset()
                        )
                        .unwrap()
                        .hash),
                },
                file_source: ZoneImageFileSource {
                    file_name: ZoneKind::CockroachDb
                        .artifact_in_install_dataset()
                        .to_owned(),
                    search_paths: vec![
                        dir.path().join(&BOOT_PATHS.install_dataset),
                        Utf8PathBuf::from(RAMDISK_IMAGE_PATH),
                    ],
                },
            }
        );

        // Look up host phase 2 contents.
        let prepared_contents = status.prepare_host_phase_2_contents(
            &logctx.log,
            &HostPhase2DesiredContents::CurrentContents,
        );
        assert_eq!(
            prepared_contents,
            HostPhase2PreparedContents::WithMupdateOverride
        );
        assert_eq!(
            prepared_contents.desired_contents(),
            &HostPhase2DesiredContents::CurrentContents
        );

        let desired =
            HostPhase2DesiredContents::Artifact { hash: ArtifactHash([2; 32]) };
        let prepared_contents =
            status.prepare_host_phase_2_contents(&logctx.log, &desired);
        assert_eq!(
            prepared_contents,
            HostPhase2PreparedContents::WithMupdateOverride,
        );
        assert_eq!(
            prepared_contents.desired_contents(),
            &HostPhase2DesiredContents::CurrentContents
        );

        logctx.cleanup_successful();
    }

    /// Test that the resolver status can be converted to inventory format.
    #[test]
    fn resolver_status_to_inventory() {
        let logctx = LogContext::new(
            "resolver_status_to_inventory",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let internal_disks_rx =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[]);
        let resolver = ZoneImageSourceResolver::new(
            &logctx.log,
            internal_disks_rx.current_with_boot_disk(),
        );

        let status = resolver.status();
        let inventory = status.to_inventory();

        // Verify the conversion works
        assert_eq!(
            inventory.zone_manifest.boot_disk_path,
            status.zone_manifest.boot_disk_path
        );
        assert_eq!(
            inventory.mupdate_override.boot_disk_path,
            status.mupdate_override.boot_disk_path
        );

        logctx.cleanup_successful();
    }
}
