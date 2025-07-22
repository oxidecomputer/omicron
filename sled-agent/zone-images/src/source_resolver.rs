// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::RAMDISK_IMAGE_PATH;
use crate::install_dataset_file_name;
use crate::mupdate_override::AllMupdateOverrides;
use crate::ramdisk_file_source;
use crate::zone_manifest::AllZoneManifests;
use camino::Utf8PathBuf;
use illumos_utils::running_zone::ZoneImageFileSource;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use sled_agent_config_reconciler::InternalDisks;
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
use sled_agent_types::zone_images::MupdateOverrideReadError;
use sled_agent_types::zone_images::ResolverStatus;
use slog::error;
use slog::o;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::sync::Mutex;

/// A zone image source.
#[derive(Clone, Debug)]
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

        ResolverStatus { mupdate_override, zone_manifest }
    }

    /// Returns a [`ZoneImageFileSource`] consisting of the file name, plus a
    /// list of potential paths to search, for a zone image.
    pub fn file_source_for(
        &self,
        zone_type: &str,
        image_source: &ZoneImageSource,
        internal_disks: InternalDisks,
    ) -> Result<ZoneImageFileSource, MupdateOverrideReadError> {
        match image_source {
            ZoneImageSource::Ramdisk => {
                // RAM disk images are always stored on the RAM disk path.
                Ok(ramdisk_file_source(zone_type))
            }
            ZoneImageSource::Omicron(image_source) => {
                let inner = self.inner.lock().unwrap();
                inner.file_source_for(zone_type, image_source, internal_disks)
            }
        }
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

    fn file_source_for(
        &self,
        zone_type: &str,
        image_source: &OmicronZoneImageSource,
        internal_disks: InternalDisks,
    ) -> Result<ZoneImageFileSource, MupdateOverrideReadError> {
        match image_source {
            OmicronZoneImageSource::InstallDataset => {
                let file_name = install_dataset_file_name(zone_type);
                // Look for the image in the RAM disk first. Note that install
                // dataset images are not stored on the RAM disk in production,
                // just in development or test workflows.
                let mut zone_image_paths =
                    vec![Utf8PathBuf::from(RAMDISK_IMAGE_PATH)];

                // Inject an image path if requested by a test.
                if let Some(path) = &self.image_directory_override {
                    zone_image_paths.push(path.clone());
                };

                // Any zones not part of the RAM disk are managed via the
                // zone manifest.
                //
                // XXX: we ask for the boot zpool to be passed in here. But
                // `AllZoneImages` also caches the boot zpool. How should we
                // reconcile the two?
                if let Some(path) = internal_disks.boot_disk_install_dataset() {
                    match self.zone_manifests.boot_disk_result() {
                        Ok(result) => {
                            match result.data.get(file_name.as_str()) {
                                Some(result) => {
                                    if result.is_valid() {
                                        zone_image_paths.push(path);
                                    } else {
                                        // If the zone is not valid, we refuse to start
                                        // it.
                                        error!(
                                            self.log,
                                            "zone {} is not valid in the zone manifest, \
                                             not returning it as a source",
                                            file_name;
                                            "error" => %result.display()
                                        );
                                    }
                                }
                                None => {
                                    error!(
                                        self.log,
                                        "zone {} is not present in the boot disk zone manifest",
                                        file_name,
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            error!(
                                self.log,
                                "error parsing boot disk zone manifest, not returning \
                                 install dataset as a source";
                                "error" => InlineErrorChain::new(error),
                            );
                        }
                    }
                }

                Ok(ZoneImageFileSource {
                    file_name,
                    search_paths: zone_image_paths,
                })
            }
            OmicronZoneImageSource::Artifact { hash } => {
                // TODO: implement mupdate override here.
                //
                // Search both artifact datasets. This iterator starts with the
                // dataset for the boot disk (if it exists), and then is followed
                // by all other disks.
                let search_paths =
                    internal_disks.all_artifact_datasets().collect();
                Ok(ZoneImageFileSource {
                    file_name: hash.to_string(),
                    search_paths,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::make_internal_disks_rx;

    use camino_tempfile_ext::prelude::*;
    use dropshot::{ConfigLogging, ConfigLoggingLevel, test_util::LogContext};
    use sled_agent_zone_images_examples::{
        BOOT_PATHS, BOOT_UUID, WriteInstallDatasetContext,
    };

    /// Test source resolver behavior when the zone manifest is invalid.
    #[test]
    fn file_source_zone_manifest_invalid() {
        let logctx = LogContext::new(
            "source_resolver_file_source_zone_manifest_invalid",
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

        // RAM disk image sources should work as expected.
        let ramdisk_source = resolver
            .file_source_for(
                "zone1",
                &ZoneImageSource::Ramdisk,
                internal_disks_rx.current(),
            )
            .unwrap();
        assert_eq!(ramdisk_source, ramdisk_file_source("zone1"));

        let file_source = resolver
            .file_source_for(
                "zone1",
                &ZoneImageSource::Omicron(
                    OmicronZoneImageSource::InstallDataset,
                ),
                internal_disks_rx.current(),
            )
            .unwrap();

        // Because the zone manifest is missing, the file source should not
        // return the install dataset.
        assert_eq!(
            file_source,
            ZoneImageFileSource {
                file_name: install_dataset_file_name("zone1"),
                search_paths: vec![Utf8PathBuf::from(RAMDISK_IMAGE_PATH)]
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
        cx.make_error_cases();

        cx.write_to(&dir.child(&BOOT_PATHS.install_dataset)).unwrap();

        let internal_disks_rx =
            make_internal_disks_rx(dir.path(), BOOT_UUID, &[]);
        let resolver = ZoneImageSourceResolver::new(
            &logctx.log,
            internal_disks_rx.current_with_boot_disk(),
        );

        // The resolver should not fail for ramdisk images.
        let file_source = resolver
            .file_source_for(
                "fake-zone",
                &ZoneImageSource::Ramdisk,
                internal_disks_rx.current(),
            )
            .unwrap();
        assert_eq!(file_source, ramdisk_file_source("fake-zone"));

        // zone1.tar.gz is valid.
        let file_source = resolver
            .file_source_for(
                "zone1",
                &ZoneImageSource::Omicron(
                    OmicronZoneImageSource::InstallDataset,
                ),
                internal_disks_rx.current(),
            )
            .unwrap();
        assert_eq!(
            file_source,
            ZoneImageFileSource {
                file_name: "zone1.tar.gz".to_string(),
                search_paths: vec![
                    Utf8PathBuf::from(RAMDISK_IMAGE_PATH),
                    dir.path().join(&BOOT_PATHS.install_dataset)
                ]
            },
        );

        // zone2, zone3, zone4 and zone5 aren't valid, and none of them will
        // return the install dataset path.
        for zone_name in ["zone2", "zone3", "zone4", "zone5"] {
            let file_source = resolver
                .file_source_for(
                    zone_name,
                    &ZoneImageSource::Omicron(
                        OmicronZoneImageSource::InstallDataset,
                    ),
                    internal_disks_rx.current(),
                )
                .unwrap();
            assert_eq!(
                file_source,
                ZoneImageFileSource {
                    file_name: install_dataset_file_name(zone_name),
                    search_paths: vec![Utf8PathBuf::from(RAMDISK_IMAGE_PATH)]
                }
            );
        }

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
