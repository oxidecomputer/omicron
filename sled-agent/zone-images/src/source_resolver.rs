// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::RAMDISK_IMAGE_PATH;
use crate::install_dataset_file_name;
use crate::mupdate_override::AllMupdateOverrides;
use crate::ramdisk_file_source;
use crate::zone_manifest::AllZoneManifests;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use illumos_utils::running_zone::ZoneImageFileSource;
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::zone_images::ZoneImageResolverStatus;
use sled_storage::dataset::INSTALL_DATASET;
use sled_storage::dataset::M2_ARTIFACT_DATASET;
use slog::error;
use slog::o;
use std::sync::Arc;
use std::sync::Mutex;

/// A description of zpools to examine for zone images.
pub struct ZoneImageZpools<'a> {
    /// The root directory, typically `/`.
    pub root: &'a Utf8Path,

    /// The full set of M.2 zpools that are currently known. Must be non-empty,
    /// but it can include the boot zpool.
    pub all_m2_zpools: Vec<ZpoolName>,
}

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
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: &ZpoolName,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ResolverInner::new(
                log, zpools, boot_zpool,
            ))),
        }
    }

    /// Overrides the image directory with another one.
    ///
    /// Intended for testing.
    pub fn override_image_directory(&self, path: Utf8PathBuf) {
        self.inner.lock().unwrap().override_image_directory(path);
    }

    /// Returns current information about resolver status and health.
    pub fn status(&self) -> ZoneImageResolverStatus {
        let inner = self.inner.lock().unwrap();
        let zone_manifest = inner.zone_manifests.status();
        let mupdate_override = inner.mupdate_overrides.status();

        ZoneImageResolverStatus { mupdate_override, zone_manifest }
    }

    /// Returns a [`ZoneImageFileSource`] consisting of the file name, plus a
    /// list of potential paths to search, for a zone image.
    pub fn file_source_for(
        &self,
        zone_type: &str,
        image_source: &ZoneImageSource,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: Option<&ZpoolName>,
    ) -> Result<ZoneImageFileSource, String> {
        match image_source {
            ZoneImageSource::Ramdisk => {
                // RAM disk images are always stored on the RAM disk path.
                Ok(ramdisk_file_source(zone_type))
            }
            ZoneImageSource::Omicron(image_source) => {
                let inner = self.inner.lock().unwrap();
                inner.file_source_for(
                    zone_type,
                    image_source,
                    zpools,
                    boot_zpool,
                )
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
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: &ZpoolName,
    ) -> Self {
        let log = log.new(o!("component" => "ZoneImageSourceResolver"));

        let zone_manifests =
            AllZoneManifests::read_all(&log, zpools, boot_zpool);
        let mupdate_overrides =
            AllMupdateOverrides::read_all(&log, zpools, boot_zpool);

        Self {
            log,
            image_directory_override: None,
            zone_manifests,
            mupdate_overrides,
        }
    }

    fn override_image_directory(
        &mut self,
        image_directory_override: Utf8PathBuf,
    ) {
        if let Some(dir) = &self.image_directory_override {
            // Allow idempotent sets to the same directory -- some tests do
            // this.
            if image_directory_override != *dir {
                panic!(
                    "image_directory_override already set to `{dir}`, \
                     attempting to set it to `{image_directory_override}`"
                );
            }
        }
        self.image_directory_override = Some(image_directory_override);
    }

    fn file_source_for(
        &self,
        zone_type: &str,
        image_source: &OmicronZoneImageSource,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: Option<&ZpoolName>,
    ) -> Result<ZoneImageFileSource, String> {
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
                if let Some(boot_zpool) = boot_zpool {
                    match self.zone_manifests.boot_disk_result() {
                        Ok(result) => {
                            match result.data.get(file_name.as_str()) {
                                Some(result) => {
                                    if result.is_valid() {
                                        zone_image_paths.push(
                                            boot_zpool.dataset_mountpoint(
                                                zpools.root,
                                                INSTALL_DATASET,
                                            ),
                                        );
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
                                "error" => error,
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
                // TODO: implement mupdate override here. This will return an
                // error if the override isn't found.
                //
                // Search both artifact datasets, but look on the boot disk first.
                // This iterator starts with the zpool for the boot disk (if it
                // exists), and then is followed by all other zpools.
                let zpool_iter = boot_zpool.into_iter().chain(
                    zpools
                        .all_m2_zpools
                        .iter()
                        .filter(|zpool| Some(zpool) != boot_zpool.as_ref()),
                );
                let search_paths = zpool_iter
                    .map(|zpool| {
                        zpool.dataset_mountpoint(
                            zpools.root,
                            M2_ARTIFACT_DATASET,
                        )
                    })
                    .collect();
                Ok(ZoneImageFileSource {
                    // Images in the artifact store are named by just their
                    // hash.
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

    use crate::test_utils::{
        BOOT_PATHS, BOOT_ZPOOL, WriteInstallDatasetContext,
    };

    use camino_tempfile_ext::prelude::*;
    use dropshot::{ConfigLogging, ConfigLoggingLevel, test_util::LogContext};

    /// Test source resolver behavior when the zone manifest is invalid.
    #[test]
    fn file_source_zone_manifest_invalid() {
        let logctx = LogContext::new(
            "source_resolver_file_source_zone_manifest_invalid",
            &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        );
        let dir = Utf8TempDir::new().unwrap();
        dir.child(&BOOT_PATHS.install_dataset).create_dir_all().unwrap();

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL],
        };
        let resolver =
            ZoneImageSourceResolver::new(&logctx.log, &zpools, &BOOT_ZPOOL);

        // RAM disk image sources should work as expected.
        let ramdisk_source = resolver
            .file_source_for(
                "zone1",
                &ZoneImageSource::Ramdisk,
                &zpools,
                Some(&BOOT_ZPOOL),
            )
            .unwrap();
        assert_eq!(ramdisk_source, ramdisk_file_source("zone1"));

        let file_source = resolver
            .file_source_for(
                "zone1",
                &ZoneImageSource::Omicron(
                    OmicronZoneImageSource::InstallDataset,
                ),
                &zpools,
                Some(&BOOT_ZPOOL),
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

        let zpools = ZoneImageZpools {
            root: dir.path(),
            all_m2_zpools: vec![BOOT_ZPOOL],
        };
        let resolver =
            ZoneImageSourceResolver::new(&logctx.log, &zpools, &BOOT_ZPOOL);

        // The resolver should not fail for ramdisk images.
        let file_source = resolver
            .file_source_for(
                "fake-zone",
                &ZoneImageSource::Ramdisk,
                &zpools,
                Some(&BOOT_ZPOOL),
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
                &zpools,
                Some(&BOOT_ZPOOL),
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
                    &zpools,
                    Some(&BOOT_ZPOOL),
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
}
