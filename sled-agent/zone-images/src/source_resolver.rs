// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::AllMupdateOverrides;
use crate::AllZoneManifests;
use crate::MupdateOverrideStatus;
use crate::RAMDISK_IMAGE_PATH;
use crate::ZoneManifestStatus;
use crate::install_dataset_file_name;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use illumos_utils::running_zone::ZoneImageFileSource;
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use sled_storage::dataset::INSTALL_DATASET;
use sled_storage::dataset::M2_ARTIFACT_DATASET;
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
        image_source: &OmicronZoneImageSource,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: Option<&ZpoolName>,
    ) -> ZoneImageFileSource {
        let inner = self.inner.lock().unwrap();
        inner.file_source_for(zone_type, image_source, zpools, boot_zpool)
    }
}

/// Current status of the zone image resolver.
#[derive(Clone, Debug)]
pub struct ResolverStatus {
    /// The zone manifest status.
    pub zone_manifest: ZoneManifestStatus,

    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideStatus,
}

#[derive(Debug)]
struct ResolverInner {
    #[expect(unused)]
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
    ) -> ZoneImageFileSource {
        match image_source {
            OmicronZoneImageSource::InstallDataset => {
                // Look for the image in the ramdisk first
                let mut zone_image_paths =
                    vec![Utf8PathBuf::from(RAMDISK_IMAGE_PATH)];
                // Inject an image path if requested by a test.
                if let Some(path) = &self.image_directory_override {
                    zone_image_paths.push(path.clone());
                };

                // If the boot disk exists, look for the image in the "install"
                // dataset on the boot zpool.
                if let Some(boot_zpool) = boot_zpool {
                    zone_image_paths.push(
                        boot_zpool
                            .dataset_mountpoint(zpools.root, INSTALL_DATASET),
                    );
                }

                ZoneImageFileSource {
                    file_name: install_dataset_file_name(zone_type),
                    search_paths: zone_image_paths,
                }
            }
            OmicronZoneImageSource::Artifact { hash } => {
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
                ZoneImageFileSource {
                    // Images in the artifact store are named by just their
                    // hash.
                    file_name: hash.to_string(),
                    search_paths,
                }
            }
        }
    }
}
