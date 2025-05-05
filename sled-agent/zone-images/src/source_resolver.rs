// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::AllMupdateOverrides;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use sled_storage::dataset::INSTALL_DATASET;
use sled_storage::dataset::M2_ARTIFACT_DATASET;
use sled_storage::resources::AllDisks;
use slog::o;
use std::sync::Arc;
use std::sync::Mutex;

/// Places to look for an Omicron zone image.
pub struct ZoneImageFileSource {
    /// A custom file name to look for, if provided.
    ///
    /// The default file name is `<zone_type>.tar.gz`.
    pub file_name: Option<String>,

    /// The paths to look for the zone image in.
    ///
    /// This represents a high-confidence belief, but not a guarantee, that the
    /// zone image will be found in one of these locations.
    pub search_paths: Vec<Utf8PathBuf>,
}

/// A description of zpools to examine for zone images.
pub struct ZoneImageZpools<'a> {
    /// The root directory, typically `/`.
    pub root: &'a Utf8Path,

    /// The full set of M.2 zpools that are currently known. Must be non-empty,
    /// but it can include the boot zpool.
    pub all_m2_zpools: Vec<ZpoolName>,
}

/// Turns [`OmicronZoneImageSource`] instances into file names and search paths.
#[derive(Clone)]
pub struct ZoneImageSourceResolver {
    // Inner state, guarded by a mutex.
    //
    // This is mostly a way to ensure that accesses to the resolver are
    // serialized.
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

    pub fn override_image_directory(&self, path: Utf8PathBuf) {
        self.inner.lock().unwrap().override_image_directory(path);
    }

    /// Returns a [`ZoneImageFileSource`] consisting of the file name, plus a
    /// list of potential paths to search, for a zone image.
    pub fn file_source_for(
        &self,
        image_source: &OmicronZoneImageSource,
        all_disks: &AllDisks,
    ) -> ZoneImageFileSource {
        let inner = self.inner.lock().unwrap();
        inner.file_source_for(image_source, all_disks)
    }
}

#[derive(Debug)]
struct ResolverInner {
    #[expect(unused)]
    log: slog::Logger,
    image_directory_override: Option<Utf8PathBuf>,
    // Store all collected information for mupdate overrides -- we're going to
    // need to report this via inventory.
    //
    // This isn't actually used yet.
    #[expect(unused)]
    mupdate_overrides: AllMupdateOverrides,
}

impl ResolverInner {
    fn new(
        log: &slog::Logger,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: &ZpoolName,
    ) -> Self {
        let log = log.new(o!("component" => "ZoneImageSourceResolver"));

        let mupdate_overrides =
            AllMupdateOverrides::read_all(&log, zpools, boot_zpool);

        // Log the results.
        mupdate_overrides.log_results(&log);

        Self { log, image_directory_override: None, mupdate_overrides }
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
        image_source: &OmicronZoneImageSource,
        all_disks: &AllDisks,
    ) -> ZoneImageFileSource {
        let file_name = match image_source {
            OmicronZoneImageSource::InstallDataset => {
                // Use the default file name for install-dataset lookups.
                None
            }
            OmicronZoneImageSource::Artifact { hash } => Some(hash.to_string()),
        };

        let search_paths = match image_source {
            OmicronZoneImageSource::InstallDataset => {
                // Look for the image in the ramdisk first
                let mut zone_image_paths =
                    vec![Utf8PathBuf::from("/opt/oxide")];
                // Inject an image path if requested by a test.
                if let Some(path) = &self.image_directory_override {
                    zone_image_paths.push(path.clone());
                };

                // If the boot disk exists, look for the image in the "install"
                // dataset there too.
                if let Some((_, boot_zpool)) = all_disks.boot_disk() {
                    zone_image_paths.push(boot_zpool.dataset_mountpoint(
                        &all_disks.mount_config().root,
                        INSTALL_DATASET,
                    ));
                }

                zone_image_paths
            }
            OmicronZoneImageSource::Artifact { .. } => {
                // Search both artifact datasets, but look on the boot disk first.
                let boot_zpool =
                    all_disks.boot_disk().map(|(_, boot_zpool)| boot_zpool);
                // This iterator starts with the zpool for the boot disk (if it
                // exists), and then is followed by all other zpools.
                let zpool_iter = boot_zpool.into_iter().chain(
                    all_disks
                        .all_m2_zpools()
                        .into_iter()
                        .filter(|zpool| Some(zpool) != boot_zpool.as_ref()),
                );
                zpool_iter
                    .map(|zpool| {
                        zpool.dataset_mountpoint(
                            &all_disks.mount_config().root,
                            M2_ARTIFACT_DATASET,
                        )
                    })
                    .collect()
            }
        };

        ZoneImageFileSource { file_name, search_paths }
    }
}
