// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use camino::Utf8PathBuf;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use sled_storage::dataset::INSTALL_DATASET;
use sled_storage::dataset::M2_ARTIFACT_DATASET;
use sled_storage::resources::AllDisks;
use std::sync::OnceLock;

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

/// Resolves [`OmicronZoneImageSource`] instances into file names and search
/// paths.
pub struct ZoneImageSourceResolver {
    image_directory_override: OnceLock<Utf8PathBuf>,
}

impl ZoneImageSourceResolver {
    pub fn new() -> Self {
        ZoneImageSourceResolver { image_directory_override: OnceLock::new() }
    }

    /// Overrides the image directory with another one.
    ///
    /// Intended for testing.
    pub fn override_image_directory(&self, path: Utf8PathBuf) {
        self.image_directory_override.set(path).unwrap();
    }

    /// Returns a [`ZoneImageFileSource`] consisting of the file name, plus a
    /// list of potential paths to search, for a zone image.
    pub fn file_source_for(
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
                if let Some(path) = self.image_directory_override.get() {
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
