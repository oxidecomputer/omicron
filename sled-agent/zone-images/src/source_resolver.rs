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
use camino::Utf8PathBuf;
use illumos_utils::running_zone::ZoneImageFileSource;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use sled_agent_config_reconciler::InternalDisks;
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
use slog::o;
use std::sync::Arc;
use std::sync::Mutex;

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
        image_source: &OmicronZoneImageSource,
        internal_disks: InternalDisks,
    ) -> ZoneImageFileSource {
        let inner = self.inner.lock().unwrap();
        inner.file_source_for(zone_type, image_source, internal_disks)
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
                if let Some(path) = internal_disks.boot_disk_install_dataset() {
                    zone_image_paths.push(path);
                }

                ZoneImageFileSource {
                    file_name: install_dataset_file_name(zone_type),
                    search_paths: zone_image_paths,
                }
            }
            OmicronZoneImageSource::Artifact { hash } => {
                // Search both artifact datasets. This iterator starts with the
                // dataset for the boot disk (if it exists), and then is followed
                // by all other disks.
                let search_paths =
                    internal_disks.all_artifact_datasets().collect();
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
