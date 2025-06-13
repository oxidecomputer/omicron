// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::AllMupdateOverrides;
use crate::AllZoneManifests;
use crate::MupdateOverrideStatus;
use crate::ZoneManifestStatus;
use camino::Utf8PathBuf;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use sled_agent_config_reconciler::InternalDisks;
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
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
        image_source: &OmicronZoneImageSource,
        internal_disks: InternalDisks,
    ) -> ZoneImageFileSource {
        let inner = self.inner.lock().unwrap();
        inner.file_source_for(image_source, internal_disks)
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
        image_source: &OmicronZoneImageSource,
        internal_disks: InternalDisks,
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
                // dataset on the boot zpool.
                if let Some(path) = internal_disks.boot_disk_install_dataset() {
                    zone_image_paths.push(path);
                }

                zone_image_paths
            }
            OmicronZoneImageSource::Artifact { .. } => {
                // Search both artifact datasets. This iterator starts with the
                // dataset for the boot disk (if it exists), and then is followed
                // by all other disks.
                internal_disks.all_artifact_datasets().collect()
            }
        };

        ZoneImageFileSource { file_name, search_paths }
    }
}
