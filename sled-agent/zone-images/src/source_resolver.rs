// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone image lookup.

use crate::AllMupdateOverrides;
use crate::MupdateOverrideReadError;
use crate::MupdateOverrideStatus;
use crate::RAMDISK_IMAGE_PATH;
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
        ResolverStatus { mupdate_override: inner.mupdate_overrides.status() }
    }

    /// Returns a [`ZoneImageFileSource`] consisting of the file name, plus a
    /// list of potential paths to search, for a zone image.
    pub fn file_source_for(
        &self,
        zone_type: &str,
        image_source: &OmicronZoneImageSource,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: Option<&ZpoolName>,
    ) -> Result<ZoneImageFileSource, MupdateOverrideReadError> {
        let inner = self.inner.lock().unwrap();
        inner.file_source_for(zone_type, image_source, zpools, boot_zpool)
    }
}

/// Current status of the zone image resolver.
#[derive(Clone, Debug)]
pub struct ResolverStatus {
    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideStatus,
}

#[derive(Debug)]
struct ResolverInner {
    log: slog::Logger,
    image_directory_override: Option<Utf8PathBuf>,
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

        let mupdate_overrides =
            AllMupdateOverrides::read_all(&log, zpools, boot_zpool);

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
        zone_type: &str,
        image_source: &OmicronZoneImageSource,
        zpools: &ZoneImageZpools<'_>,
        boot_zpool: Option<&ZpoolName>,
    ) -> Result<ZoneImageFileSource, MupdateOverrideReadError> {
        let mupdate_override_status = self.mupdate_overrides.status();

        // Error out if the install dataset is missing or if any zones fail. At
        // the moment we don't use this for the artifact dataset, but we'll use
        // it to determine mupdate override status in the future.
        //
        // XXX This does mean that zones from the RAM disk will not be started,
        // including the switch zone. That feels a bit concerning and worth
        // thinking about more carefully -- we should consider maybe exempting
        // the switch zone from this check.
        let boot_disk_override = mupdate_override_status.boot_disk_override?;

        match image_source {
            OmicronZoneImageSource::InstallDataset => {
                let file_name = install_dataset_file_name(zone_type);
                // Look for the image in the ramdisk first
                let mut zone_image_paths =
                    vec![Utf8PathBuf::from(RAMDISK_IMAGE_PATH)];
                // Inject an image path if requested by a test.
                if let Some(path) = &self.image_directory_override {
                    zone_image_paths.push(path.clone());
                };

                // Any zones not part of the ramdisk are managed via mupdate
                // overrides.
                //
                // XXX: we ask for the boot zpool to be passed in here. But
                // `AllMupdateOverrides` also caches the boot zpool. How should
                // we reconcile the two?
                if let Some(boot_zpool) = boot_zpool {
                    if let Some(info) = boot_disk_override {
                        if info.zones.contains_key(file_name.as_str()) {
                            zone_image_paths.push(
                                boot_zpool.dataset_mountpoint(
                                    zpools.root,
                                    INSTALL_DATASET,
                                ),
                            );
                        } else {
                            // The zone was requested but not found in the
                            // install dataset. This is unusual! We log a
                            // message but do not return the zone.
                            slog::warn!(
                                self.log,
                                "zone {} not found in mupdate-override.json",
                                file_name
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
