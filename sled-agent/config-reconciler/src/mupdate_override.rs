// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Behaviors of the config reconciler in the presence of mupdate overrides.

use crate::InternalDisks;
use crate::host_phase_2::HostPhase2PreparedContents;
use camino::Utf8PathBuf;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredContents;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::zone_images::ZoneImageFileSource;
use sled_agent_types::zone_images::OmicronZoneFileSource;
use sled_agent_types::zone_images::OmicronZoneImageLocation;
use sled_agent_types::zone_images::PreparedOmicronZone;
use sled_agent_types::zone_images::RAMDISK_IMAGE_PATH;
use sled_agent_types::zone_images::ResolverStatus;
use sled_agent_types::zone_images::ZoneImageLocationError;
use slog::error;
use slog_error_chain::InlineErrorChain;
use tufaceous_artifact::ArtifactHash;

/// An extension trait for `ResolverStatus`.
///
/// This trait refers to types that aren't available within `sled-agent-types`.
pub trait ResolverStatusExt {
    /// Look up the file source for an Omicron zone.
    fn omicron_file_source(
        &self,
        log: &slog::Logger,
        zone_kind: ZoneKind,
        image_source: &OmicronZoneImageSource,
        internal_disks: &InternalDisks,
    ) -> OmicronZoneFileSource;

    /// Prepare an Omicron zone for installation.
    fn prepare_omicron_zone<'a>(
        &self,
        log: &slog::Logger,
        zone_config: &'a OmicronZoneConfig,
        internal_disks: &InternalDisks,
    ) -> PreparedOmicronZone<'a> {
        let file_source = self.omicron_file_source(
            log,
            zone_config.zone_type.kind(),
            &zone_config.image_source,
            internal_disks,
        );
        PreparedOmicronZone::new(zone_config, file_source)
    }

    fn prepare_host_phase_2_contents<'a>(
        &self,
        #[expect(unused)] log: &slog::Logger,
        desired: &'a HostPhase2DesiredContents,
    ) -> HostPhase2PreparedContents<'a> {
        // TODO: Implement mupdate override logic.
        HostPhase2PreparedContents::NoMupdateOverride(desired)
    }
}

impl ResolverStatusExt for ResolverStatus {
    fn omicron_file_source(
        &self,
        log: &slog::Logger,
        zone_kind: ZoneKind,
        image_source: &OmicronZoneImageSource,
        internal_disks: &InternalDisks,
    ) -> OmicronZoneFileSource {
        match image_source {
            OmicronZoneImageSource::InstallDataset => {
                let file_name = zone_kind.artifact_in_install_dataset();

                // There's always at least one image path (the RAM disk below).
                let mut search_paths = Vec::with_capacity(1);

                // Inject an image path if requested by a test.
                if let Some(path) = &self.image_directory_override {
                    search_paths.push(path.clone());
                };

                // Any zones not part of the RAM disk are managed via the zone
                // manifest.
                let hash = install_dataset_hash(
                    log,
                    self,
                    zone_kind,
                    internal_disks,
                    |path| search_paths.push(path),
                );

                // Look for the image in the RAM disk as a fallback. Note that
                // install dataset images are not stored on the RAM disk in
                // production, just in development or test workflows.
                search_paths.push(Utf8PathBuf::from(RAMDISK_IMAGE_PATH));

                OmicronZoneFileSource {
                    location: OmicronZoneImageLocation::InstallDataset { hash },
                    file_source: ZoneImageFileSource {
                        file_name: file_name.to_owned(),
                        search_paths,
                    },
                }
            }
            OmicronZoneImageSource::Artifact { hash } => {
                // TODO: implement mupdate override here.
                //
                // Search both artifact datasets. This iterator starts with the
                // dataset for the boot disk (if it exists), and then is followed
                // by all other disks.
                let search_paths =
                    internal_disks.all_artifact_datasets().collect();
                OmicronZoneFileSource {
                    // TODO: with mupdate overrides, return InstallDataset here
                    location: OmicronZoneImageLocation::Artifact {
                        hash: Ok(*hash),
                    },
                    file_source: ZoneImageFileSource {
                        file_name: hash.to_string(),
                        search_paths,
                    },
                }
            }
        }
    }
}

fn install_dataset_hash<F>(
    log: &slog::Logger,
    resolver_status: &ResolverStatus,
    zone_kind: ZoneKind,
    internal_disks: &InternalDisks,
    mut search_paths_cb: F,
) -> Result<ArtifactHash, ZoneImageLocationError>
where
    F: FnMut(Utf8PathBuf),
{
    // XXX: we ask for the boot zpool to be passed in here. But
    // `ResolverStatus` also caches the boot zpool. How should we
    // reconcile the two?
    let hash = if let Some(path) = internal_disks.boot_disk_install_dataset() {
        let hash = resolver_status.zone_manifest.zone_hash(zone_kind);
        match hash {
            Ok(hash) => {
                search_paths_cb(path);
                Ok(hash)
            }
            Err(error) => {
                error!(
                    log,
                    "zone {} not found in the boot disk zone manifest, \
                     not returning it as a source",
                    zone_kind.report_str();
                    "file_name" => zone_kind.artifact_in_install_dataset(),
                    "error" => InlineErrorChain::new(&error),
                );
                Err(ZoneImageLocationError::ZoneHash(error))
            }
        }
    } else {
        // The boot disk is not available, so we cannot add the
        // install dataset path from it.
        error!(
            log,
            "boot disk install dataset not available, \
             not returning it as a source";
            "zone_kind" => zone_kind.report_str(),
        );
        Err(ZoneImageLocationError::BootDiskMissing)
    };
    hash
}

// Tests for this module live inside sled-agent-zone-images. (This is a bit
// weird and should probably be addressed at some point.)
