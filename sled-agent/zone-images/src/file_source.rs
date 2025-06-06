// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to construct `ZoneImageFileSource` instances.

use illumos_utils::running_zone::ZoneImageFileSource;

/// The zone in which the sled is running.

/// The location to look for images shipped with the RAM disk.
pub const RAMDISK_IMAGE_PATH: &str = "/opt/oxide";

/// Constructs a file source for the RAM disk.
pub fn ramdisk_file_source(zone_type: &str) -> ZoneImageFileSource {
    ZoneImageFileSource {
        file_name: install_dataset_file_name(zone_type),
        search_paths: vec![RAMDISK_IMAGE_PATH.into()],
    }
}

/// Returns the filename for install-dataset images.
pub fn install_dataset_file_name(zone_type: &str) -> String {
    format!("{}.tar.gz", zone_type)
}
