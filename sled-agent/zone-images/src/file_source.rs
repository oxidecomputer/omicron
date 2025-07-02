// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to construct `ZoneImageFileSource` instances.

use illumos_utils::running_zone::ZoneImageFileSource;
use sled_agent_types::zone_images::install_dataset_file_name;

/// The location to look for images shipped with the RAM disk.
pub const RAMDISK_IMAGE_PATH: &str = "/opt/oxide";

/// Constructs a file source for the RAM disk.
///
/// This accepts a `zone_type` string rather than a `ZoneType` or `ZoneKind`
/// enum because it is used to manage non-Omicron zones like propolis-server.
pub fn ramdisk_file_source(zone_type: &str) -> ZoneImageFileSource {
    ZoneImageFileSource {
        file_name: install_dataset_file_name(zone_type),
        search_paths: vec![RAMDISK_IMAGE_PATH.into()],
    }
}
