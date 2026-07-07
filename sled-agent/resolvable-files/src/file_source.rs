// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to construct `ResolvableFileSource` instances.

use omicron_common::resolvable_files::ResolvableFileSource;
use sled_agent_types::resolvable_files::RAMDISK_IMAGE_PATH;

/// Construct a file source for the RAM disk.
///
/// This accepts a `zone_type` string rather than a `ZoneType` or `ZoneKind`
/// enum because it is used to manage non-Omicron zones like propolis-server.
pub fn ramdisk_file_source(zone_type: &str) -> ResolvableFileSource {
    ResolvableFileSource {
        file_name: format!("{zone_type}.tar.gz"),
        search_paths: vec![RAMDISK_IMAGE_PATH.into()],
    }
}
