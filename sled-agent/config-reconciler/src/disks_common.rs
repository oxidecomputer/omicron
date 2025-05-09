// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality common to both internal and external managed disk.

use sled_storage::disk::Disk;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::info;
use slog::warn;

pub(crate) enum MaybeUpdatedDisk {
    Updated(Disk),
    Unchanged(Disk),
}

pub(crate) fn update_properties_from_raw_disk(
    mut disk: Disk,
    raw_disk: &RawDisk,
    log: &Logger,
) -> MaybeUpdatedDisk {
    if *raw_disk == RawDisk::from(disk.clone()) {
        return MaybeUpdatedDisk::Unchanged(disk);
    }

    // The only property we expect to change is the firmware metadata. Update
    // that and check again; if they're still not equal, something weird is
    // going on. At least log a warning.
    disk.update_firmware_metadata(raw_disk);
    if *raw_disk == RawDisk::from(disk.clone()) {
        info!(
            log, "Updated disk firmware metadata";
            "firmware" => ?disk.firmware(),
            "identity" => ?disk.identity(),
        );
    } else {
        warn!(
            log,
            "Updated disk firmware metadata from raw disk properties, \
             but other properties are different!";
            "disk" => ?disk,
            "raw_disk" => ?*raw_disk,
        );
    }
    MaybeUpdatedDisk::Updated(disk)
}
