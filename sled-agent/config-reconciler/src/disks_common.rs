// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality common to both internal and external managed disk.

use sled_storage::disk::Disk;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::info;
use slog::warn;

#[derive(Debug)]
// `Disk` is ~200 bytes, but the callers wants ownership of them, so it'd be
// annoying to box them. Suppress the clippy lint about the variants having
// large size differences.
#[allow(clippy::large_enum_variant)]
pub(crate) enum MaybeUpdatedDisk {
    Unchanged,
    Updated(Disk),
}

pub(crate) fn update_properties_from_raw_disk(
    disk: &Disk,
    raw_disk: &RawDisk,
    log: &Logger,
) -> MaybeUpdatedDisk {
    if *raw_disk == RawDisk::from(disk.clone()) {
        return MaybeUpdatedDisk::Unchanged;
    }

    // The only properties we expect to change are the firmware metadata and
    // the chassis location, which is re-read from the hardware topology on
    // every poll. Update those and check again; if they're still not equal,
    // something weird is going on. At least log a warning.
    let mut disk = disk.clone();
    disk.update_mutable_properties(raw_disk);
    if *raw_disk == RawDisk::from(disk.clone()) {
        info!(
            log, "Updated disk properties";
            "firmware" => ?disk.firmware(),
            "location" => ?disk.location(),
            "identity" => ?disk.identity(),
        );
    } else {
        warn!(
            log,
            "Updated disk properties from raw disk, \
             but other properties are different!";
            "disk" => ?disk,
            "raw_disk" => ?*raw_disk,
        );
    }
    MaybeUpdatedDisk::Updated(disk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ExternalZpoolUuid;
    use sled_agent_types::disk::DiskIdentity;
    use sled_agent_types::disk::DiskVariant;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_hardware::UnparsedDisk;

    // A change to a disk's chassis location reported by the hardware
    // topology must be copied onto the adopted disk, which is then
    // considered unchanged.
    #[test]
    fn location_change_is_propagated() {
        let logctx = dev::test_setup_log("location_change_is_propagated");

        let identity = DiskIdentity {
            vendor: "test".into(),
            model: "test".into(),
            serial: "test-u2".into(),
        };
        let firmware = DiskFirmware::new(0, None, false, 1, vec![]);
        let adopted = Disk::Real(PooledDisk {
            paths: DiskPaths {
                devfs_path: "/test-devfs".into(),
                dev_path: None,
            },
            pcie_slot: 0,
            identity: identity.clone(),
            is_boot_disk: false,
            location: "N2".to_string(),
            partitions: vec![],
            zpool_name: ZpoolName::External(ExternalZpoolUuid::new_v4()),
            firmware: firmware.clone(),
        });

        // The raw disk matches what was adopted, except for its chassis
        // location.
        let raw_disk = RawDisk::Real(UnparsedDisk::new(
            "/test-devfs".into(),
            None,
            0,
            DiskVariant::U2,
            identity,
            false,
            firmware,
            "N3".to_string(),
        ));

        let updated = assert_matches!(
            update_properties_from_raw_disk(&adopted, &raw_disk, &logctx.log),
            MaybeUpdatedDisk::Updated(disk) => disk
        );
        assert_eq!(updated.location(), "N3");
        assert_eq!(RawDisk::from(updated.clone()), raw_disk);

        // A second pass sees nothing left to update.
        assert_matches!(
            update_properties_from_raw_disk(&updated, &raw_disk, &logctx.log),
            MaybeUpdatedDisk::Unchanged
        );

        logctx.cleanup_successful();
    }
}
