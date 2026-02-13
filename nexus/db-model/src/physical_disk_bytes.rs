// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Newtype wrappers for type safety between virtual and physical byte
//! quantities.

use omicron_common::api::external::ByteCount;

/// Bytes as seen by the user (e.g., "100 GiB disk").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VirtualDiskBytes(pub ByteCount);

/// Bytes actually consumed on physical storage (includes replication +
/// overhead).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalDiskBytes(pub ByteCount);

impl VirtualDiskBytes {
    pub fn to_bytes(self) -> u64 {
        self.0.to_bytes()
    }
}

impl PhysicalDiskBytes {
    pub fn to_bytes(self) -> u64 {
        self.0.to_bytes()
    }

    pub fn into_byte_count(self) -> ByteCount {
        self.0
    }
}

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

/// Number of region copies for distributed (Crucible) disks.
const REGION_REDUNDANCY: u64 = 3;

/// ZFS overhead reservation factor: 25% extra (i.e., multiply by 1.25).
/// This matches `RegionReservationPercent::TwentyFive` in region.rs.
const ZFS_OVERHEAD_NUMERATOR: u64 = 5;
const ZFS_OVERHEAD_DENOMINATOR: u64 = 4;

/// Physical bytes for a distributed (Crucible) disk:
/// 3 regions × virtual_size × 1.25 = 3.75× virtual size.
pub fn distributed_disk_physical_bytes(
    virtual_size: VirtualDiskBytes,
) -> PhysicalDiskBytes {
    let v = virtual_size.to_bytes();
    let physical = v * REGION_REDUNDANCY * ZFS_OVERHEAD_NUMERATOR
        / ZFS_OVERHEAD_DENOMINATOR;
    PhysicalDiskBytes(ByteCount::try_from(physical).unwrap())
}

/// Physical bytes for a local disk:
/// virtual_size + (virtual_size / 1 GiB) × 70 MiB
pub fn local_disk_physical_bytes(
    virtual_size: VirtualDiskBytes,
) -> PhysicalDiskBytes {
    let v = virtual_size.to_bytes();
    let extent_count = v / GIB;
    let overhead = extent_count * 70 * MIB;
    let physical = v + overhead;
    PhysicalDiskBytes(ByteCount::try_from(physical).unwrap())
}

/// Physical bytes for a snapshot region:
/// Same as distributed_disk_physical_bytes (3 × virtual_size × 1.25).
pub fn snapshot_region_physical_bytes(
    virtual_size: VirtualDiskBytes,
) -> PhysicalDiskBytes {
    distributed_disk_physical_bytes(virtual_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_disk_physical_bytes() {
        let virtual_100_gib =
            VirtualDiskBytes(ByteCount::from_gibibytes_u32(100));
        let physical = distributed_disk_physical_bytes(virtual_100_gib);
        // 3 × 100 GiB × 1.25 = 375 GiB
        assert_eq!(physical.to_bytes(), 375 * GIB);
    }

    #[test]
    fn test_local_disk_physical_bytes() {
        let virtual_100_gib =
            VirtualDiskBytes(ByteCount::from_gibibytes_u32(100));
        let physical = local_disk_physical_bytes(virtual_100_gib);
        // 100 GiB + (100 × 70 MiB) = 100 GiB + 7000 MiB
        let expected = 100 * GIB + 100 * 70 * MIB;
        assert_eq!(physical.to_bytes(), expected);
    }

    #[test]
    fn test_snapshot_region_physical_bytes() {
        let virtual_100_gib =
            VirtualDiskBytes(ByteCount::from_gibibytes_u32(100));
        let physical = snapshot_region_physical_bytes(virtual_100_gib);
        // Same as distributed: 375 GiB
        assert_eq!(physical.to_bytes(), 375 * GIB);
    }

    #[test]
    fn test_types_are_distinct() {
        // Verify the newtypes can't be accidentally interchanged at
        // compile time - this test is more about documentation than
        // runtime checking.
        let v = VirtualDiskBytes(ByteCount::from_gibibytes_u32(1));
        let p = distributed_disk_physical_bytes(v);
        assert_ne!(v.to_bytes(), p.to_bytes());
    }
}
