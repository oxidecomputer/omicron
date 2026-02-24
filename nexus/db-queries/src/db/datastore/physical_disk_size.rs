// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Physical disk size calculation helpers.
//!
//! These functions translate virtual (user-visible) disk sizes into
//! physical (actual storage consumed) sizes, accounting for replication
//! and ZFS overhead.

// Re-exports for use by saga code. These will be used when saga integration
// is added to dual-write physical provisioning alongside virtual.
#[allow(unused_imports)]
pub use nexus_db_model::{
    PhysicalDiskBytes, VirtualDiskBytes, distributed_disk_physical_bytes,
    local_disk_physical_bytes, snapshot_region_physical_bytes,
};
