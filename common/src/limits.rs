// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Contains constants that define the hard limits of Nexus

pub const MAX_VCPU_PER_INSTANCE: u16 = 64;

pub const MIN_MEMORY_BYTES_PER_INSTANCE: u32 = 1 << 30; // 1 GiB
pub const MAX_MEMORY_BYTES_PER_INSTANCE: u64 = 256 * (1 << 30); // 256 GiB

pub const MAX_DISKS_PER_INSTANCE: u32 = 8;
pub const MIN_DISK_SIZE_BYTES: u32 = 1 << 30; // 1 GiB
pub const MAX_DISK_SIZE_BYTES: u64 = 1023 * (1 << 30); // 1023 GiB

pub const MAX_NICS_PER_INSTANCE: usize = 8;

// XXX: Might want to recast as max *floating* IPs, we have at most one
//      ephemeral (so bounded in saga by design).
//      The value here is arbitrary, but we need *a* limit for the instance
//      create saga to have a bounded DAG. We might want to only enforce
//      this during instance create (rather than live attach) in future.
pub const MAX_EXTERNAL_IPS_PER_INSTANCE: usize = 32;
pub const MAX_EPHEMERAL_IPS_PER_INSTANCE: usize = 1;
