// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `INSTANCES_EXTERNAL_SUBNETS` of the Nexus external API.
//!
//! This version (2026013000) uses the old `DiskSource` from `v2025112000`
//! (without `read_only`) and the new `DiskType` from `omicron_common`
//! (without the old `Crucible` variant). The `Disk` view type does not
//! include a `read_only` field.
//!
//! Subnet pool views still include `pool_type` and subnet pool members
//! still include identity metadata.

pub mod disk;
pub mod instance;
pub mod subnet_pool;
