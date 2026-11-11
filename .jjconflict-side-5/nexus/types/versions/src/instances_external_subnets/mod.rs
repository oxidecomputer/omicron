// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `INSTANCES_EXTERNAL_SUBNETS` of the Nexus external API.
//!
//! The `Disk` view type for this version uses the new `DiskType` from
//! `omicron_common` (without the old `Crucible` variant) but does not
//! include a `read_only` field.

pub mod disk;
