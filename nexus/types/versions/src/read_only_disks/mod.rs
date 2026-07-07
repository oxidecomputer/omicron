// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `READ_ONLY_DISKS` of the Nexus external API.
//!
//! This version introduces the `read_only` field on `DiskSource::Snapshot`
//! and `DiskSource::Image` as a required field. The subsequent version,
//! `READ_ONLY_DISKS_NULLABLE`, makes `read_only` optional via
//! `#[serde(default)]`.

pub mod disk;
pub mod instance;
