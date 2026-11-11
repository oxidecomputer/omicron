// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `READ_ONLY_DISKS_NULLABLE` of the Nexus external API.
//!
//! Makes `read_only` on `DiskSource` variants use `#[serde(default)]`,
//! allowing it to be omitted (defaulting to `false`).

pub mod disk;
pub mod instance;
