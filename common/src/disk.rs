// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types shared among crates

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Uniquely identifies a disk.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct DiskIdentity {
    pub vendor: String,
    pub serial: String,
    pub model: String,
}
