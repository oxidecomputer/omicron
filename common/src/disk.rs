// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types shared among crates

/// Uniquely identifies a disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskIdentity {
    pub vendor: String,
    pub serial: String,
    pub model: String,
}
