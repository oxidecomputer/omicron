// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct HostStartupOptions {
    pub phase2_recovery_mode: bool,
    pub kbm: bool,
    pub bootrd: bool,
    pub prom: bool,
    pub kmdb: bool,
    pub kmdb_boot: bool,
    pub boot_ramdisk: bool,
    pub boot_net: bool,
    pub verbose: bool,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ComponentFirmwareHashStatus {
    /// The hash is not available; the client must issue a separate request to
    /// begin calculating the hash.
    HashNotCalculated,
    /// The hash is currently being calculated; the client should sleep briefly
    /// then check again.
    ///
    /// We expect this operation to take a handful of seconds in practice.
    HashInProgress,
    /// The hash of the given firmware slot.
    Hashed { sha256: [u8; 32] },
}
