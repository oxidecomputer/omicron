// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct TaskDump {
    /// Index of the crashed task.
    pub task_index: u16,
    /// Hubris timestamp at which the task crash occurred.
    pub timestamp: u64,
    /// Hex-encoded Hubris archive ID.
    pub archive_id: String,
    /// `BORD` field from the caboose.
    pub bord: String,
    /// `GITC` field from the caboose.
    pub gitc: String,
    /// `VERS` field from the caboose, if present.
    pub vers: Option<String>,
    /// Base64-encoded zip file containing dehydrated task dump.
    pub base64_zip: String,
}
