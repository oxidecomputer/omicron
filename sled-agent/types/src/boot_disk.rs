// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common types related to boot disks.

use omicron_common::disk::M2Slot;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootDiskPathParams {
    pub boot_disk: M2Slot,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootDiskUpdatePathParams {
    pub boot_disk: M2Slot,
    pub update_id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootDiskWriteStartQueryParams {
    pub update_id: Uuid,
    // TODO do we already have sha2-256 hashes of the OS images, and if so
    // should we use that instead? Another option is to use the external API
    // `Digest` type, although it predates `serde_human_bytes` so just stores
    // the hash as a `String`.
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<32>")]
    pub sha3_256_digest: [u8; 32],
}

/// Current progress of an OS image being written to disk.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema, Serialize,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BootDiskOsWriteProgress {
    /// The image is still being uploaded.
    ReceivingUploadedImage { bytes_received: usize },
    /// The image is being written to disk.
    WritingImageToDisk { bytes_written: usize },
    /// The image is being read back from disk for validation.
    ValidatingWrittenImage { bytes_read: usize },
}

/// Status of an update to a boot disk OS.
#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BootDiskOsWriteStatus {
    /// No update has been started for this disk, or any previously-started
    /// update has completed and had its status cleared.
    NoUpdateStarted,
    /// An update is currently running.
    InProgress { update_id: Uuid, progress: BootDiskOsWriteProgress },
    /// The most recent update completed successfully.
    Complete { update_id: Uuid },
    /// The most recent update failed.
    Failed { update_id: Uuid, message: String },
}
