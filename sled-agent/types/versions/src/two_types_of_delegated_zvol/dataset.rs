// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Dataset and Volume details for a Local Storage dataset ensure request.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct LocalStorageDatasetEnsureRequest {
    pub zpool_id: ExternalZpoolUuid,

    /// ID of the local storage dataset allocation, not the local storage
    /// dataset!
    pub dataset_id: DatasetUuid,

    /// Size of the parent dataset
    pub dataset_size: ByteCount,

    /// Size of the zvol
    pub volume_size: ByteCount,

    /// Whether or not to use the encrypted dataset
    pub encrypted_at_rest: bool,
}

/// Dataset details for a Local Storage dataset delete request.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct LocalStorageDatasetDeleteRequest {
    pub zpool_id: ExternalZpoolUuid,

    /// ID of the local storage dataset allocation, not the local storage
    /// dataset!
    pub dataset_id: DatasetUuid,

    /// Whether or not to use the encrypted dataset
    pub encrypted_at_rest: bool,
}
