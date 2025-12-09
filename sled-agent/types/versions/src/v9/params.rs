// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request parameters for Sled Agent API version 9.
//!
//! This module contains types introduced in v9 (DELEGATE_ZVOL_TO_PROPOLIS).

use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::{DatasetUuid, ExternalZpoolUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Path parameters for Local Storage dataset related requests.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LocalStoragePathParam {
    pub zpool_id: ExternalZpoolUuid,
    pub dataset_id: DatasetUuid,
}

/// Dataset and Volume details for a Local Storage dataset ensure request.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct LocalStorageDatasetEnsureRequest {
    /// Size of the parent dataset
    pub dataset_size: ByteCount,

    /// Size of the zvol
    pub volume_size: ByteCount,
}
