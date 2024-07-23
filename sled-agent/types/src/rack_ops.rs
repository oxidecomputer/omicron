// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::{RackInitUuid, RackResetUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Current status of any rack-level operation being performed by this bootstrap
/// agent.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RackOperationStatus {
    Initializing {
        id: RackInitUuid,
    },
    /// `id` will be none if the rack was already initialized on startup.
    Initialized {
        id: Option<RackInitUuid>,
    },
    InitializationFailed {
        id: RackInitUuid,
        message: String,
    },
    InitializationPanicked {
        id: RackInitUuid,
    },
    Resetting {
        id: RackResetUuid,
    },
    /// `reset_id` will be None if the rack is in an uninitialized-on-startup,
    /// or Some if it is in an uninitialized state due to a reset operation
    /// completing.
    Uninitialized {
        reset_id: Option<RackResetUuid>,
    },
    ResetFailed {
        id: RackResetUuid,
        message: String,
    },
    ResetPanicked {
        id: RackResetUuid,
    },
}
