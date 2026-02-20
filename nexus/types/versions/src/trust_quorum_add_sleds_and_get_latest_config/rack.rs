// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack membership types for version TRUST_QUORUM_ADD_SLEDS_AND_GET_LATEST_CONFIG.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use uuid::Uuid;

/// A unique, monotonically increasing number representing the set of active
/// sleds in a rack at a given point in time.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RackMembershipVersion(pub u64);

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct RackMembershipAddSledsRequest {
    pub sled_ids: BTreeSet<BaseboardId>,
}

#[derive(Deserialize, JsonSchema)]
pub struct RackMembershipConfigPathParams {
    pub rack_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RackMembershipVersionParam {
    pub version: Option<RackMembershipVersion>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RackMembershipChangeState {
    InProgress,
    Committed,
    Aborted,
}

/// Status of the rack membership uniquely identified by the (rack_id, version) pair
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RackMembershipStatus {
    pub rack_id: Uuid,
    /// Version that uniquely identifies the rack membership at a given point in time
    pub version: RackMembershipVersion,
    pub state: RackMembershipChangeState,
    /// All members of the rack for this version
    pub members: BTreeSet<BaseboardId>,
    /// All members that have not yet confirmed this membership version
    pub unacknowledged_members: BTreeSet<BaseboardId>,
    pub time_created: DateTime<Utc>,
    pub time_committed: Option<DateTime<Utc>>,
    pub time_aborted: Option<DateTime<Utc>>,
}
