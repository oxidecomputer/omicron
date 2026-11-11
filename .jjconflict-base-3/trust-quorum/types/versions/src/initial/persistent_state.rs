// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Persistent state types.

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::configuration::BaseboardId;
use super::types::Epoch;

/// Metadata about a node being expunged from the trust quorum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExpungedMetadata {
    /// The committed epoch, later than its current configuration at which the
    /// node learned that it had been expunged.
    pub epoch: Epoch,

    /// Which node this commit information was learned from.
    pub from: BaseboardId,
}

/// A subset of information stored in persistent state that is useful
/// for validation, testing, and informational purposes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PersistentStateSummary {
    pub rack_id: Option<RackUuid>,
    pub is_lrtq_only: bool,
    pub is_uninitialized: bool,
    pub latest_config: Option<Epoch>,
    pub latest_committed_config: Option<Epoch>,
    pub latest_share: Option<Epoch>,
    pub expunged: Option<ExpungedMetadata>,
}
