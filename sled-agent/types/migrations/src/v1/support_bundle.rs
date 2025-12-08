// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for Sled Agent API v1.
//!
//! This module contains types related to support bundles used by the sled agent.
//!
//! Per RFD 619, these types are defined in the earliest version they appear in
//! and are used by business logic via floating identifiers in sled-agent-types.

use omicron_uuid_kinds::SupportBundleUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// State of a support bundle.
#[derive(Deserialize, Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SupportBundleState {
    Complete,
    Incomplete,
}

/// Metadata about a support bundle.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleMetadata {
    pub support_bundle_id: SupportBundleUuid,
    pub state: SupportBundleState,
}
