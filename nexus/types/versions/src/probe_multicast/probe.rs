// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for version PROBE_MULTICAST.
//!
//! Extends `ProbeCreate` with a `multicast_groups` field so probes can be
//! enrolled as multicast group members at creation time, mirroring how
//! `InstanceCreate` already accepts multicast group memberships.
//! Membership is fixed at probe creation. Recreate the probe to change it.

use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2026_01_05_00;
use crate::v2026_01_05_00::ip_pool::PoolSelector;
use crate::v2026_01_08_00::multicast::MulticastGroupJoinSpec;

/// Create time parameters for probes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProbeCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
    /// Pool to allocate from.
    #[serde(default)]
    pub pool_selector: PoolSelector,
    /// Multicast groups to enroll this probe as a member of at creation
    /// time. Each entry specifies a group (by name, UUID, or IP address),
    /// plus optional source IPs and IP version, matching the instance
    /// join shape. Non-existent groups are created automatically when
    /// given an IP covered by a multicast pool.
    ///
    /// If empty (the default), this leaves the probe unenrolled.
    /// Probe memberships are fixed at creation. Recreate the probe to
    /// change or modify them.
    #[serde(default)]
    pub multicast_groups: Vec<MulticastGroupJoinSpec>,
}

impl From<v2026_01_05_00::probe::ProbeCreate> for ProbeCreate {
    fn from(old: v2026_01_05_00::probe::ProbeCreate) -> ProbeCreate {
        ProbeCreate {
            identity: old.identity,
            sled: old.sled,
            pool_selector: old.pool_selector,
            multicast_groups: Vec::new(),
        }
    }
}
