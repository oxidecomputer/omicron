// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version MULTICAST_IMPLICIT_LIFECYCLE_UPDATES.

use omicron_common::api::external::{
    ByteCount, InstanceAutoRestartPolicy, InstanceCpuCount,
    InstanceCpuPlatform, NameOrId, Nullable,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::multicast::MulticastGroupJoinSpec;

/// Parameters of an `Instance` that can be reconfigured after creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceUpdate {
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: InstanceCpuCount,

    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: ByteCount,

    /// The disk the instance is configured to boot from.
    pub boot_disk: Nullable<NameOrId>,

    /// The auto-restart policy for this instance.
    pub auto_restart_policy: Nullable<InstanceAutoRestartPolicy>,

    /// The CPU platform to be used for this instance.
    pub cpu_platform: Nullable<InstanceCpuPlatform>,

    /// Multicast groups this instance should join.
    ///
    /// When specified, this replaces the instance's current multicast group
    /// membership with the new set of groups. The instance will leave any
    /// groups not listed here and join any new groups that are specified.
    ///
    /// Each entry can specify the group by name, UUID, or IP address, along with
    /// optional source IP filtering for SSM (Source-Specific Multicast). When
    /// a group doesn't exist, it will be implicitly created using the default
    /// multicast pool (or you can specify `ip_version` to disambiguate if needed).
    ///
    /// If not provided, the instance's multicast group membership will not
    /// be changed.
    #[serde(default)]
    pub multicast_groups: Option<Vec<MulticastGroupJoinSpec>>,
}

impl From<crate::v2025112000::instance::InstanceUpdate> for InstanceUpdate {
    fn from(old: crate::v2025112000::instance::InstanceUpdate) -> Self {
        Self {
            ncpus: old.ncpus,
            memory: old.memory,
            boot_disk: old.boot_disk,
            auto_restart_policy: old.auto_restart_policy,
            cpu_platform: old.cpu_platform,
            multicast_groups: old.multicast_groups.map(|groups| {
                groups
                    .into_iter()
                    .map(|g| MulticastGroupJoinSpec {
                        group: g.into(),
                        source_ips: None,
                        ip_version: None,
                    })
                    .collect()
            }),
        }
    }
}
