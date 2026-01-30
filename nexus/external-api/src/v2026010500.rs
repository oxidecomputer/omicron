// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026010500 to 2026010800.
//!
//! ## Multicast Changes
//!
//! [`InstanceCreate`] uses `Vec<NameOrId>` for `multicast_groups`. Newer
//! versions use `Vec<MulticastGroupJoinSpec>` which supports implicit group
//! lifecycle (groups are created automatically when referenced).
//!
//! Affected endpoints:
//! - `POST /v1/instances` (instance_create)
//!
//! [`InstanceCreate`]: self::InstanceCreate
//! [`MulticastGroupJoinSpec`]: nexus_types::external_api::params::MulticastGroupJoinSpec

use crate::v2026012800;
use nexus_types::external_api::params;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// v2026010500 (POOL_SELECTION_ENUMS) uses the current `InstanceNetworkInterfaceAttachment`
// with `DefaultIpv4`, `DefaultIpv6`, `DefaultDualStack` variants.
//
// Only the multicast_groups field differs (Vec<NameOrId> vs Vec<MulticastGroupJoinSpec>).
pub use params::InstanceNetworkInterfaceAttachment;

/// Create-time parameters for an `Instance`
///
/// This version uses `Vec<NameOrId>` for `multicast_groups` instead of
/// `Vec<MulticastGroupJoinSpec>` in newer versions.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: ByteCount,
    /// The hostname to be assigned to the instance
    pub hostname: Hostname,

    /// User data for instance initialization systems (such as cloud-init).
    /// Must be a Base64-encoded string, as specified in RFC 4648 § 4 (+ and /
    /// characters with padding). Maximum 32 KiB unencoded data.
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    #[serde(default)]
    pub external_ips: Vec<params::ExternalIpCreate>,

    /// The multicast groups this instance should join.
    ///
    /// The instance will be automatically added as a member of the specified
    /// multicast groups during creation, enabling it to send and receive
    /// multicast traffic for those groups.
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,

    /// A list of disks to be attached to the instance.
    #[serde(default)]
    pub disks: Vec<v2026012800::InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<v2026012800::InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
    pub start: bool,

    /// The auto-restart policy for this instance.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,

    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,

    /// The CPU platform to be used for this instance.
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl From<InstanceCreate> for params::InstanceCreate {
    fn from(value: InstanceCreate) -> Self {
        Self {
            identity: value.identity,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            user_data: value.user_data,
            network_interfaces: value.network_interfaces,
            external_ips: value.external_ips,
            multicast_groups: value
                .multicast_groups
                .into_iter()
                .map(|g| params::MulticastGroupJoinSpec {
                    group: g.into(),
                    source_ips: None,
                    ip_version: None,
                })
                .collect(),
            disks: value.disks.into_iter().map(Into::into).collect(),
            boot_disk: value.boot_disk.map(Into::into),
            ssh_public_keys: value.ssh_public_keys,
            start: value.start,
            auto_restart_policy: value.auto_restart_policy,
            anti_affinity_groups: value.anti_affinity_groups,
            cpu_platform: value.cpu_platform,
        }
    }
}
