// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external API types (version 2025112000)
//!
//! Version 2025112000 types (before [`MulticastGroupIdentifier`] was introduced
//! and before implicit group lifecycle).
//!
//! Key differences from newer API versions:
//! - Uses [`NameOrId`] for multicast group references (not [`MulticastGroupIdentifier`]).
//!   Newer versions accept name, UUID, or multicast IP address, while this version
//!   only accepts name or UUID.
//! - Has explicit [`MulticastGroupCreate`] and [`MulticastGroupUpdate`] types
//!   (newer versions create/delete groups implicitly via member operations).
//! - [`MulticastGroupMemberAdd`] doesn't have `source_ips` field.
//!
//! [`MulticastGroupIdentifier`]: params::MulticastGroupIdentifier
//! [`NameOrId`]: omicron_common::api::external::NameOrId

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use nexus_types::external_api::params::UserData;
use nexus_types::external_api::{params, views};
use nexus_types::multicast::MulticastGroupCreate as InternalMulticastGroupCreate;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, Name,
    NameOrId, Nullable,
};
use omicron_common::vlan::VlanID;
use params::{
    ExternalIpCreate, InstanceDiskAttachment,
    InstanceNetworkInterfaceAttachment,
};

/// Path parameter for multicast group operations.
///
/// Uses `NameOrId` instead of `MulticastGroupIdentifier`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupPath {
    /// Name or ID of the multicast group
    pub multicast_group: NameOrId,
}

impl From<MulticastGroupPath> for params::MulticastGroupPath {
    fn from(old: MulticastGroupPath) -> Self {
        Self { multicast_group: old.multicast_group.into() }
    }
}

/// Path parameters for multicast group member operations.
///
/// Uses `NameOrId` instead of `MulticastGroupIdentifier` for the group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberPath {
    /// Name or ID of the multicast group
    pub multicast_group: NameOrId,
    /// Name or ID of the instance
    pub instance: NameOrId,
}

impl From<MulticastGroupMemberPath> for params::MulticastGroupMemberPath {
    fn from(old: MulticastGroupMemberPath) -> Self {
        Self {
            multicast_group: old.multicast_group.into(),
            instance: old.instance,
        }
    }
}

/// Path parameters for instance multicast group operations.
///
/// Uses `NameOrId` instead of `MulticastGroupIdentifier` for the group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMulticastGroupPath {
    /// Name or ID of the instance
    pub instance: NameOrId,
    /// Name or ID of the multicast group
    pub multicast_group: NameOrId,
}

impl From<InstanceMulticastGroupPath> for params::InstanceMulticastGroupPath {
    fn from(old: InstanceMulticastGroupPath) -> Self {
        Self {
            instance: old.instance,
            multicast_group: old.multicast_group.into(),
        }
    }
}

/// Create-time parameters for a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupCreate {
    pub name: Name,
    pub description: String,
    /// The multicast IP address to allocate. If None, one will be allocated
    /// from the default pool.
    #[serde(default)]
    pub multicast_ip: Option<IpAddr>,
    /// Source IP addresses for Source-Specific Multicast (SSM).
    ///
    /// None uses default behavior (Any-Source Multicast).
    /// Empty list explicitly allows any source (Any-Source Multicast).
    /// Non-empty list restricts to specific sources (SSM).
    #[serde(default)]
    pub source_ips: Option<Vec<IpAddr>>,
    /// Name or ID of the IP pool to allocate from. If None, uses the default
    /// multicast pool.
    #[serde(default)]
    pub pool: Option<NameOrId>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// Tags packets leaving the rack to traverse VLAN-segmented upstream networks.
    ///
    /// Valid range: 2-4094 (VLAN IDs 0-1 are reserved by IEEE 802.1Q standard).
    #[serde(default)]
    pub mvlan: Option<VlanID>,
}

/// Update-time parameters for a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupUpdate {
    /// New name for the multicast group
    #[serde(default)]
    pub name: Option<Name>,
    /// New description for the multicast group
    #[serde(default)]
    pub description: Option<String>,
    /// Update source IPs for SSM
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ips: Option<Vec<IpAddr>>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// Set to null to clear the MVLAN. Valid range: 2-4094 when provided.
    /// Omit the field to leave mvlan unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mvlan: Option<Nullable<VlanID>>,
}

/// Parameters for adding an instance to a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberAdd {
    /// Name or ID of the instance to add to the multicast group
    pub instance: NameOrId,
}

impl From<MulticastGroupMemberAdd> for params::MulticastGroupMemberAdd {
    fn from(old: MulticastGroupMemberAdd) -> Self {
        Self { instance: old.instance, source_ips: None }
    }
}

/// Path parameter for looking up a multicast group by IP address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupByIpPath {
    /// IP address of the multicast group
    pub address: IpAddr,
}

impl From<MulticastGroupCreate> for InternalMulticastGroupCreate {
    fn from(old: MulticastGroupCreate) -> Self {
        Self {
            identity: IdentityMetadataCreateParams {
                name: old.name,
                description: old.description,
            },
            multicast_ip: old.multicast_ip,
            source_ips: old.source_ips,
            mvlan: old.mvlan,
        }
    }
}

/// View of a Multicast Group Member.
///
/// This version doesn't have the `multicast_ip` field which was added later.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMember {
    /// unique, immutable, system-controlled identifier for each resource
    pub id: Uuid,
    /// unique, mutable, user-controlled identifier for each resource
    pub name: Name,
    /// human-readable free-form text about a resource
    pub description: String,
    /// timestamp when this resource was created
    pub time_created: DateTime<Utc>,
    /// timestamp when this resource was last modified
    pub time_modified: DateTime<Utc>,
    /// The ID of the multicast group this member belongs to.
    pub multicast_group_id: Uuid,
    /// The ID of the instance that is a member of this group.
    pub instance_id: Uuid,
    /// Current state of the multicast group membership.
    pub state: String,
}

impl From<views::MulticastGroupMember> for MulticastGroupMember {
    fn from(v: views::MulticastGroupMember) -> Self {
        Self {
            id: v.identity.id,
            name: v.identity.name,
            description: v.identity.description,
            time_created: v.identity.time_created,
            time_modified: v.identity.time_modified,
            multicast_group_id: v.multicast_group_id,
            instance_id: v.instance_id,
            state: v.state,
        }
    }
}

/// Create-time parameters for an `Instance`.
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
    #[serde(default, with = "UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,

    /// Multicast groups this instance should be joined to upon creation.
    ///
    /// Provide a list of multicast group names or UUIDs. Newer API versions
    /// also accept multicast IP addresses.
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,

    /// A list of disks to be attached to the instance.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "bool_true")]
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

#[inline]
fn bool_true() -> bool {
    true
}

impl From<InstanceCreate> for params::InstanceCreate {
    fn from(old: InstanceCreate) -> Self {
        // Convert NameOrId to MulticastGroupIdentifier
        let multicast_groups =
            old.multicast_groups.into_iter().map(|g| g.into()).collect();

        Self {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips: old.external_ips,
            multicast_groups,
            disks: old.disks,
            boot_disk: old.boot_disk,
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
        }
    }
}

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
    /// If not provided (None), the instance's multicast group membership
    /// will not be changed.
    ///
    /// Accepts group names or UUIDs. Newer API versions also accept multicast
    /// IP addresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multicast_groups: Option<Vec<NameOrId>>,
}

impl From<InstanceUpdate> for params::InstanceUpdate {
    fn from(old: InstanceUpdate) -> Self {
        // Convert Option<Vec<NameOrId>> to Option<Vec<MulticastGroupIdentifier>>
        let multicast_groups = old
            .multicast_groups
            .map(|groups| groups.into_iter().map(|g| g.into()).collect());

        Self {
            ncpus: old.ncpus,
            memory: old.memory,
            boot_disk: old.boot_disk,
            auto_restart_policy: old.auto_restart_policy,
            cpu_platform: old.cpu_platform,
            multicast_groups,
        }
    }
}
