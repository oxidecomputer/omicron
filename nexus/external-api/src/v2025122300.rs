// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025122300 to 2025122600.
//!
//! Version 2025122300 types (before [`MulticastGroupIdentifier`] was introduced
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
//! [`MulticastGroupCreate`]: self::MulticastGroupCreate
//! [`MulticastGroupUpdate`]: self::MulticastGroupUpdate
//! [`MulticastGroupMemberAdd`]: self::MulticastGroupMemberAdd

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use nexus_types::external_api::{params, views};
use nexus_types::multicast::MulticastGroupCreate as InternalMulticastGroupCreate;
use omicron_common::api::external::{
    ByteCount, IdentityMetadata, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, Name,
    NameOrId, Nullable,
};
use omicron_common::vlan::VlanID;

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
    /// Source IP addresses for source-filtered multicast.
    ///
    /// - **ASM**: Sources are optional. None or empty list allows any source.
    ///   A non-empty list enables source filtering via IGMPv3/MLDv2.
    /// - **SSM**: Sources are required for SSM addresses (232/8, ff3x::/32).
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
    /// Update source IPs for source filtering (ASM can have sources, but
    /// SSM requires them)
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
        // Note: `source_ips` is ignored because it's per-member in new version,
        // not per-group.
        //
        // The old API field is kept for backward compatibility but ignored.
        // We still use `has_sources` for pool selection preference.
        let has_sources =
            old.source_ips.as_ref().is_some_and(|s| !s.is_empty());
        Self {
            identity: IdentityMetadataCreateParams {
                name: old.name,
                description: old.description,
            },
            multicast_ip: old.multicast_ip,
            mvlan: old.mvlan,
            has_sources,
            // Old API version doesn't have ip_version preference
            ip_version: None,
        }
    }
}

/// View of a Multicast Group.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The multicast IP address held by this resource.
    pub multicast_ip: IpAddr,
    /// Source IP addresses for multicast source filtering (SSM requires these;
    /// ASM can optionally use them via IGMPv3/MLDv2). Empty array means any source.
    pub source_ips: Vec<IpAddr>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// None means no VLAN tagging on egress.
    pub mvlan: Option<VlanID>,
    /// The ID of the IP pool this resource belongs to.
    pub ip_pool_id: Uuid,
    /// Current state of the multicast group.
    pub state: String,
}

impl From<views::MulticastGroup> for MulticastGroup {
    fn from(v: views::MulticastGroup) -> Self {
        Self {
            identity: v.identity,
            multicast_ip: v.multicast_ip,
            source_ips: v.source_ips,
            mvlan: v.mvlan,
            ip_pool_id: v.ip_pool_id,
            state: v.state,
        }
    }
}

/// View of a Multicast Group Member.
///
/// This version omits `multicast_ip` and `source_ips` fields added in later versions.
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

/// Parameters of an `Instance` that can be reconfigured after creation.
///
/// This version uses `Vec<NameOrId>` for multicast_groups instead of
/// `Vec<MulticastGroupJoinSpec>` in newer versions.
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
        Self {
            ncpus: old.ncpus,
            memory: old.memory,
            boot_disk: old.boot_disk,
            auto_restart_policy: old.auto_restart_policy,
            cpu_platform: old.cpu_platform,
            multicast_groups: old.multicast_groups.map(|groups| {
                groups
                    .into_iter()
                    .map(|g| params::MulticastGroupJoinSpec {
                        group: g.into(),
                        source_ips: None,
                        ip_version: None,
                    })
                    .collect()
            }),
        }
    }
}

/// An IP pool in the context of a silo (pre-2026010100 API version).
///
/// This version does not include `ip_version` or `pool_type` fields.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloIpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo.
    pub is_default: bool,
}

impl From<views::SiloIpPool> for SiloIpPool {
    fn from(new: views::SiloIpPool) -> SiloIpPool {
        SiloIpPool { identity: new.identity, is_default: new.is_default }
    }
}
