// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2025122300 (IP_VERSION_AND_MULTIPLE_DEFAULT_POOLS)
//! that changed in subsequent versions.
//!
//! This module contains both **views** (response bodies) and **params**
//! (request bodies) that differ from newer API versions.
//!
//! ## SiloIpPool Changes
//!
//! Valid until 2026010100 (SILO_PROJECT_IP_VERSION_AND_POOL_TYPE).
//!
//! [`SiloIpPool`] doesn't have `ip_version` or `pool_type` fields.
//! Newer versions include these fields to indicate the IP version
//! and pool type (unicast or multicast) of the pool.
//!
//! Affected endpoints:
//! - `GET /v1/ip-pools` (project_ip_pool_list)
//! - `GET /v1/ip-pools/{pool}` (project_ip_pool_view)
//! - `GET /v1/system/silos/{silo}/ip-pools` (silo_ip_pool_list)
//!
//! ## Instance Changes
//!
//! Valid until 2026010300 (DUAL_STACK_NICS).
//!
//! [`InstanceCreate`] uses `v2026010100::InstanceNetworkInterfaceAttachment`.
//!
//! ## Multicast Changes
//!
//! Valid until 2026010800 (MULTICAST_IMPLICIT_LIFECYCLE_UPDATES).
//!
//! Version 2025122300 types (before [`MulticastGroupIdentifier`] was introduced
//! and before implicit group lifecycle).
//!
//! Key differences:
//! - Uses [`NameOrId`] for multicast group references (not [`MulticastGroupIdentifier`]).
//!   Newer versions accept name, UUID, or multicast IP address, while this version
//!   only accepts name or UUID.
//! - Had explicit create/update endpoints for multicast groups (removed in newer
//!   versions which create/delete groups implicitly via member operations).
//! - [`MulticastGroupMemberAdd`] doesn't have `source_ips` field.
//!
//! Affected endpoints:
//! - `GET /v1/multicast-groups` (multicast_group_list)
//! - `GET /v1/multicast-groups/{multicast_group}` (multicast_group_view)
//! - `POST /v1/multicast-groups/{multicast_group}/members` (multicast_group_member_add)
//! - `POST /v1/instances` (instance_create)
//! - `PUT /v1/instances/{instance}` (instance_update)
//!
//! [`SiloIpPool`]: self::SiloIpPool
//! [`InstanceCreate`]: self::InstanceCreate
//! [`MulticastGroupIdentifier`]: nexus_types::external_api::params::MulticastGroupIdentifier
//! [`NameOrId`]: omicron_common::api::external::NameOrId
//! [`MulticastGroupMemberAdd`]: self::MulticastGroupMemberAdd

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use nexus_types::external_api::{params, views};
use nexus_types::multicast::MulticastGroupCreate as InternalMulticastGroupCreate;
use omicron_common::api::external::{
    self, ByteCount, Hostname, IdentityMetadata, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, Name,
    NameOrId, Nullable,
};
use omicron_common::vlan::VlanID;

use crate::{v2026010100, v2026010300};

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

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: external::IdentityMetadataCreateParams,
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
    pub network_interfaces: v2026010100::InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    ///
    /// By default, all instances have outbound connectivity, but no inbound
    /// connectivity. These external addresses can be used to provide a fixed,
    /// known IP address for making inbound connections to the instance.
    #[serde(default)]
    pub external_ips: Vec<v2026010300::ExternalIpCreate>,

    /// The multicast groups this instance should join.
    ///
    /// The instance will be automatically added as a member of the specified
    /// multicast groups during creation, enabling it to send and receive
    /// multicast traffic for those groups.
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,

    /// A list of disks to be attached to the instance.
    ///
    /// Disk attachments of type "create" will be created, while those of type
    /// "attach" must already exist.
    ///
    /// The order of this list does not guarantee a boot order for the instance.
    /// Use the boot_disk attribute to specify a boot disk. When boot_disk is
    /// specified it will count against the disk attachment limit.
    #[serde(default)]
    pub disks: Vec<params::InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    ///
    /// This disk can either be attached if it already exists or created along
    /// with the instance.
    ///
    /// Specifying a boot disk is optional but recommended to ensure predictable
    /// boot behavior. The boot disk can be set during instance creation or
    /// later if the instance is stopped. The boot disk counts against the disk
    /// attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both the
    /// instance's UEFI firmware and the guest operating system. Boot options
    /// can change as disks are attached and detached, which may result in an
    /// instance that only boots to the EFI shell until a boot disk is set.
    #[serde(default)]
    pub boot_disk: Option<params::InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    ///
    /// If not provided, all SSH public keys from the user's profile will be sent.
    /// If an empty list is provided, no public keys will be transmitted to the
    /// instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
    pub start: bool,

    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, no
    /// auto-restart policy will be explicitly configured for this instance, and
    /// the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort", so
    /// instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project basis.
    /// In that case, any configured default policy will be used if this is
    /// `null`.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,

    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,

    /// The CPU platform to be used for this instance. If this is `null`, the
    /// instance requires no particular CPU platform; when it is started the
    /// instance will have the most general CPU platform supported by the sled
    /// it is initially placed on.
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl TryFrom<InstanceCreate> for v2026010300::InstanceCreate {
    type Error = external::Error;

    fn try_from(value: InstanceCreate) -> Result<Self, Self::Error> {
        let network_interfaces = value.network_interfaces.try_into()?;
        Ok(Self {
            identity: value.identity,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            user_data: value.user_data,
            network_interfaces,
            external_ips: value.external_ips,
            multicast_groups: value.multicast_groups,
            disks: value.disks,
            boot_disk: value.boot_disk,
            ssh_public_keys: value.ssh_public_keys,
            start: value.start,
            auto_restart_policy: value.auto_restart_policy,
            anti_affinity_groups: value.anti_affinity_groups,
            cpu_platform: value.cpu_platform,
        })
    }
}
