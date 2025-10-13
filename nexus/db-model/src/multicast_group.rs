// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database model types for multicast groups and their membership.
//!
//! This module implements the bifurcated multicast design from
//! [RFD 488](https://rfd.shared.oxide.computer/rfd/488), supporting two types
//! of multicast groups:
//!
//! ## External Multicast Groups
//!
//! Customer-facing multicast groups allocated from IP pools. These groups:
//! - Use IPv4/IPv6 addresses from customer IP pools
//! - Are exposed via customer APIs for application multicast traffic
//! - Support Source-Specific Multicast (SSM) with configurable source IPs
//! - Follow the Resource trait pattern for user-facing identity management
//! - Are **fleet-scoped** (not project-scoped) to enable cross-project multicast
//! - All use `DEFAULT_MULTICAST_VNI` (77) for consistent fleet-wide behavior
//!
//! ### VNI and Security Model
//!
//! External multicast groups use VNI 77, a reserved system VNI below
//! `MIN_GUEST_VNI` (1024). This differs from VPC unicast traffic where each
//! VPC receives its own VNI for tenant isolation.
//!
//! The shared VNI design reflects multicast's fleet-scoped authorization model:
//! groups are fleet resources (like IP pools) that can span projects and silos.
//! Forwarding occurs through Dendrite's bifurcated NAT architecture, which
//! translates external multicast addresses to underlay IPv6 groups at the switch.
//!
//! **VNI Selection**: RFD 488 discusses using an "arbitrary multicast VNI for
//! multicast groups spanning VPCs" since we don't need VPC-specific VNIs for
//! groups that transcend VPC boundaries. VNI 77 serves as this default/arbitrary
//! VNI for all external multicast groups. Future implementations may support
//! per-VPC multicast VNIs if VPC-isolated multicast groups become necessary.
//!
//! Security enforcement occurs at two layers:
//! - **Control plane**: Fleet admins create groups; users attach instances via API
//! - **Dataplane**: Switch hardware validates underlay group membership
//!
//! This enables cross-project and cross-silo multicast while maintaining explicit
//! membership control through the underlay forwarding tables.
//!
//! ## Underlay Multicast Groups
//!
//! System-generated admin-scoped IPv6 multicast groups for internal forwarding:
//! - Use IPv6 admin-local scope (ff04::/16) per RFC 7346
//!   <https://www.rfc-editor.org/rfc/rfc7346>
//! - Paired 1:1 with external groups for NAT-based forwarding
//! - Handle rack-internal multicast traffic between switches
//! - Use individual field pattern for system resources
//!
//! ## Member Lifecycle (handled by RPW)
//!
//! Multicast group members follow a 3-state lifecycle managed by the
//! Reliable Persistent Workflow (RPW) reconciler:
//! - ["Joining"](MulticastGroupMemberState::Joining): Member created, awaiting
//!   dataplane configuration (via DPD)
//! - ["Joined"](MulticastGroupMemberState::Joined): Member configuration applied
//!   in the dataplane, ready to receive multicast traffic
//! - ["Left"](MulticastGroupMemberState::Left): Member configuration removed from
//!   the dataplane (e.g., instance stopped/migrated)
//! - If an instance is deleted, the member will be marked for removal with a
//!   deleted timestamp, and the reconciler will remove it from the dataplane
//!
//! The RPW ensures eventual consistency between database state and dataplane
//! configuration (applied via DPD to switches).

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use diesel::{
    AsChangeset, AsExpression, FromSqlRow, Insertable, Queryable, Selectable,
};
use ipnetwork::IpNetwork;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use db_macros::Resource;
use nexus_db_schema::schema::{
    multicast_group, multicast_group_member, underlay_multicast_group,
};
use nexus_types::external_api::views;
use nexus_types::identity::Resource as IdentityResource;
use omicron_common::api::external::{self, IdentityMetadata};
use omicron_common::vlan::VlanID;
use omicron_uuid_kinds::SledKind;

use crate::typed_uuid::DbTypedUuid;
use crate::{Generation, Name, Vni, impl_enum_type};

impl_enum_type!(
    MulticastGroupStateEnum:

    #[derive(Clone, Copy, Debug, PartialEq, Eq, AsExpression, FromSqlRow, Serialize, Deserialize, JsonSchema)]
    pub enum MulticastGroupState;

    Creating => b"creating"
    Active => b"active"
    Deleting => b"deleting"
    Deleted => b"deleted"
);

impl_enum_type!(
    MulticastGroupMemberStateEnum:

    #[derive(Clone, Copy, Debug, PartialEq, Eq, AsExpression, FromSqlRow, Serialize, Deserialize, JsonSchema)]
    pub enum MulticastGroupMemberState;

    Joining => b"joining"
    Joined => b"joined"
    Left => b"left"
);

impl std::fmt::Display for MulticastGroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MulticastGroupState::Creating => "Creating",
            MulticastGroupState::Active => "Active",
            MulticastGroupState::Deleting => "Deleting",
            MulticastGroupState::Deleted => "Deleted",
        })
    }
}

impl std::fmt::Display for MulticastGroupMemberState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MulticastGroupMemberState::Joining => "Joining",
            MulticastGroupMemberState::Joined => "Joined",
            MulticastGroupMemberState::Left => "Left",
        })
    }
}

/// Type alias for lookup resource naming convention.
///
/// This alias maps the generic name [MulticastGroup] to [ExternalMulticastGroup],
/// following the pattern used throughout Omicron where the user-facing resource
/// uses the simpler name. External multicast groups are the primary user-facing
/// multicast resources, while underlay groups are internal infrastructure.
pub type MulticastGroup = ExternalMulticastGroup;

/// An external multicast group for delivering packets to multiple recipients.
///
/// External groups are multicast groups allocated from IP pools. These are
/// distinct from [UnderlayMulticastGroup] which are system-generated IPv6 addresses for
/// NAT mapping.
#[derive(
    Queryable,
    Selectable,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = multicast_group)]
pub struct ExternalMulticastGroup {
    #[diesel(embed)]
    pub identity: ExternalMulticastGroupIdentity,
    /// IP pool this address was allocated from.
    pub ip_pool_id: Uuid,
    /// IP pool range this address was allocated from.
    pub ip_pool_range_id: Uuid,
    /// VNI for multicast group (derived or random).
    pub vni: Vni,
    /// Primary multicast IP address (overlay/external).
    pub multicast_ip: IpNetwork,
    /// Source IP addresses for Source-Specific Multicast (SSM).
    /// Empty array means any source is allowed.
    pub source_ips: Vec<IpNetwork>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    ///
    /// When specified, this VLAN ID is passed to switches (via DPD) as part of
    /// the `ExternalForwarding` configuration to tag multicast packets leaving
    /// the rack. This enables multicast traffic to traverse VLAN-segmented
    /// upstream networks (e.g., peering with external multicast sources/receivers
    /// on specific VLANs).
    ///
    /// The MVLAN value is sent to switches during group creation/updates and
    /// controls VLAN tagging for egress traffic only; it does not affect ingress
    /// multicast traffic received by the rack. Switch port selection for egress
    /// traffic remains pending (see TODO at `nexus/src/app/multicast/dataplane.rs:113-115`).
    ///
    /// Valid range when specified: 2-4094 (IEEE 802.1Q; Dendrite requires >= 2).
    ///
    /// Database Type: i16 (INT2) - this field uses `i16` (INT2) for storage
    /// efficiency, unlike other VLAN columns in the schema which use `SqlU16`
    /// (forcing INT4). Direct `i16` is appropriate here since VLANs fits in
    /// INT2's range.
    pub mvlan: Option<i16>,
    /// Associated underlay group for NAT.
    /// Initially None in ["Creating"](MulticastGroupState::Creating) state,
    /// populated by reconciler when group becomes ["Active"](MulticastGroupState::Active).
    pub underlay_group_id: Option<Uuid>,
    /// Rack ID multicast group was created on.
    pub rack_id: Uuid,
    /// Group tag for lifecycle management.
    pub tag: Option<String>,
    /// Current state of the multicast group (RPW pattern).
    /// See [MulticastGroupState] for possible values.
    pub state: MulticastGroupState,
    /// Version when this group was added.
    pub version_added: Generation,
    /// Version when this group was removed.
    pub version_removed: Option<Generation>,
}

/// Values used to create a [MulticastGroupMember] in the database.
///
/// This struct is used for database insertions and omits fields that are
/// automatically populated by the database (like version_added and version_removed
/// which use DEFAULT nextval() sequences). For complete member records with all
/// fields populated, use [MulticastGroupMember].
#[derive(Insertable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name = multicast_group_member)]
pub struct MulticastGroupMemberValues {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub external_group_id: Uuid,
    pub parent_id: Uuid,
    pub sled_id: Option<DbTypedUuid<SledKind>>,
    pub state: MulticastGroupMemberState,
    // version_added and version_removed are omitted - database assigns these
    // via DEFAULT nextval()
}

/// A member of a multicast group (instance that receives multicast traffic).
#[derive(
    Queryable,
    Selectable,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[diesel(table_name = multicast_group_member)]
pub struct MulticastGroupMember {
    /// Unique identifier for this multicast group member.
    pub id: Uuid,
    /// Timestamp for creation of this multicast group member.
    pub time_created: DateTime<Utc>,
    /// Timestamp for last modification of this multicast group member.
    pub time_modified: DateTime<Utc>,
    /// Timestamp for deletion of this multicast group member, if applicable.
    pub time_deleted: Option<DateTime<Utc>>,
    /// External multicast group this member belongs to.
    pub external_group_id: Uuid,
    /// Parent instance or service that receives multicast traffic.
    pub parent_id: Uuid,
    /// Sled hosting the parent instance.
    pub sled_id: Option<DbTypedUuid<SledKind>>,
    /// Current state of the multicast group member (RPW pattern).
    /// See [MulticastGroupMemberState] for possible values.
    pub state: MulticastGroupMemberState,
    /// Version when this member was added.
    pub version_added: Generation,
    /// Version when this member was removed.
    pub version_removed: Option<Generation>,
}

// Conversions to external API views

impl TryFrom<ExternalMulticastGroup> for views::MulticastGroup {
    type Error = external::Error;

    fn try_from(group: ExternalMulticastGroup) -> Result<Self, Self::Error> {
        let mvlan = group
            .mvlan
            .map(|vlan| VlanID::new(vlan as u16))
            .transpose()
            .map_err(|e| {
                external::Error::internal_error(&format!(
                    "invalid VLAN ID: {e:#}"
                ))
            })?;

        Ok(views::MulticastGroup {
            identity: group.identity(),
            multicast_ip: group.multicast_ip.ip(),
            source_ips: group
                .source_ips
                .into_iter()
                .map(|ip| ip.ip())
                .collect(),
            mvlan,
            ip_pool_id: group.ip_pool_id,
            state: group.state.to_string(),
        })
    }
}

impl TryFrom<MulticastGroupMember> for views::MulticastGroupMember {
    type Error = external::Error;

    fn try_from(member: MulticastGroupMember) -> Result<Self, Self::Error> {
        Ok(views::MulticastGroupMember {
            identity: IdentityMetadata {
                id: member.id,
                name: format!("member-{}", member.id).parse().map_err(|e| {
                    external::Error::internal_error(&format!(
                        "generated member name is invalid: {e}"
                    ))
                })?,
                description: format!("multicast group member {}", member.id),
                time_created: member.time_created,
                time_modified: member.time_modified,
            },
            multicast_group_id: member.external_group_id,
            instance_id: member.parent_id,
            state: member.state.to_string(),
        })
    }
}

/// An incomplete external multicast group, used to store state required for
/// issuing the database query that selects an available multicast IP and stores
/// the resulting record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompleteExternalMulticastGroup {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub ip_pool_id: Uuid,
    pub source_ips: Vec<IpNetwork>,
    // Optional address requesting that a specific multicast IP address be
    // allocated or provided
    pub explicit_address: Option<IpNetwork>,
    pub mvlan: Option<i16>,
    pub vni: Vni,
    pub tag: Option<String>,
    pub rack_id: Uuid,
}

/// Parameters for creating an incomplete external multicast group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompleteExternalMulticastGroupParams {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub ip_pool_id: Uuid,
    pub rack_id: Uuid,
    pub explicit_address: Option<IpAddr>,
    pub source_ips: Vec<IpNetwork>,
    pub mvlan: Option<i16>,
    pub vni: Vni,
    pub tag: Option<String>,
}

impl IncompleteExternalMulticastGroup {
    /// Create an incomplete multicast group from parameters.
    pub fn new(params: IncompleteExternalMulticastGroupParams) -> Self {
        Self {
            id: params.id,
            name: params.name,
            description: params.description,
            time_created: Utc::now(),
            ip_pool_id: params.ip_pool_id,
            source_ips: params.source_ips,
            explicit_address: params.explicit_address.map(|ip| ip.into()),
            mvlan: params.mvlan,
            vni: params.vni,
            tag: params.tag,
            rack_id: params.rack_id,
        }
    }
}

impl MulticastGroupMember {
    /// Generate a new multicast group member.
    ///
    /// Note: version_added will be set by the database sequence when inserted.
    pub fn new(
        id: Uuid,
        external_group_id: Uuid,
        parent_id: Uuid,
        sled_id: Option<DbTypedUuid<SledKind>>,
    ) -> Self {
        Self {
            id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
            external_group_id,
            parent_id,
            sled_id,
            state: MulticastGroupMemberState::Joining,
            // Placeholder - will be overwritten by database sequence on insert
            version_added: Generation::new(),
            version_removed: None,
        }
    }
}

/// Database representation of an underlay multicast group.
///
/// Underlay groups are system-generated admin-scoped IPv6 multicast addresses
/// used as a NAT target for internal multicast traffic.
///
/// These are distinct from [ExternalMulticastGroup] which are external-facing
/// addresses allocated from IP pools, specified by users or applications.
#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = underlay_multicast_group)]
pub struct UnderlayMulticastGroup {
    /// Unique identifier for this underlay multicast group.
    pub id: Uuid,
    /// Timestamp for creation of this underlay multicast group.
    pub time_created: DateTime<Utc>,
    /// Timestamp for last modification of this underlay multicast group.
    pub time_modified: DateTime<Utc>,
    /// Timestamp for deletion of this underlay multicast group, if applicable.
    pub time_deleted: Option<DateTime<Utc>>,
    /// Admin-scoped IPv6 multicast address (NAT target).
    pub multicast_ip: IpNetwork,
    /// VNI for this multicast group.
    pub vni: Vni,
    /// Group tag for lifecycle management.
    pub tag: Option<String>,
    /// Version when this group was added.
    pub version_added: Generation,
    /// Version when this group was removed.
    pub version_removed: Option<Generation>,
}

impl UnderlayMulticastGroup {
    /// Get the VNI as a u32.
    pub fn vni(&self) -> u32 {
        self.vni.0.into()
    }
}

/// Update data for a multicast group.
#[derive(AsChangeset, Debug, PartialEq, Eq)]
#[diesel(table_name = multicast_group)]
pub struct ExternalMulticastGroupUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub source_ips: Option<Vec<IpNetwork>>,
    // Needs to be double Option so we can set a value of null in the DB by
    // passing Some(None). None by itself is ignored by Diesel.
    pub mvlan: Option<Option<i16>>,
    pub time_modified: DateTime<Utc>,
}

impl From<nexus_types::external_api::params::MulticastGroupUpdate>
    for ExternalMulticastGroupUpdate
{
    fn from(
        params: nexus_types::external_api::params::MulticastGroupUpdate,
    ) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            source_ips: params
                .source_ips
                .map(|ips| ips.into_iter().map(IpNetwork::from).collect()),
            // mvlan is always None here - handled manually in datastore
            mvlan: None,
            time_modified: Utc::now(),
        }
    }
}
