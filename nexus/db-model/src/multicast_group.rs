// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database models for multicast groups and membership.
//!
//! Implements the bifurcated multicast design from
//! [RFD 488](https://rfd.shared.oxide.computer/rfd/488), with two types
//! of multicast groups:
//!
//! ## External Multicast Groups
//!
//! Customer-facing groups allocated from IP pools:
//! - Use IPv4/IPv6 addresses from customer IP pools
//! - Exposed via customer APIs for application multicast traffic
//! - Support source filtering via per-member source IPs (IGMPv3/MLDv2)
//! - Follow the Resource trait pattern for user-facing identity management
//! - **Fleet-scoped** (not project-scoped) to enable cross-project multicast
//! - All use `DEFAULT_MULTICAST_VNI` (77) for consistent fleet-scoped behavior
//!
//! ### VNI and Security Model
//!
//! External multicast groups use VNI 77 (i.e. an arbitrary VNI), a reserved
//! system VNI below `MIN_GUEST_VNI` (1024). This differs from VPC unicast
//! traffic where each VPC receives its own VNI for tenant isolation.
//!
//! The shared VNI design reflects multicast's fleet-scoped authorization model:
//! groups are fleet-scoped resources that can span projects and silos.
//! Forwarding occurs through Dendrite's bifurcated NAT architecture, which
//! translates external multicast addresses to underlay IPv6 groups at the switch.
//!
//! **VNI Selection**: RFD 488 discusses using an "arbitrary multicast VNI for
//! multicast groups spanning VPCs" since we don't need VPC-specific VNIs for
//! groups that transcend VPC boundaries. VNI 77 is this default VNI for all
//! external multicast groups. Future implementations may support per-VPC
//! multicast VNIs if VPC-isolated multicast groups become necessary.
//!
//! Security happens at two layers:
//! - **Control plane**: groups created implicitly via member-add; pool linking
//!   controls access
//! - **Dataplane**: switch dataplane validates underlay group membership
//!
//! This allows cross-project and cross-silo multicast while maintaining explicit
//! membership control through underlay forwarding tables.
//!
//! ## Underlay Multicast Groups
//!
//! System-generated admin-local IPv6 multicast groups for internal forwarding:
//! - Use IPv6 admin-local multicast scope (ff04::/16) per [RFC 7346]
//!   with addresses allocated within [`UNDERLAY_MULTICAST_SUBNET`] (ff04::/64)
//! - Paired 1:1 with external groups for NAT-based forwarding
//! - Handle rack-internal multicast traffic between switches
//! - Use individual field pattern for system resources
//!
//! [RFC 7346]: https://www.rfc-editor.org/rfc/rfc7346
//!
//! ## Member Lifecycle (handled by RPW)
//!
//! Multicast group members follow a 3-state lifecycle managed by the Reliable
//! Persistent Workflow (RPW) reconciler. Nexus owns the member database
//! lifecycle, per-sled forwarding propagation, and OPTE subscriptions.
//! Rear-port underlay membership is not tracked per member. `ddmd` derives it
//! from DDM peer subscriptions and programs it into the switch dataplane via
//! mg-lower/DDM.
//!
//! - ["Joining"](MulticastGroupMemberState::Joining): Member created, awaiting
//!   group activation and sled assignment
//! - ["Joined"](MulticastGroupMemberState::Joined): Member is associated with
//!   a live VMM. Nexus has propagated sled forwarding state and subscribed
//!   the VMM's OPTE port to the group
//! - ["Left"](MulticastGroupMemberState::Left): Member should not receive
//!   traffic once cleanup converges (e.g., instance stopping/stopped, explicit
//!   detach, delete)
//!
//! Migration and deletion are handled specially:
//!
//! - Migration is not a leave: the reconciler reconfigures the member in place,
//!   updating the recorded `sled_id`, re-propagating sled forwarding state, and
//!   subscribing the VMM on the new sled while the member stays "Joined".
//! - Deletion marks the member for removal with a deleted timestamp, after
//!   which the reconciler finishes cleanup.
//!
//! [`UNDERLAY_MULTICAST_SUBNET`]: omicron_common::address::UNDERLAY_MULTICAST_SUBNET

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use diesel::{AsExpression, FromSqlRow, Insertable, Queryable, Selectable};
use ipnetwork::IpNetwork;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use db_macros::Resource;
use nexus_db_schema::schema::{
    multicast_group, multicast_group_member, underlay_multicast_group,
};
use nexus_types::external_api::multicast as multicast_types;
use omicron_common::api::external::{self, IdentityMetadata};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, ProbeUuid, SledKind};

use crate::typed_uuid::DbTypedUuid;
use crate::{Generation, Name, SqlU8, Vni, impl_enum_type};

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

impl_enum_type!(
    MulticastGroupMemberOriginEnum:

    #[derive(Clone, Copy, Debug, PartialEq, Eq, AsExpression, FromSqlRow, Serialize, Deserialize, JsonSchema)]
    pub enum MulticastGroupMemberOrigin;

    Static => b"static"
    IgmpSnooped => b"igmp_snooped"
);

impl_enum_type!(
    MulticastGroupMemberParentKindEnum:

    #[derive(Clone, Copy, Debug, PartialEq, Eq, AsExpression, FromSqlRow, Serialize, Deserialize, JsonSchema)]
    pub enum MulticastGroupMemberParentKind;

    Instance => b"instance"
    Probe => b"probe"
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

impl std::fmt::Display for MulticastGroupMemberOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MulticastGroupMemberOrigin::Static => "Static",
            MulticastGroupMemberOrigin::IgmpSnooped => "IgmpSnooped",
        })
    }
}

impl std::fmt::Display for MulticastGroupMemberParentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MulticastGroupMemberParentKind::Instance => "Instance",
            MulticastGroupMemberParentKind::Probe => "Probe",
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
    /// VNI for multicast group.
    pub vni: Vni,
    /// IP pool this address was allocated from.
    pub ip_pool_id: Uuid,
    /// IP pool range this address was allocated from.
    pub ip_pool_range_id: Uuid,
    /// Primary multicast IP address (overlay/external).
    pub multicast_ip: IpNetwork,
    /// Associated underlay group for NAT.
    /// Initially None in ["Creating"](MulticastGroupState::Creating) state,
    /// populated by reconciler when group becomes ["Active"](MulticastGroupState::Active).
    pub underlay_group_id: Option<Uuid>,
    /// DPD-client tag used to couple external (overlay) and underlay entries
    /// for this multicast group.
    ///
    /// Format: `{uuid}:{multicast_ip}`. Computed during INSERT to ensure
    /// uniqueness across the group's lifecycle and prevent tag collision
    /// when group names are reused after deletion.
    pub tag: Option<String>,
    /// Current state of the multicast group (RPW pattern).
    /// See [MulticastGroupState] for possible values.
    pub state: MulticastGroupState,
    /// Version when this group was added.
    pub version_added: Generation,
    /// Version when this group was removed.
    pub version_removed: Option<Generation>,
    /// Salt used for XOR-fold collision avoidance when computing underlay IP.
    ///
    /// The underlay IP is computed deterministically from the external multicast
    /// IP using XOR-folding. In the rare case of a collision (computed underlay
    /// IP already in use), this salt is incremented and the mapping is retried.
    /// The salt must be stored to enable deterministic reconstruction of the
    /// underlay IP from the external IP.
    ///
    /// - `None` or `0`: Default, no salt applied
    /// - `1..255`: Salt value used for collision avoidance
    pub underlay_salt: Option<SqlU8>,
}

/// A reference to the resource that owns a multicast group member (its
/// "parent"), distinguishing between the parent kinds the control
/// plane supports.
///
/// Multicast group members can have either an instance or a probe (used
/// for testing network connectivity) as their parent. The two kinds resolve
/// their hosting sled differently. Instances look up their `sled_id` through
/// the `vmm` table via `instance.active_propolis_id`, while probes store `sled`
/// directly on the probe row. The parent kind also determines which sled-agent
/// endpoints the reconciler invokes for join/leave actions.
///
/// The discriminator is persisted on the member row as `parent_kind`
/// (see [`MulticastGroupMemberParentKind`]), following the same pattern as
/// [`NetworkInterfaceKind`].
///
/// [`NetworkInterfaceKind`]: crate::NetworkInterfaceKind
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemberParentRef {
    /// Member is owned by an instance. The hosting sled is resolved via
    /// `instance` joined with `vmm` on `active_propolis_id`.
    Instance(InstanceUuid),
    /// Member is owned by a probe. The hosting sled is read directly
    /// from the `probe` row.
    Probe(ProbeUuid),
}

impl MemberParentRef {
    /// Returns the underlying parent UUID without distinguishing kind.
    pub fn as_uuid(&self) -> Uuid {
        match self {
            MemberParentRef::Instance(id) => *id.as_untyped_uuid(),
            MemberParentRef::Probe(id) => *id.as_untyped_uuid(),
        }
    }

    /// Returns the persisted [`MulticastGroupMemberParentKind`] discriminator
    /// for this parent.
    pub fn kind(&self) -> MulticastGroupMemberParentKind {
        match self {
            MemberParentRef::Instance(_) => {
                MulticastGroupMemberParentKind::Instance
            }
            MemberParentRef::Probe(_) => MulticastGroupMemberParentKind::Probe,
        }
    }
}

impl From<InstanceUuid> for MemberParentRef {
    fn from(id: InstanceUuid) -> Self {
        MemberParentRef::Instance(id)
    }
}

impl From<ProbeUuid> for MemberParentRef {
    fn from(id: ProbeUuid) -> Self {
        MemberParentRef::Probe(id)
    }
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
    pub multicast_ip: IpNetwork,
    /// Source IPs for source-filtered multicast (optional for ASM, required for SSM).
    pub source_ips: Vec<IpNetwork>,
    pub parent_kind: MulticastGroupMemberParentKind,
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
    /// Parent instance or probe that receives multicast traffic.
    pub parent_id: Uuid,
    /// Sled hosting the parent.
    pub sled_id: Option<DbTypedUuid<SledKind>>,
    /// Current state of the multicast group member (RPW pattern).
    /// See [MulticastGroupMemberState] for possible values.
    pub state: MulticastGroupMemberState,
    /// Version when this member was added.
    pub version_added: Generation,
    /// Version when this member was removed.
    pub version_removed: Option<Generation>,
    /// The multicast IP address of the group this member belongs to.
    pub multicast_ip: IpNetwork,
    /// Source IPs for source-filtered multicast (optional for ASM, required for SSM).
    pub source_ips: Vec<IpNetwork>,
    /// Origin of this membership.
    ///
    /// [`Static`](MulticastGroupMemberOrigin::Static) for administratively
    /// configured (API) membership;
    /// [`IgmpSnooped`](MulticastGroupMemberOrigin::IgmpSnooped) for dynamic
    /// soft-state learned from snooped IGMP/MLD reports (RFD 488). Only snooped
    /// rows are subject to soft-state expiry.
    pub membership_origin: MulticastGroupMemberOrigin,
    /// Discriminator for `parent_id`.
    pub parent_kind: MulticastGroupMemberParentKind,
}

// Conversions to external API views

impl TryFrom<MulticastGroupMember> for multicast_types::MulticastGroupMember {
    type Error = external::Error;

    fn try_from(member: MulticastGroupMember) -> Result<Self, Self::Error> {
        let parent = member.parent_ref();
        let kind = match parent {
            MemberParentRef::Instance(_) => {
                multicast_types::MulticastGroupMemberParentKind::Instance
            }
            MemberParentRef::Probe(_) => {
                multicast_types::MulticastGroupMemberParentKind::Probe
            }
        };
        Ok(multicast_types::MulticastGroupMember {
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
            multicast_ip: member.multicast_ip.ip(),
            kind,
            parent_id: parent.as_uuid(),
            source_ips: member
                .source_ips
                .into_iter()
                .map(|ip| ip.ip())
                .collect(),
            state: member.state.to_string(),
        })
    }
}

/// An incomplete external multicast group, used to store state required for
/// issuing the database query that selects an available multicast IP and stores
/// the resulting record. Tag is computed in SQL as `{uuid}:{multicast_ip}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompleteExternalMulticastGroup {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub ip_pool_id: Uuid,
    /// Optional address requesting a specific multicast IP be allocated.
    pub explicit_address: Option<IpNetwork>,
    pub vni: Vni,
}

/// Parameters for creating an incomplete external multicast group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncompleteExternalMulticastGroupParams {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub ip_pool_id: Uuid,
    pub explicit_address: Option<IpAddr>,
    pub vni: Vni,
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
            explicit_address: params.explicit_address.map(|ip| ip.into()),
            vni: params.vni,
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
        multicast_ip: IpNetwork,
        parent: MemberParentRef,
        sled_id: Option<DbTypedUuid<SledKind>>,
        source_ips: Vec<IpNetwork>,
    ) -> Self {
        Self {
            id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
            external_group_id,
            multicast_ip,
            parent_id: parent.as_uuid(),
            parent_kind: parent.kind(),
            sled_id,
            state: MulticastGroupMemberState::Joining,
            source_ips,
            membership_origin: MulticastGroupMemberOrigin::Static,
            // Placeholder - will be overwritten by database sequence on insert
            version_added: Generation::new(),
            version_removed: None,
        }
    }

    /// Reconstruct the typed [`MemberParentRef`] from the persisted
    /// `(parent_id, parent_kind)` pair. The inverse of the split done in
    /// [`MulticastGroupMember::new`].
    pub fn parent_ref(&self) -> MemberParentRef {
        match self.parent_kind {
            MulticastGroupMemberParentKind::Instance => {
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    self.parent_id,
                ))
            }
            MulticastGroupMemberParentKind::Probe => MemberParentRef::Probe(
                ProbeUuid::from_untyped_uuid(self.parent_id),
            ),
        }
    }

    /// Typed accessor for the parent when it is an instance.
    ///
    /// # Returns
    ///
    /// `Some(InstanceUuid)` if `parent_kind` is
    /// [`MulticastGroupMemberParentKind::Instance`], `None` otherwise.
    pub fn instance_id(&self) -> Option<InstanceUuid> {
        match self.parent_ref() {
            MemberParentRef::Instance(id) => Some(id),
            MemberParentRef::Probe(_) => None,
        }
    }

    /// Typed accessor for the parent when it is a probe.
    ///
    /// # Returns
    ///
    /// `Some(ProbeUuid)` if `parent_kind` is
    /// [`MulticastGroupMemberParentKind::Probe`], `None` otherwise.
    pub fn probe_id(&self) -> Option<ProbeUuid> {
        match self.parent_ref() {
            MemberParentRef::Probe(id) => Some(id),
            MemberParentRef::Instance(_) => None,
        }
    }
}

/// Database representation of an underlay multicast group.
///
/// Underlay groups are system-generated admin-local IPv6 multicast addresses
/// used as a NAT target for internal multicast traffic. Underlay groups are
/// VNI-agnostic; the VNI is an overlay identifier carried by [ExternalMulticastGroup].
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
    /// Admin-local IPv6 multicast address (NAT target).
    pub multicast_ip: IpNetwork,
    /// Dendrite tag used to couple external/underlay state for this group.
    ///
    /// Matches the tag on the paired [ExternalMulticastGroup] so Dendrite can treat
    /// the overlay and underlay entries as a logical unit. Since tags are derived
    /// from unique group names, each active group has a unique tag, ensuring
    /// tag-based operations (like cleanup) affect only this group's configuration.
    /// See [ExternalMulticastGroup::tag] for complete semantics.
    pub tag: Option<String>,
    /// Version when this group was added.
    pub version_added: Generation,
    /// Version when this group was removed.
    pub version_removed: Option<Generation>,
}
