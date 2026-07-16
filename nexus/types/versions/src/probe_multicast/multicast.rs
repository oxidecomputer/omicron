// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast types for version PROBE_MULTICAST.
//!
//! Reshapes `MulticastGroupMember` from a single `instance_id` field to a
//! `kind` / `parent_id` pair. Members may now be parented by a probe in
//! addition to an instance, so the `instance_id` field name no longer
//! reflects reality for probe-parented rows.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, ObjectIdentity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// Kind of resource that owns a multicast group member.
///
/// Selects how to interpret `parent_id`: an instance or a probe.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MulticastGroupMemberParentKind {
    Instance,
    Probe,
}

/// View of a Multicast Group Member.
///
/// A member may be parented by either an instance or a probe. The `kind`
/// discriminator selects how to interpret `parent_id`.
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
pub struct MulticastGroupMember {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The ID of the multicast group this member belongs to.
    pub multicast_group_id: Uuid,
    /// The multicast IP address of the group this member belongs to.
    pub multicast_ip: IpAddr,
    /// Discriminator for `parent_id`.
    pub kind: MulticastGroupMemberParentKind,
    /// The UUID of the parent (instance or probe) that owns this membership.
    pub parent_id: Uuid,
    /// Source IP addresses for this member's multicast subscription.
    ///
    /// - **ASM**: Sources are optional. Empty array means any source is allowed.
    ///   Non-empty array enables source filtering (IGMPv3/MLDv2).
    /// - **SSM**: Sources are required for SSM addresses (232/8, ff3x::/32).
    pub source_ips: Vec<IpAddr>,
    /// Current state of the multicast group membership.
    pub state: String,
}

// -- Conversions between PROBE_MULTICAST and v2026_01_08_00 multicast types --

/// Down-conversion to the prior API shape, which only models instance-parented
/// members. Probe-parented members have no representation in the older view,
/// so the conversion fails with `406 Not Acceptable` to signal that the row
/// post-dates the client's pinned version.
impl TryFrom<MulticastGroupMember>
    for crate::v2026_01_08_00::multicast::MulticastGroupMember
{
    type Error = dropshot::HttpError;

    fn try_from(new: MulticastGroupMember) -> Result<Self, Self::Error> {
        match new.kind {
            MulticastGroupMemberParentKind::Instance => Ok(Self {
                identity: new.identity,
                multicast_group_id: new.multicast_group_id,
                multicast_ip: new.multicast_ip,
                instance_id: new.parent_id,
                source_ips: new.source_ips,
                state: new.state,
            }),
            MulticastGroupMemberParentKind::Probe => {
                Err(dropshot::HttpError::for_client_error(
                    Some(String::from("Not Acceptable")),
                    dropshot::ClientErrorStatusCode::NOT_ACCEPTABLE,
                    String::from(
                        "multicast group member kind not supported for \
                         client version",
                    ),
                ))
            }
        }
    }
}

/// Up-conversion: older clients only saw instance-parented members, so
/// synthesize `kind = Instance` and carry `instance_id` through as `parent_id`.
impl From<crate::v2026_01_08_00::multicast::MulticastGroupMember>
    for MulticastGroupMember
{
    fn from(
        old: crate::v2026_01_08_00::multicast::MulticastGroupMember,
    ) -> Self {
        Self {
            identity: old.identity,
            multicast_group_id: old.multicast_group_id,
            multicast_ip: old.multicast_ip,
            kind: MulticastGroupMemberParentKind::Instance,
            parent_id: old.instance_id,
            source_ips: old.source_ips,
            state: old.state,
        }
    }
}
