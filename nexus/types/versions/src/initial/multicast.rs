// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, NameOrId, ObjectIdentity,
};
use omicron_common::vlan::VlanID;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// View of a Multicast Group
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
pub struct MulticastGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The multicast IP address held by this resource.
    pub multicast_ip: IpAddr,
    /// Source IP addresses for Source-Specific Multicast (SSM).
    /// Empty array means any source is allowed.
    pub source_ips: Vec<IpAddr>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// None means no VLAN tagging on egress.
    pub mvlan: Option<VlanID>,
    /// The ID of the IP pool this resource belongs to.
    pub ip_pool_id: Uuid,
    /// Current state of the multicast group.
    pub state: String,
}

/// View of a Multicast Group Member (instance belonging to a multicast group)
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
pub struct MulticastGroupMember {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The ID of the multicast group this member belongs to.
    pub multicast_group_id: Uuid,
    /// The ID of the instance that is a member of this group.
    pub instance_id: Uuid,
    /// Current state of the multicast group membership.
    pub state: String,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct MulticastGroupSelector {
    /// Name or ID of the multicast group (fleet-scoped)
    pub multicast_group: NameOrId,
}

// MULTICAST PARAMS

use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams, Nullable,
};
use serde::de;

/// Path parameter for multicast group lookup by IP address.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupIpLookupPath {
    /// IP address of the multicast group
    pub address: IpAddr,
}

/// Create-time parameters for a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The multicast IP address to allocate. If None, one will be allocated
    /// from the default pool.
    #[serde(default, deserialize_with = "validate_multicast_ip_param")]
    pub multicast_ip: Option<IpAddr>,
    /// Source IP addresses for Source-Specific Multicast (SSM).
    ///
    /// None uses default behavior (Any-Source Multicast).
    /// Empty list explicitly allows any source (Any-Source Multicast).
    /// Non-empty list restricts to specific sources (SSM).
    #[serde(default, deserialize_with = "validate_source_ips_param")]
    pub source_ips: Option<Vec<IpAddr>>,
    /// Name or ID of the IP pool to allocate from. If None, uses the default
    /// multicast pool.
    #[serde(default)]
    pub pool: Option<NameOrId>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// Tags packets leaving the rack to traverse VLAN-segmented upstream networks.
    ///
    /// Valid range: 2-4094 (VLAN IDs 0-1 are reserved by IEEE 802.1Q standard).
    #[serde(default, deserialize_with = "validate_mvlan_option")]
    pub mvlan: Option<VlanID>,
}

/// Update-time parameters for a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    #[serde(
        default,
        deserialize_with = "validate_source_ips_param",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_ips: Option<Vec<IpAddr>>,
    /// Multicast VLAN (MVLAN) for egress multicast traffic to upstream networks.
    /// Set to null to clear the MVLAN. Valid range: 2-4094 when provided.
    /// Omit the field to leave mvlan unchanged.
    #[serde(
        default,
        deserialize_with = "validate_mvlan_option_nullable",
        skip_serializing_if = "Option::is_none"
    )]
    pub mvlan: Option<Nullable<VlanID>>,
}

/// Parameters for adding an instance to a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberAdd {
    /// Name or ID of the instance to add to the multicast group
    pub instance: NameOrId,
}

/// Parameters for removing an instance from a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberRemove {
    /// Name or ID of the instance to remove from the multicast group
    pub instance: NameOrId,
}

/// Path parameters for multicast group member operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberPath {
    /// Name or ID of the multicast group
    pub multicast_group: NameOrId,
    /// Name or ID of the instance
    pub instance: NameOrId,
}

/// Path parameters for instance multicast group operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMulticastGroupPath {
    /// Name or ID of the instance
    pub instance: NameOrId,
    /// Name or ID of the multicast group
    pub multicast_group: NameOrId,
}

// MVLAN validators

/// Dendrite requires VLAN IDs >= 2 (rejects 0 and 1)
///
/// Valid range is 2-4094
fn validate_mvlan(vlan_id: VlanID) -> Result<VlanID, String> {
    let value: u16 = vlan_id.into();
    if value >= 2 {
        Ok(vlan_id)
    } else {
        Err(format!(
            "invalid mvlan: {value} (must be >= 2, VLAN IDs 0-1 are reserved)"
        ))
    }
}

fn validate_mvlan_option<'de, D>(
    deserializer: D,
) -> Result<Option<VlanID>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<VlanID>::deserialize(deserializer)?;
    match opt {
        Some(v) => {
            validate_mvlan(v).map(Some).map_err(serde::de::Error::custom)
        }
        None => Ok(None),
    }
}

fn validate_mvlan_option_nullable<'de, D>(
    deserializer: D,
) -> Result<Option<Nullable<VlanID>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Deserialize as Nullable<VlanID> directly, which handles null properly
    // When field has null value, Nullable deserializer returns Nullable(None)
    // We always wrap in Some because if field is present, we got here
    let nullable = Nullable::<VlanID>::deserialize(deserializer)?;
    match nullable.0 {
        Some(v) => validate_mvlan(v)
            .map(|vv| Some(Nullable(Some(vv))))
            .map_err(serde::de::Error::custom),
        None => Ok(Some(Nullable(None))), // Explicit null to clear
    }
}

use crate::latest::multicast::{validate_multicast_ip, validate_source_ip};

/// Deserializer for validating multicast IP addresses.
fn validate_multicast_ip_param<'de, D>(
    deserializer: D,
) -> Result<Option<IpAddr>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let ip_opt = Option::<IpAddr>::deserialize(deserializer)?;
    if let Some(ip) = ip_opt {
        validate_multicast_ip(ip).map_err(|e| de::Error::custom(e))?;
    }
    Ok(ip_opt)
}

/// Deserializer for validating source IP addresses.
fn validate_source_ips_param<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<IpAddr>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let ips_opt = Option::<Vec<IpAddr>>::deserialize(deserializer)?;
    if let Some(ref ips) = ips_opt {
        for ip in ips {
            validate_source_ip(*ip).map_err(|e| de::Error::custom(e))?;
        }
    }
    Ok(ips_opt)
}
