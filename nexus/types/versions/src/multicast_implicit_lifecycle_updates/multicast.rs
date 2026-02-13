// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group types for version MULTICAST_IMPLICIT_LIFECYCLE_UPDATES.

use api_identity::ObjectIdentity;
use omicron_common::address::IpVersion;
use omicron_common::api::external::{
    IdentityMetadata, NameOrId, ObjectIdentity,
};
use omicron_common::vlan::VlanID;
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::net::IpAddr;
use std::str::FromStr;
use uuid::Uuid;

// Re-use validators from initial module.
use crate::v2025112000::multicast::validate_source_ips_param;

use omicron_common::api::external::Name;

/// Identifier for a multicast group: can be a Name, UUID, or IP address.
///
/// This type supports the join-by-IP pattern where users can specify
/// a multicast IP address directly, and the system will auto-discover
/// the pool and find or create the group.
#[derive(Debug, Display, Clone, PartialEq)]
#[display("{0}")]
pub enum MulticastGroupIdentifier {
    Id(Uuid),
    Name(Name),
    Ip(IpAddr),
}

impl TryFrom<String> for MulticastGroupIdentifier {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if let Ok(id) = Uuid::parse_str(&value) {
            Ok(MulticastGroupIdentifier::Id(id))
        } else if let Ok(ip) = value.parse::<IpAddr>() {
            Ok(MulticastGroupIdentifier::Ip(ip))
        } else {
            Ok(MulticastGroupIdentifier::Name(Name::try_from(value)?))
        }
    }
}

impl FromStr for MulticastGroupIdentifier {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        MulticastGroupIdentifier::try_from(String::from(value))
    }
}

impl From<Name> for MulticastGroupIdentifier {
    fn from(name: Name) -> Self {
        MulticastGroupIdentifier::Name(name)
    }
}

impl From<Uuid> for MulticastGroupIdentifier {
    fn from(id: Uuid) -> Self {
        MulticastGroupIdentifier::Id(id)
    }
}

impl From<IpAddr> for MulticastGroupIdentifier {
    fn from(ip: IpAddr) -> Self {
        MulticastGroupIdentifier::Ip(ip)
    }
}

impl From<NameOrId> for MulticastGroupIdentifier {
    fn from(value: NameOrId) -> Self {
        match value {
            NameOrId::Name(name) => MulticastGroupIdentifier::Name(name),
            NameOrId::Id(id) => MulticastGroupIdentifier::Id(id),
        }
    }
}

impl Serialize for MulticastGroupIdentifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MulticastGroupIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        MulticastGroupIdentifier::try_from(s).map_err(de::Error::custom)
    }
}

impl JsonSchema for MulticastGroupIdentifier {
    fn schema_name() -> String {
        "MulticastGroupIdentifier".to_string()
    }

    fn json_schema(
        _generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A multicast group identifier".to_string()),
                description: Some(
                    "Can be a UUID, a name, or an IP address".to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct MulticastGroupSelector {
    /// Name, ID, or IP address of the multicast group (fleet-scoped)
    pub multicast_group: MulticastGroupIdentifier,
}

/// Specification for joining a multicast group with optional source filtering.
///
/// Used in `InstanceCreate` and `InstanceUpdate` to specify multicast group
/// membership along with per-member source IP configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupJoinSpec {
    /// The multicast group to join, specified by name, UUID, or IP address.
    pub group: MulticastGroupIdentifier,

    /// Source IPs for source-filtered multicast (SSM). Optional for ASM groups,
    /// required for SSM groups (232.0.0.0/8, ff3x::/32).
    #[serde(default, deserialize_with = "validate_source_ips_param")]
    pub source_ips: Option<Vec<IpAddr>>,

    /// IP version for pool selection when creating a group by name.
    /// Required if both IPv4 and IPv6 default multicast pools are linked.
    #[serde(default)]
    pub ip_version: Option<IpVersion>,
}

/// Parameters for joining an instance to a multicast group.
///
/// When joining by IP address, the pool containing the multicast IP is
/// auto-discovered from all linked multicast pools.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Default)]
pub struct InstanceMulticastGroupJoin {
    /// Source IPs for source-filtered multicast (SSM). Optional for ASM groups,
    /// required for SSM groups (232.0.0.0/8, ff3x::/32).
    #[serde(default, deserialize_with = "validate_source_ips_param")]
    pub source_ips: Option<Vec<IpAddr>>,

    /// IP version for pool selection when creating a group by name.
    /// Required if both IPv4 and IPv6 default multicast pools are linked.
    #[serde(default)]
    pub ip_version: Option<IpVersion>,
}

/// Path parameters for multicast group operations.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct MulticastGroupPath {
    /// Name, ID, or IP address of the multicast group.
    pub multicast_group: MulticastGroupIdentifier,
}

/// Parameters for adding an instance to a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberAdd {
    /// Name or ID of the instance to add to the multicast group
    pub instance: NameOrId,
    /// Source IPs for source-filtered multicast (SSM). Optional for ASM groups,
    /// required for SSM groups (232.0.0.0/8, ff3x::/32).
    #[serde(default, deserialize_with = "validate_source_ips_param")]
    pub source_ips: Option<Vec<IpAddr>>,
}

/// Path parameters for multicast group member operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupMemberPath {
    /// Name, ID, or IP address of the multicast group
    pub multicast_group: MulticastGroupIdentifier,
    /// Name or ID of the instance
    pub instance: NameOrId,
}

/// Path parameters for instance multicast group operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMulticastGroupPath {
    /// Name or ID of the instance
    pub instance: NameOrId,
    /// Name, ID, or IP address of the multicast group
    pub multicast_group: MulticastGroupIdentifier,
}

/// View of a Multicast Group
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
pub struct MulticastGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The multicast IP address held by this resource.
    pub multicast_ip: IpAddr,
    /// Union of all member source IP addresses (computed, read-only).
    ///
    /// This field shows the combined source IPs across all group members.
    /// Individual members may subscribe to different sources; this union
    /// reflects all sources that any member is subscribed to.
    /// Empty array means no members have source filtering enabled.
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
    /// The multicast IP address of the group this member belongs to.
    pub multicast_ip: IpAddr,
    /// The ID of the instance that is a member of this group.
    pub instance_id: Uuid,
    /// Source IP addresses for this member's multicast subscription.
    ///
    /// - **ASM**: Sources are optional. Empty array means any source is allowed.
    ///   Non-empty array enables source filtering (IGMPv3/MLDv2).
    /// - **SSM**: Sources are required for SSM addresses (232/8, ff3x::/32).
    pub source_ips: Vec<IpAddr>,
    /// Current state of the multicast group membership.
    pub state: String,
}

// -- Conversions between v2026010800 and v2025112000 multicast types --

impl From<MulticastGroup> for crate::v2025112000::multicast::MulticastGroup {
    fn from(new: MulticastGroup) -> Self {
        Self {
            identity: new.identity,
            multicast_ip: new.multicast_ip,
            source_ips: new.source_ips,
            mvlan: new.mvlan,
            ip_pool_id: new.ip_pool_id,
            state: new.state,
        }
    }
}

impl From<MulticastGroupMember>
    for crate::v2025112000::multicast::MulticastGroupMember
{
    fn from(new: MulticastGroupMember) -> Self {
        // Drop multicast_ip and source_ips which were added in this version.
        Self {
            identity: new.identity,
            multicast_group_id: new.multicast_group_id,
            instance_id: new.instance_id,
            state: new.state,
        }
    }
}

impl From<crate::v2025112000::path_params::MulticastGroupPath>
    for MulticastGroupPath
{
    fn from(old: crate::v2025112000::path_params::MulticastGroupPath) -> Self {
        Self { multicast_group: old.multicast_group.into() }
    }
}

impl From<crate::v2025112000::multicast::MulticastGroupIpLookupPath>
    for MulticastGroupPath
{
    fn from(
        old: crate::v2025112000::multicast::MulticastGroupIpLookupPath,
    ) -> Self {
        Self { multicast_group: old.address.into() }
    }
}

impl From<crate::v2025112000::multicast::MulticastGroupMemberPath>
    for MulticastGroupMemberPath
{
    fn from(
        old: crate::v2025112000::multicast::MulticastGroupMemberPath,
    ) -> Self {
        Self {
            multicast_group: old.multicast_group.into(),
            instance: old.instance,
        }
    }
}

impl From<crate::v2025112000::multicast::InstanceMulticastGroupPath>
    for InstanceMulticastGroupPath
{
    fn from(
        old: crate::v2025112000::multicast::InstanceMulticastGroupPath,
    ) -> Self {
        Self {
            instance: old.instance,
            multicast_group: old.multicast_group.into(),
        }
    }
}

impl From<crate::v2025112000::multicast::MulticastGroupMemberAdd>
    for MulticastGroupMemberAdd
{
    fn from(
        old: crate::v2025112000::multicast::MulticastGroupMemberAdd,
    ) -> Self {
        Self { instance: old.instance, source_ips: None }
    }
}
