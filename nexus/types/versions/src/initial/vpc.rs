// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, Name, NameOrId, ObjectIdentity,
    RouteDestination, RouteTarget,
};
use oxnet::{Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct VpcSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC
    pub vpc: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalVpcSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC
    pub vpc: Option<NameOrId>,
}

#[derive(Deserialize, JsonSchema)]
pub struct SubnetSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `subnet` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the subnet
    pub subnet: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RouterSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `router` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router
    pub router: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct OptionalRouterSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `router` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router
    pub router: Option<NameOrId>,
}

#[derive(Deserialize, JsonSchema)]
pub struct RouteSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `router` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router, only required if `route` is provided as a `Name`
    pub router: Option<NameOrId>,
    /// Name or ID of the route
    pub route: NameOrId,
}

/// View of a VPC
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Vpc {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// ID for the project containing this VPC
    pub project_id: Uuid,

    /// ID for the system router where subnet default routes are registered
    pub system_router_id: Uuid,

    /// The unique local IPv6 address range for subnets in this VPC
    pub ipv6_prefix: Ipv6Net,

    // TODO-design should this be optional?
    /// The name used for the VPC in DNS.
    pub dns_name: Name,
}

/// A VPC subnet represents a logical grouping for instances that allows network traffic between
/// them, within an IPv4 subnetwork or optionally an IPv6 subnetwork.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnet {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The VPC to which the subnet belongs.
    pub vpc_id: Uuid,

    /// The IPv4 subnet CIDR block.
    pub ipv4_block: Ipv4Net,

    /// The IPv6 subnet CIDR block.
    pub ipv6_block: Ipv6Net,

    /// ID for an attached custom router.
    pub custom_router_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpcRouterKind {
    System,
    Custom,
}

/// A VPC router defines a series of rules that indicate where traffic
/// should be sent depending on its destination.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouter {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub kind: VpcRouterKind,

    /// The VPC to which the router belongs.
    pub vpc_id: Uuid,
}

/// Create-time parameters for a `Vpc`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv6 prefix for this VPC
    ///
    /// All IPv6 subnets created from this VPC must be taken from this range,
    /// which should be a Unique Local Address in the range `fd00::/48`. The
    /// default VPC Subnet will have the first `/64` range from this prefix.
    pub ipv6_prefix: Option<Ipv6Net>,

    pub dns_name: Name,
}

/// Updateable properties of a `Vpc`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub dns_name: Option<Name>,
}

/// Create-time parameters for a `VpcSubnet`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv4 address range for this subnet.
    ///
    /// It must be allocated from an RFC 1918 private address range, and must
    /// not overlap with any other existing subnet in the VPC.
    pub ipv4_block: Ipv4Net,

    /// The IPv6 address range for this subnet.
    ///
    /// It must be allocated from the RFC 4193 Unique Local Address range, with
    /// the prefix equal to the parent VPC's prefix. A random `/64` block will
    /// be assigned if one is not provided. It must not overlap with any
    /// existing subnet in the VPC.
    pub ipv6_block: Option<Ipv6Net>,

    /// An optional router, used to direct packets sent from hosts in this subnet
    /// to any destination address.
    ///
    /// Custom routers apply in addition to the VPC-wide *system* router, and have
    /// higher priority than the system router for an otherwise
    /// equal-prefix-length match.
    pub custom_router: Option<NameOrId>,
}

/// Updateable properties of a `VpcSubnet`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,

    /// An optional router, used to direct packets sent from hosts in this subnet
    /// to any destination address.
    pub custom_router: Option<NameOrId>,
}

/// Create-time parameters for a `VpcRouter`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a `VpcRouter`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// Create-time parameters for a `RouterRoute`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRouteCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The location that matched packets should be forwarded to.
    pub target: RouteTarget,
    /// Selects which traffic this routing rule will apply to.
    pub destination: RouteDestination,
}

/// Updateable properties of a `RouterRoute`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRouteUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    /// The location that matched packets should be forwarded to.
    pub target: RouteTarget,
    /// Selects which traffic this routing rule will apply to.
    pub destination: RouteDestination,
}
