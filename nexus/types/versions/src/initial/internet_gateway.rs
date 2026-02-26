// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internet gateway types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams, NameOrId, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// An internet gateway provides a path between VPC networks and external
/// networks.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGateway {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The VPC to which the gateway belongs.
    pub vpc_id: Uuid,
}

/// An IP pool that is attached to an internet gateway
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The associated internet gateway.
    pub internet_gateway_id: Uuid,

    /// The associated IP pool.
    pub ip_pool_id: Uuid,
}

/// An IP address that is attached to an internet gateway
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpAddress {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The associated internet gateway
    pub internet_gateway_id: Uuid,

    /// The associated IP address
    pub address: IpAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InternetGatewayDeleteSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC
    pub vpc: Option<NameOrId>,
    /// Also delete routes targeting this gateway.
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InternetGatewaySelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the internet gateway
    pub gateway: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalInternetGatewaySelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the internet gateway
    pub gateway: Option<NameOrId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct DeleteInternetGatewayElementSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the internet gateway
    pub gateway: Option<NameOrId>,
    /// Also delete routes targeting this gateway element.
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct InternetGatewayIpPoolSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the gateway, only required if `pool` is provided as a `Name`
    pub gateway: Option<NameOrId>,
    /// Name or ID of the pool
    pub pool: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct InternetGatewayIpAddressSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the gateway, only required if `address` is provided as a `Name`
    pub gateway: Option<NameOrId>,
    /// Name or ID of the address
    pub address: NameOrId,
}

/// Create-time parameters for an `InternetGateway`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ip_pool: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpAddressCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub address: IpAddr,
}
