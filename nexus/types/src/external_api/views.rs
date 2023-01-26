// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Views are response bodies, most of which are public lenses onto DB models.

use crate::external_api::shared::{
    self, IpKind, IpRange, ServiceUsingCertificate,
};
use crate::identity::AssetIdentityMetadata;
use api_identity::ObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::{
    ByteCount, Digest, IdentityMetadata, Ipv4Net, Ipv6Net, Name,
    ObjectIdentity, RoleName,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::net::SocketAddrV6;
use uuid::Uuid;

// SILOS

/// Client view of a ['Silo']
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Silo {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// A silo where discoverable is false can be retrieved only by its id - it
    /// will not be part of the "list all silos" output.
    pub discoverable: bool,

    /// How users and groups are managed in this Silo
    pub identity_mode: shared::SiloIdentityMode,
}

// IDENTITY PROVIDER

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdentityProviderType {
    /// SAML identity provider
    Saml,
}

/// Client view of an [`IdentityProvider`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IdentityProvider {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Identity provider type
    pub provider_type: IdentityProviderType,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SamlIdentityProvider {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// idp's entity id
    pub idp_entity_id: String,

    /// sp's client id
    pub sp_client_id: String,

    /// service provider endpoint where the response will be sent
    pub acs_url: String,

    /// service provider endpoint where the idp should send log out requests
    pub slo_url: String,

    /// customer's technical contact for saml configuration
    pub technical_contact_email: String,

    /// optional request signing public certificate (base64 encoded der file)
    pub public_cert: Option<String>,
}

// ORGANIZATIONS

/// Client view of an [`Organization`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Organization {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    // Important: Silo ID does not get presented to user
}

// PROJECTS

/// Client view of a [`Project`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Project {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub organization_id: Uuid,
}

// CERTIFICATES

/// Client view of a [`Certificate`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Certificate {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub service: ServiceUsingCertificate,
}

// IMAGES

/// Client view of global Images
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct GlobalImage {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// URL source of this image, if any
    pub url: Option<String>,

    /// Image distribution
    pub distribution: String,

    /// Image version
    pub version: String,

    /// Hash of the image contents, if applicable
    pub digest: Option<Digest>,

    /// size of blocks in bytes
    pub block_size: ByteCount,

    /// total size in bytes
    pub size: ByteCount,
}

/// Client view of project Images
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Image {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The project the disk belongs to
    pub project_id: Uuid,

    /// URL source of this image, if any
    pub url: Option<String>,

    /// Version of this, if any
    pub version: Option<String>,

    /// Hash of the image contents, if applicable
    pub digest: Option<Digest>,

    /// size of blocks in bytes
    pub block_size: ByteCount,

    /// total size in bytes
    pub size: ByteCount,
}

// SNAPSHOTS

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    Creating,
    Ready,
    Faulted,
    Destroyed,
}

/// Client view of a Snapshot
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Snapshot {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub project_id: Uuid,
    pub disk_id: Uuid,

    pub state: SnapshotState,

    pub size: ByteCount,
}

// VPCs

/// Client view of a [`Vpc`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Vpc {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// id for the project containing this VPC
    pub project_id: Uuid,

    /// id for the system router where subnet default routes are registered
    pub system_router_id: Uuid,

    /// The unique local IPv6 address range for subnets in this VPC
    pub ipv6_prefix: Ipv6Net,

    // TODO-design should this be optional?
    /// The name used for the VPC in DNS.
    pub dns_name: Name,
}

/// A VPC subnet represents a logical grouping for instances that allows network traffic between
/// them, within a IPv4 subnetwork or optionall an IPv6 subnetwork.
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

// IP POOLS

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolRange {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub range: IpRange,
}

// INSTANCE EXTERNAL IP ADDRESSES

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ExternalIp {
    pub ip: IpAddr,
    pub kind: IpKind,
}

// RACKS

/// Client view of an [`Rack`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Rack {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
}

// SLEDS

/// Client view of an [`Sled`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub service_address: SocketAddrV6,
    pub part_number: Option<String>,
    pub serial_number: Option<String>,
    pub revision: Option<u16>,
    pub slot: Option<u8>,
}

// SILO USERS

/// Client view of a [`User`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct User {
    pub id: Uuid,
    /** Human-readable name that can identify the user */
    pub display_name: String,

    /** Uuid of the silo to which this user belongs */
    pub silo_id: Uuid,
}

// SILO GROUPS

/// Client view of a [`Group`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Group {
    pub id: Uuid,

    /** Human-readable name that can identify the group */
    pub display_name: String,

    /** Uuid of the silo to which this group belongs */
    pub silo_id: Uuid,
}

// BUILT-IN USERS

/// Client view of a [`UserBuiltin`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltin {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

// ROLES

/// Client view of a [`Role`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Role {
    pub name: RoleName,
    pub description: String,
}

// SSH KEYS

/// Client view of a [`SshKey`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SshKey {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The user to whom this key belongs
    pub silo_user_id: Uuid,

    /// SSH public key, e.g., `"ssh-ed25519 AAAAC3NzaC..."`
    pub public_key: String,
}

// OAUTH 2.0 DEVICE AUTHORIZATION REQUESTS & TOKENS

/// Response to an initial device authorization request.
/// See RFC 8628 §3.2 (Device Authorization Response).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthResponse {
    /// The device verification code.
    pub device_code: String,

    /// The end-user verification code.
    pub user_code: String,

    /// The end-user verification URI on the authorization server.
    /// The URI should be short and easy to remember as end users
    /// may be asked to manually type it into their user agent.
    pub verification_uri: String,

    /// A verification URI that includes the `user_code`,
    /// which is designed for non-textual transmission.
    pub verification_uri_complete: String,

    /// The lifetime in seconds of the `device_code` and `user_code`.
    pub expires_in: u16,
}

/// Successful access token grant. See RFC 6749 §5.1.
/// TODO-security: `expires_in`, `refresh_token`, etc.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAccessTokenGrant {
    /// The access token issued to the client.
    pub access_token: String,

    /// The type of the token issued, as described in RFC 6749 §7.1.
    pub token_type: DeviceAccessTokenType,
}

/// The kind of token granted.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAccessTokenType {
    Bearer,
}
