// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Views are response bodies, most of which are public lenses onto DB models.

use crate::db::identity::{Asset, Resource};
use crate::db::model;
use crate::external_api::shared::{self, IpRange};
use api_identity::ObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::{
    ByteCount, Digest, IdentityMetadata, Ipv4Net, Ipv6Net, Name,
    ObjectIdentity, RoleName,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;
use uuid::Uuid;

// IDENTITY METADATA

/// Identity-related metadata that's included in "asset" public API objects
/// (which generally have no name or description)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
pub struct AssetIdentityMetadata {
    /// unique, immutable, system-controlled identifier for each resource
    pub id: Uuid,
    /// timestamp when this resource was created
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// timestamp when this resource was last modified
    pub time_modified: chrono::DateTime<chrono::Utc>,
}

impl<T> From<&T> for AssetIdentityMetadata
where
    T: Asset,
{
    fn from(t: &T) -> Self {
        AssetIdentityMetadata {
            id: t.id(),
            time_created: t.time_created(),
            time_modified: t.time_modified(),
        }
    }
}

// SILOS

/// Client view of a ['Silo']
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Silo {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// A silo where discoverable is false can be retrieved only by its id - it
    /// will not be part of the "list all silos" output.
    pub discoverable: bool,

    /// User provision type
    pub user_provision_type: shared::UserProvisionType,
}

impl Into<Silo> for model::Silo {
    fn into(self) -> Silo {
        Silo {
            identity: self.identity(),
            discoverable: self.discoverable,
            user_provision_type: self.user_provision_type.into(),
        }
    }
}

// IDENTITY PROVIDER

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdentityProviderType {
    /// SAML identity provider
    Saml,
}

impl Into<IdentityProviderType> for model::IdentityProviderType {
    fn into(self) -> IdentityProviderType {
        match self {
            model::IdentityProviderType::Saml => IdentityProviderType::Saml,
        }
    }
}

/// Client view of an [`IdentityProvider`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IdentityProvider {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Identity provider type
    pub provider_type: IdentityProviderType,
}

impl Into<IdentityProvider> for model::IdentityProvider {
    fn into(self) -> IdentityProvider {
        IdentityProvider {
            identity: self.identity(),
            provider_type: self.provider_type.into(),
        }
    }
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

impl From<model::SamlIdentityProvider> for SamlIdentityProvider {
    fn from(saml_idp: model::SamlIdentityProvider) -> Self {
        Self {
            identity: saml_idp.identity(),
            idp_entity_id: saml_idp.idp_entity_id,
            sp_client_id: saml_idp.sp_client_id,
            acs_url: saml_idp.acs_url,
            slo_url: saml_idp.slo_url,
            technical_contact_email: saml_idp.technical_contact_email,
            public_cert: saml_idp.public_cert,
        }
    }
}

// ORGANIZATIONS

/// Client view of an [`Organization`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Organization {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    // Important: Silo ID does not get presented to user
}

impl From<model::Organization> for Organization {
    fn from(org: model::Organization) -> Self {
        Self { identity: org.identity() }
    }
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

impl From<model::Project> for Project {
    fn from(project: model::Project) -> Self {
        Self {
            identity: project.identity(),
            organization_id: project.organization_id,
        }
    }
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

/// Client view of a Snapshot
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Snapshot {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub project_id: Uuid,
    pub disk_id: Uuid,
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

impl From<model::Vpc> for Vpc {
    fn from(vpc: model::Vpc) -> Self {
        Self {
            identity: vpc.identity(),
            project_id: vpc.project_id,
            system_router_id: vpc.system_router_id,
            ipv6_prefix: *vpc.ipv6_prefix,
            dns_name: vpc.dns_name.0,
        }
    }
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

impl From<model::VpcSubnet> for VpcSubnet {
    fn from(subnet: model::VpcSubnet) -> Self {
        Self {
            identity: subnet.identity(),
            vpc_id: subnet.vpc_id,
            ipv4_block: subnet.ipv4_block.0,
            ipv6_block: subnet.ipv6_block.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpcRouterKind {
    System,
    Custom,
}

impl From<model::VpcRouterKind> for VpcRouterKind {
    fn from(kind: model::VpcRouterKind) -> Self {
        match kind {
            model::VpcRouterKind::Custom => Self::Custom,
            model::VpcRouterKind::System => Self::System,
        }
    }
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

impl From<model::VpcRouter> for VpcRouter {
    fn from(router: model::VpcRouter) -> Self {
        Self {
            identity: router.identity(),
            vpc_id: router.vpc_id,
            kind: router.kind.into(),
        }
    }
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl From<model::IpPool> for IpPool {
    fn from(pool: model::IpPool) -> Self {
        Self { identity: pool.identity() }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolRange {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub range: IpRange,
}

impl From<model::IpPoolRange> for IpPoolRange {
    fn from(range: model::IpPoolRange) -> Self {
        Self {
            id: range.id,
            time_created: range.time_created,
            range: IpRange::from(&range),
        }
    }
}

// RACKS

/// Client view of an [`Rack`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Rack {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
}

impl From<model::Rack> for Rack {
    fn from(rack: model::Rack) -> Self {
        Self { identity: AssetIdentityMetadata::from(&rack) }
    }
}

// SLEDS

/// Client view of an [`Sled`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub service_address: SocketAddrV6,
}

impl From<model::Sled> for Sled {
    fn from(sled: model::Sled) -> Self {
        Self {
            identity: AssetIdentityMetadata::from(&sled),
            service_address: sled.address(),
        }
    }
}

// SILO USERS

/// Client view of a [`User`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct User {
    pub id: Uuid,
    /** Human-readable name that can identify the user */
    pub display_name: String,
}

impl From<model::SiloUser> for User {
    fn from(user: model::SiloUser) -> Self {
        Self {
            id: user.id(),
            // TODO the use of external_id as display_name is temporary
            display_name: user.external_id,
        }
    }
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

impl From<model::UserBuiltin> for UserBuiltin {
    fn from(user: model::UserBuiltin) -> Self {
        Self { identity: user.identity() }
    }
}

// ROLES

/// Client view of a [`Role`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Role {
    pub name: RoleName,
    pub description: String,
}

impl From<model::RoleBuiltin> for Role {
    fn from(role: model::RoleBuiltin) -> Self {
        Self {
            name: RoleName::new(&role.resource_type, &role.role_name),
            description: role.description,
        }
    }
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

impl From<model::SshKey> for SshKey {
    fn from(ssh_key: model::SshKey) -> Self {
        Self {
            identity: ssh_key.identity(),
            silo_user_id: ssh_key.silo_user_id,
            public_key: ssh_key.public_key,
        }
    }
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

impl DeviceAuthResponse {
    // We need the host to construct absolute verification URIs.
    pub fn from_model(model: model::DeviceAuthRequest, host: &str) -> Self {
        Self {
            // TODO-security: use HTTPS
            verification_uri: format!("http://{}/device/verify", host),
            verification_uri_complete: format!(
                "http://{}/device/verify?user_code={}",
                host, &model.user_code
            ),
            user_code: model.user_code,
            device_code: model.device_code,
            expires_in: model
                .time_expires
                .signed_duration_since(model.time_created)
                .num_seconds() as u16,
        }
    }
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

impl From<model::DeviceAccessToken> for DeviceAccessTokenGrant {
    fn from(access_token: model::DeviceAccessToken) -> Self {
        Self {
            access_token: format!("oxide-token-{}", access_token.token),
            token_type: DeviceAccessTokenType::Bearer,
        }
    }
}

/// The kind of token granted.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAccessTokenType {
    Bearer,
}
