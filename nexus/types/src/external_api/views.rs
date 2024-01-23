// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Views are response bodies, most of which are public lenses onto DB models.

use crate::external_api::shared::{
    self, Baseboard, IpKind, IpRange, ServiceUsingCertificate,
};
use crate::identity::AssetIdentityMetadata;
use api_identity::ObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::{
    ByteCount, Digest, IdentityMetadata, InstanceState, Ipv4Net, Ipv6Net, Name,
    ObjectIdentity, RoleName, SemverVersion, SimpleIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::IpAddr;
use uuid::Uuid;

use super::params::PhysicalDiskKind;

// SILOS

/// View of a Silo
///
/// A Silo is the highest level unit of isolation.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Silo {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// A silo where discoverable is false can be retrieved only by its id - it
    /// will not be part of the "list all silos" output.
    pub discoverable: bool,

    /// How users and groups are managed in this Silo
    pub identity_mode: shared::SiloIdentityMode,

    /// Mapping of which Fleet roles are conferred by each Silo role
    ///
    /// The default is that no Fleet roles are conferred by any Silo roles
    /// unless there's a corresponding entry in this map.
    pub mapped_fleet_roles:
        BTreeMap<shared::SiloRole, BTreeSet<shared::FleetRole>>,
}

/// A collection of resource counts used to describe capacity and utilization
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VirtualResourceCounts {
    /// Number of virtual CPUs
    pub cpus: i64,
    /// Amount of memory in bytes
    pub memory: ByteCount,
    /// Amount of disk storage in bytes
    pub storage: ByteCount,
}

/// A collection of resource counts used to set the virtual capacity of a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotas {
    pub silo_id: Uuid,
    #[serde(flatten)]
    pub limits: VirtualResourceCounts,
}

// For the eyes of end users
/// View of the current silo's resource utilization and capacity
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Utilization {
    /// Accounts for resources allocated to running instances or storage allocated via disks or snapshots
    /// Note that CPU and memory resources associated with a stopped instances are not counted here
    /// whereas associated disks will still be counted
    pub provisioned: VirtualResourceCounts,
    /// The total amount of resources that can be provisioned in this silo
    /// Actions that would exceed this limit will fail
    pub capacity: VirtualResourceCounts,
}

// For the eyes of an operator
/// View of a silo's resource utilization and capacity
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloUtilization {
    pub silo_id: Uuid,
    pub silo_name: Name,
    /// Accounts for resources allocated by in silos like CPU or memory for running instances and storage for disks and snapshots
    /// Note that CPU and memory resources associated with a stopped instances are not counted here
    pub provisioned: VirtualResourceCounts,
    /// Accounts for the total amount of resources reserved for silos via their quotas
    pub allocated: VirtualResourceCounts,
}

// We want to be able to paginate SiloUtilization by NameOrId
// but we can't derive ObjectIdentity because this isn't a typical asset.
// Instead we implement this new simple identity trait which is used under the
// hood by the pagination code.
impl SimpleIdentity for SiloUtilization {
    fn id(&self) -> Uuid {
        self.silo_id
    }
    fn name(&self) -> &Name {
        &self.silo_name
    }
}

// IDENTITY PROVIDER

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdentityProviderType {
    /// SAML identity provider
    Saml,
}

/// View of an Identity Provider
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

    /// IdP's entity id
    pub idp_entity_id: String,

    /// SP's client id
    pub sp_client_id: String,

    /// Service provider endpoint where the response will be sent
    pub acs_url: String,

    /// Service provider endpoint where the idp should send log out requests
    pub slo_url: String,

    /// Customer's technical contact for saml configuration
    pub technical_contact_email: String,

    /// Optional request signing public certificate (base64 encoded der file)
    pub public_cert: Option<String>,

    /// If set, attributes with this name will be considered to denote a user's
    /// group membership, where the values will be the group names.
    pub group_attribute_name: Option<String>,
}

// PROJECTS

/// View of a Project
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Project {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    // Important: Silo ID does not get presented to user
}

// CERTIFICATES

/// View of a Certificate
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Certificate {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub service: ServiceUsingCertificate,
}

// IMAGES

/// View of an image
///
/// If `project_id` is present then the image is only visible inside that
/// project. If it's not present then the image is visible to all projects in
/// the silo.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Image {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// ID of the parent project if the image is a project image
    pub project_id: Option<Uuid>,

    /// The family of the operating system like Debian, Ubuntu, etc.
    pub os: String,

    /// Version of the operating system
    pub version: String,

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

/// View of a Snapshot
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

/// View of a VPC
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

/// A collection of IP ranges. If a pool is linked to a silo, IP addresses from
/// the pool can be allocated within that silo
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

/// An IP pool in the context of a silo
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloIpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo.
    pub is_default: bool,
}

/// A link between an IP pool and a silo that allows one to allocate IPs from
/// the pool within the silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct IpPoolSiloLink {
    pub ip_pool_id: Uuid,
    pub silo_id: Uuid,
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo.
    pub is_default: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolRange {
    pub id: Uuid,
    pub ip_pool_id: Uuid,
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

/// A Floating IP is a well-known IP address which can be attached
/// and detached from instances.
#[derive(ObjectIdentity, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FloatingIp {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP address held by this resource.
    pub ip: IpAddr,
    /// The project this resource exists within.
    pub project_id: Uuid,
    /// The ID of the instance that this Floating IP is attached to,
    /// if it is presently in use.
    pub instance_id: Option<Uuid>,
}

// RACKS

/// View of an Rack
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Rack {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
}

// FRUs

// SLEDS

/// An operator's view of a Sled.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub baseboard: Baseboard,
    /// The rack to which this Sled is currently attached
    pub rack_id: Uuid,
    /// The provision state of the sled.
    pub provision_state: SledProvisionState,
    /// The number of hardware threads which can execute on this sled
    pub usable_hardware_threads: u32,
    /// Amount of RAM which may be used by the Sled's OS
    pub usable_physical_ram: ByteCount,
}

/// The provision state of a sled.
///
/// This controls whether new resources are going to be provisioned on this
/// sled.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledProvisionState {
    /// New resources will be provisioned on this sled.
    Provisionable,

    /// New resources will not be provisioned on this sled. However, existing
    /// resources will continue to be on this sled unless manually migrated
    /// off.
    NonProvisionable,
}

/// An operator's view of an instance running on a given sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SledInstance {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub active_sled_id: Uuid,
    pub migration_id: Option<Uuid>,
    pub name: Name,
    pub silo_name: Name,
    pub project_name: Name,
    pub state: InstanceState,
    pub ncpus: i64,
    pub memory: i64,
}

// SWITCHES

/// An operator's view of a Switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Switch {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub baseboard: Baseboard,
    /// The rack to which this Switch is currently attached
    pub rack_id: Uuid,
}

// PHYSICAL DISKS

/// View of a Physical Disk
///
/// Physical disks reside in a particular sled and are used to store both
/// Instance Disk data as well as internal metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct PhysicalDisk {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,

    /// The sled to which this disk is attached, if any.
    pub sled_id: Option<Uuid>,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub form_factor: PhysicalDiskKind,
}

// SILO USERS

/// View of a User
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct User {
    pub id: Uuid,
    /** Human-readable name that can identify the user */
    pub display_name: String,

    /** Uuid of the silo to which this user belongs */
    pub silo_id: Uuid,
}

// SESSION

// Add silo name to User because the console needs to display it
/// Info about the current user
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct CurrentUser {
    #[serde(flatten)]
    pub user: User,

    /** Name of the silo to which this user belongs. */
    pub silo_name: Name,
}

// SILO GROUPS

/// View of a Group
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Group {
    pub id: Uuid,

    /// Human-readable name that can identify the group
    pub display_name: String,

    /// Uuid of the silo to which this group belongs
    pub silo_id: Uuid,
}

// BUILT-IN USERS

/// View of a Built-in User
///
/// A Built-in User is explicitly created as opposed to being derived from an
/// Identify Provider.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltin {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

// ROLES

/// View of a Role
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Role {
    pub name: RoleName,
    pub description: String,
}

// SSH KEYS

/// View of an SSH Key
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

    /// The lifetime in seconds of the `device_code` and `user_code`.
    pub expires_in: u16,
}

/// Successful access token grant. See RFC 6749 §5.1.
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

// SYSTEM UPDATES

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct VersionRange {
    pub low: SemverVersion,
    pub high: SemverVersion,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum UpdateStatus {
    Updating,
    Steady,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemVersion {
    pub version_range: VersionRange,
    pub status: UpdateStatus,
    // TODO: time_released? time_last_applied? I got a fever and the only
    // prescription is more timestamps
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SystemUpdate {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub version: SemverVersion,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ComponentUpdate {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,

    pub component_type: shared::UpdateableComponentType,
    pub version: SemverVersion,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateableComponent {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,

    pub device_id: String,
    pub component_type: shared::UpdateableComponentType,
    pub version: SemverVersion,
    pub system_version: SemverVersion,
    pub status: UpdateStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateDeployment {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub version: SemverVersion,
    pub status: UpdateStatus,
}

// SYSTEM HEALTH

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PingStatus {
    Ok,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Ping {
    /// Whether the external API is reachable. Will always be Ok if the endpoint
    /// returns anything at all.
    pub status: PingStatus,
}
