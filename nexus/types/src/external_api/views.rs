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
    AllowedSourceIps as ExternalAllowedSourceIps, ByteCount, Digest, Error,
    IdentityMetadata, InstanceState, Name, ObjectIdentity, RoleName,
    SimpleIdentity,
};
use oxnet::{Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use strum::{EnumIter, IntoEnumIterator};
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
    /// The service using this certificate
    pub service: ServiceUsingCertificate,
    /// PEM-formatted string containing public certificate chain
    pub cert: String,
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
/// them, within a IPv4 subnetwork or optionally an IPv6 subnetwork.
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

// IP POOLS

/// A collection of IP ranges. If a pool is linked to a silo, IP addresses from
/// the pool can be allocated within that silo
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Ipv4Utilization {
    /// The number of IPv4 addresses allocated from this pool
    pub allocated: u32,
    /// The total number of IPv4 addresses in the pool, i.e., the sum of the
    /// lengths of the IPv4 ranges. Unlike IPv6 capacity, can be a 32-bit
    /// integer because there are only 2^32 IPv4 addresses.
    pub capacity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Ipv6Utilization {
    /// The number of IPv6 addresses allocated from this pool. A 128-bit integer
    /// string to match the capacity field.
    #[serde(with = "U128String")]
    pub allocated: u128,

    /// The total number of IPv6 addresses in the pool, i.e., the sum of the
    /// lengths of the IPv6 ranges. An IPv6 range can contain up to 2^128
    /// addresses, so we represent this value in JSON as a numeric string with a
    /// custom "uint128" format.
    #[serde(with = "U128String")]
    pub capacity: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolUtilization {
    /// Number of allocated and total available IPv4 addresses in pool
    pub ipv4: Ipv4Utilization,
    /// Number of allocated and total available IPv6 addresses in pool
    pub ipv6: Ipv6Utilization,
}

// Custom struct for serializing/deserializing u128 as a string. The serde
// docs will suggest using a module (or serialize_with and deserialize_with
// functions), but as discussed in the comments on the UserData de/serializer,
// schemars wants this to be a type, so it has to be a struct.
struct U128String;
impl U128String {
    pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for U128String {
    fn schema_name() -> String {
        "String".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("uint128".to_string()),
            ..Default::default()
        }
        .into()
    }

    fn is_referenceable() -> bool {
        false
    }
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

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExternalIp {
    Ephemeral { ip: IpAddr },
    Floating(FloatingIp),
}

impl ExternalIp {
    pub fn ip(&self) -> IpAddr {
        match self {
            Self::Ephemeral { ip } => *ip,
            Self::Floating(float) => float.ip,
        }
    }

    pub fn kind(&self) -> IpKind {
        match self {
            Self::Ephemeral { .. } => IpKind::Ephemeral,
            Self::Floating(_) => IpKind::Floating,
        }
    }
}

/// A Floating IP is a well-known IP address which can be attached
/// and detached from instances.
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct FloatingIp {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP address held by this resource.
    pub ip: IpAddr,
    /// The ID of the IP pool this resource belongs to.
    pub ip_pool_id: Uuid,
    /// The project this resource exists within.
    pub project_id: Uuid,
    /// The ID of the instance that this Floating IP is attached to,
    /// if it is presently in use.
    pub instance_id: Option<Uuid>,
}

impl From<FloatingIp> for ExternalIp {
    fn from(value: FloatingIp) -> Self {
        ExternalIp::Floating(value)
    }
}

impl TryFrom<ExternalIp> for FloatingIp {
    type Error = Error;

    fn try_from(value: ExternalIp) -> Result<Self, Self::Error> {
        match value {
            ExternalIp::Ephemeral { .. } => Err(Error::internal_error(
                "tried to convert an ephemeral IP into a floating IP",
            )),
            ExternalIp::Floating(v) => Ok(v),
        }
    }
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
    /// The operator-defined policy of a sled.
    pub policy: SledPolicy,
    /// The current state Nexus believes the sled to be in.
    pub state: SledState,
    /// The number of hardware threads which can execute on this sled
    pub usable_hardware_threads: u32,
    /// Amount of RAM which may be used by the Sled's OS
    pub usable_physical_ram: ByteCount,
}

/// The operator-defined provision policy of a sled.
///
/// This controls whether new resources are going to be provisioned on this
/// sled.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum SledProvisionPolicy {
    /// New resources will be provisioned on this sled.
    Provisionable,

    /// New resources will not be provisioned on this sled. However, if the
    /// sled is currently in service, existing resources will continue to be on
    /// this sled unless manually migrated off.
    NonProvisionable,
}

impl SledProvisionPolicy {
    /// Returns the opposite of the current provision state.
    pub const fn invert(self) -> Self {
        match self {
            Self::Provisionable => Self::NonProvisionable,
            Self::NonProvisionable => Self::Provisionable,
        }
    }
}

/// The operator-defined policy of a sled.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SledPolicy {
    /// The operator has indicated that the sled is in-service.
    InService {
        /// Determines whether new resources can be provisioned onto the sled.
        provision_policy: SledProvisionPolicy,
    },

    /// The operator has indicated that the sled has been permanently removed
    /// from service.
    ///
    /// This is a terminal state: once a particular sled ID is expunged, it
    /// will never return to service. (The actual hardware may be reused, but
    /// it will be treated as a brand-new sled.)
    ///
    /// An expunged sled is always non-provisionable.
    Expunged,
    // NOTE: if you add a new value here, be sure to add it to
    // the `IntoEnumIterator` impl below!
}

// Can't automatically derive strum::EnumIter because that doesn't provide a
// way to iterate over nested enums.
impl IntoEnumIterator for SledPolicy {
    type Iterator = std::array::IntoIter<Self, 3>;

    fn iter() -> Self::Iterator {
        [
            Self::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            Self::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            Self::Expunged,
        ]
        .into_iter()
    }
}

impl SledPolicy {
    /// Creates a new `SledPolicy` that is in-service and provisionable.
    pub fn provisionable() -> Self {
        Self::InService { provision_policy: SledProvisionPolicy::Provisionable }
    }

    /// Returns the provision policy, if the sled is in service.
    pub fn provision_policy(&self) -> Option<SledProvisionPolicy> {
        match self {
            Self::InService { provision_policy } => Some(*provision_policy),
            Self::Expunged => None,
        }
    }

    /// Returns true if the sled can be decommissioned with this policy
    ///
    /// This is a method here, rather than being a variant on `SledFilter`,
    /// because the "decommissionable" condition only has meaning for policies,
    /// not states.
    pub fn is_decommissionable(&self) -> bool {
        // This should be kept in sync with `all_decommissionable` below.
        match self {
            Self::InService { .. } => false,
            Self::Expunged => true,
        }
    }

    /// Returns all the possible policies a sled can have for it to be
    /// decommissioned.
    ///
    /// This is a method here, rather than being a variant on `SledFilter`,
    /// because the "decommissionable" condition only has meaning for policies,
    /// not states.
    pub fn all_decommissionable() -> &'static [Self] {
        &[Self::Expunged]
    }
}

impl fmt::Display for SledPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            } => write!(f, "in service"),
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            } => write!(f, "not provisionable"),
            SledPolicy::Expunged => write!(f, "expunged"),
        }
    }
}

/// The current state of the sled, as determined by Nexus.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum SledState {
    /// The sled is currently active, and has resources allocated on it.
    Active,

    /// The sled has been permanently removed from service.
    ///
    /// This is a terminal state: once a particular sled ID is decommissioned,
    /// it will never return to service. (The actual hardware may be reused,
    /// but it will be treated as a brand-new sled.)
    Decommissioned,
}

impl fmt::Display for SledState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledState::Active => write!(f, "active"),
            SledState::Decommissioned => write!(f, "decommissioned"),
        }
    }
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

    /// The operator-defined policy for a physical disk.
    pub policy: PhysicalDiskPolicy,
    /// The current state Nexus believes the disk to be in.
    pub state: PhysicalDiskState,

    /// The sled to which this disk is attached, if any.
    pub sled_id: Option<Uuid>,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub form_factor: PhysicalDiskKind,
}

/// The operator-defined policy of a physical disk.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PhysicalDiskPolicy {
    /// The operator has indicated that the disk is in-service.
    InService,

    /// The operator has indicated that the disk has been permanently removed
    /// from service.
    ///
    /// This is a terminal state: once a particular disk ID is expunged, it
    /// will never return to service. (The actual hardware may be reused, but
    /// it will be treated as a brand-new disk.)
    ///
    /// An expunged disk is always non-provisionable.
    Expunged,
    // NOTE: if you add a new value here, be sure to add it to
    // the `IntoEnumIterator` impl below!
}

// Can't automatically derive strum::EnumIter because that doesn't provide a
// way to iterate over nested enums.
impl IntoEnumIterator for PhysicalDiskPolicy {
    type Iterator = std::array::IntoIter<Self, 2>;

    fn iter() -> Self::Iterator {
        [Self::InService, Self::Expunged].into_iter()
    }
}

impl PhysicalDiskPolicy {
    /// Creates a new `PhysicalDiskPolicy` that is in-service.
    pub fn in_service() -> Self {
        Self::InService
    }

    /// Returns true if the disk can be decommissioned in this state.
    pub fn is_decommissionable(&self) -> bool {
        // This should be kept in sync with decommissionable_states below.
        match self {
            Self::InService => false,
            Self::Expunged => true,
        }
    }
}

impl fmt::Display for PhysicalDiskPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalDiskPolicy::InService => write!(f, "in service"),
            PhysicalDiskPolicy::Expunged => write!(f, "expunged"),
        }
    }
}

/// The current state of the disk, as determined by Nexus.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalDiskState {
    /// The disk is currently active, and has resources allocated on it.
    Active,

    /// The disk has been permanently removed from service.
    ///
    /// This is a terminal state: once a particular disk ID is decommissioned,
    /// it will never return to service. (The actual hardware may be reused,
    /// but it will be treated as a brand-new disk.)
    Decommissioned,
}

impl fmt::Display for PhysicalDiskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalDiskState::Active => write!(f, "active"),
            PhysicalDiskState::Decommissioned => write!(f, "decommissioned"),
        }
    }
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
/// Built-in users are identities internal to the system, used when the control
/// plane performs actions autonomously
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
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
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

// ALLOWED SOURCE IPS

/// Allowlist of IPs or subnets that can make requests to user-facing services.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AllowList {
    /// Time the list was created.
    pub time_created: DateTime<Utc>,
    /// Time the list was last modified.
    pub time_modified: DateTime<Utc>,
    /// The allowlist of IPs or subnets.
    pub allowed_ips: ExternalAllowedSourceIps,
}

// OxQL QUERIES

/// The result of a successful OxQL query.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct OxqlQueryResult {
    /// Tables resulting from the query, each containing timeseries.
    pub tables: Vec<oxql_types::Table>,
}
