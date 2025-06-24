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
use daft::Diffable;
use omicron_common::api::external::{
    AffinityPolicy, AllowedSourceIps as ExternalAllowedSourceIps, ByteCount,
    Digest, Error, FailureDomain, IdentityMetadata, InstanceState, Name,
    ObjectIdentity, RoleName, SimpleIdentity, SimpleIdentityOrName,
};
use omicron_uuid_kinds::{AlertReceiverUuid, AlertUuid};
use oxnet::{Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::sync::LazyLock;
use strum::{EnumIter, IntoEnumIterator};
use url::Url;
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
impl SimpleIdentityOrName for SiloUtilization {
    fn id(&self) -> Uuid {
        self.silo_id
    }
    fn name(&self) -> &Name {
        &self.silo_name
    }
}

/// View of silo authentication settings
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloAuthSettings {
    pub silo_id: Uuid,
    /// Maximum lifetime of a device token in seconds. If set to null, users
    /// will be able to create tokens that do not expire.
    pub device_token_max_ttl_seconds: Option<u32>,
}

// AFFINITY GROUPS

/// View of an Affinity Group
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AffinityGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// View of an Anti-Affinity Group
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AntiAffinityGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
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

// INTERNET GATEWAYS

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

    /// The associated internet gateway.
    pub internet_gateway_id: Uuid,

    /// The associated IP address,
    pub address: IpAddr,
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

/// The unique ID of a sled.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SledId {
    pub id: Uuid,
}

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
    /// The current state of the sled.
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

/// The current state of the sled.
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
    Diffable,
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
    Diffable,
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

/// View of a device access token
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DeviceAccessToken {
    /// A unique, immutable, system-controlled identifier for the token.
    /// Note that this ID is not the bearer token itself, which starts with
    /// "oxide-token-"
    pub id: Uuid,
    pub time_created: DateTime<Utc>,

    /// Expiration timestamp. A null value means the token does not automatically expire.
    pub time_expires: Option<DateTime<Utc>>,
}

impl SimpleIdentity for DeviceAccessToken {
    fn id(&self) -> Uuid {
        self.id
    }
}

// OAUTH 2.0 DEVICE AUTHORIZATION REQUESTS & TOKENS

/// Response to an initial device authorization request.
/// See RFC 8628 §3.2 (Device Authorization Response).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthResponse {
    /// The device verification code
    pub device_code: String,

    /// The end-user verification code
    pub user_code: String,

    /// The end-user verification URI on the authorization server.
    /// The URI should be short and easy to remember as end users
    /// may be asked to manually type it into their user agent.
    pub verification_uri: String,

    /// The lifetime in seconds of the `device_code` and `user_code`
    pub expires_in: u16,
}

/// Successful access token grant. See RFC 6749 §5.1.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAccessTokenGrant {
    /// The access token issued to the client
    pub access_token: String,

    /// The type of the token issued, as described in RFC 6749 §7.1.
    pub token_type: DeviceAccessTokenType,

    /// A unique, immutable, system-controlled identifier for the token
    pub token_id: Uuid,

    /// Expiration timestamp. A null value means the token does not automatically expire.
    pub time_expires: Option<DateTime<Utc>>,
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

// ALERTS

/// An alert class.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertClass {
    /// The name of the alert class.
    pub name: String,

    /// A description of what this alert class represents.
    pub description: String,
}

/// The configuration for an alert receiver.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct AlertReceiver {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The list of alert classes to which this receiver is subscribed.
    pub subscriptions: Vec<shared::AlertSubscription>,

    /// Configuration specific to the kind of alert receiver that this is.
    pub kind: AlertReceiverKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertSubscriptionCreated {
    /// The new subscription added to the receiver.
    pub subscription: shared::AlertSubscription,
}

/// The possible alert delivery mechanisms for an alert receiver.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AlertReceiverKind {
    Webhook(WebhookReceiverConfig),
}

/// The configuration for a webhook alert receiver.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct WebhookReceiver {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The list of alert classes to which this receiver is subscribed.
    pub subscriptions: Vec<shared::AlertSubscription>,

    #[serde(flatten)]
    pub config: WebhookReceiverConfig,
}

impl From<WebhookReceiver> for AlertReceiver {
    fn from(
        WebhookReceiver { identity, subscriptions, config }: WebhookReceiver,
    ) -> Self {
        Self {
            identity,
            subscriptions,
            kind: AlertReceiverKind::Webhook(config),
        }
    }
}

impl PartialEq<WebhookReceiver> for AlertReceiver {
    fn eq(&self, other: &WebhookReceiver) -> bool {
        // Will become refutable if/when more variants are added...
        #[allow(irrefutable_let_patterns)]
        let AlertReceiverKind::Webhook(ref config) = self.kind else {
            return false;
        };
        self.identity == other.identity
            && self.subscriptions == other.subscriptions
            && config == &other.config
    }
}

impl PartialEq<AlertReceiver> for WebhookReceiver {
    fn eq(&self, other: &AlertReceiver) -> bool {
        // Will become refutable if/when more variants are added...
        #[allow(irrefutable_let_patterns)]
        let AlertReceiverKind::Webhook(ref config) = other.kind else {
            return false;
        };
        self.identity == other.identity
            && self.subscriptions == other.subscriptions
            && &self.config == config
    }
}

/// Webhook-specific alert receiver configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WebhookReceiverConfig {
    /// The URL that webhook notification requests are sent to.
    pub endpoint: Url,
    // A list containing the IDs of the secret keys used to sign payloads sent
    // to this receiver.
    pub secrets: Vec<WebhookSecret>,
}

/// A list of the IDs of secrets associated with a webhook receiver.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookSecrets {
    pub secrets: Vec<WebhookSecret>,
}

/// A view of a shared secret key assigned to a webhook receiver.
///
/// Once a secret is created, the value of the secret is not available in the
/// API, as it must remain secret. Instead, secrets are referenced by their
/// unique IDs assigned when they are created.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WebhookSecret {
    /// The public unique ID of the secret.
    pub id: Uuid,

    /// The UTC timestamp at which this secret was created.
    pub time_created: DateTime<Utc>,
}

/// A delivery of a webhook event.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct AlertDelivery {
    /// The UUID of this delivery attempt.
    pub id: Uuid,

    /// The UUID of the alert receiver that this event was delivered to.
    pub receiver_id: AlertReceiverUuid,

    /// The event class.
    pub alert_class: String,

    /// The UUID of the event.
    pub alert_id: AlertUuid,

    /// The state of this delivery.
    pub state: AlertDeliveryState,

    /// Why this delivery was performed.
    pub trigger: AlertDeliveryTrigger,

    /// Individual attempts to deliver this webhook event, and their outcomes.
    pub attempts: AlertDeliveryAttempts,

    /// The time at which this delivery began (i.e. the event was dispatched to
    /// the receiver).
    pub time_started: DateTime<Utc>,
}

/// The state of a webhook delivery attempt.
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryState {
    /// The webhook event has not yet been delivered successfully.
    ///
    /// Either no delivery attempts have yet been performed, or the delivery has
    /// failed at least once but has retries remaining.
    Pending,
    /// The webhook event has been delivered successfully.
    Delivered,
    /// The webhook delivery attempt has failed permanently and will not be
    /// retried again.
    Failed,
}

impl AlertDeliveryState {
    pub const ALL: &[Self] = <Self as strum::VariantArray>::VARIANTS;

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for AlertDeliveryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AlertDeliveryState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        static EXPECTED_ONE_OF: LazyLock<String> =
            LazyLock::new(expected_one_of::<AlertDeliveryState>);

        for &v in Self::ALL {
            if s.trim().eq_ignore_ascii_case(v.as_str()) {
                return Ok(v);
            }
        }
        Err(Error::invalid_value("AlertDeliveryState", &*EXPECTED_ONE_OF))
    }
}

/// The reason an alert was delivered
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryTrigger {
    /// Delivery was triggered by the alert itself.
    Alert,
    /// Delivery was triggered by a request to resend the alert.
    Resend,
    /// This delivery is a liveness probe.
    Probe,
}

impl AlertDeliveryTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Alert => "alert",
            Self::Resend => "resend",
            Self::Probe => "probe",
        }
    }
}

impl fmt::Display for AlertDeliveryTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AlertDeliveryTrigger {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        static EXPECTED_ONE_OF: LazyLock<String> =
            LazyLock::new(expected_one_of::<AlertDeliveryTrigger>);

        for &v in <Self as strum::VariantArray>::VARIANTS {
            if s.trim().eq_ignore_ascii_case(v.as_str()) {
                return Ok(v);
            }
        }
        Err(Error::invalid_value("AlertDeliveryTrigger", &*EXPECTED_ONE_OF))
    }
}

/// A list of attempts to deliver an alert to a receiver.
///
/// The type of the delivery attempt model depends on the receiver type, as it
/// may contain information specific to that delivery mechanism. For example,
/// webhook delivery attempts contain the HTTP status code of the webhook
/// request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryAttempts {
    /// A list of attempts to deliver an alert to a webhook receiver.
    Webhook(Vec<WebhookDeliveryAttempt>),
}

/// An individual delivery attempt for a webhook event.
///
/// This represents a single HTTP request that was sent to the receiver, and its
/// outcome.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct WebhookDeliveryAttempt {
    /// The time at which the webhook delivery was attempted.
    pub time_sent: DateTime<Utc>,

    /// The attempt number.
    pub attempt: usize,

    /// The outcome of this delivery attempt: either the event was delivered
    /// successfully, or the request failed for one of several reasons.
    pub result: WebhookDeliveryAttemptResult,

    pub response: Option<WebhookDeliveryResponse>,
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
pub enum WebhookDeliveryAttemptResult {
    /// The webhook event has been delivered successfully.
    Succeeded,
    /// A webhook request was sent to the endpoint, and it
    /// returned a HTTP error status code indicating an error.
    FailedHttpError,
    /// The webhook request could not be sent to the receiver endpoint.
    FailedUnreachable,
    /// A connection to the receiver endpoint was successfully established, but
    /// no response was received within the delivery timeout.
    FailedTimeout,
}

impl WebhookDeliveryAttemptResult {
    pub const ALL: &[Self] = <Self as strum::VariantArray>::VARIANTS;
    pub const ALL_FAILED: &[Self] =
        &[Self::FailedHttpError, Self::FailedUnreachable, Self::FailedTimeout];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::FailedHttpError => "failed_http_error",
            Self::FailedTimeout => "failed_timeout",
            Self::FailedUnreachable => "failed_unreachable",
        }
    }

    /// Returns `true` if this `WebhookDeliveryAttemptResult` represents a failure
    pub fn is_failed(&self) -> bool {
        *self != Self::Succeeded
    }
}

impl fmt::Display for WebhookDeliveryAttemptResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The response received from a webhook receiver endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct WebhookDeliveryResponse {
    /// The HTTP status code returned from the webhook endpoint.
    pub status: u16,
    /// The response time of the webhook endpoint, in milliseconds.
    pub duration_ms: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct AlertDeliveryId {
    pub delivery_id: Uuid,
}

/// Data describing the result of an alert receiver liveness probe attempt.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct AlertProbeResult {
    /// The outcome of the probe delivery.
    pub probe: AlertDelivery,
    /// If the probe request succeeded, and resending failed deliveries on
    /// success was requested, the number of new delivery attempts started.
    /// Otherwise, if the probe did not succeed, or resending failed deliveries
    /// was not requested, this is null.
    ///
    /// Note that this may be 0, if there were no events found which had not
    /// been delivered successfully to this receiver.
    pub resends_started: Option<usize>,
}

// UPDATE

/// Source of a system software target release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TargetReleaseSource {
    /// Unspecified or unknown source (probably MUPdate).
    Unspecified,

    /// The specified release of the rack's system software.
    SystemVersion { version: Version },
}

/// View of a system software target release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct TargetRelease {
    /// The target-release generation number.
    pub generation: i64,

    /// The time it was set as the target release.
    pub time_requested: DateTime<Utc>,

    /// The source of the target release.
    pub release_source: TargetReleaseSource,

    /// MUPdate overrides currently in place, keyed by sled ID.
    ///
    /// After a recovery-driven update (also known as a MUPdate), this map will
    /// be non-empty until the normal update path is restored.
    pub mupdate_overrides: BTreeMap<Uuid, TargetReleaseMupdateOverride>,
}

/// MUPdate override for a target release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct TargetReleaseMupdateOverride {
    /// The override UUID.
    pub mupdate_override_id: Uuid,
}

fn expected_one_of<T: strum::VariantArray + fmt::Display>() -> String {
    use std::fmt::Write;
    let mut msg = "expected one of:".to_string();
    let mut variants = T::VARIANTS.iter().peekable();
    while let Some(variant) = variants.next() {
        if variants.peek().is_some() {
            write!(&mut msg, " '{variant}',").unwrap();
        } else {
            write!(&mut msg, " or '{variant}'").unwrap();
        }
    }
    msg
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_expected_one_of() {
        // Test this using an enum that we declare here, so that the test
        // needn't be updated if the types which actually use this helper
        // change.
        #[derive(Debug, strum::VariantArray)]
        enum Test {
            Foo,
            Bar,
            Baz,
        }

        impl fmt::Display for Test {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(self, f)
            }
        }

        assert_eq!(
            expected_one_of::<Test>(),
            "expected one of: 'Foo', 'Bar', or 'Baz'"
        );
    }
}
