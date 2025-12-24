// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo-related types for the Nexus external API.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    ByteCount, IdentityMetadata, IdentityMetadataCreateParams, Name, NameOrId,
    Nullable, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use uuid::Uuid;

use super::certificate::CertificateCreate;
use super::policy::{FleetRole, SiloRole};

/// Describes how identities are managed and users are authenticated in this
/// Silo
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SiloIdentityMode {
    /// Users are authenticated with SAML using an external authentication
    /// provider.  The system updates information about users and groups only
    /// during successful authentication (i.e,. "JIT provisioning" of users and
    /// groups).
    SamlJit,

    /// The system is the source of truth about users.  There is no linkage to
    /// an external authentication provider or identity provider.
    // NOTE: authentication for these users is not supported yet at all.  It
    // will eventually be password-based.
    LocalOnly,

    /// Users are authenticated with SAML using an external authentication
    /// provider. Users and groups are managed with SCIM API calls, likely from
    /// the same authentication provider.
    SamlScim,
}

/// How users are authenticated in this Silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationMode {
    /// Authentication is via SAML using an external authentication provider
    Saml,

    /// Authentication is local to the Oxide system
    Local,
}

/// How users will be provisioned in a silo during authentication.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UserProvisionType {
    /// Identities are managed directly by explicit calls to the external API.
    /// They are not synchronized from any external identity provider nor
    /// automatically created or updated when a user logs in.
    ApiOnly,

    /// Users and groups are created or updated during authentication using
    /// information provided by the authentication provider
    Jit,

    /// Users and groups are managed by SCIM
    Scim,
}

// View types

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
    pub identity_mode: SiloIdentityMode,

    /// Mapping of which Fleet roles are conferred by each Silo role
    ///
    /// The default is that no Fleet roles are conferred by any Silo roles
    /// unless there's a corresponding entry in this map.
    pub mapped_fleet_roles: BTreeMap<SiloRole, BTreeSet<FleetRole>>,

    /// Optionally, silos can have a group name that is automatically granted
    /// the silo admin role.
    pub admin_group_name: Option<String>,
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

/// View of silo authentication settings
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloAuthSettings {
    pub silo_id: Uuid,
    /// Maximum lifetime of a device token in seconds. If set to null, users
    /// will be able to create tokens that do not expire.
    pub device_token_max_ttl_seconds: Option<u32>,
}

// Params

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SiloSelector {
    /// Name or ID of the silo
    pub silo: NameOrId,
}

impl From<Name> for SiloSelector {
    fn from(name: Name) -> Self {
        SiloSelector { silo: name.into() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalSiloSelector {
    /// Name or ID of the silo
    pub silo: Option<NameOrId>,
}

/// Create-time parameters for a `Silo`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub discoverable: bool,

    pub identity_mode: SiloIdentityMode,

    /// If set, this group will be created during Silo creation and granted the
    /// "Silo Admin" role. Identity providers can assert that users belong to
    /// this group and those users can log in and further initialize the Silo.
    ///
    /// Note that if configuring a SAML based identity provider,
    /// group_attribute_name must be set for users to be considered part of a
    /// group. See `SamlIdentityProviderCreate` for more information.
    pub admin_group_name: Option<String>,

    /// Initial TLS certificates to be used for the new Silo's console and API
    /// endpoints.  These should be valid for the Silo's DNS name(s).
    pub tls_certificates: Vec<CertificateCreate>,

    /// Limits the amount of provisionable CPU, memory, and storage in the Silo.
    /// CPU and memory are only consumed by running instances, while storage is
    /// consumed by any disk or snapshot. A value of 0 means that resource is
    /// *not* provisionable.
    pub quotas: SiloQuotasCreate,

    /// Mapping of which Fleet roles are conferred by each Silo role
    ///
    /// The default is that no Fleet roles are conferred by any Silo roles
    /// unless there's a corresponding entry in this map.
    #[serde(default)]
    pub mapped_fleet_roles: BTreeMap<SiloRole, BTreeSet<FleetRole>>,
}

/// The amount of provisionable resources for a Silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasCreate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: i64,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: ByteCount,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: ByteCount,
}

/// Updateable properties of a Silo's resource limits.
/// If a value is omitted it will not be updated.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasUpdate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: Option<i64>,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: Option<ByteCount>,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: Option<ByteCount>,
}

/// Updateable properties of a silo's settings.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SiloAuthSettingsUpdate {
    /// Maximum lifetime of a device token in seconds. If set to null, users
    /// will be able to create tokens that do not expire.
    pub device_token_max_ttl_seconds: Nullable<NonZeroU32>,
}
