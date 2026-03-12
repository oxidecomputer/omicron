// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP pool types for version INITIAL.

use api_identity::ObjectIdentity;
use chrono::{DateTime, Utc};
use omicron_common::address::IpRange;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, IpVersion, NameOrId, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Type of IP pool.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum IpPoolType {
    /// Unicast IP pool for standard IP allocations.
    #[default]
    Unicast,
    /// Multicast IP pool for multicast group allocations.
    ///
    /// All ranges in a multicast pool must be either ASM or SSM (not mixed).
    Multicast,
}

/// A collection of IP ranges. If a pool is linked to a silo, IP addresses from
/// the pool can be allocated within that silo.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for the pool.
    pub ip_version: IpVersion,
    /// Type of IP pool (unicast or multicast).
    pub pool_type: IpPoolType,
}

/// The utilization of IP addresses in a pool.
///
/// Note that both the count of remaining addresses and the total capacity are
/// integers, reported as floating point numbers. This accommodates allocations
/// larger than a 64-bit integer, which is common with IPv6 address spaces. With
/// very large IP Pools (> 2**53 addresses), integer precision will be lost, in
/// exchange for representing the entire range. In such a case the pool still
/// has many available addresses.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolUtilization {
    /// The number of remaining addresses in the pool.
    pub remaining: f64,
    /// The total number of addresses in the pool.
    pub capacity: f64,
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
    ///
    /// A silo can have at most one default pool per combination of pool type
    /// (unicast or multicast) and IP version (IPv4 or IPv6), allowing up to 4
    /// default pools total.
    pub is_default: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolRange {
    pub id: Uuid,
    pub ip_pool_id: Uuid,
    pub time_created: DateTime<Utc>,
    pub range: IpRange,
}

/// Create-time parameters for an `IpPool`.
///
/// For multicast pools, all ranges must be either Any-Source Multicast (ASM)
/// or Source-Specific Multicast (SSM), but not both. Mixing ASM and SSM
/// ranges in the same pool is not allowed.
///
/// ASM: IPv4 addresses outside 232.0.0.0/8, IPv6 addresses with flag field != 3
/// SSM: IPv4 addresses in 232.0.0.0/8, IPv6 addresses with flag field = 3
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The IP version of the pool.
    ///
    /// The default is IPv4.
    #[serde(default = "IpVersion::v4")]
    pub ip_version: IpVersion,
    /// Type of IP pool (defaults to Unicast)
    #[serde(default)]
    pub pool_type: IpPoolType,
}

impl IpPoolCreate {
    /// Create parameters for a unicast IP pool (the default)
    pub fn new(
        identity: IdentityMetadataCreateParams,
        ip_version: IpVersion,
    ) -> Self {
        Self { identity, ip_version, pool_type: IpPoolType::Unicast }
    }

    /// Create parameters for a multicast IP pool
    pub fn new_multicast(
        identity: IdentityMetadataCreateParams,
        ip_version: IpVersion,
    ) -> Self {
        Self { identity, ip_version, pool_type: IpPoolType::Multicast }
    }
}

/// Parameters for updating an IP Pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolSiloPath {
    pub pool: NameOrId,
    pub silo: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolLinkSilo {
    pub silo: NameOrId,
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo.
    pub is_default: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolSiloUpdate {
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo, so when a pool is
    /// made default, an existing default will remain linked but will no longer
    /// be the default.
    pub is_default: bool,
}
