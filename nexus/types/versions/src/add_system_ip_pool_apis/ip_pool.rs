// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP pool types for version ADD_SYSTEM_IP_POOL_APIS.
//!
//! Adds an `assignment` field to `IpPool`, making it the new operator-facing
//! view. Introduces `IpPoolAssignment` (the enum of possible assignments) and
//! `IpPoolAssignParam` (the body for reassigning a pool). Also adds
//! `IpPoolFilter` and `SystemIpPoolFilter` for filtering the paginated
//! endpoints.

use api_identity::ObjectIdentity;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IpVersion;
use omicron_common::api::external::ObjectIdentity;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::v2025_11_20_00::ip_pool::IpPoolType;

/// Optional filter parameters for the silo-scoped IP pool listing endpoint.
#[derive(
    Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct IpPoolFilter {
    pub ip_version: Option<IpVersion>,
    pub pool_type: Option<IpPoolType>,
}

/// Optional filter parameters for the operator-scoped IP pool listing endpoint.
#[derive(
    Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct SystemIpPoolFilter {
    pub ip_version: Option<IpVersion>,
    pub assignment: Option<IpPoolAssignment>,
    pub pool_type: Option<IpPoolType>,
}

/// Assignment of an IP pool to resources and services.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum IpPoolAssignment {
    /// Pool is available to be linked to customer silos.
    #[default]
    Silos,
    /// Pool is reserved for Oxide-operated rack services (NTP, DNS, etc.).
    SystemServices,
}

/// A collection of IP ranges.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for the pool.
    pub ip_version: IpVersion,
    /// Type of IP pool (unicast or multicast).
    pub pool_type: IpPoolType,
    /// What this pool is currently assigned to.
    pub assignment: IpPoolAssignment,
}

/// Body parameters for reassigning an IP pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolAssignParam {
    pub assignment: IpPoolAssignment,
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
    /// What this pool is assigned to (defaults to Silos).
    #[serde(default)]
    pub assignment: IpPoolAssignment,
}

impl From<crate::v2025_11_20_00::ip_pool::IpPoolCreate> for IpPoolCreate {
    fn from(p: crate::v2025_11_20_00::ip_pool::IpPoolCreate) -> Self {
        Self {
            identity: p.identity,
            ip_version: p.ip_version,
            pool_type: p.pool_type,
            assignment: IpPoolAssignment::Silos,
        }
    }
}

impl From<IpPool> for crate::v2025_11_20_00::ip_pool::IpPool {
    fn from(pool: IpPool) -> Self {
        crate::v2025_11_20_00::ip_pool::IpPool {
            identity: pool.identity,
            ip_version: pool.ip_version,
            pool_type: pool.pool_type,
        }
    }
}
