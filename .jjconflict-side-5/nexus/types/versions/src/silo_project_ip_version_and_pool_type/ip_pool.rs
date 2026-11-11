// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP pool types for version SILO_PROJECT_IP_VERSION_AND_POOL_TYPE.
//!
//! This version adds `ip_version` and `pool_type` fields to SiloIpPool.

use omicron_common::api::external::{
    IdentityMetadata, IpVersion, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::ip_pool::IpPoolType;

/// An IP pool in the context of a silo
#[derive(
    api_identity::ObjectIdentity,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct SiloIpPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    ///
    /// A silo can have at most one default pool per combination of pool type
    /// (unicast or multicast) and IP version (IPv4 or IPv6), allowing up to 4
    /// default pools total.
    pub is_default: bool,

    /// The IP version for the pool.
    pub ip_version: IpVersion,

    /// Type of IP pool (unicast or multicast).
    pub pool_type: IpPoolType,
}

impl From<SiloIpPool> for crate::v2025_11_20_00::ip_pool::SiloIpPool {
    fn from(new: SiloIpPool) -> crate::v2025_11_20_00::ip_pool::SiloIpPool {
        crate::v2025_11_20_00::ip_pool::SiloIpPool {
            identity: new.identity,
            is_default: new.is_default,
        }
    }
}
