// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external API types (version 2025120500).
//!
//! Version 2025120500 types (after multicast implicit lifecycle updates but
//! before `ip_version` preference for IP pools).
//!
//! Key differences from newer API versions:
//! - [`FloatingIpCreate`] and [`EphemeralIpCreate`] don't have `ip_version`
//!   field (newer versions allow specifying IPv4/IPv6 preference for default
//!   pool selection).
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`EphemeralIpCreate`]: self::EphemeralIpCreate

use std::net::IpAddr;

use nexus_types::external_api::params;
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IpVersion, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for creating a new floating IP address for instances.
///
/// This version doesn't have the ip_version field which was added later.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// An IP address to reserve for use as a floating IP. This field is
    /// optional: when not set, an address will be automatically chosen from
    /// pool. If set, then the IP must be available in the resolved pool.
    pub ip: Option<IpAddr>,

    /// The parent IP pool that a floating IP is pulled from. If unset, the
    /// default pool is selected.
    pub pool: Option<NameOrId>,
}

impl From<FloatingIpCreate> for params::FloatingIpCreate {
    fn from(old: FloatingIpCreate) -> Self {
        Self {
            identity: old.identity,
            ip: old.ip,
            pool: old.pool,
            // This version predates `ip_version` support. V4 is used by default
            // since V6 pools were not yet supported.
            ip_version: Some(IpVersion::V4),
        }
    }
}

/// Parameters for creating an ephemeral IP address for an instance.
///
/// This version doesn't have the ip_version field which was added later.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address. If unspecified,
    /// the default IP pool will be used.
    pub pool: Option<NameOrId>,
}

impl From<EphemeralIpCreate> for params::EphemeralIpCreate {
    fn from(old: EphemeralIpCreate) -> Self {
        Self {
            pool: old.pool,
            // This version predates `ip_version` support. V4 is used by default
            // since V6 pools were not yet supported.
            ip_version: Some(IpVersion::V4),
        }
    }
}
