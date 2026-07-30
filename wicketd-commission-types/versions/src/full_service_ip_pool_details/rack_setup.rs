// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack setup (RSS) types for the `FULL_SERVICE_IP_POOL_DETAILS` version.
//!
//! Only [`PutRssUserConfigInsensitive`] and [`ServiceIpPoolConfig`] are
//! (re)defined here; every other rack-setup type is re-exported unchanged from
//! [`crate::v1::rack_setup`].

use std::collections::BTreeSet;
use std::net::IpAddr;

use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::address::IpRange;
use omicron_common::address::IpVersion;
use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::v1;
use crate::v1::rack_setup::AllowedSourceIps;
use crate::v1::rack_setup::UserSpecifiedRackNetworkConfig;

/// The portion of the RSS configuration that can be posted in one shot.
///
/// It is provided by the operator uploading a TOML file. Sensitive values
/// (certificates, the recovery password hash, and BGP authentication keys) are
/// set separately.
///
/// This version replaces the flat `internal_services_ip_pool_ranges` of the
/// initial version with fully-specified [`service_ip_pools`], letting operators
/// name and describe each pool.
///
/// [`service_ip_pools`]: Self::service_ip_pools
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(try_from = "UnvalidatedPutRssUserConfigInsensitive")]
pub struct PutRssUserConfigInsensitive {
    /// The slot numbers of the sleds to bring up during RSS.
    ///
    /// wicketd maps these back to sleds with the correct identifiers based on
    /// the bootstrap sleds it reports.
    pub bootstrap_sleds: BTreeSet<u16>,
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,
    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,
    /// The service IP pools which may be used for internal services.
    pub service_ip_pools: IdOrdMap<ServiceIpPoolConfig>,
    /// Service IP addresses on which external DNS servers are run.
    pub external_dns_ips: Vec<IpAddr>,
    /// The DNS zone name delegated to the rack for external DNS.
    pub external_dns_zone_name: String,
    /// The user-specified rack network configuration.
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
    /// IPs or subnets allowed to make requests to user-facing services.
    pub allowed_source_ips: AllowedSourceIps,
    /// Enable the fleet-wide jumbo-frames opt-in.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
}

// Shadow of `PutRssUserConfigInsensitive` that deserializes the pools as a
// plain `Vec` (the natural TOML/JSON array shape) before the `TryFrom` folds
// them into an `IdOrdMap`, failing on duplicate pool names.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnvalidatedPutRssUserConfigInsensitive {
    bootstrap_sleds: BTreeSet<u16>,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    service_ip_pools: Vec<ServiceIpPoolConfig>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    rack_network_config: UserSpecifiedRackNetworkConfig,
    allowed_source_ips: AllowedSourceIps,
    #[serde(default)]
    external_jumbo_frames_opt_in_enabled: bool,
}

impl TryFrom<UnvalidatedPutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    type Error = String;

    fn try_from(
        value: UnvalidatedPutRssUserConfigInsensitive,
    ) -> Result<Self, Self::Error> {
        let service_ip_pools =
            IdOrdMap::from_iter_unique(value.service_ip_pools)
                .map_err(|e| format!("duplicate service IP pool name: {e}"))?;
        Ok(Self {
            bootstrap_sleds: value.bootstrap_sleds,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            service_ip_pools,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            rack_network_config: value.rack_network_config,
            allowed_source_ips: value.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: value
                .external_jumbo_frames_opt_in_enabled,
        })
    }
}

// Well-known names/descriptions for the pools created from previous versions of
// the API, where we specified on the IP ranges alone.
const SERVICE_POOL_IPV4_NAME: &str = "oxide-service-pool-v4";
const SERVICE_POOL_IPV6_NAME: &str = "oxide-service-pool-v6";

// Synthesize named per-version pools from a flat list of ranges (how prior API
// versions specified service IPs). One pool is created per IP version that has
// at least one range, using the well-known names above.
fn service_ip_pools_from_ranges(
    ranges: Vec<IpRange>,
) -> IdOrdMap<ServiceIpPoolConfig> {
    let (v4_ranges, v6_ranges): (Vec<IpRange>, Vec<IpRange>) =
        ranges.into_iter().partition(|range| range.is_ipv4());
    let mut service_ip_pools = IdOrdMap::new();
    for (name, description, ranges) in [
        (SERVICE_POOL_IPV4_NAME, "IPv4 IP Pool for Oxide Services", v4_ranges),
        (SERVICE_POOL_IPV6_NAME, "IPv6 IP Pool for Oxide Services", v6_ranges),
    ] {
        match ServiceIpPoolConfig::new(
            name.parse().unwrap(),
            description.to_string(),
            ranges,
        ) {
            Ok(p) => service_ip_pools
                .insert_unique(p)
                .expect("well-known pool names are distinct"),
            Err(ServiceIpPoolError::EmptyRanges) => {}
            Err(ServiceIpPoolError::MixedIpVersions) => {
                unreachable!("just partitioned");
            }
        }
    }
    service_ip_pools
}

impl From<v1::rack_setup::PutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    fn from(old: v1::rack_setup::PutRssUserConfigInsensitive) -> Self {
        Self {
            bootstrap_sleds: old.bootstrap_sleds,
            ntp_servers: old.ntp_servers,
            dns_servers: old.dns_servers,
            service_ip_pools: service_ip_pools_from_ranges(
                old.internal_services_ip_pool_ranges,
            ),
            external_dns_ips: old.external_dns_ips,
            external_dns_zone_name: old.external_dns_zone_name,
            rack_network_config: old.rack_network_config,
            allowed_source_ips: old.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: old
                .external_jumbo_frames_opt_in_enabled,
        }
    }
}

/// Full details of a system-service IP pool, provided at rack setup (RSS).
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(try_from = "UnvalidatedServiceIpPoolConfig")]
pub struct ServiceIpPoolConfig {
    /// Name of the IP Pool
    pub name: Name,
    /// Description of the IP Pool
    pub description: String,
    /// List of IP address ranges in the pool.
    ///
    /// There is guaranteed to be at least one range, and all ranges are of the
    /// same IP version.
    // NOTE: Private to ensure the above invariants. `new()` checks them, and we
    // deserialize through `UnvalidatedServiceIpPoolConfig` to check them.
    ranges: Vec<IpRange>,
}

impl ServiceIpPoolConfig {
    /// Construct a new service IP pool configuration.
    ///
    /// Errors if `ranges` is empty, or if the ranges are a mix of IPv4 and
    /// IPv6 addresses.
    pub fn new(
        name: Name,
        description: String,
        ranges: Vec<IpRange>,
    ) -> Result<Self, ServiceIpPoolError> {
        let mut versions = ranges.iter().map(|r| r.version());
        let Some(first) = versions.next() else {
            return Err(ServiceIpPoolError::EmptyRanges);
        };
        if versions.any(|v| v != first) {
            return Err(ServiceIpPoolError::MixedIpVersions);
        }
        Ok(Self { name, description, ranges })
    }

    /// The ranges belonging to this pool.
    ///
    /// Guaranteed to be non-empty and all of the same IP version.
    pub fn ranges(&self) -> &[IpRange] {
        &self.ranges
    }

    /// The IP version of this pool, derived from its ranges.
    pub fn ip_version(&self) -> IpVersion {
        // Safety: the constructor guarantees at least one range, and that all
        // ranges share an IP version.
        self.ranges[0].version()
    }
}

/// Errors constructing a `ServiceIpPoolConfig`.
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum ServiceIpPoolError {
    #[error("must provide at least one IP range")]
    EmptyRanges,
    #[error("ranges have mixed IP versions")]
    MixedIpVersions,
}

impl From<ServiceIpPoolError> for Error {
    fn from(value: ServiceIpPoolError) -> Self {
        Error::internal_error(format!(
            "error constructing service IP pool config: {value}"
        ))
    }
}

impl IdOrdItem for ServiceIpPoolConfig {
    type Key<'a> = &'a Name;

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    id_upcast!();
}

#[derive(Deserialize)]
struct UnvalidatedServiceIpPoolConfig {
    name: Name,
    description: String,
    ranges: Vec<IpRange>,
}

impl TryFrom<UnvalidatedServiceIpPoolConfig> for ServiceIpPoolConfig {
    type Error = ServiceIpPoolError;

    fn try_from(
        value: UnvalidatedServiceIpPoolConfig,
    ) -> Result<Self, Self::Error> {
        let UnvalidatedServiceIpPoolConfig { name, description, ranges } =
            value;
        ServiceIpPoolConfig::new(name, description, ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::address;

    fn v4_range(first: &str, last: &str) -> address::IpRange {
        address::IpRange::try_from((
            first.parse::<std::net::Ipv4Addr>().unwrap(),
            last.parse::<std::net::Ipv4Addr>().unwrap(),
        ))
        .unwrap()
    }

    fn v6_range(first: &str, last: &str) -> address::IpRange {
        address::IpRange::try_from((
            first.parse::<std::net::Ipv6Addr>().unwrap(),
            last.parse::<std::net::Ipv6Addr>().unwrap(),
        ))
        .unwrap()
    }

    fn find_pool<'a>(
        pools: &'a IdOrdMap<ServiceIpPoolConfig>,
        name: &str,
    ) -> Option<&'a ServiceIpPoolConfig> {
        pools.iter().find(|p| p.name.as_str() == name)
    }

    // A rack whose service ranges are all IPv4 should produce a single IPv4
    // service pool and no IPv6 pool.
    #[test]
    fn service_ip_pools_v4_only() {
        let range = v4_range("192.168.1.20", "192.168.1.29");
        let pools = service_ip_pools_from_ranges(vec![range]);

        assert_eq!(pools.len(), 1);
        let pool =
            find_pool(&pools, SERVICE_POOL_IPV4_NAME).expect("v4 pool present");
        assert_eq!(pool.ranges(), &[range]);
        assert!(
            find_pool(&pools, SERVICE_POOL_IPV6_NAME).is_none(),
            "no v6 pool expected"
        );
    }

    // A rack whose service ranges are all IPv6 should produce a single IPv6
    // service pool and no IPv4 pool.
    #[test]
    fn service_ip_pools_v6_only() {
        let range = v6_range("fd00::20", "fd00::29");
        let pools = service_ip_pools_from_ranges(vec![range]);

        assert_eq!(pools.len(), 1);
        let pool =
            find_pool(&pools, SERVICE_POOL_IPV6_NAME).expect("v6 pool present");
        assert_eq!(pool.ranges(), &[range]);
        assert!(
            find_pool(&pools, SERVICE_POOL_IPV4_NAME).is_none(),
            "no v4 pool expected"
        );
    }

    // A dual-stack rack should produce one pool per version.
    #[test]
    fn service_ip_pools_dual_stack() {
        let v4 = v4_range("192.168.1.20", "192.168.1.29");
        let v6 = v6_range("fd00::20", "fd00::29");
        let pools = service_ip_pools_from_ranges(vec![v4, v6]);

        assert_eq!(pools.len(), 2);
        assert_eq!(
            find_pool(&pools, SERVICE_POOL_IPV4_NAME).unwrap().ranges(),
            &[v4]
        );
        assert_eq!(
            find_pool(&pools, SERVICE_POOL_IPV6_NAME).unwrap().ranges(),
            &[v6]
        );
    }

    // With no ranges the helper produces no pools. The requirement that a real
    // rack init request carry at least one pool is enforced by wicketd at the
    // time it tries to start rack setup.
    #[test]
    fn service_ip_pools_empty() {
        let pools = service_ip_pools_from_ranges(vec![]);
        assert!(pools.is_empty());
    }
}
