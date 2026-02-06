// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for rack initialization types.

use crate::latest::rack_init::RackInitializeRequest;
use crate::latest::rack_init::RackInitializeRequestParams;
use anyhow::{Result, bail};
use omicron_common::address::{
    AZ_PREFIX, IpRange, Ipv6Subnet, RACK_PREFIX, SLED_PREFIX, get_64_subnet,
};
use omicron_common::api::external::AllowedSourceIps;
use std::net::IpAddr;

impl RackInitializeRequest {
    pub fn az_subnet(&self) -> Ipv6Subnet<AZ_PREFIX> {
        Ipv6Subnet::<AZ_PREFIX>::new(
            self.rack_network_config.rack_subnet.addr(),
        )
    }

    /// Returns the subnet for our rack.
    pub fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX> {
        Ipv6Subnet::<RACK_PREFIX>::new(
            self.rack_network_config.rack_subnet.addr(),
        )
    }

    /// Returns the subnet for the `index`-th sled in the rack.
    pub fn sled_subnet(&self, index: u8) -> Ipv6Subnet<SLED_PREFIX> {
        get_64_subnet(self.rack_subnet(), index)
    }
}

// This custom debug implementation hides the private keys.
impl std::fmt::Debug for RackInitializeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // If you find a compiler error here, and you just added a field to this
        // struct, be sure to add it to the Debug impl below!
        let RackInitializeRequest {
            trust_quorum_peers,
            bootstrap_discovery,
            ntp_servers,
            dns_servers,
            internal_services_ip_pool_ranges,
            external_dns_ips,
            external_dns_zone_name,
            external_certificates: _,
            recovery_silo,
            rack_network_config,
            allowed_source_ips,
        } = &self;

        f.debug_struct("RackInitializeRequest")
            .field("trust_quorum_peers", trust_quorum_peers)
            .field("bootstrap_discovery", bootstrap_discovery)
            .field("ntp_servers", ntp_servers)
            .field("dns_servers", dns_servers)
            .field(
                "internal_services_ip_pool_ranges",
                internal_services_ip_pool_ranges,
            )
            .field("external_dns_ips", external_dns_ips)
            .field("external_dns_zone_name", external_dns_zone_name)
            .field("external_certificates", &"<redacted>")
            .field("recovery_silo", recovery_silo)
            .field("rack_network_config", rack_network_config)
            .field("allowed_source_ips", allowed_source_ips)
            .finish()
    }
}

impl RackInitializeRequestParams {
    pub fn new(
        rack_initialize_request: RackInitializeRequest,
        skip_timesync: bool,
    ) -> RackInitializeRequestParams {
        RackInitializeRequestParams { rack_initialize_request, skip_timesync }
    }
}

/// This field was added after several racks were already deployed. RSS plans
/// for those racks should default to allowing any source IP, since that is
/// effectively what they did.
pub(crate) const fn default_allowed_source_ips() -> AllowedSourceIps {
    AllowedSourceIps::Any
}

pub(crate) fn validate_external_dns(
    dns_ips: &Vec<IpAddr>,
    internal_ranges: &Vec<IpRange>,
) -> Result<()> {
    if dns_ips.is_empty() {
        bail!("At least one external DNS IP is required");
    }

    // Every external DNS IP should also be present in one of the internal
    // services IP pool ranges. This check is O(N*M), but we expect both N
    // and M to be small (~5 DNS servers, and a small number of pools).
    for &dns_ip in dns_ips {
        if !internal_ranges.iter().any(|range| range.contains(dns_ip)) {
            bail!(
                "External DNS IP {dns_ip} is not contained in \
                 `internal_services_ip_pool_ranges`"
            );
        }
    }

    Ok(())
}
