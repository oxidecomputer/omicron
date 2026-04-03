// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for rack initialization types.

// TODO-john REMOVE ALL THIS

use crate::latest::rack_init::RackInitializeRequest;
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
