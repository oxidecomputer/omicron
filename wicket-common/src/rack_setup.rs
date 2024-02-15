// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use omicron_common::address;
use omicron_common::api::internal::shared::BgpConfig;
use omicron_common::api::internal::shared::PortConfigV1;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;

/// User-specified parts of
/// [`RackNetworkConfig`](omicron_common::api::internal::shared::RackNetworkConfig).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct UserSpecifiedRackNetworkConfig {
    pub infra_ip_first: Ipv4Addr,
    pub infra_ip_last: Ipv4Addr,
    pub ports: Vec<PortConfigV1>,
    pub bgp: Vec<BgpConfig>,
}

// The portion of `CurrentRssUserConfig` that can be posted in one shot; it is
// provided by the wicket user uploading a TOML file, currently.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PutRssUserConfigInsensitive {
    /// List of slot numbers only.
    ///
    /// `wicketd` will map this back to sleds with the correct `SpIdentifier`
    /// based on the `bootstrap_sleds` it provides in
    /// `CurrentRssUserConfigInsensitive`.
    pub bootstrap_sleds: BTreeSet<u32>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
}
