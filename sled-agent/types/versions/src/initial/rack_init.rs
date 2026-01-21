// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack initialization types

use std::{collections::BTreeSet, net::IpAddr, net::Ipv6Addr};

use anyhow::{Result, bail};
use camino::Utf8PathBuf;
use omicron_common::{
    address::{
        AZ_PREFIX, IpRange, Ipv6Subnet, RACK_PREFIX, SLED_PREFIX, get_64_subnet,
    },
    api::{
        external::AllowedSourceIps,
        internal::{nexus::Certificate, shared::RackNetworkConfig},
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::Baseboard;

use crate::bootstrap_v1::rack_init::RecoverySiloConfig;

// "Shadow" copy of `RackInitializeRequest` that does no validation on its
// fields.
#[derive(Clone, Deserialize)]
struct UnvalidatedRackInitializeRequest {
    trust_quorum_peers: Option<Vec<Baseboard>>,
    bootstrap_discovery: BootstrapAddressDiscovery,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    internal_services_ip_pool_ranges: Vec<IpRange>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo: RecoverySiloConfig,
    rack_network_config: RackNetworkConfig,
    #[serde(default = "default_allowed_source_ips")]
    allowed_source_ips: AllowedSourceIps,
}

fn validate_external_dns(
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

impl TryFrom<UnvalidatedRackInitializeRequest> for RackInitializeRequest {
    type Error = anyhow::Error;

    fn try_from(value: UnvalidatedRackInitializeRequest) -> Result<Self> {
        validate_external_dns(
            &value.external_dns_ips,
            &value.internal_services_ip_pool_ranges,
        )?;

        Ok(RackInitializeRequest {
            trust_quorum_peers: value.trust_quorum_peers,
            bootstrap_discovery: value.bootstrap_discovery,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            internal_services_ip_pool_ranges: value
                .internal_services_ip_pool_ranges,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            external_certificates: value.external_certificates,
            recovery_silo: value.recovery_silo,
            rack_network_config: value.rack_network_config,
            allowed_source_ips: value.allowed_source_ips,
        })
    }
}

/// Configuration for the "rack setup service".
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(try_from = "UnvalidatedRackInitializeRequest")]
pub struct RackInitializeRequest {
    /// The set of peer_ids required to initialize trust quorum
    ///
    /// The value is `None` if we are not using trust quorum
    pub trust_quorum_peers: Option<Vec<Baseboard>>,

    /// Describes how bootstrap addresses should be collected during RSS.
    pub bootstrap_discovery: BootstrapAddressDiscovery,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,

    /// Ranges of the service IP pool which may be used for internal services.
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Eventually,
    // we want to configure multiple pools.
    pub internal_services_ip_pool_ranges: Vec<IpRange>,

    /// Service IP addresses on which we run external DNS servers.
    ///
    /// Each address must be present in `internal_services_ip_pool_ranges`.
    pub external_dns_ips: Vec<IpAddr>,

    /// DNS name for the DNS zone delegated to the rack for external DNS
    pub external_dns_zone_name: String,

    /// initial TLS certificates for the external API
    pub external_certificates: Vec<Certificate>,

    /// Configuration of the Recovery Silo (the initial Silo)
    pub recovery_silo: RecoverySiloConfig,

    /// Initial rack network configuration
    pub rack_network_config: RackNetworkConfig,

    /// IPs or subnets allowed to make requests to user-facing services
    #[serde(default = "default_allowed_source_ips")]
    pub allowed_source_ips: AllowedSourceIps,
}

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

#[derive(Debug, thiserror::Error)]
pub enum RackInitializeRequestParseError {
    #[error("Failed to read config from {path}: {err}")]
    Io {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("Failed to deserialize config from {path}: {err}")]
    Deserialize {
        path: Utf8PathBuf,
        #[source]
        err: anyhow::Error,
    },
    #[error("Loading certificate: {0}")]
    Certificate(#[source] anyhow::Error),
}

/// This field was added after several racks were already deployed. RSS plans
/// for those racks should default to allowing any source IP, since that is
/// effectively what they did.
pub const fn default_allowed_source_ips() -> AllowedSourceIps {
    AllowedSourceIps::Any
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: BTreeSet<Ipv6Addr> },
}
