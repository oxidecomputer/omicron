// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack initialization types

use crate::bootstrap_v1::rack_init::RecoverySiloConfig;
use crate::impls::rack_init::default_allowed_source_ips;
use crate::impls::rack_init::validate_external_dns;
use anyhow::Result;
use camino::Utf8PathBuf;
use omicron_common::{
    address::IpRange,
    api::{
        external::AllowedSourceIps,
        internal::{
            nexus::Certificate, shared::rack_init::v1::RackNetworkConfig,
        },
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::Baseboard;
use std::{collections::BTreeSet, net::IpAddr, net::Ipv6Addr};

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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: BTreeSet<Ipv6Addr> },
}
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

#[derive(Clone)]
pub struct RackInitializeRequestParams {
    pub rack_initialize_request: RackInitializeRequest,
    pub skip_timesync: bool,
}

impl TryFrom<UnvalidatedRackInitializeRequest> for RackInitializeRequest {
    type Error = anyhow::Error;

    fn try_from(value: UnvalidatedRackInitializeRequest) -> Result<Self> {
        validate_external_dns(
            &value.external_dns_ips,
            &value.internal_services_ip_pool_ranges,
        )?;

        Ok(Self {
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
