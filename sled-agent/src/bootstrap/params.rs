// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use anyhow::{bail, Result};
use omicron_common::address::{self, Ipv6Subnet, SLED_PREFIX};
use omicron_common::api::internal::shared::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: HashSet<Ipv6Addr> },
}

/// Configuration for the "rack setup service".
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackInitializeRequest {
    pub rack_subnet: Ipv6Addr,

    /// Describes how bootstrap addresses should be collected during RSS.
    pub bootstrap_discovery: BootstrapAddressDiscovery,

    /// The minimum number of sleds required to unlock the rack secret.
    ///
    /// If this value is less than 2, no rack secret will be created on startup;
    /// this is the typical case for single-server test/development.
    pub rack_secret_threshold: usize,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// The external DNS server addresses.
    pub dns_servers: Vec<String>,

    /// Ranges of the service IP pool which may be used for internal services.
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Eventually,
    // we want to configure multiple pools.
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,

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
    pub rack_network_config: Option<RackNetworkConfig>,
}

impl RackInitializeRequest {
    /// Perform _very basic_ validation that the parameters are self-consistent.
    /// This function returning `Ok(_)` does NOT mean that all parameters are
    /// definitely valid, but if it returns `Err(_)` it means they are
    /// definitely invalid.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.external_dns_ips.is_empty() {
            bail!("At least one external DNS IP is required");
        }

        // Every external DNS IP should also be present in one of the internal
        // services IP pool ranges. This check is O(N*M), but we expect both N
        // and M to be small (~5 DNS servers, and a small number of pools).
        for &dns_ip in &self.external_dns_ips {
            if !self
                .internal_services_ip_pool_ranges
                .iter()
                .any(|range| range.contains(dns_ip))
            {
                bail!(
                    "External DNS IP {dns_ip} is not contained in \
                     `internal_services_ip_pool_ranges`"
                );
            }
        }

        Ok(())
    }
}

pub type Certificate = nexus_client::types::Certificate;
pub type RecoverySiloConfig = nexus_client::types::RecoverySiloConfig;

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StartSledAgentRequest {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// The external NTP servers to use
    pub ntp_servers: Vec<String>,
    //
    /// The external DNS servers to use
    pub dns_servers: Vec<String>,

    // Note: The order of these fields is load bearing, because we serialize
    // `SledAgentRequest`s as toml. `subnet` serializes as a TOML table, so it
    // must come after non-table fields.
    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl StartSledAgentRequest {
    pub fn sled_address(&self) -> SocketAddrV6 {
        address::get_sled_address(self.subnet)
    }

    pub fn switch_zone_ip(&self) -> Ipv6Addr {
        address::get_switch_zone_address(self.subnet)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Request<'a> {
    /// Send configuration information for launching a Sled Agent.
    StartSledAgentRequest(Cow<'a, StartSledAgentRequest>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestEnvelope<'a> {
    pub version: u32,
    pub request: Request<'a>,
}

pub(super) mod version {
    pub(crate) const V1: u32 = 1;
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;
    use camino::Utf8PathBuf;

    #[test]
    fn parse_rack_initialization() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cannot access manifest directory");
        let manifest = Utf8PathBuf::from(manifest);

        let path =
            manifest.join("../smf/sled-agent/non-gimlet/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));

        let path = manifest
            .join("../smf/sled-agent/gimlet-standalone/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));
    }

    #[test]
    fn parse_rack_initialization_weak_hash() {
        let config = r#"
            rack_subnet = "fd00:1122:3344:0100::"
            bootstrap_discovery.type = "only_ours"
            rack_secret_threshold = 1
            ntp_servers = [ "ntp.eng.oxide.computer" ]
            dns_servers = [ "1.1.1.1", "9.9.9.9" ]
            external_dns_zone_name = "oxide.test"

            [[internal_services_ip_pool_ranges]]
            first = "192.168.1.20"
            last = "192.168.1.22"

            [recovery_silo]
            silo_name = "recovery"
            user_name = "recovery"
            user_password_hash = "$argon2i$v=19$m=16,t=2,p=1$NVR0a2QxVXNiQjlObFJXbA$iGFJWOlUqN20B8KR4Fsmrg"
        "#;

        let error = toml::from_str::<RackInitializeRequest>(config)
            .expect_err("unexpectedly parsed with bad password hash");
        println!("found error: {}", error);
        assert!(error.to_string().contains(
            "password hash: algorithm: expected argon2id, found argon2i"
        ));
    }

    #[test]
    fn json_serialization_round_trips() {
        let envelope = RequestEnvelope {
            version: 1,
            request: Request::StartSledAgentRequest(Cow::Owned(
                StartSledAgentRequest {
                    id: Uuid::new_v4(),
                    rack_id: Uuid::new_v4(),
                    ntp_servers: vec![String::from("test.pool.example.com")],
                    dns_servers: vec![String::from("1.1.1.1")],
                    subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                },
            )),
        };

        let serialized = serde_json::to_vec(&envelope).unwrap();
        let deserialized: RequestEnvelope =
            serde_json::from_slice(serialized.as_slice()).unwrap();

        assert!(envelope == deserialized, "serialization round trip failed");
    }

    #[test]
    fn validate_external_dns_ips_must_be_in_internal_services_ip_pools() {
        // Conjure up a config; we'll tweak the internal services pools and
        // external DNS IPs, but no other fields matter.
        let mut config = RackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyOurs,
            rack_secret_threshold: 0,
            ntp_servers: Vec::new(),
            dns_servers: Vec::new(),
            internal_services_ip_pool_ranges: Vec::new(),
            external_dns_ips: Vec::new(),
            external_dns_zone_name: "".to_string(),
            external_certificates: Vec::new(),
            recovery_silo: RecoverySiloConfig {
                silo_name: "recovery".parse().unwrap(),
                user_name: "recovery".parse().unwrap(),
                user_password_hash: "$argon2id$v=19$m=98304,t=13,p=1$RUlWc0ZxaHo0WFdrN0N6ZQ$S8p52j85GPvMhR/ek3GL0el/oProgTwWpHJZ8lsQQoY".parse().unwrap(),
            },
            rack_network_config: None,
        };

        // Valid configs: all external DNS IPs are contained in the IP pool
        // ranges.
        for (ip_pool_ranges, dns_ips) in [
            (
                &[("fd00::1", "fd00::10")] as &[(&str, &str)],
                &["fd00::1", "fd00::5", "fd00::10"] as &[&str],
            ),
            (
                &[("192.168.1.10", "192.168.1.20")],
                &["192.168.1.10", "192.168.1.15", "192.168.1.20"],
            ),
            (
                &[("fd00::1", "fd00::10"), ("192.168.1.10", "192.168.1.20")],
                &[
                    "fd00::1",
                    "fd00::5",
                    "fd00::10",
                    "192.168.1.10",
                    "192.168.1.15",
                    "192.168.1.20",
                ],
            ),
        ] {
            config.internal_services_ip_pool_ranges = ip_pool_ranges
                .iter()
                .map(|(a, b)| {
                    address::IpRange::try_from((
                        a.parse::<IpAddr>().unwrap(),
                        b.parse::<IpAddr>().unwrap(),
                    ))
                    .unwrap()
                })
                .collect();
            config.external_dns_ips =
                dns_ips.iter().map(|ip| ip.parse().unwrap()).collect();

            match config.validate() {
                Ok(()) => (),
                Err(err) => panic!(
                    "failure on {ip_pool_ranges:?} with DNS IPs {dns_ips:?}: \
                     {err}"
                ),
            }
        }

        // Invalid configs: either no DNS IPs, or one or more DNS IPs are not
        // contained in the ip pool ranges.
        for (ip_pool_ranges, dns_ips) in [
            (&[("fd00::1", "fd00::10")] as &[(&str, &str)], &[] as &[&str]),
            (&[("fd00::1", "fd00::10")], &["fd00::1", "fd00::5", "fd00::11"]),
            (
                &[("192.168.1.10", "192.168.1.20")],
                &["192.168.1.9", "192.168.1.15", "192.168.1.20"],
            ),
            (
                &[("fd00::1", "fd00::10"), ("192.168.1.10", "192.168.1.20")],
                &[
                    "fd00::1",
                    "fd00::5",
                    "fd00::10",
                    "192.168.1.10",
                    "192.168.1.15",
                    "192.168.1.20",
                    "192.168.1.21",
                ],
            ),
        ] {
            config.internal_services_ip_pool_ranges = ip_pool_ranges
                .iter()
                .map(|(a, b)| {
                    address::IpRange::try_from((
                        a.parse::<IpAddr>().unwrap(),
                        b.parse::<IpAddr>().unwrap(),
                    ))
                    .unwrap()
                })
                .collect();
            config.external_dns_ips =
                dns_ips.iter().map(|ip| ip.parse().unwrap()).collect();

            match config.validate() {
                Ok(()) => panic!(
                    "unexpected success on {ip_pool_ranges:?} with \
                     DNS IPs {dns_ips:?}"
                ),
                Err(_) => (),
            }
        }
    }
}
