// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use anyhow::{bail, Result};
use omicron_common::address::{self, Ipv6Subnet, SLED_PREFIX};
use omicron_common::api::internal::shared::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sled_hardware::Baseboard;
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

// "Shadow" copy of `RackInitializeRequest` that does no validation on its
// fields.
#[derive(Clone, Deserialize)]
struct UnvalidatedRackInitializeRequest {
    rack_subnet: Ipv6Addr,
    trust_quorum_peers: Option<Vec<Baseboard>>,
    bootstrap_discovery: BootstrapAddressDiscovery,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    internal_services_ip_pool_ranges: Vec<address::IpRange>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo: RecoverySiloConfig,
    rack_network_config: Option<RackNetworkConfig>,
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
    pub rack_subnet: Ipv6Addr,

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

// This custom debug implementation hides the private keys.
impl std::fmt::Debug for RackInitializeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // If you find a compiler error here, and you just added a field to this
        // struct, be sure to add it to the Debug impl below!
        let RackInitializeRequest {
            rack_subnet,
            trust_quorum_peers: trust_qurorum_peers,
            bootstrap_discovery,
            ntp_servers,
            dns_servers,
            internal_services_ip_pool_ranges,
            external_dns_ips,
            external_dns_zone_name,
            external_certificates: _,
            recovery_silo,
            rack_network_config,
        } = &self;

        f.debug_struct("RackInitializeRequest")
            .field("rack_subnet", rack_subnet)
            .field("trust_quorum_peers", trust_qurorum_peers)
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
            .finish()
    }
}

impl TryFrom<UnvalidatedRackInitializeRequest> for RackInitializeRequest {
    type Error = anyhow::Error;

    fn try_from(value: UnvalidatedRackInitializeRequest) -> Result<Self> {
        if value.external_dns_ips.is_empty() {
            bail!("At least one external DNS IP is required");
        }

        // Every external DNS IP should also be present in one of the internal
        // services IP pool ranges. This check is O(N*M), but we expect both N
        // and M to be small (~5 DNS servers, and a small number of pools).
        for &dns_ip in &value.external_dns_ips {
            if !value
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

        Ok(RackInitializeRequest {
            rack_subnet: value.rack_subnet,
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
        })
    }
}

pub type Certificate = nexus_client::types::Certificate;
pub type RecoverySiloConfig = nexus_client::types::RecoverySiloConfig;

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequest {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// The external NTP servers to use
    pub ntp_servers: Vec<String>,

    /// The external DNS servers to use
    pub dns_servers: Vec<IpAddr>,

    /// Use trust quorum for key generation
    pub use_trust_quorum: bool,

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

    /// Compute the sha3_256 digest of `self.rack_id` to use as a `salt`
    /// for disk encryption. We don't want to include other values that are
    /// consistent across sleds as it would prevent us from moving drives
    /// between sleds.
    pub fn hash_rack_id(&self) -> [u8; 32] {
        // We know the unwrap succeeds as a Sha3_256 digest is 32 bytes
        Sha3_256::digest(self.rack_id.as_bytes()).as_slice().try_into().unwrap()
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
pub fn test_config() -> RackInitializeRequest {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("Cannot access manifest directory");
    let manifest = camino::Utf8PathBuf::from(manifest);
    let path = manifest.join("../smf/sled-agent/non-gimlet/config-rss.toml");
    let contents = std::fs::read_to_string(&path).unwrap();
    toml::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e))
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
                    dns_servers: vec!["1.1.1.1".parse().unwrap()],
                    use_trust_quorum: false,
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
        let mut config = UnvalidatedRackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            trust_quorum_peers: None,
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyOurs,
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

            match RackInitializeRequest::try_from(config.clone()) {
                Ok(_) => (),
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

            match RackInitializeRequest::try_from(config.clone()) {
                Ok(_) => panic!(
                    "unexpected success on {ip_pool_ranges:?} with \
                     DNS IPs {dns_ips:?}"
                ),
                Err(_) => (),
            }
        }
    }
}
