// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack initialization types.

use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv6Addr},
};

use anyhow::{Result, bail};
use camino::{Utf8Path, Utf8PathBuf};
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
pub use sled_agent_types_migrations::latest::rack_init::RecoverySiloConfig;
use sled_hardware_types::Baseboard;

/// Structures and routines used to maintain backwards compatibility.  The
/// contents of this module should only be used to convert older data into the
/// current format, and not for any ongoing run-time operations.
pub mod back_compat {
    use omicron_common::api::internal::nexus::Certificate;

    use crate::early_networking::back_compat::RackNetworkConfigV1;

    use super::*;

    #[derive(Clone, Deserialize)]
    struct UnvalidatedRackInitializeRequestV1 {
        trust_quorum_peers: Option<Vec<Baseboard>>,
        bootstrap_discovery: BootstrapAddressDiscovery,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        internal_services_ip_pool_ranges: Vec<IpRange>,
        external_dns_ips: Vec<IpAddr>,
        external_dns_zone_name: String,
        external_certificates: Vec<Certificate>,
        recovery_silo: RecoverySiloConfig,
        rack_network_config: RackNetworkConfigV1,
        #[serde(default = "default_allowed_source_ips")]
        allowed_source_ips: AllowedSourceIps,
    }

    /// This is a deprecated format, maintained to allow importing from older
    /// versions.
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(try_from = "UnvalidatedRackInitializeRequestV1")]
    pub struct RackInitializeRequestV1 {
        pub trust_quorum_peers: Option<Vec<Baseboard>>,
        pub bootstrap_discovery: BootstrapAddressDiscovery,
        pub ntp_servers: Vec<String>,
        pub dns_servers: Vec<IpAddr>,
        pub internal_services_ip_pool_ranges: Vec<IpRange>,
        pub external_dns_ips: Vec<IpAddr>,
        pub external_dns_zone_name: String,
        pub external_certificates: Vec<Certificate>,
        pub recovery_silo: RecoverySiloConfig,
        pub rack_network_config: RackNetworkConfigV1,
        #[serde(default = "default_allowed_source_ips")]
        pub allowed_source_ips: AllowedSourceIps,
    }

    impl TryFrom<UnvalidatedRackInitializeRequestV1> for RackInitializeRequestV1 {
        type Error = anyhow::Error;

        fn try_from(value: UnvalidatedRackInitializeRequestV1) -> Result<Self> {
            validate_external_dns(
                &value.external_dns_ips,
                &value.internal_services_ip_pool_ranges,
            )?;

            Ok(RackInitializeRequestV1 {
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

    impl From<RackInitializeRequestV1> for RackInitializeRequest {
        fn from(v1: RackInitializeRequestV1) -> Self {
            RackInitializeRequest {
                trust_quorum_peers: v1.trust_quorum_peers,
                bootstrap_discovery: v1.bootstrap_discovery,
                ntp_servers: v1.ntp_servers,
                dns_servers: v1.dns_servers,
                internal_services_ip_pool_ranges: v1
                    .internal_services_ip_pool_ranges,
                external_dns_ips: v1.external_dns_ips,
                external_dns_zone_name: v1.external_dns_zone_name,
                external_certificates: v1.external_certificates,
                recovery_silo: v1.recovery_silo,
                rack_network_config: v1.rack_network_config.into(),
                allowed_source_ips: v1.allowed_source_ips,
            }
        }
    }
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
    pub fn from_file<P: AsRef<Utf8Path>>(
        path: P,
    ) -> Result<Self, RackInitializeRequestParseError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path).map_err(|err| {
            RackInitializeRequestParseError::Io { path: path.into(), err }
        })?;
        let mut raw_config =
            Self::from_toml_with_fallback(&contents).map_err(|err| {
                RackInitializeRequestParseError::Deserialize {
                    path: path.into(),
                    err,
                }
            })?;

        // In the same way that sled-agent itself (our caller) discovers the
        // optional config-rss.toml in a well-known path relative to its config
        // file, we look for a pair of well-known paths adjacent to
        // config-rss.toml that specify an extra TLS certificate and private
        // key.  This is used by the end-to-end tests.  Any developer can also
        // use this to inject a TLS certificate into their setup.
        // (config-rss.toml is only used for dev/test, not production
        // deployments, which will always get their RSS configuration from
        // Wicket.)
        if let Some(parent) = path.parent() {
            let cert_path = parent.join("initial-tls-cert.pem");
            let key_path = parent.join("initial-tls-key.pem");
            let cert_bytes = std::fs::read_to_string(&cert_path);
            let key_bytes = std::fs::read_to_string(&key_path);
            match (cert_bytes, key_bytes) {
                (Ok(cert), Ok(key)) => {
                    raw_config
                        .external_certificates
                        .push(Certificate { key, cert });
                }
                (Err(cert_error), Err(key_error))
                    if cert_error.kind() == std::io::ErrorKind::NotFound
                        && key_error.kind() == std::io::ErrorKind::NotFound =>
                {
                    // Fine.  No extra cert was provided.
                }
                (Err(cert_error), _) => {
                    return Err(RackInitializeRequestParseError::Certificate(
                        anyhow::Error::new(cert_error).context(format!(
                            "loading certificate from {:?}",
                            cert_path
                        )),
                    ));
                }
                (_, Err(key_error)) => {
                    return Err(RackInitializeRequestParseError::Certificate(
                        anyhow::Error::new(key_error).context(format!(
                            "loading private key from {:?}",
                            key_path
                        )),
                    ));
                }
            };
        }

        Ok(raw_config)
    }

    pub fn from_toml_with_fallback(
        data: &str,
    ) -> Result<RackInitializeRequest> {
        // Note that if we fail to parse the request as any known
        // version, we return the error corresponding to the parse
        // failure for the newest schema.
        toml::from_str::<RackInitializeRequest>(&data).or_else(
            |latest_version_err| match toml::from_str::<
                back_compat::RackInitializeRequestV1,
            >(&data)
            {
                Ok(v1) => Ok(v1.into()),
                Err(_v1_err) => Err(latest_version_err.into()),
            },
        )
    }

    /// Return a configuration suitable for testing.
    pub fn test_config() -> Self {
        // Use env! rather than std::env::var because this might be called from
        // a dependent crate.
        let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
        let path = manifest_dir
            .join("../../smf/sled-agent/non-gimlet/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e))
    }

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
const fn default_allowed_source_ips() -> AllowedSourceIps {
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

#[derive(Debug, Clone)]
pub struct RackInitializeRequestParams {
    pub rack_initialize_request: RackInitializeRequest,
    pub skip_timesync: bool,
}

impl RackInitializeRequestParams {
    pub fn new(
        rack_initialize_request: RackInitializeRequest,
        skip_timesync: bool,
    ) -> RackInitializeRequestParams {
        RackInitializeRequestParams { rack_initialize_request, skip_timesync }
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    use super::*;
    use anyhow::Context;
    use oxnet::Ipv6Net;

    #[test]
    fn parse_rack_initialization() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cannot access manifest directory");
        let manifest = Utf8PathBuf::from(manifest);

        let path =
            manifest.join("../../smf/sled-agent/non-gimlet/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));

        let path = manifest
            .join("../../smf/sled-agent/gimlet-standalone/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));
    }

    #[test]
    fn parse_rack_initialization_weak_hash() {
        let config = r#"
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
    fn validate_external_dns_ips_must_be_in_internal_services_ip_pools() {
        // Conjure up a config; we'll tweak the internal services pools and
        // external DNS IPs, but no other fields matter.
        let mut config = UnvalidatedRackInitializeRequest {
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
                // Generated via `cargo run --example argon2 -- --input oxide`.
                user_password_hash:
                    "$argon2id$v=19$m=98304,t=23,p=1$Effh/p6M2ZKdnpJFeGqtGQ$\
                     ZtUwcVODAvUAVK6EQ5FJMv+GMlUCo9PQQsy9cagL+EU"
                        .parse()
                        .unwrap(),
            },
            rack_network_config: RackNetworkConfig {
                rack_subnet: Ipv6Net::host_net(Ipv6Addr::LOCALHOST),
                infra_ip_first: Ipv4Addr::LOCALHOST,
                infra_ip_last: Ipv4Addr::LOCALHOST,
                ports: Vec::new(),
                bgp: Vec::new(),
                bfd: Vec::new(),
            },
            allowed_source_ips: AllowedSourceIps::Any,
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
                    IpRange::try_from((
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
                    IpRange::try_from((
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

    #[test]
    fn test_subnets() {
        let cfg = RackInitializeRequest {
            trust_quorum_peers: None,
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyOurs,
            ntp_servers: vec![String::from("test.pool.example.com")],
            dns_servers: vec!["1.1.1.1".parse().unwrap()],
            external_dns_zone_name: String::from("oxide.test"),
            internal_services_ip_pool_ranges: vec![IpRange::from(IpAddr::V4(
                Ipv4Addr::new(129, 168, 1, 20),
            ))],
            external_dns_ips: vec![],
            external_certificates: vec![],
            recovery_silo: RecoverySiloConfig {
                silo_name: "test-silo".parse().unwrap(),
                user_name: "dummy".parse().unwrap(),
                // This is a hash for the password "oxide".  It doesn't matter,
                // though; it's not used.
                user_password_hash:
                    "$argon2id$v=19$m=98304,t=23,p=1$Effh/p6M2ZKdnpJFeGqtGQ$\
                     ZtUwcVODAvUAVK6EQ5FJMv+GMlUCo9PQQsy9cagL+EU"
                        .parse()
                        .unwrap(),
            },
            rack_network_config: RackNetworkConfig {
                rack_subnet: Ipv6Net::new(
                    "fd00:1122:3344:0100::".parse().unwrap(),
                    RACK_PREFIX,
                )
                .unwrap(),
                infra_ip_first: Ipv4Addr::LOCALHOST,
                infra_ip_last: Ipv4Addr::LOCALHOST,
                ports: Vec::new(),
                bgp: Vec::new(),
                bfd: Vec::new(),
            },
            allowed_source_ips: AllowedSourceIps::Any,
        };

        assert_eq!(
            Ipv6Subnet::<AZ_PREFIX>::new(
                //              Masked out in AZ Subnet
                //              vv
                "fd00:1122:3344:0000::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.az_subnet()
        );
        assert_eq!(
            Ipv6Subnet::<RACK_PREFIX>::new(
                //              Shows up from Rack Subnet
                //              vv
                "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.rack_subnet()
        );
        assert_eq!(
            Ipv6Subnet::<SLED_PREFIX>::new(
                //                0th Sled Subnet
                //                vv
                "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(0)
        );
        assert_eq!(
            Ipv6Subnet::<SLED_PREFIX>::new(
                //                1st Sled Subnet
                //                vv
                "fd00:1122:3344:0101::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(1)
        );
        assert_eq!(
            Ipv6Subnet::<SLED_PREFIX>::new(
                //                Last Sled Subnet
                //                vv
                "fd00:1122:3344:01ff::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(255)
        );
    }

    #[test]
    fn test_extra_certs() {
        // The stock non-Gimlet config has no TLS certificates.
        let path = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../smf/sled-agent/non-gimlet/config-rss.toml");
        let cfg = RackInitializeRequest::from_file(&path)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));
        assert!(cfg.external_certificates.is_empty());

        // Now let's create a configuration that does have an adjacent
        // certificate and key.
        let tempdir =
            camino_tempfile::tempdir().expect("creating temporary directory");
        println!("using temp path: {:?}", tempdir);

        // Generate the certificate.
        let domain = format!(
            "{}.sys.{}",
            cfg.external_dns_zone_name,
            cfg.recovery_silo.silo_name.as_str(),
        );
        let cert = rcgen::generate_simple_self_signed(vec![domain.clone()])
            .unwrap_or_else(|error| {
                panic!(
                    "generating certificate for domain {:?}: {}",
                    domain, error
                )
            });

        // Write the configuration file.
        let cfg_path = tempdir.path().join("config-rss.toml");
        let _ = std::fs::copy(&path, &cfg_path)
            .with_context(|| {
                format!("failed to copy file {:?} to {:?}", &path, &cfg_path)
            })
            .unwrap();

        // Write the certificate.
        let cert_bytes = cert
            .serialize_pem()
            .expect("serializing generated certificate")
            .into_bytes();
        let cert_path = tempdir.path().join("initial-tls-cert.pem");
        std::fs::write(&cert_path, &cert_bytes)
            .with_context(|| format!("failed to write to {:?}", &cert_path))
            .unwrap();

        // Write the private key.
        let key_path = tempdir.path().join("initial-tls-key.pem");
        let key_bytes = cert.serialize_private_key_pem().into_bytes();
        std::fs::write(&key_path, &key_bytes)
            .with_context(|| format!("failed to write to {:?}", &key_path))
            .unwrap();

        // Now try to load it all.
        let read_cfg = RackInitializeRequest::from_file(&cfg_path)
            .expect("failed to read generated config with certificate");
        assert_eq!(read_cfg.external_certificates.len(), 1);
        let cert = read_cfg.external_certificates.first().unwrap();
        let _ = rcgen::KeyPair::from_pem(&cert.key)
            .expect("generated PEM did not parse as KeyPair");
    }
}
