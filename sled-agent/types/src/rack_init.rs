// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack initialization types.

use std::net::IpAddr;

use anyhow::Result;
use camino::Utf8Path;
use omicron_common::{
    address::IpRange,
    api::{external::AllowedSourceIps, internal::nexus::Certificate},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use sled_agent_types_versions::latest::rack_init::*;
use sled_hardware_types::Baseboard;

use crate::early_networking::back_compat::RackNetworkConfigV1;

/// Structures and routines used to maintain backwards compatibility.  The
/// contents of this module should only be used to convert older data into the
/// current format, and not for any ongoing run-time operations.
pub mod back_compat {
    use omicron_common::api::internal::nexus::Certificate;

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

    fn validate_external_dns(
        dns_ips: &Vec<IpAddr>,
        internal_ranges: &Vec<IpRange>,
    ) -> Result<()> {
        use anyhow::bail;
        if dns_ips.is_empty() {
            bail!("At least one external DNS IP is required");
        }

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

/// Load a RackInitializeRequest from a file path.
pub fn rack_initialize_request_from_file<P: AsRef<Utf8Path>>(
    path: P,
) -> Result<RackInitializeRequest, RackInitializeRequestParseError> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(&path).map_err(|err| {
        RackInitializeRequestParseError::Io { path: path.into(), err }
    })?;
    let mut raw_config = rack_initialize_request_from_toml_with_fallback(
        &contents,
    )
    .map_err(|err| RackInitializeRequestParseError::Deserialize {
        path: path.into(),
        err,
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

/// Parse a RackInitializeRequest from TOML, with fallback to older versions.
pub fn rack_initialize_request_from_toml_with_fallback(
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

/// Return a RackInitializeRequest configuration suitable for testing.
pub fn rack_initialize_request_test_config() -> RackInitializeRequest {
    // Use env! rather than std::env::var because this might be called from
    // a dependent crate.
    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
    let path =
        manifest_dir.join("../../smf/sled-agent/non-gimlet/config-rss.toml");
    let contents = std::fs::read_to_string(&path).unwrap();
    toml::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e))
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    use camino::Utf8PathBuf;
    use omicron_common::address::{AZ_PREFIX, RACK_PREFIX, SLED_PREFIX};
    use omicron_common::api::internal::shared::RackNetworkConfig;

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
            omicron_common::address::Ipv6Subnet::<AZ_PREFIX>::new(
                //              Masked out in AZ Subnet
                //              vv
                "fd00:1122:3344:0000::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.az_subnet()
        );
        assert_eq!(
            omicron_common::address::Ipv6Subnet::<RACK_PREFIX>::new(
                //              Shows up from Rack Subnet
                //              vv
                "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.rack_subnet()
        );
        assert_eq!(
            omicron_common::address::Ipv6Subnet::<SLED_PREFIX>::new(
                //                0th Sled Subnet
                //                vv
                "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(0)
        );
        assert_eq!(
            omicron_common::address::Ipv6Subnet::<SLED_PREFIX>::new(
                //                1st Sled Subnet
                //                vv
                "fd00:1122:3344:0101::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(1)
        );
        assert_eq!(
            omicron_common::address::Ipv6Subnet::<SLED_PREFIX>::new(
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
        let cfg = rack_initialize_request_from_file(&path)
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
        let read_cfg = rack_initialize_request_from_file(&cfg_path)
            .expect("failed to read generated config with certificate");
        assert_eq!(read_cfg.external_certificates.len(), 1);
        let cert = read_cfg.external_certificates.first().unwrap();
        let _ = rcgen::KeyPair::from_pem(&cert.key)
            .expect("generated PEM did not parse as KeyPair");
    }
}
