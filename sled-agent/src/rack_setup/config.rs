// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with RSS config.

use crate::config::ConfigError;
use camino::Utf8Path;
use omicron_common::address::{
    get_64_subnet, Ipv6Subnet, AZ_PREFIX, RACK_PREFIX, SLED_PREFIX,
};

use crate::bootstrap::params::Certificate;
pub use crate::bootstrap::params::RackInitializeRequest as SetupServiceConfig;
pub use crate::bootstrap::params::RackInitializeRequestV1 as SetupServiceConfigV1;

impl SetupServiceConfig {
    pub fn from_file<P: AsRef<Utf8Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        let mut raw_config =
            SetupServiceConfig::from_toml_with_fallback(&contents)
                .map_err(|err| ConfigError::Parse { path: path.into(), err })?;

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
                    return Err(ConfigError::Certificate(
                        anyhow::Error::new(cert_error).context(format!(
                            "loading certificate from {:?}",
                            cert_path
                        )),
                    ));
                }
                (_, Err(key_error)) => {
                    return Err(ConfigError::Certificate(
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::bootstrap::params::BootstrapAddressDiscovery;
    use crate::bootstrap::params::RecoverySiloConfig;
    use anyhow::Context;
    use camino::Utf8PathBuf;
    use omicron_common::address::IpRange;
    use omicron_common::api::internal::shared::AllowedSourceIps;
    use omicron_common::api::internal::shared::RackNetworkConfig;
    use oxnet::Ipv6Net;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_subnets() {
        let cfg = SetupServiceConfig {
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
                user_password_hash: "$argon2id$v=19$m=98304,t=13,p=1$\
                    RUlWc0ZxaHo0WFdrN0N6ZQ$S8p52j85GPvMhR/\
                    ek3GL0el/oProgTwWpHJZ8lsQQoY"
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
            .join("../smf/sled-agent/non-gimlet/config-rss.toml");
        let cfg = SetupServiceConfig::from_file(&path)
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
        let read_cfg = SetupServiceConfig::from_file(&cfg_path)
            .expect("failed to read generated config with certificate");
        assert_eq!(read_cfg.external_certificates.len(), 1);
        let cert = read_cfg.external_certificates.first().unwrap();
        let _ = rcgen::KeyPair::from_pem(&cert.key)
            .expect("generated PEM did not parse as KeyPair");
    }
}
