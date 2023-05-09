// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with RSS config.

use crate::config::ConfigError;
use anyhow::Context;
use camino::Utf8Path;
use omicron_common::address::{
    get_64_subnet, Ipv6Subnet, AZ_PREFIX, RACK_PREFIX, SLED_PREFIX,
};
use serde::Deserialize;
use serde::Serialize;

use crate::bootstrap::params::Certificate;
pub use crate::bootstrap::params::RackInitializeRequest as SetupServiceConfig;

// XXX-dap TODO-doc
#[derive(Deserialize, Serialize)]
struct FileBasedConfig {
    #[serde(flatten)]
    literal: SetupServiceConfig,

    #[serde(default)]
    extra_cert: ExtraCert,
}

#[derive(Default, Deserialize, Serialize)]
enum ExtraCert {
    #[default]
    None,
    GenerateSelfSigned,
}

impl TryFrom<FileBasedConfig> for SetupServiceConfig {
    type Error = ConfigError;

    fn try_from(raw_config: FileBasedConfig) -> Result<Self, Self::Error> {
        let extra_cert = match raw_config.extra_cert {
            ExtraCert::None => None,
            ExtraCert::GenerateSelfSigned => {
                let domain = format!(
                    "*.sys.{}",
                    raw_config.literal.external_dns_zone_name
                );
                let cert =
                    rcgen::generate_simple_self_signed(vec![domain.clone()])
                        .with_context(|| {
                            format!(
                                "generating certificate for domain {:?}",
                                domain
                            )
                        })
                        .map_err(ConfigError::GenerateCertificate)?;
                let key_bytes = cert.serialize_private_key_pem().into_bytes();
                let cert_bytes = cert
                    .serialize_pem()
                    .context("serializing generated certificate")
                    .map_err(ConfigError::GenerateCertificate)?
                    .into_bytes();
                Some(Certificate { key: key_bytes, cert: cert_bytes })
            }
        };

        let mut rv = raw_config.literal;
        if let Some(cert) = extra_cert {
            rv.external_certificates.push(cert)
        }

        Ok(rv)
    }
}

impl SetupServiceConfig {
    pub fn from_file<P: AsRef<Utf8Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        let raw_config: FileBasedConfig = toml::from_str(&contents)
            .map_err(|err| ConfigError::Parse { path: path.into(), err })?;
        SetupServiceConfig::try_from(raw_config)
    }

    pub fn az_subnet(&self) -> Ipv6Subnet<AZ_PREFIX> {
        Ipv6Subnet::<AZ_PREFIX>::new(self.rack_subnet)
    }

    /// Returns the subnet for our rack.
    pub fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX> {
        Ipv6Subnet::<RACK_PREFIX>::new(self.rack_subnet)
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
    use omicron_common::address::IpRange;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn test_config() -> SetupServiceConfig {
        SetupServiceConfig {
            rack_subnet: "fd00:1122:3344:0100::".parse().unwrap(),
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyOurs,
            rack_secret_threshold: 0,
            ntp_servers: vec![String::from("test.pool.example.com")],
            dns_servers: vec![String::from("1.1.1.1")],
            external_dns_zone_name: String::from("oxide.test"),
            internal_services_ip_pool_ranges: vec![IpRange::from(IpAddr::V4(
                Ipv4Addr::new(129, 168, 1, 20),
            ))],
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
        }
    }

    #[test]
    fn test_subnets() {
        let cfg = test_config();

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
    fn text_extra_certs() {
        let cfg = test_config();
        assert!(cfg.external_certificates.is_empty());

        // First, test a configuration that requests nothing in particular.
        let file_cfg = FileBasedConfig {
            literal: cfg.clone(),
            extra_cert: ExtraCert::None,
        };
        let read_cfg = SetupServiceConfig::try_from(file_cfg).unwrap();
        assert!(read_cfg.external_certificates.is_empty());

        // Now test a configuration that requests a generated certificate.
        let file_cfg = FileBasedConfig {
            literal: cfg.clone(),
            extra_cert: ExtraCert::GenerateSelfSigned,
        };
        let read_cfg = SetupServiceConfig::try_from(file_cfg).unwrap();
        assert_eq!(read_cfg.external_certificates.len(), 1);
        let cert = read_cfg.external_certificates.iter().next().unwrap();
        let key_pem = std::str::from_utf8(&cert.key)
            .expect("generated PEM was not UTF-8");
        let _ = rcgen::KeyPair::from_pem(&key_pem)
            .expect("generated PEM did not parse as KeyPair");
    }
}
