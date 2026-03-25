// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 1 of the external IP types.

use super::SourceNatConfigError;
use crate::address::NUM_SOURCE_NAT_PORTS;
use daft::Diffable;
use itertools::Either;
use itertools::Itertools as _;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Note that `Deserialize` is manually implemented; if you make any changes to
// the fields of this structure, you must make them to that implementation too.
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct SourceNatConfig {
    /// The external address provided to the instance or service.
    pub ip: IpAddr,
    /// The first port used for source NAT, inclusive.
    first_port: u16,
    /// The last port used for source NAT, also inclusive.
    last_port: u16,
}

// We implement `Deserialize` manually to add validity checking on the port
// range.
impl<'de> Deserialize<'de> for SourceNatConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // The fields of `SourceNatConfigShadow` should exactly match the fields
        // of `SourceNatConfig`. We're not really using serde's remote derive,
        // but by adding the attribute we get compile-time checking that all the
        // field names and types match. (It doesn't check the _order_, but that
        // should be fine as long as we're using JSON or similar formats.)
        #[derive(Deserialize)]
        #[serde(remote = "SourceNatConfig")]
        struct SourceNatConfigShadow {
            ip: IpAddr,
            first_port: u16,
            last_port: u16,
        }

        let shadow = SourceNatConfigShadow::deserialize(deserializer)?;
        SourceNatConfig::new(shadow.ip, shadow.first_port, shadow.last_port)
            .map_err(D::Error::custom)
    }
}

impl SourceNatConfig {
    /// Construct a `SourceNatConfig` with the given port range, both inclusive.
    ///
    /// # Errors
    ///
    /// Fails if `(first_port, last_port)` is not aligned to
    /// [`NUM_SOURCE_NAT_PORTS`].
    pub fn new(
        ip: IpAddr,
        first_port: u16,
        last_port: u16,
    ) -> Result<Self, SourceNatConfigError> {
        if first_port.is_multiple_of(NUM_SOURCE_NAT_PORTS)
            && last_port
                .checked_sub(first_port)
                .and_then(|diff| diff.checked_add(1))
                == Some(NUM_SOURCE_NAT_PORTS)
        {
            Ok(Self { ip, first_port, last_port })
        } else {
            Err(SourceNatConfigError::UnalignedPortPair {
                first_port,
                last_port,
            })
        }
    }

    /// Get the port range.
    ///
    /// Guaranteed to be aligned to [`NUM_SOURCE_NAT_PORTS`].
    pub fn port_range(&self) -> std::ops::RangeInclusive<u16> {
        self.first_port..=self.last_port
    }

    /// Get the port range as a raw tuple; both values are inclusive.
    ///
    /// Guaranteed to be aligned to [`NUM_SOURCE_NAT_PORTS`].
    pub fn port_range_raw(&self) -> (u16, u16) {
        self.port_range().into_inner()
    }
}

/// A single- or dual-stack external IP configuration.
// This is version 1 of `ExternalIpConfig`, kept for compatibility with
// older versions of the sled-agent API which used this format. New code
// should use `ExternalIpConfig` from the parent module.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ExternalIpConfig {
    /// Single-stack IPv4 external IP configuration.
    V4(super::ExternalIpv4Config),
    /// Single-stack IPv6 external IP configuration.
    V6(super::ExternalIpv6Config),
    /// Both IPv4 and IPv6 external IP configuration.
    DualStack { v4: super::ExternalIpv4Config, v6: super::ExternalIpv6Config },
}

impl From<super::ExternalIpv4Config> for ExternalIpConfig {
    fn from(cfg: super::ExternalIpv4Config) -> Self {
        Self::V4(cfg)
    }
}

impl From<super::ExternalIpv6Config> for ExternalIpConfig {
    fn from(cfg: super::ExternalIpv6Config) -> Self {
        Self::V6(cfg)
    }
}

impl ExternalIpConfig {
    /// Return the IPv4 configuration, if it exists.
    pub fn ipv4_config(&self) -> Option<&super::ExternalIpv4Config> {
        match self {
            ExternalIpConfig::V4(v4)
            | ExternalIpConfig::DualStack { v4, .. } => Some(v4),
            ExternalIpConfig::V6(_) => None,
        }
    }

    /// Return a mutable reference to the IPv4 configuration, if it exists.
    pub fn ipv4_config_mut(
        &mut self,
    ) -> Option<&mut super::ExternalIpv4Config> {
        match self {
            ExternalIpConfig::V4(v4)
            | ExternalIpConfig::DualStack { v4, .. } => Some(v4),
            ExternalIpConfig::V6(_) => None,
        }
    }

    /// Return the IPv6 configuration, if it exists.
    pub fn ipv6_config(&self) -> Option<&super::ExternalIpv6Config> {
        match self {
            ExternalIpConfig::V6(v6)
            | ExternalIpConfig::DualStack { v6, .. } => Some(v6),
            ExternalIpConfig::V4(_) => None,
        }
    }

    /// Return a mutable reference to the IPv6 configuration, if it exists.
    pub fn ipv6_config_mut(
        &mut self,
    ) -> Option<&mut super::ExternalIpv6Config> {
        match self {
            ExternalIpConfig::V6(v6)
            | ExternalIpConfig::DualStack { v6, .. } => Some(v6),
            ExternalIpConfig::V4(_) => None,
        }
    }

    /// Attempt to convert from generic IP addressing information.
    ///
    /// This is used to convert older API versions which used version-agnostic
    /// IP address types (`IpAddr`) rather than concrete `Ipv4Addr`/`Ipv6Addr`.
    ///
    /// Returns an error if there are no addresses at all, or if there are
    /// mixed IP versions.
    pub fn try_from_generic(
        source_nat: Option<SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: Vec<IpAddr>,
    ) -> Result<Self, ExternalIpsError> {
        let snat_v4;
        let snat_v6;
        match source_nat {
            Some(snat) => {
                let (first_port, last_port) = snat.port_range_raw();
                match snat.ip {
                    IpAddr::V4(ipv4) => {
                        snat_v6 = None;
                        snat_v4 = Some(super::SourceNatConfig::new(
                            ipv4, first_port, last_port,
                        )?);
                    }
                    IpAddr::V6(ipv6) => {
                        snat_v6 = Some(super::SourceNatConfig::new(
                            ipv6, first_port, last_port,
                        )?);
                        snat_v4 = None;
                    }
                }
            }
            None => {
                snat_v4 = None;
                snat_v6 = None;
            }
        };

        let (fips_v4, fips_v6): (Vec<_>, Vec<_>) =
            floating_ips.into_iter().partition_map(|fip| match fip {
                IpAddr::V4(v4) => Either::Left(v4),
                IpAddr::V6(v6) => Either::Right(v6),
            });

        match (snat_v4, snat_v6, ephemeral_ip) {
            (None, None, None) => {
                match (fips_v4.is_empty(), fips_v6.is_empty()) {
                    (true, true) => Err(ExternalIpsError::NoIps),
                    (true, false) => Ok(super::ExternalIps {
                        source_nat: None,
                        ephemeral_ip: None,
                        floating_ips: fips_v6,
                    }
                    .into()),
                    (false, true) => Ok(super::ExternalIps {
                        source_nat: None,
                        ephemeral_ip: None,
                        floating_ips: fips_v4,
                    }
                    .into()),
                    (false, false) => Err(ExternalIpsError::MixedIpVersions),
                }
            }
            (None, None, Some(IpAddr::V4(v4))) => {
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(super::ExternalIps {
                    source_nat: None,
                    ephemeral_ip: Some(v4),
                    floating_ips: fips_v4,
                }
                .into())
            }
            (None, None, Some(IpAddr::V6(v6))) => {
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(super::ExternalIps {
                    source_nat: None,
                    ephemeral_ip: Some(v6),
                    floating_ips: fips_v6,
                }
                .into())
            }
            (None, Some(snat_v6), None) => {
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(super::ExternalIps {
                    source_nat: Some(snat_v6),
                    ephemeral_ip: None,
                    floating_ips: fips_v6,
                }
                .into())
            }
            (None, Some(snat_v6), Some(IpAddr::V6(eip_v6))) => {
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(super::ExternalIps {
                    source_nat: Some(snat_v6),
                    ephemeral_ip: Some(eip_v6),
                    floating_ips: fips_v6,
                }
                .into())
            }
            (Some(snat_v4), None, None) => {
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(super::ExternalIps {
                    source_nat: Some(snat_v4),
                    ephemeral_ip: None,
                    floating_ips: fips_v4,
                }
                .into())
            }
            (Some(snat_v4), None, Some(IpAddr::V4(eip_v4))) => {
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(super::ExternalIps {
                    source_nat: Some(snat_v4),
                    ephemeral_ip: Some(eip_v4),
                    floating_ips: fips_v4,
                }
                .into())
            }
            (Some(_), None, Some(IpAddr::V6(_)))
            | (None, Some(_), Some(IpAddr::V4(_))) => {
                Err(ExternalIpsError::MixedIpVersions)
            }
            (Some(_), Some(_), None) | (Some(_), Some(_), Some(_)) => {
                Err(ExternalIpsError::MixedIpVersions)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExternalIpsError {
    #[error(
        "Must specify at least one SNAT, ephemeral, or floating IP address"
    )]
    NoIps,
    #[error(
        "SNAT, ephemeral, and floating IPs must all be of the same IP version"
    )]
    MixedIpVersions,
    #[error(transparent)]
    SourceNat(#[from] super::SourceNatConfigError),
}
