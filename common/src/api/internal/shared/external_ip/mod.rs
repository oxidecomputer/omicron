// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP types.

pub mod v1;

use crate::address::NUM_SOURCE_NAT_PORTS;
use daft::Diffable;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

pub trait Ip:
    Clone
    + Copy
    + std::fmt::Debug
    + Diffable
    + Eq
    + JsonSchema
    + std::hash::Hash
    + PartialOrd
    + PartialEq
    + Ord
    + Serialize
{
}
impl Ip for Ipv4Addr {}
impl Ip for Ipv6Addr {}
impl Ip for IpAddr {}

pub trait ConcreteIp: Ip {}
impl ConcreteIp for Ipv4Addr {}
impl ConcreteIp for Ipv6Addr {}

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
pub struct SourceNatConfig<T: Ip> {
    /// The external address provided to the instance or service.
    pub ip: T,
    /// The first port used for source NAT, inclusive.
    first_port: u16,
    /// The last port used for source NAT, also inclusive.
    last_port: u16,
}

pub type SourceNatConfigV4 = SourceNatConfig<Ipv4Addr>;
pub type SourceNatConfigV6 = SourceNatConfig<Ipv6Addr>;
pub type SourceNatConfigGeneric = SourceNatConfig<IpAddr>;

// We implement `Deserialize` manually to add validity checking on the port
// range.
impl<'de, T: Ip + Deserialize<'de>> Deserialize<'de> for SourceNatConfig<T> {
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
        struct SourceNatConfigShadow<T: Ip> {
            ip: T,
            first_port: u16,
            last_port: u16,
        }

        let shadow = SourceNatConfigShadow::deserialize(deserializer)?;
        SourceNatConfig::new(shadow.ip, shadow.first_port, shadow.last_port)
            .map_err(D::Error::custom)
    }
}

impl<T: Ip> SourceNatConfig<T> {
    /// Construct a `SourceNatConfig` with the given port range, both inclusive.
    ///
    /// # Errors
    ///
    /// Fails if `(first_port, last_port)` is not aligned to
    /// [`NUM_SOURCE_NAT_PORTS`].
    pub fn new(
        ip: T,
        first_port: u16,
        last_port: u16,
    ) -> Result<Self, SourceNatConfigError> {
        if first_port % NUM_SOURCE_NAT_PORTS == 0
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

#[derive(Debug, thiserror::Error)]
pub enum SourceNatConfigError {
    #[error(
        "snat port range is not aligned to {NUM_SOURCE_NAT_PORTS}: \
         ({first_port}, {last_port})"
    )]
    UnalignedPortPair { first_port: u16, last_port: u16 },
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ExternalIps<T: ConcreteIp> {
    source_nat: Option<SourceNatConfig<T>>,
    ephemeral_ip: Option<T>,
    floating_ips: Vec<T>,
}

impl<T: ConcreteIp> ExternalIps<T> {
    pub fn source_nat(&self) -> &Option<SourceNatConfig<T>> {
        &self.source_nat
    }

    pub fn ephemeral_ip(&self) -> &Option<T> {
        &self.ephemeral_ip
    }

    pub fn floating_ips(&self) -> &[T] {
        &self.floating_ips
    }
}

pub type ExternalIpv4Config = ExternalIps<Ipv4Addr>;
pub type ExternalIpv6Config = ExternalIps<Ipv6Addr>;

// We implement `Deserialize` manually to ensure we have at least one of the
// SNAT, ephemeral, and floating IPs in the input data.
impl<'de, T: ConcreteIp + Deserialize<'de>> Deserialize<'de>
    for ExternalIps<T>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        #[derive(Deserialize)]
        #[serde(remote = "ExternalIps")]
        struct ExternalIpsShadow<T: ConcreteIp> {
            source_nat: Option<SourceNatConfig<T>>,
            ephemeral_ip: Option<T>,
            floating_ips: Vec<T>,
        }

        let ExternalIps { source_nat, ephemeral_ip, floating_ips } =
            ExternalIpsShadow::deserialize(deserializer)?;
        let mut builder = ExternalIpsBuilder::new();
        if let Some(snat) = source_nat {
            builder.with_source_nat(snat);
        }
        if let Some(ip) = ephemeral_ip {
            builder.with_ephemeral_ip(ip);
        }
        builder.with_floating_ips(floating_ips);
        builder.build().map_err(D::Error::custom)
    }
}

pub struct ExternalIpsBuilder<T: ConcreteIp> {
    source_nat: Option<SourceNatConfig<T>>,
    ephemeral_ip: Option<T>,
    floating_ips: Vec<T>,
}

impl<T: ConcreteIp> ExternalIpsBuilder<T> {
    pub fn new() -> Self {
        Self { source_nat: None, ephemeral_ip: None, floating_ips: Vec::new() }
    }

    pub fn with_source_nat(
        &mut self,
        source_nat: SourceNatConfig<T>,
    ) -> &mut Self {
        self.source_nat = Some(source_nat);
        self
    }

    pub fn with_ephemeral_ip(&mut self, ip: T) -> &mut Self {
        self.ephemeral_ip = Some(ip);
        self
    }

    pub fn with_floating_ips(&mut self, ips: Vec<T>) -> &mut Self {
        self.floating_ips = ips;
        self
    }

    pub fn build(self) -> Result<ExternalIps<T>, ExternalIpsError> {
        let ExternalIpsBuilder { source_nat, ephemeral_ip, floating_ips } =
            self;
        if source_nat.is_none()
            && ephemeral_ip.is_none()
            && floating_ips.is_empty()
        {
            return Err(ExternalIpsError::NoIps);
        }
        Ok(ExternalIps { source_nat, ephemeral_ip, floating_ips })
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
    SourceNat(#[from] SourceNatConfigError),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ExternalIpConfig {
    V4(ExternalIpv4Config),
    V6(ExternalIpv6Config),
    DualStack { v4: ExternalIpv4Config, v6: ExternalIpv6Config },
}

impl From<ExternalIpv4Config> for ExternalIpConfig {
    fn from(cfg: ExternalIpv4Config) -> Self {
        Self::V4(cfg)
    }
}

impl From<ExternalIpv6Config> for ExternalIpConfig {
    fn from(cfg: ExternalIpv6Config) -> Self {
        Self::V6(cfg)
    }
}

impl ExternalIpConfig {
    pub fn try_from_generic(
        source_nat: Option<v1::SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: Vec<IpAddr>,
    ) -> Result<Self, ExternalIpsError> {
        // This is a terrible conversion method. We need to handle all the
        // possible IP versions for three different sources: SNAT, Ephemeral,
        // and Floating IPs. We also need to handle the possibility that the
        // floating IPs are not all the same version.
        //
        // This proceeds in 3 steps:
        //
        // - Convert the provided SNAT configuration into either an SNATv4 or
        //   SNATv6 configuration. At most one of these should be non-none.
        // - Extract all the floating IPs into arrays of one or the other
        //   version. At most one of these should be non-empty.
        // - Match on all of the possible states, which is huge and annoying.

        // Populate one or the other SNAT configuration.
        let snat_v4;
        let snat_v6;
        match source_nat {
            Some(snat) => {
                let (first_port, last_port) = snat.port_range_raw();
                match snat.ip {
                    IpAddr::V4(ipv4) => {
                        snat_v6 = None;
                        snat_v4 = Some(SourceNatConfig::new(
                            ipv4, first_port, last_port,
                        )?);
                    }
                    IpAddr::V6(ipv6) => {
                        snat_v6 = Some(SourceNatConfig::new(
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

        // Populate one or the other arrays of floating IPs.
        let mut fips_v4 = vec![];
        let mut fips_v6 = vec![];
        for fip in floating_ips.into_iter() {
            match fip {
                IpAddr::V4(v4) => fips_v4.push(v4),
                IpAddr::V6(v6) => fips_v6.push(v6),
            }
        }

        match (snat_v4, snat_v6, ephemeral_ip) {
            (None, None, None) => {
                // Only floating IPs.
                match (fips_v4.is_empty(), fips_v6.is_empty()) {
                    (true, true) => return Err(ExternalIpsError::NoIps),
                    (true, false) => {
                        let mut builder = ExternalIpsBuilder::new();
                        builder.with_floating_ips(fips_v4);
                        builder.build().map(Into::into)
                    }
                    (false, true) => {
                        let mut builder = ExternalIpsBuilder::new();
                        builder.with_floating_ips(fips_v6);
                        builder.build().map(Into::into)
                    }
                    (false, false) => {
                        return Err(ExternalIpsError::MixedIpVersions);
                    }
                }
            }
            (None, None, Some(IpAddr::V4(v4))) => {
                // Ephemeral IPv4, ensure we have no v6 floating IPs.
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                let mut builder = ExternalIpsBuilder::new();
                builder.with_ephemeral_ip(v4).with_floating_ips(fips_v4);
                builder.build().map(Into::into)
            }
            (None, None, Some(IpAddr::V6(v6))) => {
                // Ephemeral IPv6, ensure we have no v4 floating IPs.
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                let mut builder = ExternalIpsBuilder::new();
                builder.with_ephemeral_ip(v6).with_floating_ips(fips_v6);
                builder.build().map(Into::into)
            }
            (None, Some(snat_v6), None) => {
                // IPv6 source nat, ensure we have no v4 floating IPs.
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                let mut builder = ExternalIpsBuilder::new();
                builder.with_source_nat(snat_v6).with_floating_ips(fips_v6);
                builder.build().map(Into::into)
            }
            (None, Some(snat_v6), Some(IpAddr::V6(eip_v6))) => {
                // IPv6 source nat and ephemeral, ensure we have no v4 floating
                // IPs.
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                let mut builder = ExternalIpsBuilder::new();
                builder
                    .with_source_nat(snat_v6)
                    .with_ephemeral_ip(eip_v6)
                    .with_floating_ips(fips_v6);
                builder.build().map(Into::into)
            }
            (Some(snat_v4), None, None) => {
                // IPv4 source nat, ensure we have no v6 floating IPs.
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                let mut builder = ExternalIpsBuilder::new();
                builder.with_source_nat(snat_v4).with_floating_ips(fips_v4);
                builder.build().map(Into::into)
            }
            (Some(snat_v4), None, Some(IpAddr::V4(eip_v4))) => {
                // IPv4 source nat and ephemeral, ensure we have no v6 floating
                // IPs.
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                let mut builder = ExternalIpsBuilder::new();
                builder
                    .with_source_nat(snat_v4)
                    .with_ephemeral_ip(eip_v4)
                    .with_floating_ips(fips_v4);
                builder.build().map(Into::into)
            }
            (Some(_), None, Some(IpAddr::V6(_)))
            | (None, Some(_), Some(IpAddr::V4(_))) => {
                return Err(ExternalIpsError::MixedIpVersions);
            }
            (Some(_), Some(_), None) | (Some(_), Some(_), Some(_)) => {
                // Both SNAT v4 and v6 configurations. Shouldn't be possible,
                // but not valid.
                return Err(ExternalIpsError::MixedIpVersions);
            }
        }
    }

    pub fn ipv4_config(&self) -> Option<&ExternalIpv4Config> {
        match self {
            ExternalIpConfig::V4(v4)
            | ExternalIpConfig::DualStack { v4, .. } => Some(v4),
            ExternalIpConfig::V6(_) => None,
        }
    }

    pub fn ipv6_config(&self) -> Option<&ExternalIpv6Config> {
        match self {
            ExternalIpConfig::V6(v6)
            | ExternalIpConfig::DualStack { v6, .. } => Some(v6),
            ExternalIpConfig::V4(_) => None,
        }
    }
}
