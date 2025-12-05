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

/// Trait for any IP address type.
///
/// This is used to constrain the external addressing types below.
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
    + SnatSchema
{
}
impl Ip for Ipv4Addr {}
impl Ip for Ipv6Addr {}
impl Ip for IpAddr {}

/// Helper trait specifying the name of the JSON Schema for a `SourceNatConfig`.
///
/// This exists so we can use a generic type and have the names of the concrete
/// type aliases be the same as the name of schema object.
pub trait SnatSchema {
    fn json_schema_name() -> String;
}

impl SnatSchema for Ipv4Addr {
    fn json_schema_name() -> String {
        String::from("SourceNatConfigV4")
    }
}

impl SnatSchema for Ipv6Addr {
    fn json_schema_name() -> String {
        String::from("SourceNatConfigV6")
    }
}

impl SnatSchema for IpAddr {
    fn json_schema_name() -> String {
        String::from("SourceNatConfigGeneric")
    }
}

/// An IP address of a specific version, IPv4 or IPv6.
pub trait ConcreteIp: Ip {
    fn into_ipaddr(self) -> IpAddr;
}

impl ConcreteIp for Ipv4Addr {
    fn into_ipaddr(self) -> IpAddr {
        IpAddr::V4(self)
    }
}

impl ConcreteIp for Ipv6Addr {
    fn into_ipaddr(self) -> IpAddr {
        IpAddr::V6(self)
    }
}

/// Helper trait specifying the name of the JSON Schema for an
/// `ExternalIpConfig` object.
///
/// This exists so we can use a generic type and have the names of the concrete
/// type aliases be the same as the name of the schema oject.
pub trait ExternalIpSchema {
    fn json_schema_name() -> String;
}

impl ExternalIpSchema for Ipv4Addr {
    fn json_schema_name() -> String {
        String::from("ExternalIpv4Config")
    }
}

impl ExternalIpSchema for Ipv6Addr {
    fn json_schema_name() -> String {
        String::from("ExternalIpv6Config")
    }
}

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Note that `Deserialize` is manually implemented; if you make any changes to
// the fields of this structure, you must make them to that implementation too.
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
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

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Private type only used for deriving the actual JSON schema object itself,
// and for checked deserialization.
//
// The fields of `SourceNatConfigShadow` should exactly match the fields
// of `SourceNatConfig`. We're not really using serde's remote derive,
// but by adding the attribute we get compile-time checking that all the
// field names and types match. (It doesn't check the _order_, but that
// should be fine as long as we're using JSON or similar formats.)
#[derive(Deserialize, JsonSchema)]
#[serde(remote = "SourceNatConfig")]
struct SourceNatConfigShadow<T: Ip> {
    /// The external address provided to the instance or service.
    ip: T,
    /// The first port used for source NAT, inclusive.
    first_port: u16,
    /// The last port used for source NAT, also inclusive.
    last_port: u16,
}

pub type SourceNatConfigV4 = SourceNatConfig<Ipv4Addr>;
pub type SourceNatConfigV6 = SourceNatConfig<Ipv6Addr>;
pub type SourceNatConfigGeneric = SourceNatConfig<IpAddr>;

impl<T> JsonSchema for SourceNatConfig<T>
where
    T: Ip,
{
    fn schema_name() -> String {
        <T as SnatSchema>::json_schema_name()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        SourceNatConfigShadow::<T>::json_schema(generator)
    }
}

// We implement `Deserialize` manually to add validity checking on the port
// range.
impl<'de, T: Ip + Deserialize<'de>> Deserialize<'de> for SourceNatConfig<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

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

impl SourceNatConfigGeneric {
    /// Try to convert this to a concrete IPv4 configuration.
    ///
    /// Return None if this is an IPv6 configuration.
    pub fn try_as_ipv4(&self) -> Option<SourceNatConfigV4> {
        let IpAddr::V4(ip) = self.ip else {
            return None;
        };
        Some(SourceNatConfig {
            ip,
            first_port: self.first_port,
            last_port: self.last_port,
        })
    }

    /// Try to convert this to a concrete IPv6 configuration.
    ///
    /// Return None if this is an IPv4 configuration.
    pub fn try_as_ipv6(&self) -> Option<SourceNatConfigV6> {
        let IpAddr::V6(ip) = self.ip else {
            return None;
        };
        Some(SourceNatConfig {
            ip,
            first_port: self.first_port,
            last_port: self.last_port,
        })
    }
}

impl<T> TryFrom<SourceNatConfig<T>> for v1::SourceNatConfig
where
    T: Ip,
    IpAddr: From<T>,
{
    type Error = SourceNatConfigError;

    fn try_from(value: SourceNatConfig<T>) -> Result<Self, Self::Error> {
        v1::SourceNatConfig::new(
            value.ip.into(),
            value.first_port,
            value.last_port,
        )
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

/// External IP address configuration.
///
/// This encapsulates all the external addresses of a single IP version,
/// including source NAT, Ephemeral, and Floating IPs. Note that not all of
/// these need to be specified, but this type can only be constructed if _at
/// least one_ of them is.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ExternalIps<T>
where
    T: ConcreteIp,
{
    /// Source NAT configuration, for outbound-only connectivity.
    source_nat: Option<SourceNatConfig<T>>,
    /// An Ephemeral address for in- and outbound connectivity.
    ephemeral_ip: Option<T>,
    /// Additional Floating IPs for in- and outbound connectivity.
    floating_ips: Vec<T>,
}

impl<T: ConcreteIp> ExternalIps<T>
where
    T: ConcreteIp,
{
    /// Return a reference to the SNAT address, if it exists.
    pub fn source_nat(&self) -> &Option<SourceNatConfig<T>> {
        &self.source_nat
    }

    /// Return a reference to the Ephemeral adress, if it exists.
    pub fn ephemeral_ip(&self) -> &Option<T> {
        &self.ephemeral_ip
    }

    /// Return a mutable reference to the Ephemeral address, if it exists.
    pub fn ephemeral_ip_mut(&mut self) -> &mut Option<T> {
        &mut self.ephemeral_ip
    }

    /// Return the list of Floating IP addresses.
    ///
    /// The slice will be empty if no such addresses exist.
    pub fn floating_ips(&self) -> &[T] {
        &self.floating_ips
    }

    /// Return a mutable reference to the list of Floating IP addresses.
    ///
    /// The slice will be empty if no such addresses exist.
    pub fn floating_ips_mut(&mut self) -> &mut Vec<T> {
        &mut self.floating_ips
    }
}

pub type ExternalIpv4Config = ExternalIps<Ipv4Addr>;
pub type ExternalIpv6Config = ExternalIps<Ipv6Addr>;

/// External IP address configuration.
///
/// This encapsulates all the external addresses of a single IP version,
/// including source NAT, Ephemeral, and Floating IPs. Note that not all of
/// these need to be specified, but this type can only be constructed if _at
/// least one_ of them is.
#[derive(Deserialize, JsonSchema)]
#[serde(remote = "ExternalIps")]
struct ExternalIpsShadow<T>
where
    T: ConcreteIp,
{
    /// Source NAT configuration, for outbound-only connectivity.
    source_nat: Option<SourceNatConfig<T>>,
    /// An Ephemeral address for in- and outbound connectivity.
    ephemeral_ip: Option<T>,
    /// Additional Floating IPs for in- and outbound connectivity.
    floating_ips: Vec<T>,
}

impl JsonSchema for ExternalIpv4Config {
    fn schema_name() -> String {
        String::from("ExternalIpv4Config")
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ExternalIpsShadow::<Ipv4Addr>::json_schema(generator)
    }
}

impl JsonSchema for ExternalIpv6Config {
    fn schema_name() -> String {
        String::from("ExternalIpv6Config")
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ExternalIpsShadow::<Ipv6Addr>::json_schema(generator)
    }
}

// We implement `Deserialize` manually to ensure we have at least one of the
// SNAT, ephemeral, and floating IPs in the input data.
impl<'de, T> Deserialize<'de> for ExternalIps<T>
where
    T: ConcreteIp + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let ExternalIps { source_nat, ephemeral_ip, floating_ips } =
            ExternalIpsShadow::deserialize(deserializer)?;
        let mut builder = ExternalIpConfigBuilder::new();
        if let Some(snat) = source_nat {
            builder = builder.with_source_nat(snat);
        }
        if let Some(ip) = ephemeral_ip {
            builder = builder.with_ephemeral_ip(ip);
        }
        builder
            .with_floating_ips(floating_ips)
            .build()
            .map_err(D::Error::custom)
    }
}

/// A builder for `ExternalIps` and the concrete type aliases.
///
/// This enforces the constraint that `ExternalIps` always has _at least one_ IP
/// address.
pub struct ExternalIpConfigBuilder<T: ConcreteIp> {
    source_nat: Option<SourceNatConfig<T>>,
    ephemeral_ip: Option<T>,
    floating_ips: Vec<T>,
}

impl<T> ExternalIpConfigBuilder<T>
where
    T: ConcreteIp,
{
    /// Construct a new builder.
    pub fn new() -> Self {
        Self { source_nat: None, ephemeral_ip: None, floating_ips: Vec::new() }
    }

    /// Add a source NAT address.
    ///
    /// Note that this overwrites any previous such information.
    pub fn with_source_nat(mut self, source_nat: SourceNatConfig<T>) -> Self {
        self.source_nat = Some(source_nat);
        self
    }

    /// Add an Ephemeral IP address.
    ///
    /// Note that this overwrites any previous such information.
    pub fn with_ephemeral_ip(mut self, ip: T) -> Self {
        self.ephemeral_ip = Some(ip);
        self
    }

    /// Add floating IPs.
    ///
    /// Note that this overwrites any previous such information.
    pub fn with_floating_ips(mut self, ips: Vec<T>) -> Self {
        self.floating_ips = ips;
        self
    }

    /// Attempt to build an `ExternalIps`.
    ///
    /// This will fail if no addresses at all have been specified.
    pub fn build(self) -> Result<ExternalIps<T>, ExternalIpsError> {
        let ExternalIpConfigBuilder { source_nat, ephemeral_ip, floating_ips } =
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

/// A single- or dual-stack external IP configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ExternalIpConfig {
    /// Single-stack IPv4 external IP configuration.
    V4(ExternalIpv4Config),
    /// Single-stack IPv6 external IP configuration.
    V6(ExternalIpv6Config),
    /// Both IPv4 and IPv6 external IP configuration.
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
    /// Attempt to convert from generic IP addressing information.
    ///
    /// This is used to convert previous versions of the external addressing
    /// information, which used version-agnostic IP address types, like
    /// `IpAddr`, rather than concrete `Ipv4Addr` or `Ipv6Addr`. This handles
    /// all the possible flavors of addresses for SNAT, Ephemeral, and Floating
    /// IPs.
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
                    (true, true) => Err(ExternalIpsError::NoIps),
                    (true, false) => ExternalIpConfigBuilder::new()
                        .with_floating_ips(fips_v6)
                        .build()
                        .map(Into::into),
                    (false, true) => ExternalIpConfigBuilder::new()
                        .with_floating_ips(fips_v4)
                        .build()
                        .map(Into::into),
                    (false, false) => Err(ExternalIpsError::MixedIpVersions),
                }
            }
            (None, None, Some(IpAddr::V4(v4))) => {
                // Ephemeral IPv4, ensure we have no v6 floating IPs.
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                ExternalIpConfigBuilder::new()
                    .with_ephemeral_ip(v4)
                    .with_floating_ips(fips_v4)
                    .build()
                    .map(Into::into)
            }
            (None, None, Some(IpAddr::V6(v6))) => {
                // Ephemeral IPv6, ensure we have no v4 floating IPs.
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                ExternalIpConfigBuilder::new()
                    .with_ephemeral_ip(v6)
                    .with_floating_ips(fips_v6)
                    .build()
                    .map(Into::into)
            }
            (None, Some(snat_v6), None) => {
                // IPv6 source nat, ensure we have no v4 floating IPs.
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                ExternalIpConfigBuilder::new()
                    .with_source_nat(snat_v6)
                    .with_floating_ips(fips_v6)
                    .build()
                    .map(Into::into)
            }
            (None, Some(snat_v6), Some(IpAddr::V6(eip_v6))) => {
                // IPv6 source nat and ephemeral, ensure we have no v4 floating
                // IPs.
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                ExternalIpConfigBuilder::new()
                    .with_source_nat(snat_v6)
                    .with_ephemeral_ip(eip_v6)
                    .with_floating_ips(fips_v6)
                    .build()
                    .map(Into::into)
            }
            (Some(snat_v4), None, None) => {
                // IPv4 source nat, ensure we have no v6 floating IPs.
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                ExternalIpConfigBuilder::new()
                    .with_source_nat(snat_v4)
                    .with_floating_ips(fips_v4)
                    .build()
                    .map(Into::into)
            }
            (Some(snat_v4), None, Some(IpAddr::V4(eip_v4))) => {
                // IPv4 source nat and ephemeral, ensure we have no v6 floating
                // IPs.
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                ExternalIpConfigBuilder::new()
                    .with_source_nat(snat_v4)
                    .with_ephemeral_ip(eip_v4)
                    .with_floating_ips(fips_v4)
                    .build()
                    .map(Into::into)
            }
            (Some(_), None, Some(IpAddr::V6(_)))
            | (None, Some(_), Some(IpAddr::V4(_))) => {
                Err(ExternalIpsError::MixedIpVersions)
            }
            (Some(_), Some(_), None) | (Some(_), Some(_), Some(_)) => {
                // Both SNAT v4 and v6 configurations. Shouldn't be possible,
                // but not valid.
                Err(ExternalIpsError::MixedIpVersions)
            }
        }
    }

    /// Return the IPv4 configuration, if it exists.
    pub fn ipv4_config(&self) -> Option<&ExternalIpv4Config> {
        match self {
            ExternalIpConfig::V4(v4)
            | ExternalIpConfig::DualStack { v4, .. } => Some(v4),
            ExternalIpConfig::V6(_) => None,
        }
    }

    /// Return a mutable reference to the IPv4 configuration, if it exists.
    pub fn ipv4_config_mut(&mut self) -> Option<&mut ExternalIpv4Config> {
        match self {
            ExternalIpConfig::V4(v4)
            | ExternalIpConfig::DualStack { v4, .. } => Some(v4),
            ExternalIpConfig::V6(_) => None,
        }
    }

    /// Return the IPv6 configuration, if it exists.
    pub fn ipv6_config(&self) -> Option<&ExternalIpv6Config> {
        match self {
            ExternalIpConfig::V6(v6)
            | ExternalIpConfig::DualStack { v6, .. } => Some(v6),
            ExternalIpConfig::V4(_) => None,
        }
    }

    /// Return a mutable reference to the IPv6 configuration, if it exists.
    pub fn ipv6_config_mut(&mut self) -> Option<&mut ExternalIpv6Config> {
        match self {
            ExternalIpConfig::V6(v6)
            | ExternalIpConfig::DualStack { v6, .. } => Some(v6),
            ExternalIpConfig::V4(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_generic_fails_with_no_ips() {
        assert!(
            ExternalIpConfig::try_from_generic(None, None, vec![]).is_err(),
            "Should fail to construct an ExternalIpConfig from no IP addresses"
        );
    }

    #[test]
    fn try_from_generic_fails_with_mixed_address_versions() {
        let v4 = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let v6 = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1));
        let snat_v4 = v1::SourceNatConfig::new(v4, 0, 16383).unwrap();
        let snat_v6 = v1::SourceNatConfig::new(v6, 0, 16383).unwrap();

        for (snat, ephemeral, floating) in [
            (Some(snat_v4), Some(v6), vec![]),
            (Some(snat_v4), None, vec![v6]),
            (Some(snat_v6), Some(v4), vec![]),
            (Some(snat_v6), None, vec![v4]),
            (None, Some(v4), vec![v6]),
            (None, Some(v6), vec![v4]),
        ] {
            assert!(
                ExternalIpConfig::try_from_generic(snat, ephemeral, floating,)
                    .is_err(),
                "Should fail to construct an ExternalIpConfig with mixed IP versions"
            );
        }
    }

    #[test]
    fn try_from_generic_successes() {
        let ipv4 = Ipv4Addr::new(1, 1, 1, 1);
        let v4 = IpAddr::V4(ipv4);
        let ipv6 = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let v6 = IpAddr::V6(ipv6);
        let snat_v4 = v1::SourceNatConfig::new(v4, 0, 16383).unwrap();
        let snat_v6 = v1::SourceNatConfig::new(v6, 0, 16383).unwrap();

        for (ipv4_snat, ipv4_ephemeral, ipv4_floating) in [
            (Some(snat_v4), None, vec![]),
            (None, Some(v4), vec![]),
            (None, None, vec![v4]),
            (Some(snat_v4), Some(v4), vec![]),
            (Some(snat_v4), None, vec![v4]),
            (None, Some(v4), vec![v4]),
            (Some(snat_v4), Some(v4), vec![v4]),
        ] {
            let config = ExternalIpConfig::try_from_generic(
                ipv4_snat,
                ipv4_ephemeral,
                ipv4_floating.clone(),
            )
            .unwrap();
            assert!(config.ipv6_config().is_none());
            let ipv4_config = config.ipv4_config().unwrap();

            match (ipv4_config.source_nat(), ipv4_snat) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    let IpAddr::V4(addr) = expected.ip else {
                        unreachable!("Should have an IPv4 address");
                    };
                    assert_eq!(actual.ip, addr);
                    let (expected_first, expected_last) =
                        expected.port_range_raw();
                    assert_eq!(actual.first_port, expected_first);
                    assert_eq!(actual.last_port, expected_last);
                }
                (None, Some(_)) | (Some(_), None) => {
                    panic!("IPv4 SNAT was not carried over correctly",)
                }
            }

            match (ipv4_config.ephemeral_ip(), ipv4_ephemeral) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    let IpAddr::V4(addr) = expected else {
                        unreachable!("Should have an IPv4 address");
                    };
                    assert_eq!(actual, &addr);
                }
                (None, Some(_)) | (Some(_), None) => {
                    panic!("IPv4 Ephemeral IP not carried over correctly")
                }
            }

            let actual_fips = ipv4_config.floating_ips();
            assert_eq!(actual_fips.len(), ipv4_floating.len());
            for (actual, expected) in
                actual_fips.iter().zip(ipv4_floating.iter())
            {
                let IpAddr::V4(expected) = expected else {
                    unreachable!("Should have an IPv4 address");
                };
                assert_eq!(actual, expected);
            }
        }

        for (ipv6_snat, ipv6_ephemeral, ipv6_floating) in [
            (Some(snat_v6), None, vec![]),
            (None, Some(v6), vec![]),
            (None, None, vec![v6]),
            (Some(snat_v6), Some(v6), vec![]),
            (Some(snat_v6), None, vec![v6]),
            (None, Some(v6), vec![v6]),
            (Some(snat_v6), Some(v6), vec![v6]),
        ] {
            let config = ExternalIpConfig::try_from_generic(
                ipv6_snat,
                ipv6_ephemeral,
                ipv6_floating.clone(),
            )
            .unwrap();
            assert!(config.ipv4_config().is_none());
            let ipv6_config = config.ipv6_config().unwrap();

            match (ipv6_config.source_nat(), ipv6_snat) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    let IpAddr::V6(addr) = expected.ip else {
                        unreachable!("Should have an IPv6 address");
                    };
                    assert_eq!(actual.ip, addr);
                    let (expected_first, expected_last) =
                        expected.port_range_raw();
                    assert_eq!(actual.first_port, expected_first);
                    assert_eq!(actual.last_port, expected_last);
                }
                (None, Some(_)) | (Some(_), None) => {
                    panic!("IPv6 SNAT was not carried over correctly",)
                }
            }

            match (ipv6_config.ephemeral_ip(), ipv6_ephemeral) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    let IpAddr::V6(addr) = expected else {
                        unreachable!("Should have an IPv6 address");
                    };
                    assert_eq!(actual, &addr);
                }
                (None, Some(_)) | (Some(_), None) => {
                    panic!("IPv6 Ephemeral IP not carried over correctly")
                }
            }

            let actual_fips = ipv6_config.floating_ips();
            assert_eq!(actual_fips.len(), ipv6_floating.len());
            for (actual, expected) in
                actual_fips.iter().zip(ipv6_floating.iter())
            {
                let IpAddr::V6(expected) = expected else {
                    unreachable!("Should have an IPv6 address");
                };
                assert_eq!(actual, expected);
            }
        }
    }
}
