// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP types.

pub mod v1;

use crate::address::ConcreteIp;
use crate::address::Ip;
use crate::address::NUM_SOURCE_NAT_PORTS;
use daft::Diffable;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

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
struct SourceNatConfigShadow<T: Ip + SnatSchema> {
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
    T: Ip + SnatSchema,
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
impl<'de, T> Deserialize<'de> for SourceNatConfig<T>
where
    T: Ip + SnatSchema + Deserialize<'de>,
{
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
/// including source NAT, Ephemeral, and Floating IPs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(bound = "T: ConcreteIp + SnatSchema + serde::de::DeserializeOwned")]
pub struct ExternalIps<T>
where
    T: ConcreteIp,
{
    /// Source NAT configuration, for outbound-only connectivity.
    pub source_nat: Option<SourceNatConfig<T>>,
    /// An Ephemeral address for in- and outbound connectivity.
    pub ephemeral_ip: Option<T>,
    /// Additional Floating IPs for in- and outbound connectivity.
    pub floating_ips: Vec<T>,
}

impl<T: ConcreteIp> Default for ExternalIps<T> {
    fn default() -> Self {
        Self { source_nat: None, ephemeral_ip: None, floating_ips: Vec::new() }
    }
}

pub type ExternalIpv4Config = ExternalIps<Ipv4Addr>;
pub type ExternalIpv6Config = ExternalIps<Ipv6Addr>;

// Private type only used for deriving the JSON schema for `ExternalIps`.
#[derive(JsonSchema)]
#[allow(dead_code)]
struct ExternalIpsSchema<T>
where
    T: ConcreteIp + SnatSchema + ExternalIpSchema,
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
        ExternalIpsSchema::<Ipv4Addr>::json_schema(generator)
    }
}

impl JsonSchema for ExternalIpv6Config {
    fn schema_name() -> String {
        String::from("ExternalIpv6Config")
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ExternalIpsSchema::<Ipv6Addr>::json_schema(generator)
    }
}

/// External IP address configuration.
///
/// This encapsulates all external addresses for an instance, with separate
/// optional configurations for IPv4 and IPv6.
#[derive(
    Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct ExternalIpConfig {
    /// IPv4 external IP configuration.
    pub v4: Option<ExternalIpv4Config>,
    /// IPv6 external IP configuration.
    pub v6: Option<ExternalIpv6Config>,
}

impl From<v1::ExternalIpConfig> for ExternalIpConfig {
    fn from(old: v1::ExternalIpConfig) -> Self {
        match old {
            v1::ExternalIpConfig::V4(v4) => Self { v4: Some(v4), v6: None },
            v1::ExternalIpConfig::V6(v6) => Self { v4: None, v6: Some(v6) },
            v1::ExternalIpConfig::DualStack { v4, v6 } => {
                Self { v4: Some(v4), v6: Some(v6) }
            }
        }
    }
}
