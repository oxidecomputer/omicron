// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! This version strengthens the type-level validation of [`RackNetworkConfig`]
//! by adding [`UplinkPorts`], a newtype wrapper around a
//! [`PortConfig`](v30::PortConfig) list that is guaranteed to be non-empty. A
//! [`RackNetworkConfig`] with no ports doesn't make any sense -- encoding this
//! fact in the type makes that invalid state unrepresentable, preventing a
//! transient failure from being persisted in the bootstore as an empty config.
//! See #10640.

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v30::early_networking as v30;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Error returned when constructing a [`UplinkPorts`] from an empty list.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "a rack network config must contain at least one uplink port, \
     but the port list was empty"
)]
pub struct EmptyUplinkPortsError;

/// A non-empty list of uplink [`PortConfig`](v30::PortConfig)s.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UplinkPorts(Vec<v30::PortConfig>);

impl UplinkPorts {
    /// Constructs an `UplinkPorts` from a list of ports, returning an error if
    /// the list is empty.
    pub fn new(
        ports: Vec<v30::PortConfig>,
    ) -> Result<Self, EmptyUplinkPortsError> {
        if ports.is_empty() {
            return Err(EmptyUplinkPortsError);
        }
        Ok(Self(ports))
    }

    /// Returns the first port.
    pub fn first(&self) -> &v30::PortConfig {
        &self.0[0]
    }

    /// Returns the number of ports, which is always at least one.
    //
    // An `UplinkPorts` is never empty, so `is_empty` would always returns
    // `false`. There's no point providing it.
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns an iterator over the ports.
    pub fn iter(&self) -> std::slice::Iter<'_, v30::PortConfig> {
        self.0.iter()
    }

    /// Returns the ports as a (non-empty) slice.
    pub fn as_slice(&self) -> &[v30::PortConfig] {
        &self.0
    }

    /// Consumes `self`, returning the inner (non-empty) list of ports.
    pub fn into_vec(self) -> Vec<v30::PortConfig> {
        self.0
    }
}

impl IntoIterator for UplinkPorts {
    type Item = v30::PortConfig;
    type IntoIter = std::vec::IntoIter<v30::PortConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a UplinkPorts {
    type Item = &'a v30::PortConfig;
    type IntoIter = std::slice::Iter<'a, v30::PortConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'de> Deserialize<'de> for UplinkPorts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ports = Vec::<v30::PortConfig>::deserialize(deserializer)?;
        UplinkPorts::new(ports).map_err(|EmptyUplinkPortsError| {
            serde::de::Error::invalid_length(0, &"at least one uplink port")
        })
    }
}

impl JsonSchema for UplinkPorts {
    fn schema_name() -> String {
        "UplinkPorts".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Array.into()),
            array: Some(Box::new(schemars::schema::ArrayValidation {
                items: Some(
                    generator.subschema_for::<v30::PortConfig>().into(),
                ),
                min_items: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct RackNetworkConfig {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: IpAddr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: IpAddr,
    /// Uplinks for connecting the rack to external networks
    pub ports: UplinkPorts,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<v20::BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v1::BfdPeerConfig>,
}

// This conversion is fallible because the prior version allowed a config that
// the latest version forbids: an empty port list.
//
// There is a subtle argument here: the `early_networking::serialization`
// contract permits conversion to the latest version to be fallible only when
// the set of configs that are rejected describes a rack that can't run anyway.
// That actually is true here! An empty list of ports is a corrupted bootstore
// config that, with prior versions of sled-agent, would panic during early
// networking setup. (See #10640 for example logs.) This is actually an
// improvement because the config is now rejected, and Sled Agent waits for a
// new config to be pushed rather than panicking.
impl TryFrom<v30::RackNetworkConfig> for RackNetworkConfig {
    type Error = EmptyUplinkPortsError;

    fn try_from(old: v30::RackNetworkConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            rack_subnet: old.rack_subnet,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            ports: UplinkPorts::new(old.ports)?,
            bgp: old.bgp,
            bfd: old.bfd,
        })
    }
}

impl From<RackNetworkConfig> for v30::RackNetworkConfig {
    fn from(new: RackNetworkConfig) -> Self {
        Self {
            rack_subnet: new.rack_subnet,
            infra_ip_first: new.infra_ip_first,
            infra_ip_last: new.infra_ip_last,
            ports: new.ports.into_vec(),
            bgp: new.bgp,
            bfd: new.bfd,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uplink_ports_rejects_empty() {
        assert_eq!(UplinkPorts::new(vec![]), Err(EmptyUplinkPortsError));
    }

    #[test]
    fn uplink_ports_deserialize_rejects_empty() {
        // Deserializing an empty array must fail with a length error.
        let err = serde_json::from_str::<UplinkPorts>("[]")
            .expect_err("an empty uplink port list must fail to deserialize");
        assert!(
            err.to_string().contains("invalid length 0"),
            "expected an invalid-length error, got: {err}"
        );
    }
}
