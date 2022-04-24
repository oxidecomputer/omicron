// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request body types for the bootstrap agent

use omicron_common::{
    api::external::Ipv6Net,
    address::SLED_PREFIX,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Identity signed by local RoT and Oxide certificate chain.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
pub enum SubnetError {
    #[error("Subnet {subnet} has unexpected prefix length, wanted {}", SLED_PREFIX)]
    BadPrefixLength {
        subnet: ipnetwork::Ipv6Network,
    },
}

/// Represents subnets belonging to Sleds.
///
/// This is a thin wrapper around the [`Ipv6Net`] type - which may be accessed
/// by [`AsRef<Ipv6Net>`] - which adds additional validation that this is a /64
/// subnet with an expected prefix.
// Note: The inner field is intentionally non-pub; this makes it
// more difficult to construct a sled subnet which avoids the
// validation performed by the constructor.
#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct SledSubnet(Ipv6Net);

impl SledSubnet {
    pub fn new(net: Ipv6Net) -> Result<Self, SubnetError> {
        let prefix = net.0.prefix();
        if prefix != SLED_PREFIX {
            return Err(SubnetError::BadPrefixLength { subnet: net.0 });
        }
        Ok(SledSubnet(net))
    }
}

impl<'de> serde::Deserialize<'de> for SledSubnet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let net = Ipv6Net::deserialize(deserializer)?;
        SledSubnet::new(net).map_err(serde::de::Error::custom)
    }
}

impl AsRef<Ipv6Net> for SledSubnet {
    fn as_ref(&self) -> &Ipv6Net {
        &self.0
    }
}

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledAgentRequest {
    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: SledSubnet,
}
