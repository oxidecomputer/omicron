// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use super::trust_quorum::SerializableShareDistribution;
use omicron_common::address::{self, Ipv6Subnet, SLED_PREFIX};
use omicron_common::api::external::MacAddr;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

/// Configuration for the "rack setup service".
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackInitializeRequest {
    pub rack_subnet: Ipv6Addr,

    /// The minimum number of sleds required to unlock the rack secret.
    ///
    /// If this value is less than 2, no rack secret will be created on startup;
    /// this is the typical case for single-server test/development.
    pub rack_secret_threshold: usize,

    /// Internet gateway information.
    pub gateway: Gateway,

    /// The address on which Nexus should serve an external interface.
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Eventually,
    // this should be pulled from a pool of addresses.
    pub nexus_external_address: IpAddr,
}

/// Information about the internet gateway used for externally-facing services.
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct Gateway {
    /// IP address of the Internet gateway, which is particularly
    /// relevant for external-facing services (such as Nexus).
    pub address: Option<Ipv4Addr>,

    /// MAC address of the internet gateway above. This is used to provide
    /// external connectivity into guests, by allowing OPTE to forward traffic
    /// destined for the broader network to the gateway.
    pub mac: MacAddr,
}

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SledAgentRequest {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// Information about internet gateway to use
    // NOTE: This information is currently being configured and sent from RSS,
    // but it contains dynamic information that could plausibly change during
    // the duration of the sled's lifetime.
    //
    // Longer-term, it probably makes sense to store this in CRDB and transfer
    // it to Sled Agent as part of the request to launch Nexus.
    pub gateway: Gateway,

    // Note: The order of these fields is load bearing, because we serialize
    // `SledAgentRequest`s as toml. `subnet` serializes as a TOML table, so it
    // must come after non-table fields.
    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl SledAgentRequest {
    pub fn sled_address(&self) -> SocketAddrV6 {
        address::get_sled_address(self.subnet)
    }

    pub fn switch_zone_ip(&self) -> Ipv6Addr {
        address::get_switch_zone_address(self.subnet)
    }
}

// We intentionally DO NOT derive `Debug` or `Serialize`; both provide avenues
// by which we may accidentally log the contents of our `share`. To serialize a
// request, use `RequestEnvelope::danger_serialize_as_json()`.
#[derive(Clone, Deserialize, PartialEq)]
// Clippy wants us to put the SledAgentRequest in a Box, but (a) it's not _that_
// big (a couple hundred bytes), and (b) that makes matching annoying.
// `Request`s are relatively rare over the life of a sled agent.
#[allow(clippy::large_enum_variant)]
pub enum Request<'a> {
    /// Send configuration information for launching a Sled Agent.
    SledAgentRequest(
        Cow<'a, SledAgentRequest>,
        Option<SerializableShareDistribution>,
    ),

    /// Request the sled's share of the rack secret.
    ShareRequest,
}

#[derive(Clone, Deserialize, PartialEq)]
pub struct RequestEnvelope<'a> {
    pub version: u32,
    pub request: Request<'a>,
}

impl RequestEnvelope<'_> {
    /// On success, the returned `Vec` will contain our raw
    /// trust quorum share. This method is named `danger_*` to remind the
    /// caller that they must not log it.
    pub(crate) fn danger_serialize_as_json(
        &self,
    ) -> Result<Vec<u8>, serde_json::Error> {
        #[derive(Serialize)]
        #[serde(remote = "Request")]
        #[allow(clippy::large_enum_variant)]
        pub enum RequestDef<'a> {
            /// Send configuration information for launching a Sled Agent.
            SledAgentRequest(
                Cow<'a, SledAgentRequest>,
                Option<SerializableShareDistribution>,
            ),

            /// Request the sled's share of the rack secret.
            ShareRequest,
        }

        #[derive(Serialize)]
        #[serde(remote = "RequestEnvelope")]
        struct RequestEnvelopeDef<'a> {
            version: u32,
            #[serde(borrow, with = "RequestDef")]
            request: Request<'a>,
        }

        let mut writer = Vec::with_capacity(128);
        let mut serializer = serde_json::Serializer::new(&mut writer);
        RequestEnvelopeDef::serialize(self, &mut serializer)?;
        Ok(writer)
    }
}

pub(super) mod version {
    pub(crate) const V1: u32 = 1;
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;
    use crate::bootstrap::trust_quorum::RackSecret;
    use crate::bootstrap::trust_quorum::ShareDistribution;
    use macaddr::MacAddr6;
    use std::path::PathBuf;

    #[test]
    fn parse_rack_initialization() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cannot access manifest directory");
        let path = PathBuf::from(manifest)
            .join("../smf/sled-agent/nongimlet/config-rss.toml");
        let contents = std::fs::read_to_string(path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents).unwrap();
    }

    #[test]
    fn parse_gateway() {
        let _: Gateway = toml::from_str(
            r#"
            mac = "18:c0:4d:d:a0:2a"
        "#,
        )
        .unwrap();
    }

    #[test]
    fn json_serialization_round_trips() {
        let secret = RackSecret::new();
        let (mut shares, verifier) = secret.split(2, 4).unwrap();

        let envelope = RequestEnvelope {
            version: 1,
            request: Request::SledAgentRequest(
                Cow::Owned(SledAgentRequest {
                    id: Uuid::new_v4(),
                    rack_id: Uuid::new_v4(),
                    gateway: Gateway {
                        address: None,
                        mac: MacAddr6::nil().into(),
                    },
                    subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                }),
                Some(
                    ShareDistribution {
                        threshold: 2,
                        verifier,
                        share: shares.pop().unwrap(),
                        member_device_id_certs: vec![],
                    }
                    .into(),
                ),
            ),
        };

        let serialized = envelope.danger_serialize_as_json().unwrap();
        let deserialized: RequestEnvelope =
            serde_json::from_slice(&serialized).unwrap();

        assert!(envelope == deserialized, "serialization round trip failed");
    }
}
