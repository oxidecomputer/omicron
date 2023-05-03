// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use super::trust_quorum::SerializableShareDistribution;
use omicron_common::address::{self, Ipv6Subnet, SLED_PREFIX};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: HashSet<Ipv6Addr> },
}

/// Configuration for the "rack setup service".
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackInitializeRequest {
    pub rack_subnet: Ipv6Addr,

    /// Describes how bootstrap addresses should be collected during RSS.
    pub bootstrap_discovery: BootstrapAddressDiscovery,

    /// The minimum number of sleds required to unlock the rack secret.
    ///
    /// If this value is less than 2, no rack secret will be created on startup;
    /// this is the typical case for single-server test/development.
    pub rack_secret_threshold: usize,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// The external DNS server addresses.
    pub dns_servers: Vec<String>,

    /// Ranges of the service IP pool which may be used for internal services.
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Eventually,
    // we want to configure multiple pools.
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,

    /// DNS name for the DNS zone delegated to the rack for external DNS
    pub external_dns_zone_name: String,

    /// Configuration of the Recovery Silo (the initial Silo)
    pub recovery_silo: RecoverySiloConfig,
}

pub type RecoverySiloConfig = nexus_client::types::RecoverySiloConfig;

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SledAgentRequest {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// The external NTP servers to use
    pub ntp_servers: Vec<String>,
    //
    /// The external DNS servers to use
    pub dns_servers: Vec<String>,

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
    use camino::Utf8PathBuf;

    #[test]
    fn parse_rack_initialization() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cannot access manifest directory");
        let manifest = Utf8PathBuf::from(manifest);

        let path =
            manifest.join("../smf/sled-agent/non-gimlet/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));

        let path = manifest
            .join("../smf/sled-agent/gimlet-standalone/config-rss.toml");
        let contents = std::fs::read_to_string(&path).unwrap();
        let _: RackInitializeRequest = toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e));
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
                    ntp_servers: vec![String::from("test.pool.example.com")],
                    dns_servers: vec![String::from("1.1.1.1")],
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
