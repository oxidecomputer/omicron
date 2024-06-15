// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request types for the bootstrap agent

use crate::bootstrap::early_networking::RackNetworkConfigV1;
use anyhow::{bail, Result};
use async_trait::async_trait;
use omicron_common::address::{self, Ipv6Subnet, SLED_PREFIX};
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sled_hardware_types::Baseboard;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: BTreeSet<Ipv6Addr> },
}

/// This is a deprecated format, maintained to allow importing from older
/// versions.
#[derive(Clone, Deserialize)]
struct UnvalidatedRackInitializeRequestV1 {
    trust_quorum_peers: Option<Vec<Baseboard>>,
    bootstrap_discovery: BootstrapAddressDiscovery,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    internal_services_ip_pool_ranges: Vec<address::IpRange>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo: RecoverySiloConfig,
    rack_network_config: RackNetworkConfigV1,
    #[serde(default = "default_allowed_source_ips")]
    allowed_source_ips: AllowedSourceIps,
}

/// This is a deprecated format, maintained to allow importing from older
/// versions.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(try_from = "UnvalidatedRackInitializeRequestV1")]
pub struct RackInitializeRequestV1 {
    pub trust_quorum_peers: Option<Vec<Baseboard>>,
    pub bootstrap_discovery: BootstrapAddressDiscovery,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub external_certificates: Vec<Certificate>,
    pub recovery_silo: RecoverySiloConfig,
    pub rack_network_config: RackNetworkConfigV1,
    #[serde(default = "default_allowed_source_ips")]
    pub allowed_source_ips: AllowedSourceIps,
}

impl From<RackInitializeRequestV1> for RackInitializeRequest {
    fn from(v1: RackInitializeRequestV1) -> Self {
        RackInitializeRequest {
            trust_quorum_peers: v1.trust_quorum_peers,
            bootstrap_discovery: v1.bootstrap_discovery,
            ntp_servers: v1.ntp_servers,
            dns_servers: v1.dns_servers,
            internal_services_ip_pool_ranges: v1
                .internal_services_ip_pool_ranges,
            external_dns_ips: v1.external_dns_ips,
            external_dns_zone_name: v1.external_dns_zone_name,
            external_certificates: v1.external_certificates,
            recovery_silo: v1.recovery_silo,
            rack_network_config: v1.rack_network_config.into(),
            allowed_source_ips: v1.allowed_source_ips,
        }
    }
}

// "Shadow" copy of `RackInitializeRequest` that does no validation on its
// fields.
#[derive(Clone, Deserialize)]
struct UnvalidatedRackInitializeRequest {
    trust_quorum_peers: Option<Vec<Baseboard>>,
    bootstrap_discovery: BootstrapAddressDiscovery,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    internal_services_ip_pool_ranges: Vec<address::IpRange>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo: RecoverySiloConfig,
    rack_network_config: RackNetworkConfig,
    #[serde(default = "default_allowed_source_ips")]
    allowed_source_ips: AllowedSourceIps,
}

/// Configuration for the "rack setup service".
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(try_from = "UnvalidatedRackInitializeRequest")]
pub struct RackInitializeRequest {
    /// The set of peer_ids required to initialize trust quorum
    ///
    /// The value is `None` if we are not using trust quorum
    pub trust_quorum_peers: Option<Vec<Baseboard>>,

    /// Describes how bootstrap addresses should be collected during RSS.
    pub bootstrap_discovery: BootstrapAddressDiscovery,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,

    /// Ranges of the service IP pool which may be used for internal services.
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Eventually,
    // we want to configure multiple pools.
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,

    /// Service IP addresses on which we run external DNS servers.
    ///
    /// Each address must be present in `internal_services_ip_pool_ranges`.
    pub external_dns_ips: Vec<IpAddr>,

    /// DNS name for the DNS zone delegated to the rack for external DNS
    pub external_dns_zone_name: String,

    /// initial TLS certificates for the external API
    pub external_certificates: Vec<Certificate>,

    /// Configuration of the Recovery Silo (the initial Silo)
    pub recovery_silo: RecoverySiloConfig,

    /// Initial rack network configuration
    pub rack_network_config: RackNetworkConfig,

    /// IPs or subnets allowed to make requests to user-facing services
    #[serde(default = "default_allowed_source_ips")]
    pub allowed_source_ips: AllowedSourceIps,
}

impl RackInitializeRequest {
    pub fn from_toml_with_fallback(
        data: &str,
    ) -> Result<RackInitializeRequest> {
        let v2_err = match toml::from_str::<RackInitializeRequest>(&data) {
            Ok(req) => return Ok(req),
            Err(e) => e,
        };
        if let Ok(v1) = toml::from_str::<RackInitializeRequestV1>(&data) {
            return Ok(v1.into());
        }

        Err(v2_err.into())
    }
}

/// This field was added after several racks were already deployed. RSS plans
/// for those racks should default to allowing any source IP, since that is
/// effectively what they did.
const fn default_allowed_source_ips() -> AllowedSourceIps {
    AllowedSourceIps::Any
}

// This custom debug implementation hides the private keys.
impl std::fmt::Debug for RackInitializeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // If you find a compiler error here, and you just added a field to this
        // struct, be sure to add it to the Debug impl below!
        let RackInitializeRequest {
            trust_quorum_peers: trust_qurorum_peers,
            bootstrap_discovery,
            ntp_servers,
            dns_servers,
            internal_services_ip_pool_ranges,
            external_dns_ips,
            external_dns_zone_name,
            external_certificates: _,
            recovery_silo,
            rack_network_config,
            allowed_source_ips,
        } = &self;

        f.debug_struct("RackInitializeRequest")
            .field("trust_quorum_peers", trust_qurorum_peers)
            .field("bootstrap_discovery", bootstrap_discovery)
            .field("ntp_servers", ntp_servers)
            .field("dns_servers", dns_servers)
            .field(
                "internal_services_ip_pool_ranges",
                internal_services_ip_pool_ranges,
            )
            .field("external_dns_ips", external_dns_ips)
            .field("external_dns_zone_name", external_dns_zone_name)
            .field("external_certificates", &"<redacted>")
            .field("recovery_silo", recovery_silo)
            .field("rack_network_config", rack_network_config)
            .field("allowed_source_ips", allowed_source_ips)
            .finish()
    }
}

fn validate_external_dns(
    dns_ips: &Vec<IpAddr>,
    internal_ranges: &Vec<address::IpRange>,
) -> Result<()> {
    if dns_ips.is_empty() {
        bail!("At least one external DNS IP is required");
    }

    // Every external DNS IP should also be present in one of the internal
    // services IP pool ranges. This check is O(N*M), but we expect both N
    // and M to be small (~5 DNS servers, and a small number of pools).
    for &dns_ip in dns_ips {
        if !internal_ranges.iter().any(|range| range.contains(dns_ip)) {
            bail!(
                "External DNS IP {dns_ip} is not contained in \
                     `internal_services_ip_pool_ranges`"
            );
        }
    }
    Ok(())
}

impl TryFrom<UnvalidatedRackInitializeRequestV1> for RackInitializeRequestV1 {
    type Error = anyhow::Error;

    fn try_from(value: UnvalidatedRackInitializeRequestV1) -> Result<Self> {
        validate_external_dns(
            &value.external_dns_ips,
            &value.internal_services_ip_pool_ranges,
        )?;

        Ok(RackInitializeRequestV1 {
            trust_quorum_peers: value.trust_quorum_peers,
            bootstrap_discovery: value.bootstrap_discovery,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            internal_services_ip_pool_ranges: value
                .internal_services_ip_pool_ranges,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            external_certificates: value.external_certificates,
            recovery_silo: value.recovery_silo,
            rack_network_config: value.rack_network_config,
            allowed_source_ips: value.allowed_source_ips,
        })
    }
}
impl TryFrom<UnvalidatedRackInitializeRequest> for RackInitializeRequest {
    type Error = anyhow::Error;

    fn try_from(value: UnvalidatedRackInitializeRequest) -> Result<Self> {
        validate_external_dns(
            &value.external_dns_ips,
            &value.internal_services_ip_pool_ranges,
        )?;

        Ok(RackInitializeRequest {
            trust_quorum_peers: value.trust_quorum_peers,
            bootstrap_discovery: value.bootstrap_discovery,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            internal_services_ip_pool_ranges: value
                .internal_services_ip_pool_ranges,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            external_certificates: value.external_certificates,
            recovery_silo: value.recovery_silo,
            rack_network_config: value.rack_network_config,
            allowed_source_ips: value.allowed_source_ips,
        })
    }
}

pub type Certificate = nexus_client::types::Certificate;
pub type RecoverySiloConfig = nexus_client::types::RecoverySiloConfig;

/// A representation of a Baseboard ID as used in the inventory subsystem
/// This type is essentially the same as a `Baseboard` except it doesn't have a
/// revision or HW type (Gimlet, PC, Unknown).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct BaseboardId {
    /// Oxide Part Number
    pub part_number: String,
    /// Serial number (unique for a given part number)
    pub serial_number: String,
}

/// A request to Add a given sled after rack initialization has occurred
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct AddSledRequest {
    pub sled_id: BaseboardId,
    pub start_request: StartSledAgentRequest,
}

// A wrapper around StartSledAgentRequestV0 that was used
// for the ledger format.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
struct PersistentSledAgentRequest {
    request: StartSledAgentRequestV0,
}

/// The version of `StartSledAgentRequest` we originally shipped with.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequestV0 {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// The external NTP servers to use
    pub ntp_servers: Vec<String>,

    /// The external DNS servers to use
    pub dns_servers: Vec<IpAddr>,

    /// Use trust quorum for key generation
    pub use_trust_quorum: bool,

    // Note: The order of these fields is load bearing, because we serialize
    // `SledAgentRequest`s as toml. `subnet` serializes as a TOML table, so it
    // must come after non-table fields.
    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

/// Configuration information for launching a Sled Agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequest {
    /// The current generation number of data as stored in CRDB.
    ///
    /// The initial generation is set during RSS time and then only mutated
    /// by Nexus. For now, we don't actually anticipate mutating this data,
    /// but we leave open the possiblity.
    pub generation: u64,

    // Which version of the data structure do we have. This is to help with
    // deserialization and conversion in future updates.
    pub schema_version: u32,

    // The actual configuration details
    pub body: StartSledAgentRequestBody,
}

/// This is the actual app level data of `StartSledAgentRequest`
///
/// We nest it below the "header" of `generation` and `schema_version` so that
/// we can perform partial deserialization of `EarlyNetworkConfig` to only read
/// the header and defer deserialization of the body once we know the schema
/// version. This is possible via the use of [`serde_json::value::RawValue`] in
/// future (post-v1) deserialization paths.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct StartSledAgentRequestBody {
    /// Uuid of the Sled Agent to be created.
    pub id: Uuid,

    /// Uuid of the rack to which this sled agent belongs.
    pub rack_id: Uuid,

    /// Use trust quorum for key generation
    pub use_trust_quorum: bool,

    /// Is this node an LRTQ learner node?
    ///
    /// We only put the node into learner mode if `use_trust_quorum` is also
    /// true.
    pub is_lrtq_learner: bool,

    /// Portion of the IP space to be managed by the Sled Agent.
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl StartSledAgentRequest {
    pub fn sled_address(&self) -> SocketAddrV6 {
        address::get_sled_address(self.body.subnet)
    }

    pub fn switch_zone_ip(&self) -> Ipv6Addr {
        address::get_switch_zone_address(self.body.subnet)
    }

    /// Compute the sha3_256 digest of `self.rack_id` to use as a `salt`
    /// for disk encryption. We don't want to include other values that are
    /// consistent across sleds as it would prevent us from moving drives
    /// between sleds.
    pub fn hash_rack_id(&self) -> [u8; 32] {
        // We know the unwrap succeeds as a Sha3_256 digest is 32 bytes
        Sha3_256::digest(self.body.rack_id.as_bytes())
            .as_slice()
            .try_into()
            .unwrap()
    }
}

impl From<StartSledAgentRequestV0> for StartSledAgentRequest {
    fn from(v0: StartSledAgentRequestV0) -> Self {
        StartSledAgentRequest {
            generation: 0,
            schema_version: 1,
            body: StartSledAgentRequestBody {
                id: v0.id,
                rack_id: v0.rack_id,
                use_trust_quorum: v0.use_trust_quorum,
                is_lrtq_learner: false,
                subnet: v0.subnet,
            },
        }
    }
}

#[async_trait]
impl Ledgerable for StartSledAgentRequest {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    fn generation_bump(&mut self) {
        // DO NOTHING!
        //
        // Generation bumps must only ever come from nexus and will be encoded
        // in the struct itself
    }

    // Attempt to deserialize the v1 or v0 version and return
    // the v1 version.
    fn deserialize(
        s: &str,
    ) -> Result<StartSledAgentRequest, serde_json::Error> {
        // Try to deserialize the latest version of the data structure (v1). If
        // that succeeds we are done.
        if let Ok(val) = serde_json::from_str::<StartSledAgentRequest>(s) {
            return Ok(val);
        }

        // We don't have the latest version. Try to deserialize v0 and then
        // convert it to the latest version.
        let v0 = serde_json::from_str::<PersistentSledAgentRequest>(s)?.request;
        Ok(v0.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Request<'a> {
    /// Send configuration information for launching a Sled Agent.
    StartSledAgentRequest(Cow<'a, StartSledAgentRequest>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestEnvelope<'a> {
    pub version: u32,
    pub request: Request<'a>,
}

pub(super) mod version {
    pub(crate) const V1: u32 = 1;
}

#[cfg(test)]
pub fn test_config() -> RackInitializeRequest {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("Cannot access manifest directory");
    let manifest = camino::Utf8PathBuf::from(manifest);
    let path = manifest.join("../smf/sled-agent/non-gimlet/config-rss.toml");
    let contents = std::fs::read_to_string(&path).unwrap();
    toml::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", &path, e))
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    use super::*;
    use camino::Utf8PathBuf;
    use oxnet::Ipv6Net;

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
    fn parse_rack_initialization_weak_hash() {
        let config = r#"
            bootstrap_discovery.type = "only_ours"
            ntp_servers = [ "ntp.eng.oxide.computer" ]
            dns_servers = [ "1.1.1.1", "9.9.9.9" ]
            external_dns_zone_name = "oxide.test"

            [[internal_services_ip_pool_ranges]]
            first = "192.168.1.20"
            last = "192.168.1.22"

            [recovery_silo]
            silo_name = "recovery"
            user_name = "recovery"
            user_password_hash = "$argon2i$v=19$m=16,t=2,p=1$NVR0a2QxVXNiQjlObFJXbA$iGFJWOlUqN20B8KR4Fsmrg"
        "#;

        let error = toml::from_str::<RackInitializeRequest>(config)
            .expect_err("unexpectedly parsed with bad password hash");
        println!("found error: {}", error);
        assert!(error.to_string().contains(
            "password hash: algorithm: expected argon2id, found argon2i"
        ));
    }

    #[test]
    fn json_serialization_round_trips() {
        let envelope = RequestEnvelope {
            version: 1,
            request: Request::StartSledAgentRequest(Cow::Owned(
                StartSledAgentRequest {
                    generation: 0,
                    schema_version: 1,
                    body: StartSledAgentRequestBody {
                        id: Uuid::new_v4(),
                        rack_id: Uuid::new_v4(),
                        use_trust_quorum: false,
                        is_lrtq_learner: false,
                        subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                    },
                },
            )),
        };

        let serialized = serde_json::to_vec(&envelope).unwrap();
        let deserialized: RequestEnvelope =
            serde_json::from_slice(serialized.as_slice()).unwrap();

        assert!(envelope == deserialized, "serialization round trip failed");
    }

    #[test]
    fn serialize_start_sled_agent_v0_deserialize_v1() {
        let v0 = PersistentSledAgentRequest {
            request: StartSledAgentRequestV0 {
                id: Uuid::new_v4(),
                rack_id: Uuid::new_v4(),
                ntp_servers: vec![String::from("test.pool.example.com")],
                dns_servers: vec!["1.1.1.1".parse().unwrap()],
                use_trust_quorum: false,
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
            },
        };
        let serialized = serde_json::to_string(&v0).unwrap();
        let expected = StartSledAgentRequest {
            generation: 0,
            schema_version: 1,
            body: StartSledAgentRequestBody {
                id: v0.request.id,
                rack_id: v0.request.rack_id,
                use_trust_quorum: v0.request.use_trust_quorum,
                is_lrtq_learner: false,
                subnet: v0.request.subnet,
            },
        };

        let actual: StartSledAgentRequest =
            Ledgerable::deserialize(&serialized).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn validate_external_dns_ips_must_be_in_internal_services_ip_pools() {
        // Conjure up a config; we'll tweak the internal services pools and
        // external DNS IPs, but no other fields matter.
        let mut config = UnvalidatedRackInitializeRequest {
            trust_quorum_peers: None,
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyOurs,
            ntp_servers: Vec::new(),
            dns_servers: Vec::new(),
            internal_services_ip_pool_ranges: Vec::new(),
            external_dns_ips: Vec::new(),
            external_dns_zone_name: "".to_string(),
            external_certificates: Vec::new(),
            recovery_silo: RecoverySiloConfig {
                silo_name: "recovery".parse().unwrap(),
                user_name: "recovery".parse().unwrap(),
                user_password_hash: "$argon2id$v=19$m=98304,t=13,p=1$RUlWc0ZxaHo0WFdrN0N6ZQ$S8p52j85GPvMhR/ek3GL0el/oProgTwWpHJZ8lsQQoY".parse().unwrap(),
            },
            rack_network_config: RackNetworkConfig {
                rack_subnet: Ipv6Net::host_net(Ipv6Addr::LOCALHOST),
                infra_ip_first: Ipv4Addr::LOCALHOST,
                infra_ip_last: Ipv4Addr::LOCALHOST,
                ports: Vec::new(),
                bgp: Vec::new(),
                bfd: Vec::new(),
            },
            allowed_source_ips: AllowedSourceIps::Any,
        };

        // Valid configs: all external DNS IPs are contained in the IP pool
        // ranges.
        for (ip_pool_ranges, dns_ips) in [
            (
                &[("fd00::1", "fd00::10")] as &[(&str, &str)],
                &["fd00::1", "fd00::5", "fd00::10"] as &[&str],
            ),
            (
                &[("192.168.1.10", "192.168.1.20")],
                &["192.168.1.10", "192.168.1.15", "192.168.1.20"],
            ),
            (
                &[("fd00::1", "fd00::10"), ("192.168.1.10", "192.168.1.20")],
                &[
                    "fd00::1",
                    "fd00::5",
                    "fd00::10",
                    "192.168.1.10",
                    "192.168.1.15",
                    "192.168.1.20",
                ],
            ),
        ] {
            config.internal_services_ip_pool_ranges = ip_pool_ranges
                .iter()
                .map(|(a, b)| {
                    address::IpRange::try_from((
                        a.parse::<IpAddr>().unwrap(),
                        b.parse::<IpAddr>().unwrap(),
                    ))
                    .unwrap()
                })
                .collect();
            config.external_dns_ips =
                dns_ips.iter().map(|ip| ip.parse().unwrap()).collect();

            match RackInitializeRequest::try_from(config.clone()) {
                Ok(_) => (),
                Err(err) => panic!(
                    "failure on {ip_pool_ranges:?} with DNS IPs {dns_ips:?}: \
                     {err}"
                ),
            }
        }

        // Invalid configs: either no DNS IPs, or one or more DNS IPs are not
        // contained in the ip pool ranges.
        for (ip_pool_ranges, dns_ips) in [
            (&[("fd00::1", "fd00::10")] as &[(&str, &str)], &[] as &[&str]),
            (&[("fd00::1", "fd00::10")], &["fd00::1", "fd00::5", "fd00::11"]),
            (
                &[("192.168.1.10", "192.168.1.20")],
                &["192.168.1.9", "192.168.1.15", "192.168.1.20"],
            ),
            (
                &[("fd00::1", "fd00::10"), ("192.168.1.10", "192.168.1.20")],
                &[
                    "fd00::1",
                    "fd00::5",
                    "fd00::10",
                    "192.168.1.10",
                    "192.168.1.15",
                    "192.168.1.20",
                    "192.168.1.21",
                ],
            ),
        ] {
            config.internal_services_ip_pool_ranges = ip_pool_ranges
                .iter()
                .map(|(a, b)| {
                    address::IpRange::try_from((
                        a.parse::<IpAddr>().unwrap(),
                        b.parse::<IpAddr>().unwrap(),
                    ))
                    .unwrap()
                })
                .collect();
            config.external_dns_ips =
                dns_ips.iter().map(|ip| ip.parse().unwrap()).collect();

            match RackInitializeRequest::try_from(config.clone()) {
                Ok(_) => panic!(
                    "unexpected success on {ip_pool_ranges:?} with \
                     DNS IPs {dns_ips:?}"
                ),
                Err(_) => (),
            }
        }
    }
}
