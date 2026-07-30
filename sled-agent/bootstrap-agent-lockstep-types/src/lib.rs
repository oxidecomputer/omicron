// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used in the `bootstrap-agent-lockstep` API.
//!
//! These types are not versioned because they are only used by lockstep APIs.
//! If any of these types start to be used in versioned APIs, they must be moved
//! out of this crate and into the relevant `*-types-versions` / `*-types` crate
//! pairs.

use anyhow::Context as _;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::address::AZ_PREFIX_LENGTH;
use omicron_common::address::IpRange;
use omicron_common::address::IpVersion;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX_LENGTH;
use omicron_common::address::SLED_PREFIX_LENGTH;
use omicron_common::address::get_64_subnet;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use omicron_common::api::external::UserId;
use omicron_common::api::internal::nexus::Certificate;
use omicron_uuid_kinds::MultirackJoinUuid;
use omicron_uuid_kinds::RackInitUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use strum::EnumCount;
use strum::EnumIter;
use strum::IntoEnumIterator;

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
    pub trust_quorum_peers: Option<Vec<BaseboardId>>,

    /// Describes how bootstrap addresses should be collected during RSS.
    pub bootstrap_discovery: BootstrapAddressDiscovery,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,

    /// Configuration for service IP Pools.
    pub service_ip_pools: IdOrdMap<ServiceIpPoolConfig>,

    /// Service IP addresses on which we run external DNS servers.
    ///
    /// Each address must be present in the ranges of the pools in
    /// `service_ip_pools`.
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
    pub allowed_source_ips: AllowedSourceIps,

    /// When true, end users may opt in to jumbo frames on the primary interface
    /// of an instance, and control plane services with external-facing OPTE
    /// NICs are brought up with an 8500 byte MTU. Operators can toggle this at
    /// runtime via the fleet networking settings API.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
}

// This custom debug implementation hides the private keys.
impl std::fmt::Debug for RackInitializeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // If you find a compiler error here, and you just added a field to this
        // struct, be sure to add it to the Debug impl below!
        let RackInitializeRequest {
            trust_quorum_peers,
            bootstrap_discovery,
            ntp_servers,
            dns_servers,
            service_ip_pools,
            external_dns_ips,
            external_dns_zone_name,
            external_certificates: _,
            recovery_silo,
            rack_network_config,
            allowed_source_ips,
            external_jumbo_frames_opt_in_enabled,
        } = &self;

        f.debug_struct("RackInitializeRequest")
            .field("trust_quorum_peers", trust_quorum_peers)
            .field("bootstrap_discovery", bootstrap_discovery)
            .field("ntp_servers", ntp_servers)
            .field("dns_servers", dns_servers)
            .field("service_ip_pools", service_ip_pools)
            .field("external_dns_ips", external_dns_ips)
            .field("external_dns_zone_name", external_dns_zone_name)
            .field("external_certificates", &"<redacted>")
            .field("recovery_silo", recovery_silo)
            .field("rack_network_config", rack_network_config)
            .field("allowed_source_ips", allowed_source_ips)
            .field(
                "external_jumbo_frames_opt_in_enabled",
                external_jumbo_frames_opt_in_enabled,
            )
            .finish()
    }
}

impl RackInitializeRequest {
    pub fn az_subnet(&self) -> Ipv6Subnet<AZ_PREFIX_LENGTH> {
        Ipv6Subnet::<AZ_PREFIX_LENGTH>::new(
            self.rack_network_config.rack_subnet.addr(),
        )
    }

    /// Returns the subnet for our rack.
    pub fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX_LENGTH> {
        Ipv6Subnet::<RACK_PREFIX_LENGTH>::new(
            self.rack_network_config.rack_subnet.addr(),
        )
    }

    /// Returns the subnet for the `index`-th sled in the rack.
    pub fn sled_subnet(&self, index: u8) -> Ipv6Subnet<SLED_PREFIX_LENGTH> {
        get_64_subnet(self.rack_subnet(), index)
    }
}

// "Shadow" copy of `RackInitializeRequest` that does no validation on its
// fields.
#[derive(Clone, Deserialize)]
struct UnvalidatedRackInitializeRequest {
    trust_quorum_peers: Option<Vec<BaseboardId>>,
    bootstrap_discovery: BootstrapAddressDiscovery,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    service_ip_pools: Vec<ServiceIpPoolConfig>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo: RecoverySiloConfig,
    rack_network_config: RackNetworkConfig,
    // TODO-cleanup Can we remove the `#[serde(default)]? All callers should be
    // passing this field.
    #[serde(default = "default_allowed_source_ips")]
    allowed_source_ips: AllowedSourceIps,
    #[serde(default)]
    external_jumbo_frames_opt_in_enabled: bool,
}

impl TryFrom<UnvalidatedRackInitializeRequest> for RackInitializeRequest {
    type Error = anyhow::Error;

    fn try_from(
        value: UnvalidatedRackInitializeRequest,
    ) -> anyhow::Result<Self> {
        let service_ip_pools =
            IdOrdMap::from_iter_unique(value.service_ip_pools)
                .context("duplicate names in service IP Pool configuration")?;
        if service_ip_pools.is_empty() {
            anyhow::bail!("at least one service IP pool is required");
        }
        validate_external_dns(&value.external_dns_ips, &service_ip_pools)?;

        Ok(Self {
            trust_quorum_peers: value.trust_quorum_peers,
            bootstrap_discovery: value.bootstrap_discovery,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            service_ip_pools,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            external_certificates: value.external_certificates,
            recovery_silo: value.recovery_silo,
            rack_network_config: value.rack_network_config,
            allowed_source_ips: value.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: value
                .external_jumbo_frames_opt_in_enabled,
        })
    }
}

/// This field was added after several racks were already deployed. RSS plans
/// for those racks should default to allowing any source IP, since that is
/// effectively what they did.
const fn default_allowed_source_ips() -> AllowedSourceIps {
    AllowedSourceIps::Any
}

fn validate_external_dns(
    dns_ips: &Vec<IpAddr>,
    service_ip_pools: &IdOrdMap<ServiceIpPoolConfig>,
) -> anyhow::Result<()> {
    if dns_ips.is_empty() {
        anyhow::bail!("At least one external DNS IP is required");
    }
    let internal_ranges =
        service_ip_pools.iter().flat_map(|p| p.ranges()).collect::<Vec<_>>();

    // Every external DNS IP should also be present in one of the internal
    // services IP pool ranges. This check is O(N*M), but we expect both N
    // and M to be small (~5 DNS servers, and a small number of pools).
    for &dns_ip in dns_ips {
        if !internal_ranges.iter().any(|range| range.contains(dns_ip)) {
            anyhow::bail!(
                "External DNS IP {dns_ip} is not contained in \
                any service IP pool range"
            );
        }
    }

    Ok(())
}

#[derive(Clone, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct MultirackJoinRequest {
    /// The set of peers required to initialize trust quorum
    ///
    /// Unlike RSS, this is not optional for multirack setups. Bootstrap
    /// addresses are discovered by the bootstrap agent and mapped to the
    /// `BaseboardId`s.
    pub trust_quorum_peers: BTreeSet<BaseboardId>,

    /// The rack network configuration for this joining rack
    pub rack_network_config: RackNetworkConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: BTreeSet<Ipv6Addr> },
}

/// Configuration for the recovery silo created during rack setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RecoverySiloConfig {
    pub silo_name: Name,
    pub user_name: UserId,
    pub user_password_hash: omicron_passwords::NewPasswordHash,
}

/// Current status of any rack-level operation being performed by this bootstrap
/// agent.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RackOperationStatus {
    Uninitialized,
    Initializing {
        id: RackInitUuid,
        step: RssStep,
    },
    /// `id` will be none if the rack was already initialized on startup.
    Initialized {
        id: Option<RackInitUuid>,
    },
    InitializationFailed {
        id: RackInitUuid,
        message: String,
    },
    InitializationPanicked {
        id: RackInitUuid,
    },
    MultirackJoinInProgress {
        // Status is queried via a different API
        id: MultirackJoinUuid,
    },
    MultirackJoinCompleted {
        id: Option<MultirackJoinUuid>,
    },
    MultirackJoinFailed {
        id: MultirackJoinUuid,
        message: String,
    },
    MultirackJoinPanicked {
        id: MultirackJoinUuid,
    },
}

/// Steps we go through during initial rack setup.
/// Keep this list in order that they happen.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    EnumCount,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RssStep {
    Requested,
    Starting,
    LoadExistingPlan,
    CreateSledPlan,
    InitTrustQuorum,
    InitialNetworkConfigUpdate,
    SledInit,
    FinalNetworkConfigUpdate,
    InitDns,
    ConfigureDns,
    InitNtp,
    WaitForTimeSync,
    WaitForDatabase,
    ClusterInit,
    ZonesInit,
    NexusHandoff,
}

impl RssStep {
    pub fn max_step(&self) -> usize {
        RssStep::COUNT
    }

    pub fn index(&self) -> usize {
        for (index, variant) in RssStep::iter().enumerate() {
            if *self == variant {
                return index;
            }
        }
        return 0;
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::Requested => "Requested",
            Self::Starting => "Starting",
            Self::LoadExistingPlan => "Loading existing plan",
            Self::CreateSledPlan => "Creating sled plan",
            Self::InitTrustQuorum => "Initializing trust quorum",
            Self::InitialNetworkConfigUpdate => "Initial network config update",
            Self::SledInit => "Initializing sleds",
            Self::FinalNetworkConfigUpdate => "Final network config update",
            Self::InitDns => "Initializing DNS",
            Self::ConfigureDns => "Configuring DNS",
            Self::InitNtp => "Initializing NTP",
            Self::WaitForTimeSync => "Waiting for time sync",
            Self::WaitForDatabase => "Waiting for database",
            Self::ClusterInit => "Initializing cluster",
            Self::ZonesInit => "Initializing zones",
            Self::NexusHandoff => "Handing off to Nexus",
        }
    }
}

/// Wrapper for optional contents of the replicated network config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicatedNetworkConfig {
    pub contents: Option<ReplicatedNetworkConfigContents>,
}

/// Contents of the replicated network config.
///
/// Analogous to the bootstore `NetworkConfig` type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicatedNetworkConfigContents {
    pub generation: u64,
    /// The `NetworkConfig` arbitrary blob, encoded using the standard base64
    /// alphabet.
    ///
    /// Currently, the contents are expected to be JSON, but
    /// serialization/deserialization of the contents is performed outside the
    /// replication engine, which just deals with a binary blob.
    pub base64_blob: String,
}

/// Full details of a system-service IP pool, provided at rack setup (RSS).
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(try_from = "UnvalidatedServiceIpPoolConfig")]
pub struct ServiceIpPoolConfig {
    /// Name of the IP Pool
    pub name: Name,
    /// Description of the IP Pool
    pub description: String,
    /// List of IP address ranges in the pool.
    ///
    /// There is guaranteed to be at least one range, and all ranges are of the
    /// same IP version.
    // NOTE: Private to ensure the above invariants. `new()` checks them, and we
    // deserialize through `UnvalidatedServiceIpPoolConfig` to check them.
    ranges: Vec<IpRange>,
}

impl ServiceIpPoolConfig {
    /// Construct a new service IP pool configuration.
    ///
    /// Errors if `ranges` is empty, or if the ranges are a mix of IPv4 and
    /// IPv6 addresses.
    pub fn new(
        name: Name,
        description: String,
        ranges: Vec<IpRange>,
    ) -> Result<Self, ServiceIpPoolError> {
        let mut versions = ranges.iter().map(|r| r.version());
        let Some(first) = versions.next() else {
            return Err(ServiceIpPoolError::EmptyRanges);
        };
        if versions.any(|v| v != first) {
            return Err(ServiceIpPoolError::MixedIpVersions);
        }
        Ok(Self { name, description, ranges })
    }

    /// The ranges belonging to this pool.
    ///
    /// Guaranteed to be non-empty and all of the same IP version.
    pub fn ranges(&self) -> &[IpRange] {
        &self.ranges
    }

    /// The IP version of this pool, derived from its ranges.
    pub fn ip_version(&self) -> IpVersion {
        // Safety: the constructor guarantees at least one range, and that all
        // ranges share an IP version.
        self.ranges[0].version()
    }
}

/// Errors constructing a `ServiceIpPoolConfig`.
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum ServiceIpPoolError {
    #[error("must provide at least one IP range")]
    EmptyRanges,
    #[error("ranges have mixed IP versions")]
    MixedIpVersions,
}

impl From<ServiceIpPoolError> for Error {
    fn from(value: ServiceIpPoolError) -> Self {
        Error::internal_error(format!(
            "error constructing service IP pool config: {value}"
        ))
    }
}

impl iddqd::IdOrdItem for ServiceIpPoolConfig {
    type Key<'a> = &'a Name;

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    id_upcast!();
}

#[derive(Deserialize)]
struct UnvalidatedServiceIpPoolConfig {
    name: Name,
    description: String,
    ranges: Vec<IpRange>,
}

impl TryFrom<UnvalidatedServiceIpPoolConfig> for ServiceIpPoolConfig {
    type Error = ServiceIpPoolError;

    fn try_from(
        value: UnvalidatedServiceIpPoolConfig,
    ) -> Result<Self, Self::Error> {
        let UnvalidatedServiceIpPoolConfig { name, description, ranges } =
            value;
        ServiceIpPoolConfig::new(name, description, ranges)
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct BootstrapIpOfBaseboardId {
    pub id: BaseboardId,
    pub ip: Ipv6Addr,
}

impl IdOrdItem for BootstrapIpOfBaseboardId {
    type Key<'a> = &'a BaseboardId;

    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    id_upcast!();
}

/// All `BaseboardId`s that can be found by sprockets connections on the
/// bootstrap network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BaseboardIds {
    pub data: IdOrdMap<BootstrapIpOfBaseboardId>,
}
