// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent

use anyhow::Context;
use async_trait::async_trait;
use omicron_common::api::internal::shared::NetworkInterface;
use std::convert::TryFrom;
use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::SocketAddr;
use uuid::Uuid;

progenitor::generate_api!(
    spec = "../../openapi/sled-agent.json",
    derives = [ schemars::JsonSchema, PartialEq ],
    inner_type = slog::Logger,
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
    patch = {
        BfdPeerConfig = { derives = [PartialEq, Eq, Hash, Serialize, Deserialize] },
        BgpConfig = { derives = [PartialEq, Eq, Hash, Serialize, Deserialize] },
        BgpPeerConfig = { derives = [PartialEq, Eq, Hash, Serialize, Deserialize] },
        PortConfigV1 = { derives = [PartialEq, Eq, Hash, Serialize, Deserialize] },
        RouteConfig = { derives = [PartialEq, Eq, Hash, Serialize, Deserialize] },
        IpNet = { derives = [PartialEq, Eq, Hash, Serialize, Deserialize] },
    },
    //TODO trade the manual transformations later in this file for the
    //     replace directives below?
    replace = {
        ByteCount = omicron_common::api::external::ByteCount,
        DiskIdentity = omicron_common::disk::DiskIdentity,
        Generation = omicron_common::api::external::Generation,
        MacAddr = omicron_common::api::external::MacAddr,
        Name = omicron_common::api::external::Name,
        SwitchLocation = omicron_common::api::external::SwitchLocation,
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
        Ipv6Network = ipnetwork::Ipv6Network,
        IpNetwork = ipnetwork::IpNetwork,
        PortFec = omicron_common::api::internal::shared::PortFec,
        PortSpeed = omicron_common::api::internal::shared::PortSpeed,
        SourceNatConfig = omicron_common::api::internal::shared::SourceNatConfig,
        Vni = omicron_common::api::external::Vni,
        NetworkInterface = omicron_common::api::internal::shared::NetworkInterface,
        TypedUuidForZpoolKind = omicron_uuid_kinds::ZpoolUuid,
        ZpoolKind = omicron_common::zpool_name::ZpoolKind,
        ZpoolName = omicron_common::zpool_name::ZpoolName,
    }
);

// We cannot easily configure progenitor to derive `Eq` on all the client-
// generated types because some have floats and other types that can't impl
// `Eq`.  We impl it explicitly for a few types on which we need it.
impl Eq for types::OmicronPhysicalDiskConfig {}
impl Eq for types::OmicronPhysicalDisksConfig {}
impl Eq for types::OmicronZonesConfig {}
impl Eq for types::OmicronZoneConfig {}
impl Eq for types::OmicronZoneType {}
impl Eq for types::OmicronZoneDataset {}

/// Like [`types::OmicronZoneType`], but without any associated data.
///
/// We have a few enums of this form floating around. This particular one is
/// meant to correspond exactly 1:1 with `OmicronZoneType`.
///
/// The [`fmt::Display`] impl for this type is a human-readable label, meant
/// for testing and reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ZoneKind {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    CockroachDb,
    Crucible,
    CruciblePantry,
    ExternalDns,
    InternalDns,
    InternalNtp,
    Nexus,
    Oximeter,
}

impl fmt::Display for ZoneKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZoneKind::BoundaryNtp => write!(f, "boundary_ntp"),
            ZoneKind::Clickhouse => write!(f, "clickhouse"),
            ZoneKind::ClickhouseKeeper => write!(f, "clickhouse_keeper"),
            ZoneKind::CockroachDb => write!(f, "cockroach_db"),
            ZoneKind::Crucible => write!(f, "crucible"),
            ZoneKind::CruciblePantry => write!(f, "crucible_pantry"),
            ZoneKind::ExternalDns => write!(f, "external_dns"),
            ZoneKind::InternalDns => write!(f, "internal_dns"),
            ZoneKind::InternalNtp => write!(f, "internal_ntp"),
            ZoneKind::Nexus => write!(f, "nexus"),
            ZoneKind::Oximeter => write!(f, "oximeter"),
        }
    }
}

impl types::OmicronZoneType {
    /// Returns the [`ZoneKind`] corresponding to this variant.
    pub fn kind(&self) -> ZoneKind {
        match self {
            types::OmicronZoneType::BoundaryNtp { .. } => ZoneKind::BoundaryNtp,
            types::OmicronZoneType::Clickhouse { .. } => ZoneKind::Clickhouse,
            types::OmicronZoneType::ClickhouseKeeper { .. } => {
                ZoneKind::ClickhouseKeeper
            }
            types::OmicronZoneType::CockroachDb { .. } => ZoneKind::CockroachDb,
            types::OmicronZoneType::Crucible { .. } => ZoneKind::Crucible,
            types::OmicronZoneType::CruciblePantry { .. } => {
                ZoneKind::CruciblePantry
            }
            types::OmicronZoneType::ExternalDns { .. } => ZoneKind::ExternalDns,
            types::OmicronZoneType::InternalDns { .. } => ZoneKind::InternalDns,
            types::OmicronZoneType::InternalNtp { .. } => ZoneKind::InternalNtp,
            types::OmicronZoneType::Nexus { .. } => ZoneKind::Nexus,
            types::OmicronZoneType::Oximeter { .. } => ZoneKind::Oximeter,
        }
    }

    /// Identifies whether this is an NTP zone
    pub fn is_ntp(&self) -> bool {
        match self {
            types::OmicronZoneType::BoundaryNtp { .. }
            | types::OmicronZoneType::InternalNtp { .. } => true,

            types::OmicronZoneType::Clickhouse { .. }
            | types::OmicronZoneType::ClickhouseKeeper { .. }
            | types::OmicronZoneType::CockroachDb { .. }
            | types::OmicronZoneType::Crucible { .. }
            | types::OmicronZoneType::CruciblePantry { .. }
            | types::OmicronZoneType::ExternalDns { .. }
            | types::OmicronZoneType::InternalDns { .. }
            | types::OmicronZoneType::Nexus { .. }
            | types::OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// Identifies whether this is a Nexus zone
    pub fn is_nexus(&self) -> bool {
        match self {
            types::OmicronZoneType::Nexus { .. } => true,

            types::OmicronZoneType::BoundaryNtp { .. }
            | types::OmicronZoneType::InternalNtp { .. }
            | types::OmicronZoneType::Clickhouse { .. }
            | types::OmicronZoneType::ClickhouseKeeper { .. }
            | types::OmicronZoneType::CockroachDb { .. }
            | types::OmicronZoneType::Crucible { .. }
            | types::OmicronZoneType::CruciblePantry { .. }
            | types::OmicronZoneType::ExternalDns { .. }
            | types::OmicronZoneType::InternalDns { .. }
            | types::OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// Identifies whether this a Crucible (not Crucible pantry) zone
    pub fn is_crucible(&self) -> bool {
        match self {
            types::OmicronZoneType::Crucible { .. } => true,

            types::OmicronZoneType::BoundaryNtp { .. }
            | types::OmicronZoneType::InternalNtp { .. }
            | types::OmicronZoneType::Clickhouse { .. }
            | types::OmicronZoneType::ClickhouseKeeper { .. }
            | types::OmicronZoneType::CockroachDb { .. }
            | types::OmicronZoneType::CruciblePantry { .. }
            | types::OmicronZoneType::ExternalDns { .. }
            | types::OmicronZoneType::InternalDns { .. }
            | types::OmicronZoneType::Nexus { .. }
            | types::OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// This zone's external IP
    pub fn external_ip(&self) -> anyhow::Result<Option<IpAddr>> {
        match self {
            types::OmicronZoneType::Nexus { external_ip, .. } => {
                Ok(Some(*external_ip))
            }

            types::OmicronZoneType::ExternalDns { dns_address, .. } => {
                let dns_address =
                    dns_address.parse::<SocketAddr>().with_context(|| {
                        format!(
                            "failed to parse ExternalDns address {dns_address}"
                        )
                    })?;
                Ok(Some(dns_address.ip()))
            }

            types::OmicronZoneType::BoundaryNtp { snat_cfg, .. } => {
                Ok(Some(snat_cfg.ip))
            }

            types::OmicronZoneType::InternalNtp { .. }
            | types::OmicronZoneType::Clickhouse { .. }
            | types::OmicronZoneType::ClickhouseKeeper { .. }
            | types::OmicronZoneType::CockroachDb { .. }
            | types::OmicronZoneType::Crucible { .. }
            | types::OmicronZoneType::CruciblePantry { .. }
            | types::OmicronZoneType::InternalDns { .. }
            | types::OmicronZoneType::Oximeter { .. } => Ok(None),
        }
    }

    /// The service vNIC providing external connectivity to this zone
    pub fn service_vnic(&self) -> Option<&NetworkInterface> {
        match self {
            types::OmicronZoneType::Nexus { nic, .. }
            | types::OmicronZoneType::ExternalDns { nic, .. }
            | types::OmicronZoneType::BoundaryNtp { nic, .. } => Some(nic),

            types::OmicronZoneType::InternalNtp { .. }
            | types::OmicronZoneType::Clickhouse { .. }
            | types::OmicronZoneType::ClickhouseKeeper { .. }
            | types::OmicronZoneType::CockroachDb { .. }
            | types::OmicronZoneType::Crucible { .. }
            | types::OmicronZoneType::CruciblePantry { .. }
            | types::OmicronZoneType::InternalDns { .. }
            | types::OmicronZoneType::Oximeter { .. } => None,
        }
    }
}

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}

impl From<omicron_common::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(
        s: omicron_common::api::internal::nexus::InstanceRuntimeState,
    ) -> Self {
        Self {
            propolis_id: s.propolis_id,
            dst_propolis_id: s.dst_propolis_id,
            migration_id: s.migration_id,
            gen: s.gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<omicron_common::api::external::InstanceState>
    for types::InstanceState
{
    fn from(s: omicron_common::api::external::InstanceState) -> Self {
        use omicron_common::api::external::InstanceState::*;
        match s {
            Creating => Self::Creating,
            Starting => Self::Starting,
            Running => Self::Running,
            Stopping => Self::Stopping,
            Stopped => Self::Stopped,
            Rebooting => Self::Rebooting,
            Migrating => Self::Migrating,
            Repairing => Self::Repairing,
            Failed => Self::Failed,
            Destroyed => Self::Destroyed,
        }
    }
}

impl From<omicron_common::api::external::InstanceCpuCount>
    for types::InstanceCpuCount
{
    fn from(s: omicron_common::api::external::InstanceCpuCount) -> Self {
        Self(s.0)
    }
}

impl From<types::InstanceRuntimeState>
    for omicron_common::api::internal::nexus::InstanceRuntimeState
{
    fn from(s: types::InstanceRuntimeState) -> Self {
        Self {
            propolis_id: s.propolis_id,
            dst_propolis_id: s.dst_propolis_id,
            migration_id: s.migration_id,
            gen: s.gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<types::VmmRuntimeState>
    for omicron_common::api::internal::nexus::VmmRuntimeState
{
    fn from(s: types::VmmRuntimeState) -> Self {
        Self { state: s.state.into(), gen: s.gen, time_updated: s.time_updated }
    }
}

impl From<types::SledInstanceState>
    for omicron_common::api::internal::nexus::SledInstanceState
{
    fn from(s: types::SledInstanceState) -> Self {
        Self {
            instance_state: s.instance_state.into(),
            propolis_id: s.propolis_id,
            vmm_state: s.vmm_state.into(),
        }
    }
}

impl From<types::InstanceState>
    for omicron_common::api::external::InstanceState
{
    fn from(s: types::InstanceState) -> Self {
        use types::InstanceState::*;
        match s {
            Creating => Self::Creating,
            Starting => Self::Starting,
            Running => Self::Running,
            Stopping => Self::Stopping,
            Stopped => Self::Stopped,
            Rebooting => Self::Rebooting,
            Migrating => Self::Migrating,
            Repairing => Self::Repairing,
            Failed => Self::Failed,
            Destroyed => Self::Destroyed,
        }
    }
}

impl From<types::InstanceCpuCount>
    for omicron_common::api::external::InstanceCpuCount
{
    fn from(s: types::InstanceCpuCount) -> Self {
        Self(s.0)
    }
}

impl From<omicron_common::api::internal::nexus::DiskRuntimeState>
    for types::DiskRuntimeState
{
    fn from(s: omicron_common::api::internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            gen: s.gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<omicron_common::api::external::DiskState> for types::DiskState {
    fn from(s: omicron_common::api::external::DiskState) -> Self {
        use omicron_common::api::external::DiskState::*;
        match s {
            Creating => Self::Creating,
            Detached => Self::Detached,
            ImportReady => Self::ImportReady,
            ImportingFromUrl => Self::ImportingFromUrl,
            ImportingFromBulkWrites => Self::ImportingFromBulkWrites,
            Finalizing => Self::Finalizing,
            Maintenance => Self::Maintenance,
            Attaching(u) => Self::Attaching(u),
            Attached(u) => Self::Attached(u),
            Detaching(u) => Self::Detaching(u),
            Destroyed => Self::Destroyed,
            Faulted => Self::Faulted,
        }
    }
}

impl From<types::DiskRuntimeState>
    for omicron_common::api::internal::nexus::DiskRuntimeState
{
    fn from(s: types::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            gen: s.gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<types::DiskState> for omicron_common::api::external::DiskState {
    fn from(s: types::DiskState) -> Self {
        use types::DiskState::*;
        match s {
            Creating => Self::Creating,
            Detached => Self::Detached,
            ImportReady => Self::ImportReady,
            ImportingFromUrl => Self::ImportingFromUrl,
            ImportingFromBulkWrites => Self::ImportingFromBulkWrites,
            Finalizing => Self::Finalizing,
            Maintenance => Self::Maintenance,
            Attaching(u) => Self::Attaching(u),
            Attached(u) => Self::Attached(u),
            Detaching(u) => Self::Detaching(u),
            Destroyed => Self::Destroyed,
            Faulted => Self::Faulted,
        }
    }
}

impl From<omicron_common::api::external::Ipv4Net> for types::Ipv4Net {
    fn from(n: omicron_common::api::external::Ipv4Net) -> Self {
        Self::try_from(n.to_string()).unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<omicron_common::api::external::Ipv6Net> for types::Ipv6Net {
    fn from(n: omicron_common::api::external::Ipv6Net) -> Self {
        Self::try_from(n.to_string()).unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<omicron_common::api::external::IpNet> for types::IpNet {
    fn from(s: omicron_common::api::external::IpNet) -> Self {
        use omicron_common::api::external::IpNet;
        match s {
            IpNet::V4(v4) => Self::V4(v4.into()),
            IpNet::V6(v6) => Self::V6(v6.into()),
        }
    }
}

impl From<ipnetwork::Ipv4Network> for types::Ipv4Net {
    fn from(n: ipnetwork::Ipv4Network) -> Self {
        Self::try_from(n.to_string()).unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<types::Ipv4Net> for ipnetwork::Ipv4Network {
    fn from(n: types::Ipv4Net) -> Self {
        n.parse().unwrap()
    }
}

impl From<ipnetwork::Ipv4Network> for types::Ipv4Network {
    fn from(n: ipnetwork::Ipv4Network) -> Self {
        Self::try_from(n.to_string()).unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<ipnetwork::Ipv6Network> for types::Ipv6Net {
    fn from(n: ipnetwork::Ipv6Network) -> Self {
        Self::try_from(n.to_string()).unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<types::Ipv6Net> for ipnetwork::Ipv6Network {
    fn from(n: types::Ipv6Net) -> Self {
        n.parse().unwrap()
    }
}

impl From<ipnetwork::IpNetwork> for types::IpNet {
    fn from(n: ipnetwork::IpNetwork) -> Self {
        use ipnetwork::IpNetwork;
        match n {
            IpNetwork::V4(v4) => Self::V4(v4.into()),
            IpNetwork::V6(v6) => Self::V6(v6.into()),
        }
    }
}

impl From<types::IpNet> for ipnetwork::IpNetwork {
    fn from(n: types::IpNet) -> Self {
        match n {
            types::IpNet::V4(v4) => ipnetwork::IpNetwork::V4(v4.into()),
            types::IpNet::V6(v6) => ipnetwork::IpNetwork::V6(v6.into()),
        }
    }
}

impl From<std::net::Ipv4Addr> for types::Ipv4Net {
    fn from(n: std::net::Ipv4Addr) -> Self {
        Self::try_from(format!("{n}/32"))
            .unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<std::net::Ipv6Addr> for types::Ipv6Net {
    fn from(n: std::net::Ipv6Addr) -> Self {
        Self::try_from(format!("{n}/128"))
            .unwrap_or_else(|e| panic!("{}: {}", n, e))
    }
}

impl From<std::net::IpAddr> for types::IpNet {
    fn from(s: std::net::IpAddr) -> Self {
        match s {
            IpAddr::V4(v4) => Self::V4(v4.into()),
            IpAddr::V6(v6) => Self::V6(v6.into()),
        }
    }
}

impl From<omicron_common::api::external::L4PortRange> for types::L4PortRange {
    fn from(s: omicron_common::api::external::L4PortRange) -> Self {
        Self::try_from(s.to_string()).unwrap_or_else(|e| panic!("{}: {}", s, e))
    }
}

impl From<omicron_common::api::internal::nexus::UpdateArtifactId>
    for types::UpdateArtifactId
{
    fn from(s: omicron_common::api::internal::nexus::UpdateArtifactId) -> Self {
        types::UpdateArtifactId {
            name: s.name,
            version: s.version.into(),
            kind: s.kind.into(),
        }
    }
}

impl From<omicron_common::api::external::SemverVersion>
    for types::SemverVersion
{
    fn from(s: omicron_common::api::external::SemverVersion) -> Self {
        s.to_string().parse().expect(
            "semver should generate output that matches validation regex",
        )
    }
}

impl From<omicron_common::api::internal::nexus::KnownArtifactKind>
    for types::KnownArtifactKind
{
    fn from(
        s: omicron_common::api::internal::nexus::KnownArtifactKind,
    ) -> Self {
        use omicron_common::api::internal::nexus::KnownArtifactKind;

        match s {
            KnownArtifactKind::GimletSp => types::KnownArtifactKind::GimletSp,
            KnownArtifactKind::GimletRot => types::KnownArtifactKind::GimletRot,
            KnownArtifactKind::Host => types::KnownArtifactKind::Host,
            KnownArtifactKind::Trampoline => {
                types::KnownArtifactKind::Trampoline
            }
            KnownArtifactKind::ControlPlane => {
                types::KnownArtifactKind::ControlPlane
            }
            KnownArtifactKind::PscSp => types::KnownArtifactKind::PscSp,
            KnownArtifactKind::PscRot => types::KnownArtifactKind::PscRot,
            KnownArtifactKind::SwitchSp => types::KnownArtifactKind::SwitchSp,
            KnownArtifactKind::SwitchRot => types::KnownArtifactKind::SwitchRot,
        }
    }
}

impl From<omicron_common::api::internal::nexus::HostIdentifier>
    for types::HostIdentifier
{
    fn from(s: omicron_common::api::internal::nexus::HostIdentifier) -> Self {
        use omicron_common::api::internal::nexus::HostIdentifier::*;
        match s {
            Ip(net) => Self::Ip(net.into()),
            Vpc(vni) => Self::Vpc(vni),
        }
    }
}

impl From<omicron_common::api::external::VpcFirewallRuleAction>
    for types::VpcFirewallRuleAction
{
    fn from(s: omicron_common::api::external::VpcFirewallRuleAction) -> Self {
        use omicron_common::api::external::VpcFirewallRuleAction::*;
        match s {
            Allow => Self::Allow,
            Deny => Self::Deny,
        }
    }
}

impl From<omicron_common::api::external::VpcFirewallRuleDirection>
    for types::VpcFirewallRuleDirection
{
    fn from(
        s: omicron_common::api::external::VpcFirewallRuleDirection,
    ) -> Self {
        use omicron_common::api::external::VpcFirewallRuleDirection::*;
        match s {
            Inbound => Self::Inbound,
            Outbound => Self::Outbound,
        }
    }
}

impl From<omicron_common::api::external::VpcFirewallRuleStatus>
    for types::VpcFirewallRuleStatus
{
    fn from(s: omicron_common::api::external::VpcFirewallRuleStatus) -> Self {
        use omicron_common::api::external::VpcFirewallRuleStatus::*;
        match s {
            Enabled => Self::Enabled,
            Disabled => Self::Disabled,
        }
    }
}

impl From<omicron_common::api::external::VpcFirewallRuleProtocol>
    for types::VpcFirewallRuleProtocol
{
    fn from(s: omicron_common::api::external::VpcFirewallRuleProtocol) -> Self {
        use omicron_common::api::external::VpcFirewallRuleProtocol::*;
        match s {
            Tcp => Self::Tcp,
            Udp => Self::Udp,
            Icmp => Self::Icmp,
        }
    }
}

impl From<omicron_common::api::internal::shared::NetworkInterfaceKind>
    for types::NetworkInterfaceKind
{
    fn from(
        s: omicron_common::api::internal::shared::NetworkInterfaceKind,
    ) -> Self {
        use omicron_common::api::internal::shared::NetworkInterfaceKind::*;
        match s {
            Instance { id } => Self::Instance(id),
            Service { id } => Self::Service(id),
            Probe { id } => Self::Probe(id),
        }
    }
}

/// Exposes additional [`Client`] interfaces for use by the test suite. These
/// are bonus endpoints, not generated in the real client.
#[async_trait]
pub trait TestInterfaces {
    async fn instance_finish_transition(&self, id: Uuid);
    async fn disk_finish_transition(&self, id: Uuid);
}

#[async_trait]
impl TestInterfaces for Client {
    async fn instance_finish_transition(&self, id: Uuid) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{}/instances/{}/poke", baseurl, id);
        client
            .post(url)
            .send()
            .await
            .expect("instance_finish_transition() failed unexpectedly");
    }

    async fn disk_finish_transition(&self, id: Uuid) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{}/disks/{}/poke", baseurl, id);
        client
            .post(url)
            .send()
            .await
            .expect("disk_finish_transition() failed unexpectedly");
    }
}
