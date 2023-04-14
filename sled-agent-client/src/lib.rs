// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent

use async_trait::async_trait;
use omicron_common::generate_logging_api;
use std::convert::TryFrom;
use uuid::Uuid;

generate_logging_api!("../openapi/sled-agent.json");

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
            run_state: s.run_state.into(),
            sled_id: s.sled_id,
            propolis_id: s.propolis_id,
            dst_propolis_id: s.dst_propolis_id,
            propolis_addr: s.propolis_addr.map(|addr| addr.to_string()),
            migration_id: s.migration_id,
            propolis_gen: s.propolis_gen.into(),
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
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

impl From<omicron_common::api::external::ByteCount> for types::ByteCount {
    fn from(s: omicron_common::api::external::ByteCount) -> Self {
        Self(s.to_bytes())
    }
}

impl From<omicron_common::api::external::Generation> for types::Generation {
    fn from(s: omicron_common::api::external::Generation) -> Self {
        Self(i64::from(&s) as u64)
    }
}

impl From<types::InstanceRuntimeState>
    for omicron_common::api::internal::nexus::InstanceRuntimeState
{
    fn from(s: types::InstanceRuntimeState) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_id: s.sled_id,
            propolis_id: s.propolis_id,
            dst_propolis_id: s.dst_propolis_id,
            propolis_addr: s.propolis_addr.map(|addr| addr.parse().unwrap()),
            migration_id: s.migration_id,
            propolis_gen: s.propolis_gen.into(),
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
            time_updated: s.time_updated,
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

impl From<types::ByteCount> for omicron_common::api::external::ByteCount {
    fn from(s: types::ByteCount) -> Self {
        Self::try_from(s.0).unwrap_or_else(|e| panic!("{}: {}", s.0, e))
    }
}

impl From<types::Generation> for omicron_common::api::external::Generation {
    fn from(s: types::Generation) -> Self {
        Self::try_from(s.0 as i64).unwrap()
    }
}

impl From<omicron_common::api::internal::nexus::DiskRuntimeState>
    for types::DiskRuntimeState
{
    fn from(s: omicron_common::api::internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            gen: s.gen.into(),
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
            gen: s.gen.into(),
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

impl From<&omicron_common::api::external::Name> for types::Name {
    fn from(s: &omicron_common::api::external::Name) -> Self {
        Self::try_from(<&str>::from(s))
            .unwrap_or_else(|e| panic!("{}: {}", s, e))
    }
}

impl From<omicron_common::api::external::Vni> for types::Vni {
    fn from(v: omicron_common::api::external::Vni) -> Self {
        Self(u32::from(v))
    }
}

impl From<types::Vni> for omicron_common::api::external::Vni {
    fn from(s: types::Vni) -> Self {
        Self::try_from(s.0).unwrap()
    }
}

impl From<omicron_common::api::external::MacAddr> for types::MacAddr {
    fn from(s: omicron_common::api::external::MacAddr) -> Self {
        Self::try_from(s.0.to_string())
            .unwrap_or_else(|e| panic!("{}: {}", s.0, e))
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

impl From<ipnetwork::Ipv6Network> for types::Ipv6Net {
    fn from(n: ipnetwork::Ipv6Network) -> Self {
        Self::try_from(n.to_string()).unwrap_or_else(|e| panic!("{}: {}", n, e))
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
        use std::net::IpAddr;
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
            Vpc(vni) => Self::Vpc(vni.into()),
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
