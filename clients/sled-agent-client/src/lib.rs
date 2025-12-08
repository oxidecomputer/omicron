// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent

use async_trait::async_trait;
use omicron_uuid_kinds::PropolisUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use uuid::Uuid;

pub use propolis_client::{CrucibleOpts, VolumeConstructionRequest};

progenitor::generate_api!(
    spec = "../../openapi/sled-agent/sled-agent-latest.json",
    interface = Positional,
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
    derives = [schemars::JsonSchema, PartialEq],
    patch = {
        BfdPeerConfig = { derives = [Eq, Hash] },
        BgpConfig = { derives = [Eq, Hash] },
        BgpPeerConfig = { derives = [Eq, Hash] },
        LldpPortConfig = { derives = [Eq, Hash, PartialOrd, Ord] },
        TxEqConfig = { derives = [Eq, Hash] },
        OmicronPhysicalDiskConfig = { derives = [Eq, Hash, PartialOrd, Ord] },
        PortConfigV2 = { derives = [Eq, Hash] },
        RouteConfig = { derives = [Eq, Hash] },
        UplinkAddressConfig = { derives = [Eq, Hash] },
        VirtualNetworkInterfaceHost = { derives = [Eq, Hash] },
    },
    crates = {
        "omicron-uuid-kinds" = "*",
        "oxnet" = "0.1.0",
    },
    replace = {
        Baseboard = sled_agent_types_migrations::latest::inventory::Baseboard,
        ByteCount = omicron_common::api::external::ByteCount,
        DatasetsConfig = omicron_common::disk::DatasetsConfig,
        DatasetManagementStatus = omicron_common::disk::DatasetManagementStatus,
        DatasetKind = omicron_common::api::internal::shared::DatasetKind,
        DiskIdentity = omicron_common::disk::DiskIdentity,
        DiskManagementStatus = omicron_common::disk::DiskManagementStatus,
        DiskManagementError = omicron_common::disk::DiskManagementError,
        DiskVariant = omicron_common::disk::DiskVariant,
        ExternalIpGatewayMap = omicron_common::api::internal::shared::ExternalIpGatewayMap,
        Generation = omicron_common::api::external::Generation,
        Hostname = omicron_common::api::external::Hostname,
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
        Inventory = sled_agent_types_migrations::latest::inventory::Inventory,
        InventoryDisk = sled_agent_types_migrations::latest::inventory::InventoryDisk,
        InventoryZpool = sled_agent_types_migrations::latest::inventory::InventoryZpool,
        MacAddr = omicron_common::api::external::MacAddr,
        MupdateOverrideBootInventory = sled_agent_types_migrations::latest::inventory::MupdateOverrideBootInventory,
        Name = omicron_common::api::external::Name,
        NetworkInterface = omicron_common::api::internal::shared::NetworkInterface,
        OmicronPhysicalDiskConfig = omicron_common::disk::OmicronPhysicalDiskConfig,
        OmicronPhysicalDisksConfig = omicron_common::disk::OmicronPhysicalDisksConfig,
        OmicronSledConfig = sled_agent_types_migrations::latest::inventory::OmicronSledConfig,
        OmicronZoneConfig = sled_agent_types_migrations::latest::inventory::OmicronZoneConfig,
        OmicronZoneDataset = sled_agent_types_migrations::latest::inventory::OmicronZoneDataset,
        OmicronZoneImageSource = sled_agent_types_migrations::latest::inventory::OmicronZoneImageSource,
        OmicronZoneType = sled_agent_types_migrations::latest::inventory::OmicronZoneType,
        OmicronZonesConfig = sled_agent_types_migrations::latest::inventory::OmicronZonesConfig,
        PortFec = omicron_common::api::internal::shared::PortFec,
        PortSpeed = omicron_common::api::internal::shared::PortSpeed,
        RouterId = omicron_common::api::internal::shared::RouterId,
        ResolvedVpcFirewallRule = omicron_common::api::internal::shared::ResolvedVpcFirewallRule,
        ResolvedVpcRoute = omicron_common::api::internal::shared::ResolvedVpcRoute,
        ResolvedVpcRouteSet = omicron_common::api::internal::shared::ResolvedVpcRouteSet,
        RouterTarget = omicron_common::api::internal::shared::RouterTarget,
        RouterVersion = omicron_common::api::internal::shared::RouterVersion,
        SledRole = sled_agent_types_migrations::latest::inventory::SledRole,
        SourceNatConfig = omicron_common::api::internal::shared::SourceNatConfig,
        SwitchLocation = omicron_common::api::external::SwitchLocation,
        Vni = omicron_common::api::external::Vni,
        VpcFirewallIcmpFilter = omicron_common::api::external::VpcFirewallIcmpFilter,
        ZpoolKind = omicron_common::zpool_name::ZpoolKind,
        ZpoolName = omicron_common::zpool_name::ZpoolName,
    }
);

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}

impl From<omicron_common::api::internal::nexus::VmmState> for types::VmmState {
    fn from(s: omicron_common::api::internal::nexus::VmmState) -> Self {
        use omicron_common::api::internal::nexus::VmmState as Input;
        match s {
            Input::Starting => types::VmmState::Starting,
            Input::Running => types::VmmState::Running,
            Input::Stopping => types::VmmState::Stopping,
            Input::Stopped => types::VmmState::Stopped,
            Input::Rebooting => types::VmmState::Rebooting,
            Input::Migrating => types::VmmState::Migrating,
            Input::Failed => types::VmmState::Failed,
            Input::Destroyed => types::VmmState::Destroyed,
        }
    }
}

impl From<types::VmmState> for omicron_common::api::internal::nexus::VmmState {
    fn from(s: types::VmmState) -> Self {
        use omicron_common::api::internal::nexus::VmmState as Output;
        match s {
            types::VmmState::Starting => Output::Starting,
            types::VmmState::Running => Output::Running,
            types::VmmState::Stopping => Output::Stopping,
            types::VmmState::Stopped => Output::Stopped,
            types::VmmState::Rebooting => Output::Rebooting,
            types::VmmState::Migrating => Output::Migrating,
            types::VmmState::Failed => Output::Failed,
            types::VmmState::Destroyed => Output::Destroyed,
        }
    }
}

impl From<types::VmmRuntimeState>
    for omicron_common::api::internal::nexus::VmmRuntimeState
{
    fn from(s: types::VmmRuntimeState) -> Self {
        Self {
            state: s.state.into(),
            generation: s.r#gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<types::SledVmmState>
    for omicron_common::api::internal::nexus::SledVmmState
{
    fn from(s: types::SledVmmState) -> Self {
        Self {
            vmm_state: s.vmm_state.into(),
            migration_in: s.migration_in.map(Into::into),
            migration_out: s.migration_out.map(Into::into),
        }
    }
}

impl From<types::MigrationRuntimeState>
    for omicron_common::api::internal::nexus::MigrationRuntimeState
{
    fn from(s: types::MigrationRuntimeState) -> Self {
        Self {
            migration_id: s.migration_id,
            state: s.state.into(),
            generation: s.r#gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<types::MigrationState>
    for omicron_common::api::internal::nexus::MigrationState
{
    fn from(s: types::MigrationState) -> Self {
        use omicron_common::api::internal::nexus::MigrationState as Output;
        match s {
            types::MigrationState::Pending => Output::Pending,
            types::MigrationState::InProgress => Output::InProgress,
            types::MigrationState::Failed => Output::Failed,
            types::MigrationState::Completed => Output::Completed,
        }
    }
}

impl From<omicron_common::api::internal::nexus::DiskRuntimeState>
    for types::DiskRuntimeState
{
    fn from(s: omicron_common::api::internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            r#gen: s.generation,
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
            generation: s.r#gen,
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

impl From<omicron_common::api::external::L4PortRange> for types::L4PortRange {
    fn from(s: omicron_common::api::external::L4PortRange) -> Self {
        Self::try_from(s.to_string()).unwrap_or_else(|e| panic!("{}: {}", s, e))
    }
}

impl From<omicron_common::api::internal::nexus::HostIdentifier>
    for types::HostIdentifier
{
    fn from(s: omicron_common::api::internal::nexus::HostIdentifier) -> Self {
        use omicron_common::api::internal::nexus::HostIdentifier::*;
        match s {
            Ip(net) => Self::Ip(net),
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
            Icmp(v) => Self::Icmp(v),
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

// TODO-cleanup This is icky; can we move these methods to a separate client so
// we don't need to add this header by hand?
// https://github.com/oxidecomputer/omicron/issues/8900
trait ApiVersionHeader {
    fn api_version_header(self, api_version: &'static str) -> Self;
}

impl ApiVersionHeader for reqwest::RequestBuilder {
    fn api_version_header(self, api_version: &'static str) -> Self {
        self.header("api-version", api_version)
    }
}

/// Exposes additional [`Client`] interfaces for use by the test suite. These
/// are bonus endpoints, not generated in the real client.
#[async_trait]
pub trait TestInterfaces {
    async fn vmm_single_step(&self, id: PropolisUuid);
    async fn vmm_finish_transition(&self, id: PropolisUuid);
    async fn vmm_simulate_migration_source(
        &self,
        id: PropolisUuid,
        params: SimulateMigrationSource,
    );
    async fn disk_finish_transition(&self, id: Uuid);
}

#[async_trait]
impl TestInterfaces for Client {
    async fn vmm_single_step(&self, id: PropolisUuid) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{}/vmms/{}/poke-single-step", baseurl, id);
        client
            .post(url)
            .api_version_header(self.api_version())
            .send()
            .await
            .expect("instance_single_step() failed unexpectedly");
    }

    async fn vmm_finish_transition(&self, id: PropolisUuid) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{}/vmms/{}/poke", baseurl, id);
        client
            .post(url)
            .api_version_header(self.api_version())
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
            .api_version_header(self.api_version())
            .send()
            .await
            .expect("disk_finish_transition() failed unexpectedly");
    }

    async fn vmm_simulate_migration_source(
        &self,
        id: PropolisUuid,
        params: SimulateMigrationSource,
    ) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{baseurl}/vmms/{id}/sim-migration-source");
        client
            .post(url)
            .api_version_header(self.api_version())
            .json(&params)
            .send()
            .await
            .expect("instance_simulate_migration_source() failed unexpectedly");
    }
}

/// Parameters to the `/instances/{id}/sim-migration-source` test API.
///
/// This message type is not included in the OpenAPI spec, because this API
/// exists only in test builds.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SimulateMigrationSource {
    /// The ID of the migration out of the instance's current active VMM.
    pub migration_id: Uuid,
    /// What migration result (success or failure) to simulate.
    pub result: SimulatedMigrationResult,
}

/// The result of a simulated migration out from an instance's current active
/// VMM.
#[derive(Serialize, Deserialize, JsonSchema)]
pub enum SimulatedMigrationResult {
    /// Simulate a successful migration out.
    Success,
    /// Simulate a failed migration out.
    ///
    /// # Note
    ///
    /// This is not currently implemented by the simulated sled-agent.
    Failure,
}
