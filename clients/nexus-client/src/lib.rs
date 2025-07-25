// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to the Oxide control plane at large
//! from within the control plane

use iddqd::IdOrdItem;
use iddqd::id_upcast;
use std::collections::HashMap;
use uuid::Uuid;

progenitor::generate_api!(
    spec = "../../openapi/nexus-internal.json",
    interface = Positional,
    derives = [schemars::JsonSchema, PartialEq],
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
    crates = {
        "iddqd" = "*",
        "oxnet" = "0.1.0",
    },
    replace = {
        // It's kind of unfortunate to pull in such a complex and unstable type
        // as "blueprint" this way, but we have really useful functionality
        // (e.g., diff'ing) that's implemented on our local type.
        Blueprint = nexus_types::deployment::Blueprint,
        BlueprintPhysicalDiskConfig = nexus_types::deployment::BlueprintPhysicalDiskConfig,
        BlueprintPhysicalDiskDisposition = nexus_types::deployment::BlueprintPhysicalDiskDisposition,
        BlueprintZoneImageSource = nexus_types::deployment::BlueprintZoneImageSource,
        Certificate = omicron_common::api::internal::nexus::Certificate,
        ClickhouseMode = nexus_types::deployment::ClickhouseMode,
        ClickhousePolicy = nexus_types::deployment::ClickhousePolicy,
        DatasetKind = omicron_common::api::internal::shared::DatasetKind,
        DnsConfigParams = nexus_types::internal_api::params::DnsConfigParams,
        DnsConfigZone = nexus_types::internal_api::params::DnsConfigZone,
        DnsRecord = nexus_types::internal_api::params::DnsRecord,
        Generation = omicron_common::api::external::Generation,
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
        MacAddr = omicron_common::api::external::MacAddr,
        MgsUpdateDriverStatus = nexus_types::internal_api::views::MgsUpdateDriverStatus,
        Name = omicron_common::api::external::Name,
        NetworkInterface = omicron_common::api::internal::shared::NetworkInterface,
        NetworkInterfaceKind = omicron_common::api::internal::shared::NetworkInterfaceKind,
        NewPasswordHash = omicron_passwords::NewPasswordHash,
        OmicronPhysicalDiskConfig = omicron_common::disk::OmicronPhysicalDiskConfig,
        OmicronPhysicalDisksConfig = omicron_common::disk::OmicronPhysicalDisksConfig,
        OximeterReadMode = nexus_types::deployment::OximeterReadMode,
        OximeterReadPolicy = nexus_types::deployment::OximeterReadPolicy,
        PendingMgsUpdate = nexus_types::deployment::PendingMgsUpdate,
        PlannerChickenSwitches = nexus_types::deployment::PlannerChickenSwitches,
        ReconfiguratorChickenSwitches = nexus_types::deployment::ReconfiguratorChickenSwitches,
        ReconfiguratorChickenSwitchesParam = nexus_types::deployment::ReconfiguratorChickenSwitchesParam,
        ReconfiguratorChickenSwitchesView = nexus_types::deployment::ReconfiguratorChickenSwitchesView,
        RecoverySiloConfig = nexus_sled_agent_shared::recovery_silo::RecoverySiloConfig,
        Srv = nexus_types::internal_api::params::Srv,
        TypedUuidForBlueprintKind = omicron_uuid_kinds::BlueprintUuid,
        TypedUuidForCollectionKind = omicron_uuid_kinds::CollectionUuid,
        TypedUuidForDatasetKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::DatasetKind>,
        TypedUuidForDemoSagaKind = omicron_uuid_kinds::DemoSagaUuid,
        TypedUuidForDownstairsKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::DownstairsKind>,
        TypedUuidForPhysicalDiskKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::PhysicalDiskKind>,
        TypedUuidForPropolisKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::PropolisKind>,
        TypedUuidForSledKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::SledKind>,
        TypedUuidForUpstairsKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsKind>,
        TypedUuidForUpstairsRepairKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsRepairKind>,
        TypedUuidForUpstairsSessionKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsSessionKind>,
        TypedUuidForVolumeKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::VolumeKind>,
        TypedUuidForZpoolKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::ZpoolKind>,
        UpdateStatus = nexus_types::internal_api::views::UpdateStatus,
        ZoneStatus = nexus_types::internal_api::views::ZoneStatus,
        ZoneStatusVersion = nexus_types::internal_api::views::ZoneStatusVersion,
        ZpoolName = omicron_common::zpool_name::ZpoolName,
    },
    patch = {
        SledAgentInfo = { derives = [PartialEq, Eq] },
        ByteCount = { derives = [PartialEq, Eq] },
        Baseboard = { derives = [PartialEq, Eq] }
    }
);

impl IdOrdItem for types::RunningSagaInfo {
    type Key<'a> = &'a Uuid;

    fn key(&self) -> Self::Key<'_> {
        &self.saga_id
    }

    id_upcast!();
}

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}

impl From<omicron_common::api::external::ByteCount> for types::ByteCount {
    fn from(s: omicron_common::api::external::ByteCount) -> Self {
        Self(s.to_bytes())
    }
}

impl From<types::DiskState> for omicron_common::api::external::DiskState {
    fn from(s: types::DiskState) -> Self {
        match s {
            types::DiskState::Creating => Self::Creating,
            types::DiskState::Detached => Self::Detached,
            types::DiskState::ImportReady => Self::ImportReady,
            types::DiskState::ImportingFromUrl => Self::ImportingFromUrl,
            types::DiskState::ImportingFromBulkWrites => {
                Self::ImportingFromBulkWrites
            }
            types::DiskState::Finalizing => Self::Finalizing,
            types::DiskState::Maintenance => Self::Maintenance,
            types::DiskState::Attaching(u) => Self::Attaching(u),
            types::DiskState::Attached(u) => Self::Attached(u),
            types::DiskState::Detaching(u) => Self::Detaching(u),
            types::DiskState::Destroyed => Self::Destroyed,
            types::DiskState::Faulted => Self::Faulted,
        }
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

impl From<omicron_common::api::internal::nexus::VmmRuntimeState>
    for types::VmmRuntimeState
{
    fn from(s: omicron_common::api::internal::nexus::VmmRuntimeState) -> Self {
        Self { gen: s.gen, state: s.state.into(), time_updated: s.time_updated }
    }
}

impl From<omicron_common::api::internal::nexus::SledVmmState>
    for types::SledVmmState
{
    fn from(s: omicron_common::api::internal::nexus::SledVmmState) -> Self {
        Self {
            vmm_state: s.vmm_state.into(),
            migration_in: s.migration_in.map(Into::into),
            migration_out: s.migration_out.map(Into::into),
        }
    }
}

impl From<omicron_common::api::internal::nexus::MigrationRuntimeState>
    for types::MigrationRuntimeState
{
    fn from(
        s: omicron_common::api::internal::nexus::MigrationRuntimeState,
    ) -> Self {
        Self {
            migration_id: s.migration_id,
            state: s.state.into(),
            gen: s.gen,
            time_updated: s.time_updated,
        }
    }
}

impl From<omicron_common::api::internal::nexus::MigrationState>
    for types::MigrationState
{
    fn from(s: omicron_common::api::internal::nexus::MigrationState) -> Self {
        use omicron_common::api::internal::nexus::MigrationState as Input;
        match s {
            Input::Pending => Self::Pending,
            Input::InProgress => Self::InProgress,
            Input::Completed => Self::Completed,
            Input::Failed => Self::Failed,
        }
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
        use omicron_common::api::external::DiskState;
        match s {
            DiskState::Creating => Self::Creating,
            DiskState::Detached => Self::Detached,
            DiskState::ImportReady => Self::ImportReady,
            DiskState::ImportingFromUrl => Self::ImportingFromUrl,
            DiskState::ImportingFromBulkWrites => Self::ImportingFromBulkWrites,
            DiskState::Finalizing => Self::Finalizing,
            DiskState::Maintenance => Self::Maintenance,
            DiskState::Attaching(u) => Self::Attaching(u),
            DiskState::Attached(u) => Self::Attached(u),
            DiskState::Detaching(u) => Self::Detaching(u),
            DiskState::Destroyed => Self::Destroyed,
            DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<omicron_common::api::internal::nexus::ProducerKind>
    for types::ProducerKind
{
    fn from(kind: omicron_common::api::internal::nexus::ProducerKind) -> Self {
        use omicron_common::api::internal::nexus::ProducerKind;
        match kind {
            ProducerKind::ManagementGateway => Self::ManagementGateway,
            ProducerKind::SledAgent => Self::SledAgent,
            ProducerKind::Service => Self::Service,
            ProducerKind::Instance => Self::Instance,
        }
    }
}

impl From<&omicron_common::api::internal::nexus::ProducerEndpoint>
    for types::ProducerEndpoint
{
    fn from(
        s: &omicron_common::api::internal::nexus::ProducerEndpoint,
    ) -> Self {
        Self {
            address: s.address.to_string(),
            id: s.id,
            kind: s.kind.into(),
            interval: s.interval.into(),
        }
    }
}

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { secs: s.as_secs(), nanos: s.subsec_nanos() }
    }
}

impl From<types::Duration> for std::time::Duration {
    fn from(s: types::Duration) -> Self {
        std::time::Duration::from_nanos(
            s.secs * 1000000000 + u64::from(s.nanos),
        )
    }
}

impl From<omicron_common::address::IpRange> for types::IpRange {
    fn from(r: omicron_common::address::IpRange) -> Self {
        use omicron_common::address::IpRange;
        match r {
            IpRange::V4(r) => types::IpRange::V4(r.into()),
            IpRange::V6(r) => types::IpRange::V6(r.into()),
        }
    }
}

impl From<omicron_common::address::Ipv4Range> for types::Ipv4Range {
    fn from(r: omicron_common::address::Ipv4Range) -> Self {
        Self { first: r.first, last: r.last }
    }
}

impl From<omicron_common::address::Ipv6Range> for types::Ipv6Range {
    fn from(r: omicron_common::address::Ipv6Range) -> Self {
        Self { first: r.first, last: r.last }
    }
}

impl From<&omicron_common::api::internal::shared::SourceNatConfig>
    for types::SourceNatConfig
{
    fn from(
        r: &omicron_common::api::internal::shared::SourceNatConfig,
    ) -> Self {
        let (first_port, last_port) = r.port_range_raw();
        Self { ip: r.ip, first_port, last_port }
    }
}

impl From<omicron_common::api::internal::shared::PortSpeed>
    for types::PortSpeed
{
    fn from(value: omicron_common::api::internal::shared::PortSpeed) -> Self {
        match value {
            omicron_common::api::internal::shared::PortSpeed::Speed0G => {
                types::PortSpeed::Speed0G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed1G => {
                types::PortSpeed::Speed1G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed10G => {
                types::PortSpeed::Speed10G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed25G => {
                types::PortSpeed::Speed25G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed40G => {
                types::PortSpeed::Speed40G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed50G => {
                types::PortSpeed::Speed50G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed100G => {
                types::PortSpeed::Speed100G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed200G => {
                types::PortSpeed::Speed200G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed400G => {
                types::PortSpeed::Speed400G
            }
        }
    }
}

impl From<omicron_common::api::internal::shared::PortFec> for types::PortFec {
    fn from(value: omicron_common::api::internal::shared::PortFec) -> Self {
        match value {
            omicron_common::api::internal::shared::PortFec::Firecode => {
                types::PortFec::Firecode
            }
            omicron_common::api::internal::shared::PortFec::None => {
                types::PortFec::None
            }
            omicron_common::api::internal::shared::PortFec::Rs => {
                types::PortFec::Rs
            }
        }
    }
}

impl From<omicron_common::api::internal::shared::SwitchLocation>
    for types::SwitchLocation
{
    fn from(
        value: omicron_common::api::internal::shared::SwitchLocation,
    ) -> Self {
        match value {
            omicron_common::api::internal::shared::SwitchLocation::Switch0 => {
                types::SwitchLocation::Switch0
            }
            omicron_common::api::internal::shared::SwitchLocation::Switch1 => {
                types::SwitchLocation::Switch1
            }
        }
    }
}

impl From<omicron_common::api::internal::shared::ExternalPortDiscovery>
    for types::ExternalPortDiscovery
{
    fn from(
        value: omicron_common::api::internal::shared::ExternalPortDiscovery,
    ) -> Self {
        match value {
            omicron_common::api::internal::shared::ExternalPortDiscovery::Auto(val) => {
                let new: HashMap<_, _> = val.iter().map(|(slot, addr)| {
                    (slot.to_string(), *addr)
                }).collect();
                types::ExternalPortDiscovery::Auto(new)
            },
            omicron_common::api::internal::shared::ExternalPortDiscovery::Static(val) => {
                let new: HashMap<_, _> = val.iter().map(|(slot, ports)| {
                    (slot.to_string(), ports.clone())
                }).collect();
                types::ExternalPortDiscovery::Static(new)
            },
        }
    }
}

impl From<types::ProducerKind>
    for omicron_common::api::internal::nexus::ProducerKind
{
    fn from(kind: types::ProducerKind) -> Self {
        use omicron_common::api::internal::nexus::ProducerKind;
        match kind {
            types::ProducerKind::ManagementGateway => {
                ProducerKind::ManagementGateway
            }
            types::ProducerKind::SledAgent => ProducerKind::SledAgent,
            types::ProducerKind::Instance => ProducerKind::Instance,
            types::ProducerKind::Service => ProducerKind::Service,
        }
    }
}

impl TryFrom<types::ProducerEndpoint>
    for omicron_common::api::internal::nexus::ProducerEndpoint
{
    type Error = String;

    fn try_from(ep: types::ProducerEndpoint) -> Result<Self, String> {
        let Ok(address) = ep.address.parse() else {
            return Err(format!("Invalid IP address: {}", ep.address));
        };
        Ok(Self {
            id: ep.id,
            kind: ep.kind.into(),
            address,
            interval: ep.interval.into(),
        })
    }
}

impl From<&omicron_common::api::external::AllowedSourceIps>
    for types::AllowedSourceIps
{
    fn from(ips: &omicron_common::api::external::AllowedSourceIps) -> Self {
        use omicron_common::api::external::AllowedSourceIps;
        match ips {
            AllowedSourceIps::Any => types::AllowedSourceIps::Any,
            AllowedSourceIps::List(list) => {
                types::AllowedSourceIps::List(list.iter().cloned().collect())
            }
        }
    }
}
