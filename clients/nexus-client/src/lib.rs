// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to the Oxide control plane at large
//! from within the control plane

#[cfg(feature = "omicron-internal")]
progenitor::generate_api!(
    spec = "../../openapi/nexus-internal.json",
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
        "oxnet" = "0.1.0",
    },
    replace = {
        // It's kind of unfortunate to pull in such a complex and unstable type
        // as "blueprint" this way, but we have really useful functionality
        // (e.g., diff'ing) that's implemented on our local type.
        Blueprint = nexus_types::deployment::Blueprint,
        Certificate = omicron_common::api::internal::nexus::Certificate,
        DatasetKind = omicron_common::api::internal::shared::DatasetKind,
        Generation = omicron_common::api::external::Generation,
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
        MacAddr = omicron_common::api::external::MacAddr,
        Name = omicron_common::api::external::Name,
        NetworkInterface = omicron_common::api::internal::shared::NetworkInterface,
        NetworkInterfaceKind = omicron_common::api::internal::shared::NetworkInterfaceKind,
        NewPasswordHash = omicron_passwords::NewPasswordHash,
        OmicronPhysicalDiskConfig = nexus_types::disk::OmicronPhysicalDiskConfig,
        OmicronPhysicalDisksConfig = nexus_types::disk::OmicronPhysicalDisksConfig,
        RecoverySiloConfig = nexus_sled_agent_shared::recovery_silo::RecoverySiloConfig,

        TypedUuidForCollectionKind = omicron_uuid_kinds::CollectionUuid,
        TypedUuidForDemoSagaKind = omicron_uuid_kinds::DemoSagaUuid,
        TypedUuidForDownstairsKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::DownstairsKind>,
        TypedUuidForPropolisKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::PropolisKind>,
        TypedUuidForSledKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::SledKind>,
        TypedUuidForUpstairsKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsKind>,
        TypedUuidForUpstairsRepairKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsRepairKind>,
        TypedUuidForUpstairsSessionKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsSessionKind>,
        TypedUuidForZpoolKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::ZpoolKind>,
    },
    patch = {
        SledAgentInfo = { derives = [PartialEq, Eq] },
        ByteCount = { derives = [PartialEq, Eq] },
        Baseboard = { derives = [PartialEq, Eq] },
    }
);

#[cfg(not(feature = "omicron-internal"))]
pub use omicron_uuid_kinds;

#[cfg(not(feature = "omicron-internal"))]
progenitor::generate_api!(
    spec = "../../openapi/nexus-internal.json",
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
        "oxnet" = "0.1.0",
    },
    replace = {
        NewPasswordHash = omicron_passwords::NewPasswordHash,

        TypedUuidForCollectionKind = omicron_uuid_kinds::CollectionUuid,
        TypedUuidForDemoSagaKind = omicron_uuid_kinds::DemoSagaUuid,
        TypedUuidForDownstairsKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::DownstairsKind>,
        TypedUuidForPropolisKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::PropolisKind>,
        TypedUuidForSledKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::SledKind>,
        TypedUuidForUpstairsKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsKind>,
        TypedUuidForUpstairsRepairKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsRepairKind>,
        TypedUuidForUpstairsSessionKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::UpstairsSessionKind>,
        TypedUuidForZpoolKind = omicron_uuid_kinds::TypedUuid<omicron_uuid_kinds::ZpoolKind>,
    },
    patch = {
        SledAgentInfo = { derives = [PartialEq, Eq] },
        ByteCount = { derives = [PartialEq, Eq] },
        Baseboard = { derives = [PartialEq, Eq] },
        Generation = { derives = [PartialEq, Eq] },
    }
);

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

#[cfg(feature = "omicron-internal")]
mod internal_conversions {
    use super::types;
    use omicron_common::api::external as external_api;
    use omicron_common::api::internal as internal_api;

    use std::collections::HashMap;

    impl From<internal_api::nexus::VmmState> for types::VmmState {
        fn from(s: internal_api::nexus::VmmState) -> Self {
            use internal_api::nexus::VmmState as Input;
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

    impl From<types::VmmState> for internal_api::nexus::VmmState {
        fn from(s: types::VmmState) -> Self {
            use internal_api::nexus::VmmState as Output;
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

    impl From<internal_api::nexus::MigrationState> for types::MigrationState {
        fn from(s: internal_api::nexus::MigrationState) -> Self {
            use internal_api::nexus::MigrationState as Input;
            match s {
                Input::Pending => Self::Pending,
                Input::InProgress => Self::InProgress,
                Input::Completed => Self::Completed,
                Input::Failed => Self::Failed,
            }
        }
    }
    impl From<internal_api::nexus::MigrationRuntimeState>
        for types::MigrationRuntimeState
    {
        fn from(s: internal_api::nexus::MigrationRuntimeState) -> Self {
            Self {
                migration_id: s.migration_id,
                state: s.state.into(),
                gen: s.gen,
                time_updated: s.time_updated,
            }
        }
    }
    impl From<internal_api::nexus::SledVmmState> for types::SledVmmState {
        fn from(s: internal_api::nexus::SledVmmState) -> Self {
            Self {
                vmm_state: s.vmm_state.into(),
                migration_in: s.migration_in.map(Into::into),
                migration_out: s.migration_out.map(Into::into),
            }
        }
    }

    impl external_api::ClientError for types::Error {
        fn message(&self) -> String {
            self.message.clone()
        }
    }

    impl From<external_api::ByteCount> for types::ByteCount {
        fn from(s: external_api::ByteCount) -> Self {
            Self(s.to_bytes())
        }
    }

    impl From<types::DiskState> for external_api::DiskState {
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

    impl From<types::InstanceState> for external_api::InstanceState {
        fn from(s: types::InstanceState) -> Self {
            match s {
                types::InstanceState::Creating => Self::Creating,
                types::InstanceState::Starting => Self::Starting,
                types::InstanceState::Running => Self::Running,
                types::InstanceState::Stopping => Self::Stopping,
                types::InstanceState::Stopped => Self::Stopped,
                types::InstanceState::Rebooting => Self::Rebooting,
                types::InstanceState::Migrating => Self::Migrating,
                types::InstanceState::Repairing => Self::Repairing,
                types::InstanceState::Failed => Self::Failed,
                types::InstanceState::Destroyed => Self::Destroyed,
            }
        }
    }

    impl From<internal_api::nexus::VmmRuntimeState> for types::VmmRuntimeState {
        fn from(s: internal_api::nexus::VmmRuntimeState) -> Self {
            Self {
                gen: s.gen,
                state: s.state.into(),
                time_updated: s.time_updated,
            }
        }
    }

    impl From<external_api::InstanceState> for types::InstanceState {
        fn from(s: external_api::InstanceState) -> Self {
            use external_api::InstanceState;
            match s {
                InstanceState::Creating => Self::Creating,
                InstanceState::Starting => Self::Starting,
                InstanceState::Running => Self::Running,
                InstanceState::Stopping => Self::Stopping,
                InstanceState::Stopped => Self::Stopped,
                InstanceState::Rebooting => Self::Rebooting,
                InstanceState::Migrating => Self::Migrating,
                InstanceState::Repairing => Self::Repairing,
                InstanceState::Failed => Self::Failed,
                InstanceState::Destroyed => Self::Destroyed,
            }
        }
    }

    impl From<internal_api::nexus::DiskRuntimeState> for types::DiskRuntimeState {
        fn from(s: internal_api::nexus::DiskRuntimeState) -> Self {
            Self {
                disk_state: s.disk_state.into(),
                gen: s.gen,
                time_updated: s.time_updated,
            }
        }
    }

    impl From<external_api::DiskState> for types::DiskState {
        fn from(s: external_api::DiskState) -> Self {
            use external_api::DiskState;
            match s {
                DiskState::Creating => Self::Creating,
                DiskState::Detached => Self::Detached,
                DiskState::ImportReady => Self::ImportReady,
                DiskState::ImportingFromUrl => Self::ImportingFromUrl,
                DiskState::ImportingFromBulkWrites => {
                    Self::ImportingFromBulkWrites
                }
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

    impl From<&types::InstanceState> for external_api::InstanceState {
        fn from(state: &types::InstanceState) -> Self {
            match state {
                types::InstanceState::Creating => Self::Creating,
                types::InstanceState::Starting => Self::Starting,
                types::InstanceState::Running => Self::Running,
                types::InstanceState::Stopping => Self::Stopping,
                types::InstanceState::Stopped => Self::Stopped,
                types::InstanceState::Rebooting => Self::Rebooting,
                types::InstanceState::Migrating => Self::Migrating,
                types::InstanceState::Repairing => Self::Repairing,
                types::InstanceState::Failed => Self::Failed,
                types::InstanceState::Destroyed => Self::Destroyed,
            }
        }
    }

    impl From<internal_api::nexus::ProducerKind> for types::ProducerKind {
        fn from(kind: internal_api::nexus::ProducerKind) -> Self {
            use internal_api::nexus::ProducerKind;
            match kind {
                ProducerKind::ManagementGateway => Self::ManagementGateway,
                ProducerKind::SledAgent => Self::SledAgent,
                ProducerKind::Service => Self::Service,
                ProducerKind::Instance => Self::Instance,
            }
        }
    }

    impl From<&internal_api::nexus::ProducerEndpoint> for types::ProducerEndpoint {
        fn from(s: &internal_api::nexus::ProducerEndpoint) -> Self {
            Self {
                address: s.address.to_string(),
                id: s.id,
                kind: s.kind.into(),
                interval: s.interval.into(),
            }
        }
    }

    impl From<external_api::SemverVersion> for types::SemverVersion {
        fn from(s: external_api::SemverVersion) -> Self {
            s.to_string().parse().expect(
                "semver should generate output that matches validation regex",
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

    impl From<&internal_api::shared::SourceNatConfig> for types::SourceNatConfig {
        fn from(r: &internal_api::shared::SourceNatConfig) -> Self {
            let (first_port, last_port) = r.port_range_raw();
            Self { ip: r.ip, first_port, last_port }
        }
    }

    impl From<internal_api::shared::PortSpeed> for types::PortSpeed {
        fn from(value: internal_api::shared::PortSpeed) -> Self {
            match value {
                internal_api::shared::PortSpeed::Speed0G => {
                    types::PortSpeed::Speed0G
                }
                internal_api::shared::PortSpeed::Speed1G => {
                    types::PortSpeed::Speed1G
                }
                internal_api::shared::PortSpeed::Speed10G => {
                    types::PortSpeed::Speed10G
                }
                internal_api::shared::PortSpeed::Speed25G => {
                    types::PortSpeed::Speed25G
                }
                internal_api::shared::PortSpeed::Speed40G => {
                    types::PortSpeed::Speed40G
                }
                internal_api::shared::PortSpeed::Speed50G => {
                    types::PortSpeed::Speed50G
                }
                internal_api::shared::PortSpeed::Speed100G => {
                    types::PortSpeed::Speed100G
                }
                internal_api::shared::PortSpeed::Speed200G => {
                    types::PortSpeed::Speed200G
                }
                internal_api::shared::PortSpeed::Speed400G => {
                    types::PortSpeed::Speed400G
                }
            }
        }
    }

    impl From<internal_api::shared::PortFec> for types::PortFec {
        fn from(value: internal_api::shared::PortFec) -> Self {
            match value {
                internal_api::shared::PortFec::Firecode => {
                    types::PortFec::Firecode
                }
                internal_api::shared::PortFec::None => types::PortFec::None,
                internal_api::shared::PortFec::Rs => types::PortFec::Rs,
            }
        }
    }

    impl From<internal_api::shared::SwitchLocation> for types::SwitchLocation {
        fn from(value: internal_api::shared::SwitchLocation) -> Self {
            match value {
                internal_api::shared::SwitchLocation::Switch0 => {
                    types::SwitchLocation::Switch0
                }
                internal_api::shared::SwitchLocation::Switch1 => {
                    types::SwitchLocation::Switch1
                }
            }
        }
    }

    impl From<internal_api::shared::ExternalPortDiscovery>
        for types::ExternalPortDiscovery
    {
        fn from(value: internal_api::shared::ExternalPortDiscovery) -> Self {
            match value {
                internal_api::shared::ExternalPortDiscovery::Auto(val) => {
                    let new: HashMap<_, _> = val
                        .iter()
                        .map(|(slot, addr)| (slot.to_string(), *addr))
                        .collect();
                    types::ExternalPortDiscovery::Auto(new)
                }
                internal_api::shared::ExternalPortDiscovery::Static(val) => {
                    let new: HashMap<_, _> = val
                        .iter()
                        .map(|(slot, ports)| (slot.to_string(), ports.clone()))
                        .collect();
                    types::ExternalPortDiscovery::Static(new)
                }
            }
        }
    }

    impl From<types::ProducerKind> for internal_api::nexus::ProducerKind {
        fn from(kind: types::ProducerKind) -> Self {
            use internal_api::nexus::ProducerKind;
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
        for internal_api::nexus::ProducerEndpoint
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

    impl From<&external_api::AllowedSourceIps> for types::AllowedSourceIps {
        fn from(ips: &external_api::AllowedSourceIps) -> Self {
            use external_api::AllowedSourceIps;
            match ips {
                AllowedSourceIps::Any => types::AllowedSourceIps::Any,
                AllowedSourceIps::List(list) => {
                    let out = list.iter().map(|x| *x).collect::<Vec<_>>();
                    types::AllowedSourceIps::List(out)
                }
            }
        }
    }
}

// #[cfg(not(feature = "omicron-internal"))]
// mod conversions {
//     use super::types;
// }
