// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to the Oxide control plane at large
//! from within the control plane

progenitor::generate_api!(
    spec = "../../openapi/nexus-internal/nexus-internal-1.0.0-6d8ade.json",
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
        "omicron-uuid-kinds" = "*",
        "oxnet" = "0.1.0",
    },
    replace = {
        Generation = omicron_common::api::external::Generation,
        MacAddr = omicron_common::api::external::MacAddr,
        Name = omicron_common::api::external::Name,
        NetworkInterface = omicron_common::api::internal::shared::NetworkInterface,
        NetworkInterfaceKind = omicron_common::api::internal::shared::NetworkInterfaceKind,
    },
    patch = {
        SledAgentInfo = { derives = [PartialEq, Eq] },
        ByteCount = { derives = [PartialEq, Eq] },
        Baseboard = { derives = [PartialEq, Eq] }
    }
);

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
        Self {
            r#gen: s.generation,
            state: s.state.into(),
            time_updated: s.time_updated,
        }
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
            r#gen: s.generation,
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
            r#gen: s.generation,
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

impl From<nexus_types::external_api::shared::Baseboard> for types::Baseboard {
    fn from(value: nexus_types::external_api::shared::Baseboard) -> Self {
        types::Baseboard {
            part: value.part,
            revision: value.revision,
            serial: value.serial,
        }
    }
}
