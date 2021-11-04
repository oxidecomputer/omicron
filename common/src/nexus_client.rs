/*!
 * Interface for making API requests to the Oxide control plane at large
 * from within the control plane
 */

use std::convert::TryFrom;

generate_logging_api!("../openapi/nexus-internal.json");

impl From<types::Generation> for crate::api::external::Generation {
    fn from(s: types::Generation) -> Self {
        Self::try_from(s.0 as i64).unwrap()
    }
}

impl From<crate::api::external::ByteCount> for types::ByteCount {
    fn from(s: crate::api::external::ByteCount) -> Self {
        Self(s.to_bytes())
    }
}

impl From<types::DiskState> for crate::api::external::DiskState {
    fn from(s: types::DiskState) -> Self {
        match s {
            types::DiskState::Creating => Self::Creating,
            types::DiskState::Detached => Self::Detached,
            types::DiskState::Attaching(u) => Self::Attaching(u),
            types::DiskState::Attached(u) => Self::Attached(u),
            types::DiskState::Detaching(u) => Self::Detaching(u),
            types::DiskState::Destroyed => Self::Destroyed,
            types::DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<types::InstanceState> for crate::api::external::InstanceState {
    fn from(s: types::InstanceState) -> Self {
        match s {
            types::InstanceState::Creating => Self::Creating,
            types::InstanceState::Starting => Self::Starting,
            types::InstanceState::Running => Self::Running,
            types::InstanceState::Stopping => Self::Stopping,
            types::InstanceState::Stopped => Self::Stopped,
            types::InstanceState::Rebooting => Self::Rebooting,
            types::InstanceState::Repairing => Self::Repairing,
            types::InstanceState::Failed => Self::Failed,
            types::InstanceState::Destroyed => Self::Destroyed,
        }
    }
}

impl From<crate::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(s: crate::api::internal::nexus::InstanceRuntimeState) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_uuid: s.sled_uuid,
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<&crate::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(s: &crate::api::internal::nexus::InstanceRuntimeState) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_uuid: s.sled_uuid,
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname.clone(),
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<crate::api::external::InstanceState> for types::InstanceState {
    fn from(s: crate::api::external::InstanceState) -> Self {
        match s {
            crate::api::external::InstanceState::Creating => Self::Creating,
            crate::api::external::InstanceState::Starting => Self::Starting,
            crate::api::external::InstanceState::Running => Self::Running,
            crate::api::external::InstanceState::Stopping => Self::Stopping,
            crate::api::external::InstanceState::Stopped => Self::Stopped,
            crate::api::external::InstanceState::Rebooting => Self::Rebooting,
            crate::api::external::InstanceState::Repairing => Self::Repairing,
            crate::api::external::InstanceState::Failed => Self::Failed,
            crate::api::external::InstanceState::Destroyed => Self::Destroyed,
        }
    }
}

impl From<crate::api::external::InstanceCpuCount> for types::InstanceCpuCount {
    fn from(s: crate::api::external::InstanceCpuCount) -> Self {
        Self(s.0)
    }
}

impl From<crate::api::external::Generation> for types::Generation {
    fn from(s: crate::api::external::Generation) -> Self {
        Self(i64::from(&s) as u64)
    }
}

impl From<crate::api::internal::nexus::DiskRuntimeState>
    for types::DiskRuntimeState
{
    fn from(s: crate::api::internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<crate::api::external::DiskState> for types::DiskState {
    fn from(s: crate::api::external::DiskState) -> Self {
        match s {
            crate::api::external::DiskState::Creating => Self::Creating,
            crate::api::external::DiskState::Detached => Self::Detached,
            crate::api::external::DiskState::Attaching(u) => Self::Attaching(u),
            crate::api::external::DiskState::Attached(u) => Self::Attached(u),
            crate::api::external::DiskState::Detaching(u) => Self::Detaching(u),
            crate::api::external::DiskState::Destroyed => Self::Destroyed,
            crate::api::external::DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<&types::InstanceState> for crate::api::external::InstanceState {
    fn from(state: &types::InstanceState) -> Self {
        match state {
            types::InstanceState::Creating => Self::Creating,
            types::InstanceState::Starting => Self::Starting,
            types::InstanceState::Running => Self::Running,
            types::InstanceState::Stopping => Self::Stopping,
            types::InstanceState::Stopped => Self::Stopped,
            types::InstanceState::Rebooting => Self::Rebooting,
            types::InstanceState::Repairing => Self::Repairing,
            types::InstanceState::Failed => Self::Failed,
            types::InstanceState::Destroyed => Self::Destroyed,
        }
    }
}

impl From<&crate::api::internal::nexus::ProducerEndpoint>
    for types::ProducerEndpoint
{
    fn from(s: &crate::api::internal::nexus::ProducerEndpoint) -> Self {
        Self {
            address: s.address.to_string(),
            base_route: s.base_route.clone(),
            id: s.id,
            interval: s.interval.into(),
        }
    }
}

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { nanos: s.subsec_nanos(), secs: s.as_secs() }
    }
}
