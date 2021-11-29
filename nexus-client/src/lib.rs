// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Interface for making API requests to the Oxide control plane at large
 * from within the control plane
 */

use std::convert::TryFrom;

use omicron_common::generate_logging_api;

generate_logging_api!("../openapi/nexus-internal.json");

impl From<types::Generation> for omicron_common::api::external::Generation {
    fn from(s: types::Generation) -> Self {
        Self::try_from(s.0 as i64).unwrap()
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
            types::DiskState::Attaching(u) => Self::Attaching(u),
            types::DiskState::Attached(u) => Self::Attached(u),
            types::DiskState::Detaching(u) => Self::Detaching(u),
            types::DiskState::Destroyed => Self::Destroyed,
            types::DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<types::InstanceState>
    for omicron_common::api::external::InstanceState
{
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

impl From<omicron_common::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(
        s: omicron_common::api::internal::nexus::InstanceRuntimeState,
    ) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_uuid: s.sled_uuid,
            propolis_uuid: s.propolis_uuid,
            propolis_addr: s.propolis_addr.map(|addr| addr.to_string()),
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<&omicron_common::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(
        s: &omicron_common::api::internal::nexus::InstanceRuntimeState,
    ) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_uuid: s.sled_uuid,
            propolis_uuid: s.propolis_uuid,
            propolis_addr: s.propolis_addr.map(|addr| addr.to_string()),
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname.clone(),
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<omicron_common::api::external::InstanceState>
    for types::InstanceState
{
    fn from(s: omicron_common::api::external::InstanceState) -> Self {
        match s {
            omicron_common::api::external::InstanceState::Creating => {
                Self::Creating
            }
            omicron_common::api::external::InstanceState::Starting => {
                Self::Starting
            }
            omicron_common::api::external::InstanceState::Running => {
                Self::Running
            }
            omicron_common::api::external::InstanceState::Stopping => {
                Self::Stopping
            }
            omicron_common::api::external::InstanceState::Stopped => {
                Self::Stopped
            }
            omicron_common::api::external::InstanceState::Rebooting => {
                Self::Rebooting
            }
            omicron_common::api::external::InstanceState::Repairing => {
                Self::Repairing
            }
            omicron_common::api::external::InstanceState::Failed => {
                Self::Failed
            }
            omicron_common::api::external::InstanceState::Destroyed => {
                Self::Destroyed
            }
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

impl From<omicron_common::api::external::Generation> for types::Generation {
    fn from(s: omicron_common::api::external::Generation) -> Self {
        Self(i64::from(&s) as u64)
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
        match s {
            omicron_common::api::external::DiskState::Creating => {
                Self::Creating
            }
            omicron_common::api::external::DiskState::Detached => {
                Self::Detached
            }
            omicron_common::api::external::DiskState::Attaching(u) => {
                Self::Attaching(u)
            }
            omicron_common::api::external::DiskState::Attached(u) => {
                Self::Attached(u)
            }
            omicron_common::api::external::DiskState::Detaching(u) => {
                Self::Detaching(u)
            }
            omicron_common::api::external::DiskState::Destroyed => {
                Self::Destroyed
            }
            omicron_common::api::external::DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<&types::InstanceState>
    for omicron_common::api::external::InstanceState
{
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

impl From<&omicron_common::api::internal::nexus::ProducerEndpoint>
    for types::ProducerEndpoint
{
    fn from(
        s: &omicron_common::api::internal::nexus::ProducerEndpoint,
    ) -> Self {
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
        Self { secs: s.as_secs(), nanos: s.subsec_nanos() }
    }
}
