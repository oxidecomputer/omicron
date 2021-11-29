// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Interface for making API requests to a Sled Agent
 */

use async_trait::async_trait;
use omicron_common::generate_logging_api;
use std::convert::TryFrom;
use uuid::Uuid;

generate_logging_api!("../openapi/sled-agent.json");

impl From<omicron_common::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(
        s: omicron_common::api::internal::nexus::InstanceRuntimeState,
    ) -> Self {
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

impl From<omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested>
    for types::InstanceRuntimeStateRequested
{
    fn from(
        s: omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested,
    ) -> Self {
        Self { run_state: s.run_state.into() }
    }
}

impl From<omicron_common::api::internal::sled_agent::InstanceStateRequested>
    for types::InstanceStateRequested
{
    fn from(
        s: omicron_common::api::internal::sled_agent::InstanceStateRequested,
    ) -> Self {
        match s {
            omicron_common::api::internal::sled_agent::InstanceStateRequested::Running => Self::Running,
            omicron_common::api::internal::sled_agent::InstanceStateRequested::Stopped => Self::Stopped,
            omicron_common::api::internal::sled_agent::InstanceStateRequested::Reboot => Self::Reboot,
            omicron_common::api::internal::sled_agent::InstanceStateRequested::Destroyed => Self::Destroyed,
        }
    }
}

impl From<types::InstanceRuntimeState>
    for omicron_common::api::internal::nexus::InstanceRuntimeState
{
    fn from(s: types::InstanceRuntimeState) -> Self {
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

impl From<types::InstanceCpuCount>
    for omicron_common::api::external::InstanceCpuCount
{
    fn from(s: types::InstanceCpuCount) -> Self {
        Self(s.0)
    }
}

impl From<types::ByteCount> for omicron_common::api::external::ByteCount {
    fn from(s: types::ByteCount) -> Self {
        Self::try_from(s.0).unwrap()
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

impl From<omicron_common::api::internal::sled_agent::InstanceHardware>
    for types::InstanceHardware
{
    fn from(
        s: omicron_common::api::internal::sled_agent::InstanceHardware,
    ) -> Self {
        Self {
            nics: s.nics.iter().map(Into::into).collect(),
            runtime: s.runtime.into(),
        }
    }
}
impl From<&omicron_common::api::external::NetworkInterface>
    for types::NetworkInterface
{
    fn from(s: &omicron_common::api::external::NetworkInterface) -> Self {
        Self {
            identity: (&s.identity).into(),
            ip: s.ip.to_string(),
            mac: s.mac.into(),
            subnet_id: s.subnet_id,
            vpc_id: s.vpc_id,
        }
    }
}

impl From<&omicron_common::api::external::IdentityMetadata>
    for types::IdentityMetadata
{
    fn from(s: &omicron_common::api::external::IdentityMetadata) -> Self {
        Self {
            description: s.description.clone(),
            id: s.id,
            name: (&s.name).into(),
            time_created: s.time_created,
            time_modified: s.time_modified,
        }
    }
}

impl From<&omicron_common::api::external::Name> for types::Name {
    fn from(s: &omicron_common::api::external::Name) -> Self {
        Self(<&str>::from(s).to_string())
    }
}

impl From<omicron_common::api::external::MacAddr> for types::MacAddr {
    fn from(s: omicron_common::api::external::MacAddr) -> Self {
        Self(s.0.to_string())
    }
}
/**
 * Exposes additional [`Client`] interfaces for use by the test suite. These
 * are bonus endpoints, not generated in the real client.
 */
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
