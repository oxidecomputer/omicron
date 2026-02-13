// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version READ_ONLY_DISKS.

use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::instance::InstanceDiskAttach;
use crate::v2025_11_20_00::instance::{UserData, bool_true};
use crate::v2026_01_03_00::instance::InstanceNetworkInterfaceAttachment;
use crate::v2026_01_05_00::instance::ExternalIpCreate;
use crate::v2026_01_08_00::multicast::MulticastGroupJoinSpec;

use super::disk::DiskCreate;

/// Describe the instance's disks at creation time
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstanceDiskAttachment {
    /// During instance creation, create and attach disks
    Create(DiskCreate),
    /// During instance creation, attach this disk
    Attach(InstanceDiskAttach),
}

impl From<crate::v2026_01_08_00::instance::InstanceDiskAttachment>
    for InstanceDiskAttachment
{
    fn from(
        old: crate::v2026_01_08_00::instance::InstanceDiskAttachment,
    ) -> Self {
        match old {
            crate::v2026_01_08_00::instance::InstanceDiskAttachment::Create(
                create,
            ) => Self::Create(create.into()),
            crate::v2026_01_08_00::instance::InstanceDiskAttachment::Attach(
                attach,
            ) => Self::Attach(attach),
        }
    }
}

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: Hostname,
    #[serde(default, with = "UserData")]
    pub user_data: Vec<u8>,
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,
    #[serde(default)]
    pub multicast_groups: Vec<MulticastGroupJoinSpec>,
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,
    #[serde(default)]
    pub boot_disk: Option<InstanceDiskAttachment>,
    pub ssh_public_keys: Option<Vec<NameOrId>>,
    #[serde(default = "bool_true")]
    pub start: bool,
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl From<crate::v2026_01_08_00::instance::InstanceCreate> for InstanceCreate {
    fn from(old: crate::v2026_01_08_00::instance::InstanceCreate) -> Self {
        Self {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips: old.external_ips,
            multicast_groups: old.multicast_groups,
            disks: old.disks.into_iter().map(Into::into).collect(),
            boot_disk: old.boot_disk.map(Into::into),
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
        }
    }
}
