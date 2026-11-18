// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SledCpuFamily;

use super::impl_enum_type;
use nexus_types::external_api::instance as instance_api;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    InstanceCpuPlatformEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize
    )]
    pub enum InstanceCpuPlatform;

    AmdMilan => b"amd_milan"
    AmdTurin => b"amd_turin"
    AmdTurinV2 => b"amd_turin_v2"
);

impl InstanceCpuPlatform {
    /// A convenience helper for the set of sled CPU families which support this
    /// instance CPU platform, without callers having to convert to a matching
    /// VMM CPU platform first.
    pub fn compatible_sled_cpu_families(&self) -> &'static [SledCpuFamily] {
        let vmm_platform: crate::VmmCpuPlatform = (*self).into();
        vmm_platform.compatible_sled_cpu_families()
    }
}

impl From<instance_api::InstanceCpuPlatform> for InstanceCpuPlatform {
    fn from(value: instance_api::InstanceCpuPlatform) -> Self {
        use instance_api::InstanceCpuPlatform as ApiPlatform;
        match value {
            ApiPlatform::AmdMilan => Self::AmdMilan,
            ApiPlatform::AmdTurin => Self::AmdTurin,
            ApiPlatform::AmdTurinV2 => Self::AmdTurinV2,
        }
    }
}

impl From<InstanceCpuPlatform> for instance_api::InstanceCpuPlatform {
    fn from(value: InstanceCpuPlatform) -> Self {
        match value {
            InstanceCpuPlatform::AmdMilan => Self::AmdMilan,
            InstanceCpuPlatform::AmdTurin => Self::AmdTurin,
            InstanceCpuPlatform::AmdTurinV2 => Self::AmdTurinV2,
        }
    }
}
