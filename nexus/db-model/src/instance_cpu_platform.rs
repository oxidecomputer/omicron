// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SledCpuFamily;

use super::impl_enum_type;
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
);

impl InstanceCpuPlatform {
    /// Returns a slice containing the set of sled CPU families that can
    /// accommodate an instance with this CPU platform.
    pub fn compatible_sled_cpu_families(&self) -> &'static [SledCpuFamily] {
        match self {
            // Turin-based sleds have a superset of the features made available
            // in a guest's Milan CPU platform
            Self::AmdMilan => {
                &[SledCpuFamily::AmdMilan, SledCpuFamily::AmdTurin]
            }
            Self::AmdTurin => &[SledCpuFamily::AmdTurin],
        }
    }
}

impl From<omicron_common::api::external::InstanceCpuPlatform>
    for InstanceCpuPlatform
{
    fn from(value: omicron_common::api::external::InstanceCpuPlatform) -> Self {
        use omicron_common::api::external::InstanceCpuPlatform as ApiPlatform;
        match value {
            ApiPlatform::AmdMilan => Self::AmdMilan,
            ApiPlatform::AmdTurin => Self::AmdTurin,
        }
    }
}

impl From<InstanceCpuPlatform>
    for omicron_common::api::external::InstanceCpuPlatform
{
    fn from(value: InstanceCpuPlatform) -> Self {
        match value {
            InstanceCpuPlatform::AmdMilan => Self::AmdMilan,
            InstanceCpuPlatform::AmdTurin => Self::AmdTurin,
        }
    }
}
