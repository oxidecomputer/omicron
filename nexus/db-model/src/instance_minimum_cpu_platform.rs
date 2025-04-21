// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SledCpuFamily;

use super::impl_enum_type;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    InstanceMinimumCpuPlatformEnum:

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
    pub enum InstanceMinimumCpuPlatform;

    AmdMilan => b"amd_milan"
    AmdTurin => b"amd_turin"
);

impl InstanceMinimumCpuPlatform {
    /// Returns a slice containing the set of sled CPU families that can
    /// accommodate an instance with this minimum CPU platform.
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

impl From<omicron_common::api::external::InstanceMinimumCpuPlatform>
    for InstanceMinimumCpuPlatform
{
    fn from(
        value: omicron_common::api::external::InstanceMinimumCpuPlatform,
    ) -> Self {
        use omicron_common::api::external::InstanceMinimumCpuPlatform as ApiPlatform;
        match value {
            ApiPlatform::AmdMilan => Self::AmdMilan,
            ApiPlatform::AmdTurin => Self::AmdTurin,
        }
    }
}

impl From<InstanceMinimumCpuPlatform>
    for omicron_common::api::external::InstanceMinimumCpuPlatform
{
    fn from(value: InstanceMinimumCpuPlatform) -> Self {
        match value {
            InstanceMinimumCpuPlatform::AmdMilan => Self::AmdMilan,
            InstanceMinimumCpuPlatform::AmdTurin => Self::AmdTurin,
        }
    }
}
