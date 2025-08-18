// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    SledCpuFamilyEnum:

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
    pub enum SledCpuFamily;

    Unknown => b"unknown"
    AmdMilan => b"amd_milan"
    AmdTurin => b"amd_turin"
    AmdTurinDense => b"amd_turin_dense"
);

impl From<nexus_sled_agent_shared::inventory::SledCpuFamily> for SledCpuFamily {
    fn from(value: nexus_sled_agent_shared::inventory::SledCpuFamily) -> Self {
        use nexus_sled_agent_shared::inventory::SledCpuFamily as InputFamily;
        match value {
            InputFamily::Unknown => Self::Unknown,
            InputFamily::AmdMilan => Self::AmdMilan,
            InputFamily::AmdTurin => Self::AmdTurin,
            InputFamily::AmdTurinDense => Self::AmdTurinDense,
        }
    }
}

impl From<SledCpuFamily> for nexus_sled_agent_shared::inventory::SledCpuFamily {
    fn from(value: SledCpuFamily) -> Self {
        match value {
            SledCpuFamily::Unknown => Self::Unknown,
            SledCpuFamily::AmdMilan => Self::AmdMilan,
            SledCpuFamily::AmdTurin => Self::AmdTurin,
            SledCpuFamily::AmdTurinDense => Self::AmdTurinDense,
        }
    }
}
