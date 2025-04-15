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
);

impl From<nexus_types::internal_api::params::SledCpuFamily> for SledCpuFamily {
    fn from(value: nexus_types::internal_api::params::SledCpuFamily) -> Self {
        use nexus_types::internal_api::params::SledCpuFamily as InputFamily;
        match value {
            InputFamily::Unknown => Self::Unknown,
            InputFamily::AmdMilan => Self::AmdMilan,
            InputFamily::AmdTurin => Self::AmdTurin,
        }
    }
}

impl From<SledCpuFamily> for nexus_types::internal_api::params::SledCpuFamily {
    fn from(value: SledCpuFamily) -> Self {
        match value {
            SledCpuFamily::Unknown => Self::Unknown,
            SledCpuFamily::AmdMilan => Self::AmdMilan,
            SledCpuFamily::AmdTurin => Self::AmdTurin,
        }
    }
}

impl From<SledCpuFamily> for nexus_types::external_api::views::SledCpuFamily {
    fn from(value: SledCpuFamily) -> Self {
        match value {
            SledCpuFamily::Unknown => Self::Unknown,
            SledCpuFamily::AmdMilan => Self::AmdMilan,
            SledCpuFamily::AmdTurin => Self::AmdTurin,
        }
    }
}
