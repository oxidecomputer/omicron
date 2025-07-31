// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{InstanceCpuPlatform, SledCpuFamily};

use super::impl_enum_type;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    VmmCpuPlatformEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        strum::Display
    )]
    pub enum VmmCpuPlatform;

    SledDefault => b"sled_default"
    AmdMilan => b"amd_milan"
    AmdTurin => b"amd_turin"
);

impl VmmCpuPlatform {
    /// If this VMM has a well-known CPU platform, returns a `Some` containing
    /// the set of sled CPU families that can host that the VMM. Returns `None`
    /// if there is insufficient information to determine what CPU families
    /// could host this VMM.
    pub fn compatible_sled_cpu_families(&self) -> Option<&[SledCpuFamily]> {
        match self {
            // Milan-based instances can run on both Milan and Turin processors.
            // Turin and Turin Dense are equally viable from a features
            // perspective.
            Self::AmdMilan => {
                Some(&[SledCpuFamily::AmdMilan, SledCpuFamily::AmdTurin, SledCpuFamily::AmdTurinDense])
            }
            Self::AmdTurin => Some(&[SledCpuFamily::AmdTurin, SledCpuFamily::AmdTurinDense]),

            // VMMs get the "sled default" CPU platform when an instance starts
            // up on a sled that hasn't reported a well-known CPU family. Assume
            // that nothing is known about the VM's compatible CPU platforms in
            // this case.
            Self::SledDefault => None,
        }
    }
}

impl From<InstanceCpuPlatform> for VmmCpuPlatform {
    fn from(value: InstanceCpuPlatform) -> Self {
        match value {
            InstanceCpuPlatform::AmdMilan => Self::AmdMilan,
            InstanceCpuPlatform::AmdTurin => Self::AmdTurin,
        }
    }
}
