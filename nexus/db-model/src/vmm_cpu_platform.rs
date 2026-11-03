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
    AmdTurinV2 => b"amd_turin_v2"
);

impl VmmCpuPlatform {
    /// If this VMM has a well-known CPU platform, returns a set of sled CPU
    /// families that can host the VMM. Otherwise, returns an empty set to
    /// indicate that a VMM CPU platform of "the target sled's defaults" cannot
    /// be generalized to any `SledCpuFamily`.
    pub fn compatible_sled_cpu_families(&self) -> &'static [SledCpuFamily] {
        match self {
            // Milan-based instances can run on both Milan and Turin processors.
            // Turin and Turin Dense are equally viable from a features
            // perspective.
            Self::AmdMilan => &[
                SledCpuFamily::AmdMilan,
                SledCpuFamily::AmdTurin,
                SledCpuFamily::AmdTurinDense,
            ],
            Self::AmdTurin => {
                &[SledCpuFamily::AmdTurin, SledCpuFamily::AmdTurinDense]
            }
            Self::AmdTurinV2 => {
                &[SledCpuFamily::AmdTurin, SledCpuFamily::AmdTurinDense]
            }

            // VMMs get the "sled default" CPU platform when an instance starts
            // up on a sled that hasn't reported a well-known CPU family. Assume
            // that nothing is known about the VM's compatible CPU platforms in
            // this case.
            Self::SledDefault => &[],
        }
    }
}

// An InstanceCpuPlatform is an instance's request for how it wants vCPUs to
// look, whereas a VmmCpuPlatform is the actual chosen platform at the point
// that instance was incarnated. A critical difference is that VmmCpuPlatform
// has an escape hatch in the form of `SledDefault` if an instance is to be run
// on a kind of hardware the control plane does not yet know about - this is
// only a possible value if both the instance did not have an explicit CPU
// platform *and* it was placed on a sled whose family we don't know.
//
// While this means we can convert between Instance and VMM CPU platforms, in
// practice Instance->VMM is the only useful direction. Since that is expected
// to be injective, `From` suffices.
impl From<InstanceCpuPlatform> for VmmCpuPlatform {
    fn from(value: InstanceCpuPlatform) -> Self {
        match value {
            InstanceCpuPlatform::AmdMilan => Self::AmdMilan,
            InstanceCpuPlatform::AmdTurin => Self::AmdTurin,
            InstanceCpuPlatform::AmdTurinV2 => Self::AmdTurinV2,
        }
    }
}
