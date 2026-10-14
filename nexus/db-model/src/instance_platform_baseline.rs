// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::{Deserialize, Serialize};
use crate::InstanceVirtualPlatform;

impl_enum_type!(
    InstancePlatformBaselineEnum:

    /// A collection of defaults for an instance's virtual platform.
    ///
    /// Each variant of `InstancePlatformBaseline` represents an
    /// `InstanceVirtualPlatform`.
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
    pub enum InstancePlatformBaseline;

    V1 => b"v1"
    V2 => b"v2"
    V3 => b"v3"
);

impl InstancePlatformBaseline {
    pub fn as_virtual_platform(&self) -> InstanceVirtualPlatform {
        match self {
            Self::V1 => V1_VIRTUAL_PLATFORM,
            Self::V2 => V2_VIRTUAL_PLATFORM,
            Self::V3 => V3_VIRTUAL_PLATFORM,
        }
    }
}

/// Instance virtual platforms as they were defined when virtual platforms were
/// introduced. This includes a handful of quirks to have a bug-compatible
/// baseline for instances createad before virtual platforms were explicitly
/// recorded.
const V1_VIRTUAL_PLATFORM: InstanceVirtualPlatform = InstanceVirtualPlatform {
    platform_baseline: InstancePlatformBaseline::V1,
    cpu_platform: None,
};

/// Very similar to V1 virtual platforms, but without the quirks.
const V2_VIRTUAL_PLATFORM: InstanceVirtualPlatform = InstanceVirtualPlatform {
    platform_baseline: InstancePlatformBaseline::V2,
    cpu_platform: None,
};

/// Adds Hyper-V with TSC, changes guest CPU topology from many-socket single-core to single-socket
/// many-core (and limits to even vCPU count as a result)
const V3_VIRTUAL_PLATFORM: InstanceVirtualPlatform = InstanceVirtualPlatform {
    platform_baseline: InstancePlatformBaseline::V3,
    cpu_platform: None,
};

impl From<omicron_common::api::external::InstancePlatformBaseline>
    for InstancePlatformBaseline
{
    fn from(value: omicron_common::api::external::InstancePlatformBaseline) -> Self {
        use omicron_common::api::external::InstancePlatformBaseline as ApiPlatform;
        match value {
            ApiPlatform::V1 => Self::V1,
            ApiPlatform::V2 => Self::V2,
            ApiPlatform::V3 => Self::V3,
        }
    }
}

impl From<InstancePlatformBaseline>
    for omicron_common::api::external::InstancePlatformBaseline
{
    fn from(value: InstancePlatformBaseline) -> Self {
        match value {
            InstancePlatformBaseline::V1 => Self::V1,
            InstancePlatformBaseline::V2 => Self::V2,
            InstancePlatformBaseline::V3 => Self::V3,
        }
    }
}
