// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::vmm_virtual_platform::VmmVirtualPlatform;
use nexus_types::external_api::instance as instance_api;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    InstanceVirtualPlatformEnum:

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
    pub enum InstanceVirtualPlatform;

    Initial => b"initial"
    // TODO: should this be "GenericX86"/"generic_x86"?
    Generic20261122 => b"generic_20261122"
    Generic => b"generic"
);

impl From<instance_api::InstanceVirtualPlatform> for InstanceVirtualPlatform {
    fn from(value: instance_api::InstanceVirtualPlatform) -> Self {
        use instance_api::InstanceVirtualPlatform as ApiPlatform;
        match value {
            ApiPlatform::Initial => Self::Initial,
            ApiPlatform::Generic20261122 => Self::Generic20261122,
            ApiPlatform::Generic => Self::Generic,
        }
    }
}

impl From<InstanceVirtualPlatform> for instance_api::InstanceVirtualPlatform {
    fn from(value: InstanceVirtualPlatform) -> Self {
        match value {
            InstanceVirtualPlatform::Initial => Self::Initial,
            InstanceVirtualPlatform::Generic20261122 => Self::Generic20261122,
            InstanceVirtualPlatform::Generic => Self::Generic,
        }
    }
}

/// An InstanceVirtualPlatform is an instance's request for how the VM should be
/// shaped and what features it can have, as is a VmmVirtualPlatform. The
/// difference between these is that a a VmmVirtualPlatform describes a specific
/// virtual platform at some revision, while an InstanceVirtualPlatform can be a
/// looser request for whichever revision of a platform is compatible with the
/// request.
///
/// Concretely, an InstanceVirtualPlatform's `Generic` means "any version of
/// Generic will do" and lets us pick the latest `Generic`, including
/// potentially across instance reboots! That "pick a version of this platform
/// that will do" behavior happens.. here.
impl InstanceVirtualPlatform {
    pub fn select_vmm_platform(&self) -> VmmVirtualPlatform {
        match self {
            InstanceVirtualPlatform::Initial => VmmVirtualPlatform::Initial,
            InstanceVirtualPlatform::Generic => {
                VmmVirtualPlatform::Generic20261122
            }
            InstanceVirtualPlatform::Generic20261122 => {
                VmmVirtualPlatform::Generic20261122
            }
        }
    }
}
