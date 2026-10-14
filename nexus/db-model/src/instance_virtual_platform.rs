// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
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
    Generic => b"Generic"
);

impl From<instance_api::InstanceVirtualPlatform> for InstanceVirtualPlatform {
    fn from(value: instance_api::InstanceVirtualPlatform) -> Self {
        use instance_api::InstanceVirtualPlatform as ApiPlatform;
        match value {
            ApiPlatform::Initial => Self::Initial,
            ApiPlatform::Generic => Self::Generic,
        }
    }
}

impl From<InstanceVirtualPlatform> for instance_api::InstanceVirtualPlatform {
    fn from(value: InstanceVirtualPlatform) -> Self {
        match value {
            InstanceVirtualPlatform::Initial => Self::Initial,
            InstanceVirtualPlatform::Generic => Self::Generic,
        }
    }
}
