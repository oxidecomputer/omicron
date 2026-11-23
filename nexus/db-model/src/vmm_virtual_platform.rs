// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::{Deserialize, Serialize};

use std::fmt::{Display, Formatter, Result};

impl_enum_type!(
    VmmVirtualPlatformEnum:

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
    pub enum VmmVirtualPlatform;

    Initial => b"initial"
    // TODO: should this be "GenericX86"/"generic_x86"?
    Generic20261122 => b"generic_20261122"
);

impl VmmVirtualPlatform {
    fn name(&self) -> &'static str {
        match self {
            VmmVirtualPlatform::Initial => "initial",
            VmmVirtualPlatform::Generic20261122 => "generic20261122",
        }
    }
}

impl Display for VmmVirtualPlatform {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}", self.name())
    }
}
