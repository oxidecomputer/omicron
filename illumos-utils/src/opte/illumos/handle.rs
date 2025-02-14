// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handle to the OPTE kernel driver.

use opte_ioctl::Error as OpteError;
use opte_ioctl::OpteHdl;

pub struct Handle {
    inner: OpteHdl,
}

impl Handle {
    /// Construct a new handle to the OPTE kernel driver.
    pub fn new() -> Result<Self, OpteError> {
        OpteHdl::open(OpteHdl::XDE_CTL).map(|inner| Self { inner })
    }
}

impl std::ops::Deref for Handle {
    type Target = OpteHdl;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
