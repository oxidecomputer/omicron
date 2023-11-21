// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::IpccError;

pub struct IpccHandle;

impl IpccHandle {
    pub fn new() -> Result<Self, IpccError> {
        panic!("ipcc unavailable on this platform")
    }

    pub(crate) fn key_lookup(
        &self,
        _key: u8,
        _buf: &mut [u8],
    ) -> Result<usize, IpccError> {
        panic!("ipcc unavailable on this platform")
    }
}
