// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Stub definition of the `Ipcc` type for compiling (but not running) on
//! non-Oxide systems.

use crate::InstallinatorImageId;
use crate::InstallinatorImageIdError;
use crate::PingError;
use crate::Pong;
use std::io;

pub struct Ipcc {}

impl Ipcc {
    pub fn open() -> io::Result<Self> {
        panic!("ipcc unavailable on this platform")
    }

    pub fn ping(&self) -> Result<Pong, PingError> {
        panic!("ipcc unavailable on this platform")
    }

    pub fn installinator_image_id(
        &self,
    ) -> Result<InstallinatorImageId, InstallinatorImageIdError> {
        panic!("ipcc unavailable on this platform")
    }
}
