// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrappers around reservoir controls

use omicron_common::api::external::ByteCount;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Reservoir size must be a multiple of MiB, not: {0}")]
    InvalidSize(ByteCount),

    #[error("Failed to set reservoir size: {0}")]
    ReservoirError(#[from] std::io::Error),
}

/// Controls the size of the memory reservoir
pub struct ReservoirControl {}

impl ReservoirControl {
    /// Sets the reservoir to a particular size.
    ///
    /// - "size" must be exactly divisible by a MiB
    pub fn set(size: ByteCount) -> Result<(), Error> {
        if size.to_bytes() % (1024 * 1024) != 0 {
            return Err(Error::InvalidSize(size));
        }

        let ctl = bhyve_api::VmmCtlFd::open()?;
        ctl.reservoir_resize(
            size.to_bytes().try_into().map_err(|_| Error::InvalidSize(size))?,
            0,
        )
        .map_err(std::io::Error::from)?;

        Ok(())
    }
}
