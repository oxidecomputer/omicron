// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrappers around VMM Reservoir controls

use omicron_common::api::external::ByteCount;

#[allow(non_upper_case_globals)]
const MiB: u64 = 1024 * 1024;
#[allow(non_upper_case_globals)]
const GiB: u64 = 1024 * 1024 * 1024;

/// The control plane's required alignment for a reservoir size request.
//
// The ioctl interface for resizing the reservoir only requires rounding the
// requested size to PAGESIZE. But we choose a larger value here of 2MiB to
// better enable large pages for guests in the future: Currently, the reservoir
// does not have support for large pages, but choosing a size that aligns with
// the minimum large page size on x86 (2 MiB) will simplify things later.
///
pub const RESERVOIR_SZ_ALIGN: u64 = 2 * MiB;

// Chunk size to request when resizing the reservoir. It's okay if this value is
// greater than the requested size.
const RESERVOIR_CHUNK_SZ: usize = GiB as usize;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Reservoir size must be aligned to 2 MiB: {0}")]
    InvalidSize(ByteCount),

    #[error("Failed to resize reservoir: {0}")]
    ReservoirError(#[from] std::io::Error),
}

/// Controls the size of the memory reservoir
pub struct ReservoirControl {}

impl ReservoirControl {
    /// Sets the reservoir to a particular size.
    ///
    /// The size must be aligned on RESERVOIR_SZ_ALIGN.
    pub fn set(size: ByteCount) -> Result<(), Error> {
        if !reservoir_size_is_aligned(size.to_bytes()) {
            return Err(Error::InvalidSize(size));
        }

        let ctl = bhyve_api::VmmCtlFd::open()?;
        ctl.reservoir_resize(
            size.to_bytes().try_into().map_err(|_| Error::InvalidSize(size))?,
            RESERVOIR_CHUNK_SZ,
        )
        .map_err(std::io::Error::from)?;

        Ok(())
    }
}

pub fn align_reservoir_size(size_bytes: u64) -> u64 {
    size_bytes - (size_bytes % RESERVOIR_SZ_ALIGN)
}

pub fn reservoir_size_is_aligned(size_bytes: u64) -> bool {
    (size_bytes % RESERVOIR_SZ_ALIGN) == 0
}
