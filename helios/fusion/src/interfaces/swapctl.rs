// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface to the swapctl API

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error listing swap devices: {0}")]
    ListDevices(String),

    #[error("Error adding swap device: {msg} (path=\"{path}\", start={start}, length={length})")]
    AddDevice { msg: String, path: String, start: u64, length: u64 },
}

/// A representation of a swap device, as returned from swapctl(2) SC_LIST
#[derive(Debug, Clone)]
pub struct SwapDevice {
    /// path to the resource
    pub path: String,

    /// starting block on device used for swap
    pub start: u64,

    /// length of swap area
    pub length: u64,

    /// total number of pages used for swapping
    pub total_pages: u64,

    /// free npages for swapping
    pub free_pages: u64,

    pub flags: i64,
}

pub trait Swapctl {
    /// List swap devices on the system.
    fn list_swap_devices(&self) -> Result<Vec<SwapDevice>, Error>;
    fn add_swap_device(
        &self,
        path: String,
        start: u64,
        length: u64,
    ) -> Result<(), Error>;
}
