// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A thin wrapper of interfaces to the GPT.
//!
//! Enables either real or faked GPT access.

use libefi_illumos::Error;
use std::path::Path;

/// Trait to sub-in for access to the libefi_illumos::Gpt.
///
/// Feel free to extend this interface to exactly match the methods exposed
/// by libefi_illumos::Gpt for testing.
pub(crate) trait LibEFIGpt {
    type Partition<'a>: LibEFIPartition
    where
        Self: 'a;
    fn read<P: AsRef<Path>>(path: P) -> Result<Self, Error>
    where
        Self: Sized;
    fn partitions(&self) -> Vec<Self::Partition<'_>>;
}

impl LibEFIGpt for libefi_illumos::Gpt {
    type Partition<'a> = libefi_illumos::Partition<'a>;
    fn read<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        libefi_illumos::Gpt::read(path)
    }

    fn partitions(&self) -> Vec<Self::Partition<'_>> {
        self.partitions().collect()
    }
}

/// Trait to sub-in for access to the libefi_illumos::Partition.
///
/// Feel free to extend this interface to exactly match the methods exposed
/// by libefi_illumos::Partition for testing.
pub(crate) trait LibEFIPartition {
    fn index(&self) -> usize;
}

impl LibEFIPartition for libefi_illumos::Partition<'_> {
    fn index(&self) -> usize {
        self.index()
    }
}
