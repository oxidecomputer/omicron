// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Local storage abstraction for use by sled-agent
//!
//! This abstraction operates at the ZFS level and relies on zpool setup on
//! hardware partitions from the `sled-hardware` crate. It utilizes the
//! `illumos-utils` crate to actually perform ZFS related OS calls.

pub mod dataset;
pub(crate) mod disk;
pub(crate) mod dump_setup;
pub mod error;
pub(crate) mod keyfile;
pub mod manager;
pub(crate) mod pool;
pub mod resources;
