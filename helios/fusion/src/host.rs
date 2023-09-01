// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Represents the entire emulated host system

use crate::interfaces::libc::Libc;
use crate::interfaces::swapctl::Swapctl;
use crate::Executor;
use std::sync::Arc;

/// The common wrapper around the host system, which makes it trivially
/// shareable.
pub type HostSystem = Arc<dyn Host>;

/// Describes the interface used by Omicron when interacting with a host OS.
pub trait Host: Send + Sync {
    /// Access the executor, for creating new processes
    fn executor(&self) -> &dyn Executor;

    /// Access libswapctl
    fn swapctl(&self) -> &dyn Swapctl;

    /// Access libc
    fn libc(&self) -> &dyn Libc;
}
