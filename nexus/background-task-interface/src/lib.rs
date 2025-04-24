// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common interface for describint and activating Nexus background tasks.
//!
//! This crate defines the [`BackgroundTasks`] type, which lists out all of the
//! background tasks within Nexus, and provides handles to activate them.
//!
//! For more about background tasks, see the documentation at
//! `nexus/src/app/background/mod.rs`.

mod activator;
mod init;

pub use activator::*;
pub use init::*;
