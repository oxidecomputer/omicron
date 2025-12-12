// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Machinery for sled-agent to run periodic health checks.
//!
//! The initial entry point to this system is [`HealthMonitorHandle::new()`].
//! This should be called early in sled-agent startup. Later during the
//! sled-agent start process, sled-agent should spawn each of the polling tasks
//! found in the health_checks module.
//!
//! The health checks we run are:
//!
//! * A task that checks for services in maintenance every minute.

pub mod handle;
pub mod health_checks;

pub use handle::HealthMonitorHandle;
