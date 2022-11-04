// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::Logger;
use tokio::sync::broadcast;

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        mod illumos;
        use illumos::*;
    } else {
        mod non_illumos;
        use non_illumos::*;
    }
}

/// A platform-independent interface for interacting with the underlying hardware.
pub(crate) struct HardwareManager {
    _log: Logger,
    inner: Hardware,
}

impl HardwareManager {
    pub fn new(
        config: &crate::config::Config,
        log: Logger,
    ) -> Result<Self, String> {
        // Unless explicitly specified, we assume this device is a Gimlet until
        // told otherwise.
        let is_scrimlet = if let Some(is_scrimlet) = config.force_scrimlet {
            is_scrimlet
        } else {
            false
        };

        let log = log.new(o!("component" => "HardwareManager"));
        Ok(Self { _log: log.clone(), inner: Hardware::new(log, is_scrimlet)? })
    }

    pub fn is_scrimlet(&self) -> bool {
        self.inner.is_scrimlet()
    }

    // Monitors the underlying hardware for updates.
    pub fn monitor(&self) -> broadcast::Receiver<HardwareUpdate> {
        self.inner.monitor()
    }
}

/// Provides information from the underlying hardware about updates
/// which may require action on behalf of the Sled Agent.
///
/// These updates should generally be "non-opinionated" - the higher
/// layers of the sled agent can make the call to ignore these updates
/// or not.
#[derive(Clone)]
#[allow(dead_code)]
pub enum HardwareUpdate {
    TofinoLoaded,
    // TODO: Notify about disks being added / removed, etc.
}
