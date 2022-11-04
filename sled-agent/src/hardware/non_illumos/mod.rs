// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::Logger;
use std::sync::Mutex;
use tokio::sync::broadcast;

struct HardwareInner {
    is_scrimlet: bool,
}

/// A simulated representation of the underlying hardware.
///
/// This is intended for non-illumos systems to have roughly the same interface
/// as illumos systems.
pub struct Hardware {
    log: Logger,
    inner: Mutex<HardwareInner>,
    tx: broadcast::Sender<super::HardwareUpdate>,
}

impl Hardware {
    pub fn new(log: Logger, is_scrimlet: bool) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { log, inner: Mutex::new(HardwareInner { is_scrimlet }), tx }
    }

    pub fn is_scrimlet(&self) -> bool {
        self.inner.lock().unwrap().is_scrimlet
    }

    pub fn monitor(&self) -> broadcast::Receiver<super::HardwareUpdate> {
        info!(self.log, "Monitoring for hardware updates");
        self.tx.subscribe()
    }
}
