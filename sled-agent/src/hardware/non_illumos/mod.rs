// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::Logger;
use tokio::sync::broadcast;

/// An unimplemented, stub representation of the underlying hardware.
///
/// This is intended for non-illumos systems to have roughly the same interface
/// as illumos systems - it allows compilation to "work" on non-illumos
/// platforms, which can be handy for editor support.
///
/// If you're actually trying to run the Sled Agent on non-illumos platforms,
/// use the simulated sled agent, which does not attempt to abstract hardware.
pub struct HardwareManager {}

impl HardwareManager {
    pub fn new(
        _log: Logger,
        _stub_scrimlet: Option<bool>,
    ) -> Result<Self, String> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn is_scrimlet(&self) -> bool {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn is_scrimlet_driver_loaded(&self) -> bool {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn monitor(&self) -> broadcast::Receiver<super::HardwareUpdate> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }
}
