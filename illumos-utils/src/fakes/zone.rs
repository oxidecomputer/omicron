// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::zone::Api;
use crate::zpool::PathInPool;

use camino::Utf8Path;
use slog::Logger;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;

/// A fake implementation of [crate::zone::Zones].
///
/// This struct implements the [crate::zone::Api] interface but avoids
/// interacting with the host OS.
pub struct Zones {
    zones: Mutex<Vec<zone::Zone>>,
    halt_ctl: Option<HaltControl>,
}

/// Controls whether and when a particular zone can be halted.
struct HaltControl {
    /// Requests to _start_ halting a named zone.
    started: mpsc::UnboundedSender<String>,
    /// Whether a zone can continue halting.
    //
    // NOTE: We ask to control a specific zone for testing, but all zones are
    // released. If we want to control individual zones, this could be a hashmap
    // of bools instead, keyed on the zone name. We don't need that at this
    // time.
    released: watch::Receiver<bool>,
}

/// Handle to control when a zone starts and finishes halting.
///
/// This is used to instrument and control zone shutdown. Callers should use
/// `Zones::new_with_halt_control()` to construct an instance. Callers can then
/// use `halt_started()` to wait until a zone has _started_ halting, i.e.,
/// `Zones::halt_and_remove()` has been called. That routine will then pause
/// until `HaltController::release()` is called, indicating that zone shutdown
/// can proceed. This sort of acts as a waitable barrier in the middle of
/// `Zones::halt_and_remove()`.
///
/// NOTE: This cannot be used reliably when there are multiple zones. Every halt
/// is reported, but `release()` allows every zone's halt to proceed, so it's
/// only useful in tests with a single zone.
pub struct HaltController {
    /// Which zones have started halting.
    started: mpsc::UnboundedReceiver<String>,
    /// Ask to release the halting zone.
    release: watch::Sender<bool>,
}

impl HaltController {
    /// Wait until the next request to halt a zone, returning its name.
    pub async fn halt_started(&mut self) -> Option<String> {
        self.started.recv().await
    }

    /// Let the currently-blocked halt of a zone continue, as well as any
    /// future calls to it.
    pub fn release(&self) {
        let _ = self.release.send(true);
    }
}

impl Zones {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { zones: Mutex::new(vec![]), halt_ctl: None })
    }

    /// Construct a fake zones impl that lets callers instrument the shutdown
    /// of a single zone.
    pub fn new_with_halt_control() -> (Arc<Self>, HaltController) {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let (released_tx, released_rx) = watch::channel(false);
        let zones = Arc::new(Self {
            zones: Mutex::new(vec![]),
            halt_ctl: Some(HaltControl {
                started: started_tx,
                released: released_rx,
            }),
        });
        (zones, HaltController { started: started_rx, release: released_tx })
    }
}

#[async_trait::async_trait]
impl Api for Zones {
    async fn get(&self) -> Result<Vec<zone::Zone>, crate::zone::AdmError> {
        Ok(self.zones.lock().unwrap().clone())
    }

    async fn install_omicron_zone(
        &self,
        _log: &Logger,
        _zone_root_path: &PathInPool,
        _zone_name: &str,
        _zone_image: &Utf8Path,
        _datasets: &[zone::Dataset],
        _filesystems: &[zone::Fs],
        _devices: &[zone::Device],
        _links: Vec<String>,
        _limit_priv: Vec<String>,
    ) -> Result<(), crate::zone::AdmError> {
        Ok(())
    }

    async fn boot(&self, _name: &str) -> Result<(), crate::zone::AdmError> {
        Ok(())
    }

    // NOTE: Once we have better signal fidelity within our fake Zone
    // implementation (accurately tracking booted zones, and implementing 'get'
    // with a non-empty Vec) we can delete this implementation.
    async fn id(
        &self,
        _name: &str,
    ) -> Result<Option<i32>, crate::zone::AdmError> {
        Ok(Some(1))
    }

    async fn wait_for_service(
        &self,
        _zone: Option<&str>,
        _fmri: &str,
        _log: Logger,
    ) -> Result<(), omicron_common::api::external::Error> {
        Ok(())
    }

    async fn halt_and_remove(
        &self,
        name: &str,
    ) -> Result<Option<zone::State>, crate::zone::AdmError> {
        // Check if we need to notify a caller about halts.
        if let Some(ctl) = &self.halt_ctl {
            // Indicate that we've started halting this zone.
            let _ = ctl.started.send(name.to_string());
            // Now wait for us to be released by the caller.
            let mut released = ctl.released.clone();
            let _ = released.wait_for(|r| *r).await;
        }

        // Either we're not instrumenting zone shutdown, or we have been
        // explicitly released by the caller.
        Ok(Some(zone::State::Down))
    }
}
