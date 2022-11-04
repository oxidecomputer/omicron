// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_devinfo::DevInfo;
use slog::Logger;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

// A cached copy of "our latest view of what hardware exists".
//
// This struct can be expanded arbitrarily, as it's useful for the Sled Agent
// to perceive hardware.
//
// Q: Why bother caching this information at all? Why not rely on devinfo for
// all queries?
// A: By keeping an in-memory representation, we can "diff" with the information
// reported from libdevinfo to decide when to send notifications.
struct HardwareInner {
    is_scrimlet: bool,
    // TODO: Add U.2s, M.2s, other devices.
}

/// A representation of the underlying hardware.
///
/// This structure provides interfaces for both querying and for receiving new
/// events.
pub struct Hardware {
    log: Logger,
    inner: Arc<Mutex<HardwareInner>>,
    tx: broadcast::Sender<super::HardwareUpdate>,
    _worker: JoinHandle<()>,
}

// Performs a single walk of the device info tree, updating our view of hardware
// and sending notifications to any subscribers.
fn poll_device_tree(
    log: &Logger,
    inner: &Arc<Mutex<HardwareInner>>,
    tx: &broadcast::Sender<super::HardwareUpdate>,
) -> Result<(), String> {
    let mut device_info = DevInfo::new().map_err(|e| e.to_string())?;
    let mut node_walker = device_info.walk_node();
    while let Some(node) =
        node_walker.next().transpose().map_err(|e| e.to_string())?
    {
        if let Some(driver_name) = node.driver_name() {
            if driver_name == "tofino" {
                let scrimlet_status_updated = {
                    let mut inner = inner.lock().unwrap();
                    if !inner.is_scrimlet {
                        inner.is_scrimlet = true;
                        true
                    } else {
                        false
                    }
                };
                if scrimlet_status_updated {
                    info!(log, "Polling device tree: Found tofino driver");
                    let _ = tx.send(super::HardwareUpdate::TofinoLoaded);
                }
            }
        }
    }
    Ok(())
}

async fn hardware_tracking_task(
    log: Logger,
    inner: Arc<Mutex<HardwareInner>>,
    tx: broadcast::Sender<super::HardwareUpdate>,
) {
    loop {
        if let Err(err) = poll_device_tree(&log, &inner, &tx) {
            warn!(log, "Failed to query device tree: {err}");
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

impl Hardware {
    /// Creates a new representation of the underlying hardware, and initialize
    /// a task which periodically updates that representation.
    pub fn new(log: Logger, is_scrimlet: bool) -> Result<Self, String> {
        let (tx, _) = broadcast::channel(1024);
        let inner = Arc::new(Mutex::new(HardwareInner { is_scrimlet }));

        // Force the device tree to be polled at least once before returning.
        // This mitigates issues where the Sled Agent could try to propagate
        // an "empty" view of hardware to other consumers before the first
        // query.
        poll_device_tree(&log, &inner, &tx)
            .map_err(|err| format!("Failed to poll device tree: {err}"))?;

        let log2 = log.clone();
        let inner2 = inner.clone();
        let tx2 = tx.clone();
        let _worker = tokio::task::spawn(async move {
            hardware_tracking_task(log2, inner2, tx2).await
        });

        Ok(Self { log, inner, tx, _worker })
    }

    pub fn is_scrimlet(&self) -> bool {
        self.inner.lock().unwrap().is_scrimlet
    }

    pub fn monitor(&self) -> broadcast::Receiver<super::HardwareUpdate> {
        info!(self.log, "Monitoring for hardware updates");
        self.tx.subscribe()
        // TODO: Do we want to send initial messages, based on the existing
        // state? Or should we leave this responsibility to the caller, to
        // start monitoring, and then query for the initial state?
        //
        // This could simplify the `SledAgent::monitor` function?
    }
}
