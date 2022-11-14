// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_devinfo::DevInfo;
use slog::Logger;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

// A snapshot of information about the underlying Tofino device
struct TofinoSnapshot {
    exists: bool,
    driver_loaded: bool,
}

impl TofinoSnapshot {
    fn new() -> Self {
        Self { exists: false, driver_loaded: false }
    }
}

// A snapshot of information about the underlying hardware
struct HardwareSnapshot {
    tofino: TofinoSnapshot,
}

impl HardwareSnapshot {
    fn new() -> Self {
        Self { tofino: TofinoSnapshot::new() }
    }
}

// Describes a view of the Tofino switch.
enum TofinoView {
    // The view of the Tofino switch exactly matches the snapshot of hardware.
    Real(TofinoSnapshot),
    // The Tofino switch has been "stubbed out", and the underlying hardware is
    // being ignored.
    Stub { active: bool },
}

// A cached copy of "our latest view of what hardware exists".
//
// This struct can be expanded arbitrarily, as it's useful for the Sled Agent
// to perceive hardware.
//
// Q: Why bother caching this information at all? Why not rely on devinfo for
// all queries?
// A: By keeping an in-memory representation, we can "diff" with the information
// reported from libdevinfo to decide when to send notifications and change
// which services are currently executing.
struct HardwareView {
    tofino: TofinoView,
    // TODO: Add U.2s, M.2s, other devices.
}

impl HardwareView {
    fn new() -> Self {
        Self { tofino: TofinoView::Real(TofinoSnapshot::new()) }
    }

    fn new_stub_tofino(active: bool) -> Self {
        Self { tofino: TofinoView::Stub { active } }
    }
}

const TOFINO_SUBSYSTEM_VID: i32 = 0x1d1c;
const TOFINO_SUBSYSTEM_ID: i32 = 0x100;

fn node_name(subsystem_vid: i32, subsystem_id: i32) -> String {
    format!("pci{subsystem_vid:x},{subsystem_id:x}")
}

// Performs a single walk of the device info tree, updating our view of hardware
// and sending notifications to any subscribers.
fn poll_device_tree(
    log: &Logger,
    inner: &Arc<Mutex<HardwareView>>,
    tx: &broadcast::Sender<super::HardwareUpdate>,
) -> Result<(), String> {
    // Construct a view of hardware by walking the device tree.
    let mut device_info = DevInfo::new().map_err(|e| e.to_string())?;
    let mut node_walker = device_info.walk_node();
    let mut polled_hw = HardwareSnapshot::new();
    while let Some(node) =
        node_walker.next().transpose().map_err(|e| e.to_string())?
    {
        if node.node_name()
            == node_name(TOFINO_SUBSYSTEM_VID, TOFINO_SUBSYSTEM_ID)
        {
            polled_hw.tofino.exists = true;
            polled_hw.tofino.driver_loaded =
                node.driver_name().as_deref() == Some("tofino");
        }
    }

    // After inspecting the device tree, diff with the old view, and provide
    // necessary updates.
    let mut updates = vec![];
    {
        let mut inner = inner.lock().unwrap();
        match inner.tofino {
            TofinoView::Real(TofinoSnapshot { driver_loaded, exists }) => {
                use super::HardwareUpdate::*;
                // Identify if the Tofino device changed power states.
                if exists != polled_hw.tofino.exists {
                    updates.push(TofinoDeviceChange);
                }

                // Identify if the Tofino driver was recently loaded/unloaded.
                match (driver_loaded, polled_hw.tofino.driver_loaded) {
                    (false, true) => updates.push(TofinoLoaded),
                    (true, false) => updates.push(TofinoUnloaded),
                    _ => (),
                };

                // Update our view of the underlying hardware
                inner.tofino = TofinoView::Real(polled_hw.tofino);
            }
            TofinoView::Stub { .. } => (),
        }
    };

    for update in updates.into_iter() {
        info!(log, "Update from polling device tree: {:?}", update);
        let _ = tx.send(update);
    }

    Ok(())
}

async fn hardware_tracking_task(
    log: Logger,
    inner: Arc<Mutex<HardwareView>>,
    tx: broadcast::Sender<super::HardwareUpdate>,
) {
    loop {
        if let Err(err) = poll_device_tree(&log, &inner, &tx) {
            warn!(log, "Failed to query device tree: {err}");
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

/// A representation of the underlying hardware.
///
/// This structure provides interfaces for both querying and for receiving new
/// events.
pub struct HardwareManager {
    log: Logger,
    inner: Arc<Mutex<HardwareView>>,
    tx: broadcast::Sender<super::HardwareUpdate>,
    _worker: JoinHandle<()>,
}

impl HardwareManager {
    /// Creates a new representation of the underlying hardware, and initializes
    /// a task which periodically updates that representation.
    ///
    /// Arguments:
    /// - `stub_scrimlet`: Identifies if we should ignore the attached Tofino
    /// device, and assume the device is a scrimlet (true) or gimlet (false).
    /// If this argument is not supplied, we assume the device is a gimlet until
    /// device scanning informs us otherwise.
    pub fn new(
        log: Logger,
        stub_scrimlet: Option<bool>,
    ) -> Result<Self, String> {
        let log = log.new(o!("component" => "HardwareManager"));

        // The size of the broadcast channel is arbitrary, but bounded.
        // If the channel fills up, old notifications will be dropped, and the
        // receiver will receive a tokio::sync::broadcast::error::RecvError::Lagged
        // error, indicating they should re-scan the hardware themselves.
        let (tx, _) = broadcast::channel(1024);
        let hw = match stub_scrimlet {
            None => HardwareView::new(),
            Some(active) => HardwareView::new_stub_tofino(active),
        };
        let inner = Arc::new(Mutex::new(hw));

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
        let inner = self.inner.lock().unwrap();
        match inner.tofino {
            TofinoView::Real(TofinoSnapshot { exists, .. }) => exists,
            TofinoView::Stub { active } => active,
        }
    }

    pub fn is_scrimlet_driver_loaded(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        match inner.tofino {
            TofinoView::Real(TofinoSnapshot { driver_loaded, .. }) => {
                driver_loaded
            }
            TofinoView::Stub { active } => active,
        }
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
