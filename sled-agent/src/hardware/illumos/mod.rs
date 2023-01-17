// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::hardware::{Disk, DiskVariant, HardwareUpdate};
use illumos_devinfo::{DevInfo, Node};
use slog::Logger;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

mod disk;
pub use disk::parse_partition_layout;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Failed to access devinfo: {0}")]
    DevInfo(anyhow::Error),

    #[error("Expected property {name} to have type {ty}")]
    UnexpectedPropertyType { name: String, ty: String },
}

// A snapshot of information about the underlying Tofino device
#[derive(Copy, Clone)]
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
    disks: HashSet<Disk>,
}

impl HardwareSnapshot {
    // Walk the device tree to capture a view of the current hardware.
    fn new(log: &Logger) -> Result<Self, Error> {
        let mut device_info = DevInfo::new().map_err(|e| Error::DevInfo(e))?;
        let mut node_walker = device_info.walk_node();

        let mut tofino = TofinoSnapshot::new();
        let mut disks = HashSet::new();
        while let Some(node) =
            node_walker.next().transpose().map_err(|e| Error::DevInfo(e))?
        {
            poll_tofino_node(&mut tofino, &node);
            poll_blkdev_node(&log, &mut disks, &node)?;
        }

        Ok(Self { tofino, disks })
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
    disks: HashSet<Disk>,
}

impl HardwareView {
    fn new() -> Self {
        Self {
            tofino: TofinoView::Real(TofinoSnapshot::new()),
            disks: HashSet::new(),
        }
    }

    fn new_stub_tofino(active: bool) -> Self {
        Self { tofino: TofinoView::Stub { active }, disks: HashSet::new() }
    }

    // Updates our view of the Tofino switch against a snapshot.
    fn update_tofino(
        &mut self,
        polled_hw: &HardwareSnapshot,
        updates: &mut Vec<HardwareUpdate>,
    ) {
        match self.tofino {
            TofinoView::Real(TofinoSnapshot { driver_loaded, exists }) => {
                use HardwareUpdate::*;
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
                self.tofino = TofinoView::Real(polled_hw.tofino);
            }
            TofinoView::Stub { .. } => (),
        }
    }

    // Updates our view of block devices against a snapshot.
    fn update_blkdev(
        &mut self,
        polled_hw: &HardwareSnapshot,
        updates: &mut Vec<HardwareUpdate>,
    ) {
        // In old set, not in new set.
        let removed = self.disks.difference(&polled_hw.disks);
        // In new set, not in old set.
        let added = polled_hw.disks.difference(&self.disks);

        use HardwareUpdate::*;
        for disk in removed {
            updates.push(DiskRemoved(disk.clone()));
        }
        for disk in added {
            updates.push(DiskAdded(disk.clone()));
        }

        self.disks = polled_hw.disks.clone();
    }
}

const TOFINO_SUBSYSTEM_VID: i32 = 0x1d1c;
const TOFINO_SUBSYSTEM_ID: i32 = 0x100;

fn node_name(subsystem_vid: i32, subsystem_id: i32) -> String {
    format!("pci{subsystem_vid:x},{subsystem_id:x}")
}

fn slot_to_disk_variant(slot: i64) -> Option<DiskVariant> {
    match slot {
        0x00..=0x09 => Some(DiskVariant::U2),
        0x11..=0x12 => Some(DiskVariant::M2),
        _ => None,
    }
}

fn poll_tofino_node(tofino: &mut TofinoSnapshot, node: &Node<'_>) {
    if node.node_name() == node_name(TOFINO_SUBSYSTEM_VID, TOFINO_SUBSYSTEM_ID)
    {
        tofino.exists = true;
        tofino.driver_loaded = node.driver_name().as_deref() == Some("tofino");
    }
}

fn poll_blkdev_node(
    log: &Logger,
    disks: &mut HashSet<Disk>,
    original_node: &Node<'_>,
) -> Result<(), Error> {
    if let Some(driver_name) = original_node.driver_name() {
        if driver_name == "blkdev" {
            let devfs_path =
                original_node.devfs_path().map_err(|e| Error::DevInfo(e))?;
            let node = original_node;
            while let Some(ref node) = node.parent() {
                let slot_prop_name = "physical-slot#".to_string();
                if let Some(Ok(slot_prop)) =
                    node.props().into_iter().find(|prop| {
                        if let Ok(prop) = prop {
                            prop.name() == slot_prop_name
                        } else {
                            false
                        }
                    })
                {
                    let slot = slot_prop.as_i64().ok_or_else(|| {
                        Error::UnexpectedPropertyType {
                            name: slot_prop_name,
                            ty: "i64".to_string(),
                        }
                    })?;

                    let variant = if let Some(variant) =
                        slot_to_disk_variant(slot)
                    {
                        variant
                    } else {
                        warn!(log, "Slot# {slot} is not recognized as a disk: {devfs_path}");
                        break;
                    };
                    info!(log, "Found a {variant:?} device in slot# {slot}: {devfs_path}");
                    let disk = match Disk::new(
                        PathBuf::from(devfs_path),
                        slot,
                        variant,
                    ) {
                        Ok(disk) => disk,
                        Err(err) => {
                            warn!(log, "Failed to load disk: {err}");
                            break;
                        }
                    };
                    disks.insert(disk);
                    break;
                }
            }
        }
    }
    Ok(())
}

// Performs a single walk of the device info tree, updating our view of hardware
// and sending notifications to any subscribers.
fn poll_device_tree(
    log: &Logger,
    inner: &Arc<Mutex<HardwareView>>,
    tx: &broadcast::Sender<HardwareUpdate>,
) -> Result<(), String> {
    // Construct a view of hardware by walking the device tree.
    let polled_hw = HardwareSnapshot::new(log).map_err(|e| e.to_string())?;

    // After inspecting the device tree, diff with the old view, and provide
    // necessary updates.
    let mut updates = vec![];
    {
        let mut inner = inner.lock().unwrap();
        inner.update_tofino(&polled_hw, &mut updates);
        inner.update_blkdev(&polled_hw, &mut updates);
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
    tx: broadcast::Sender<HardwareUpdate>,
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
    tx: broadcast::Sender<HardwareUpdate>,
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

    pub fn monitor(&self) -> broadcast::Receiver<HardwareUpdate> {
        info!(self.log, "Monitoring for hardware updates");
        self.tx.subscribe()
        // TODO: Do we want to send initial messages, based on the existing
        // state? Or should we leave this responsibility to the caller, to
        // start monitoring, and then query for the initial state?
        //
        // This could simplify the `SledAgent::monitor` function?
    }
}
