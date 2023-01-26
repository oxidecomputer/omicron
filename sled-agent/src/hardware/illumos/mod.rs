// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::hardware::{
    DiskIdentity, DiskVariant, HardwareUpdate, UnparsedDisk,
};
use illumos_devinfo::{DevInfo, DevLinkType, DevLinks, Node};
use slog::Logger;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

mod disk;
mod gpt;
mod nvme;

pub use disk::ensure_partition_layout;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Failed to access devinfo: {0}")]
    DevInfo(anyhow::Error),

    #[error("Expected property {name} to have type {ty}")]
    UnexpectedPropertyType { name: String, ty: String },

    #[error("Could not translate {0} to '/dev' path: no links")]
    NoDevLinks(PathBuf),

    #[error("Cannot query for NVME identity of {path}: {err}")]
    MissingNVMEIdentity { path: PathBuf, err: std::io::Error },

    #[error("Not an NVME device: {0}")]
    NotAnNVMEDevice(PathBuf),
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
    disks: HashSet<UnparsedDisk>,
}

impl HardwareSnapshot {
    // Walk the device tree to capture a view of the current hardware.
    fn new(log: &Logger) -> Result<Self, Error> {
        let mut device_info = DevInfo::new().map_err(Error::DevInfo)?;
        let mut node_walker = device_info.walk_node();

        let mut tofino = TofinoSnapshot::new();
        let mut disks = HashSet::new();
        while let Some(node) =
            node_walker.next().transpose().map_err(Error::DevInfo)?
        {
            poll_tofino_node(&log, &mut tofino, &node);
            poll_blkdev_node(&log, &mut disks, node)?;
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
    disks: HashSet<UnparsedDisk>,
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
        // For the source of these values, refer to:
        //
        // https://github.com/oxidecomputer/illumos-gate/blob/87a8bbb8edfb89ad5012beb17fa6f685c7795416/usr/src/uts/oxide/milan/milan_dxio_data.c#L823-L847
        0x00..=0x09 => Some(DiskVariant::U2),
        0x11..=0x12 => Some(DiskVariant::M2),
        _ => None,
    }
}

fn poll_tofino_node(
    log: &Logger,
    tofino: &mut TofinoSnapshot,
    node: &Node<'_>,
) {
    if node.node_name() == node_name(TOFINO_SUBSYSTEM_VID, TOFINO_SUBSYSTEM_ID)
    {
        tofino.exists = true;
        tofino.driver_loaded = node.driver_name().as_deref() == Some("tofino");
        debug!(
            log,
            "Found tofino node, with driver {}loaded",
            if tofino.driver_loaded { "" } else { "not " }
        );
    }
}

fn get_dev_path_of_whole_disk(
    node: &Node<'_>,
) -> Result<Option<PathBuf>, Error> {
    let mut wm = node.minors();
    while let Some(m) = wm.next().transpose().map_err(Error::DevInfo)? {
        // "wd" stands for "whole disk"
        if m.name() != "wd" {
            continue;
        }
        let links = {
            match DevLinks::new(true) {
                Ok(links) => links,
                Err(_) => DevLinks::new(false).map_err(Error::DevInfo)?,
            }
        };
        let devfs_path = m.devfs_path().map_err(Error::DevInfo)?;

        let paths = links
            .links_for_path(&devfs_path)
            .map_err(Error::DevInfo)?
            .into_iter()
            .filter(|l| {
                // Devices in "/dev/dsk" have names that denote their purpose,
                // of the form "controller, disk, slice" or "controller, disk,
                // partition".
                //
                // The suffix of "d0" is typical of an individual disk, and is
                // the expected device to correspond with the "wd" device in
                // the "/devices" hierarchy.
                l.linktype() == DevLinkType::Primary
                    && l.path()
                        .file_name()
                        .map(|f| f.to_string_lossy().ends_with("d0"))
                        .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        if paths.is_empty() {
            return Err(Error::NoDevLinks(PathBuf::from(devfs_path)));
        }
        return Ok(Some(paths[0].path().to_path_buf()));
    }
    Ok(None)
}

// Gather a notion of "device identity".
fn blkdev_device_identity(devfs_path: &str) -> Result<DiskIdentity, Error> {
    let path = PathBuf::from(devfs_path);
    let ctrl_id = nvme::identify_controller(&path)
        .map_err(|err| Error::MissingNVMEIdentity { path, err })?;

    let vendor =
        format!("{:x}{:x}", ctrl_id.vendor_id, ctrl_id.subsystem_vendor_id);
    let serial = ctrl_id.serial;
    let model = ctrl_id.model;
    Ok(DiskIdentity { vendor, serial, model })
}

fn poll_blkdev_node(
    log: &Logger,
    disks: &mut HashSet<UnparsedDisk>,
    mut node: Node<'_>,
) -> Result<(), Error> {
    let Some(driver_name) = node.driver_name() else {
        return Ok(());
    };

    if driver_name != "blkdev" {
        return Ok(());
    }

    let devfs_path = node.devfs_path().map_err(Error::DevInfo)?;
    let dev_path = get_dev_path_of_whole_disk(&node)?;

    // For some reason, libdevfs doesn't prepend "/devices" when
    // referring to the path, but it still returns an absolute path.
    //
    // Validate that we're still using this leading slash, but make the
    // path "real".
    assert!(devfs_path.starts_with('/'));
    let devfs_path = format!("/devices{devfs_path}");
    let mut device_id = None;
    while let Some(parent) = node.parent().map_err(Error::DevInfo)? {
        node = parent;

        if let Some(driver_name) = node.driver_name() {
            if driver_name == "nvme" {
                device_id = Some(blkdev_device_identity(&format!(
                    "/devices{}:devctl",
                    node.devfs_path().map_err(Error::DevInfo)?
                ))?);
            }
        }
        let slot_prop_name = "physical-slot#".to_string();
        if let Some(Ok(slot_prop)) = node.props().into_iter().find(|prop| {
            if let Ok(prop) = prop {
                prop.name() == slot_prop_name
            } else {
                false
            }
        }) {
            let slot = slot_prop.as_i64().ok_or_else(|| {
                Error::UnexpectedPropertyType {
                    name: slot_prop_name,
                    ty: "i64".to_string(),
                }
            })?;

            let Some(variant) = slot_to_disk_variant(slot) else {
                warn!(log, "Slot# {slot} is not recognized as a disk: {devfs_path}");
                break;
            };
            let disk = UnparsedDisk::new(
                PathBuf::from(&devfs_path),
                dev_path,
                slot,
                variant,
                device_id.ok_or_else(|| {
                    Error::NotAnNVMEDevice(PathBuf::from(devfs_path))
                })?,
            );
            disks.insert(disk);
            break;
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
    let polled_hw = HardwareSnapshot::new(log).map_err(|e| {
        warn!(log, "Failed to poll device tree: {e}");
        e.to_string()
    })?;

    // After inspecting the device tree, diff with the old view, and provide
    // necessary updates.
    let mut updates = vec![];
    {
        let mut inner = inner.lock().unwrap();
        inner.update_tofino(&polled_hw, &mut updates);
        inner.update_blkdev(&polled_hw, &mut updates);
    };

    if updates.is_empty() {
        debug!(log, "No updates from polling device tree");
    }

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
        info!(log, "Creating HardwareManager");

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

    pub fn disks(&self) -> HashSet<UnparsedDisk> {
        self.inner.lock().unwrap().disks.clone()
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
