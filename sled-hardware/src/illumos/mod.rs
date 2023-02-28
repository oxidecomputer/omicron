// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    Baseboard, DiskIdentity, DiskVariant, HardwareUpdate, UnparsedDisk,
};
use illumos_devinfo::{DevInfo, DevLinkType, DevLinks, Node, Property};
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

mod disk;
mod gpt;

pub use disk::ensure_partition_layout;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Failed to access devinfo: {0}")]
    DevInfo(anyhow::Error),

    #[error("Device does not appear to be an Oxide Gimlet: {0}")]
    NotAGimlet(String),

    #[error("Node {node} missing device property {name}")]
    MissingDeviceProperty { node: String, name: String },

    #[error("Unrecognized slot for device {slot}")]
    UnrecognizedSlot { slot: i64 },

    #[error("Expected property {name} to have type {ty}")]
    UnexpectedPropertyType { name: String, ty: String },

    #[error("Could not translate {0} to '/dev' path: no links")]
    NoDevLinks(PathBuf),
}

const GIMLET_ROOT_NODE_NAME: &str = "Oxide,Gimlet";

/// Return true if the host system is an Oxide Gimlet.
pub fn is_gimlet() -> anyhow::Result<bool> {
    let mut device_info = DevInfo::new()?;
    let mut node_walker = device_info.walk_node();
    let Some(root) = node_walker.next().transpose()? else {
        anyhow::bail!("No nodes in device tree");
    };
    Ok(root.node_name() == GIMLET_ROOT_NODE_NAME)
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
    baseboard: Baseboard,
}

impl HardwareSnapshot {
    // Walk the device tree to capture a view of the current hardware.
    fn new(log: &Logger) -> Result<Self, Error> {
        let mut device_info =
            DevInfo::new_force_load().map_err(Error::DevInfo)?;

        let mut node_walker = device_info.walk_node();

        // First, check the root node. If we aren't running on a Gimlet, bail.
        let Some(root) = node_walker.next().transpose().map_err(Error::DevInfo)? else {
            return Err(Error::DevInfo(anyhow::anyhow!("No nodes in device tree")));
        };
        let root_node = root.node_name();
        if root_node != GIMLET_ROOT_NODE_NAME {
            return Err(Error::NotAGimlet(root_node));
        }

        let properties = find_properties(
            &root,
            ["baseboard-identifier", "baseboard-model", "baseboard-revision"],
        )?;
        let baseboard = Baseboard::new(
            string_from_property(&properties[0])?,
            string_from_property(&properties[1])?,
            i64_from_property(&properties[2])?,
        );

        // Monitor for the Tofino device and driver.
        let tofino = get_tofino_snapshot(log, &mut device_info);

        // Monitor for block devices.
        let mut disks = HashSet::new();
        let mut node_walker = device_info.walk_driver("blkdev");
        while let Some(node) =
            node_walker.next().transpose().map_err(Error::DevInfo)?
        {
            poll_blkdev_node(&log, &mut disks, node)?;
        }

        Ok(Self { tofino, disks, baseboard })
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
    baseboard: Option<Baseboard>,
}

impl HardwareView {
    // TODO: We should populate these constructors with real data from the
    // first attempt at polling hardware.
    //
    // Otherwise, values that we really expect to be static will need to be
    // nullable.
    fn new() -> Self {
        Self {
            tofino: TofinoView::Real(TofinoSnapshot::new()),
            disks: HashSet::new(),
            baseboard: None,
        }
    }

    fn new_stub_tofino(active: bool) -> Self {
        Self {
            tofino: TofinoView::Stub { active },
            disks: HashSet::new(),
            baseboard: None,
        }
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

fn get_tofino_snapshot(log: &Logger, devinfo: &mut DevInfo) -> TofinoSnapshot {
    let (exists, driver_loaded) = match tofino::get_tofino_from_devinfo(devinfo)
    {
        Ok(None) => (false, false),
        Ok(Some(node)) => (node.has_asic(), node.has_driver()),
        Err(e) => {
            error!(log, "failed to get tofino state: {e:?}");
            (false, false)
        }
    };
    if exists {
        debug!(
            log,
            "Found tofino node, with driver {}loaded",
            if driver_loaded { "" } else { "not " }
        );
    }
    TofinoSnapshot { exists, driver_loaded }
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

fn get_parent_node<'a>(
    node: &Node<'a>,
    expected_parent_driver_name: &'static str,
) -> Result<Node<'a>, Error> {
    let Some(parent) = node.parent().map_err(Error::DevInfo)? else {
        return Err(Error::DevInfo(anyhow::anyhow!("{} has no parent node", node.node_name())));
    };
    if parent.driver_name().as_deref() != Some(expected_parent_driver_name) {
        return Err(Error::DevInfo(anyhow::anyhow!(
            "{} has non-{} parent node",
            node.node_name(),
            expected_parent_driver_name
        )));
    }
    Ok(parent)
}

fn i64_from_property(prop: &Property<'_>) -> Result<i64, Error> {
    prop.as_i64().ok_or_else(|| Error::UnexpectedPropertyType {
        name: prop.name(),
        ty: "i64".to_string(),
    })
}

fn string_from_property(prop: &Property<'_>) -> Result<String, Error> {
    prop.to_str().ok_or_else(|| Error::UnexpectedPropertyType {
        name: prop.name(),
        ty: "String".to_string(),
    })
}

// Looks up multiple property names on a devinfo node.
//
// Returns all the properties in the same order of the input names.
// Returns an error if any of the properties are missing.
fn find_properties<'a, const N: usize>(
    node: &'a Node<'_>,
    property_names: [&'static str; N],
) -> Result<[Property<'a>; N], Error> {
    // The properties could show up in any order, so first we place
    // them into a HashMap of "property name -> property".
    let name_set = HashSet::from(property_names);
    let mut properties = HashMap::new();
    for property in node.props() {
        let property = property.map_err(Error::DevInfo)?;
        if let Some(name) = name_set.get(property.name().as_str()) {
            properties.insert(*name, property);
        }
    }

    // Next, we convert the properties back to an array, with values in the same
    // indices as the input.
    let mut output = Vec::with_capacity(N);
    for name in &property_names {
        let Some(property) = properties.remove(name) else {
            return Err(Error::MissingDeviceProperty {
                node: node.node_name(),
                name: name.to_string(),
            });
        };
        output.push(property);
    }

    // Unwrap safety: the "output" vec should have one entry for each of the
    // "property_names", so they should have the same length (N).
    Ok(output.try_into().map_err(|_| "Unexpected output size").unwrap())
}

fn poll_blkdev_node(
    log: &Logger,
    disks: &mut HashSet<UnparsedDisk>,
    node: Node<'_>,
) -> Result<(), Error> {
    let Some(driver_name) = node.driver_name() else {
        return Ok(());
    };

    if driver_name != "blkdev" {
        return Ok(());
    }

    let devfs_path = node.devfs_path().map_err(Error::DevInfo)?;
    let dev_path = get_dev_path_of_whole_disk(&node)?;

    // libdevfs doesn't prepend "/devices" when referring to the path, but it
    // still returns an absolute path. This is the absolute path from the
    // kernel's perspective, but in userspace, it is typically mounted under
    // "/devices"
    //
    // Validate that we're still using this leading slash, and also make the
    // path usable.
    assert!(devfs_path.starts_with('/'));
    let devfs_path = format!("/devices{devfs_path}");

    let properties = find_properties(
        &node,
        ["inquiry-serial-no", "inquiry-product-id", "inquiry-vendor-id"],
    )?;
    let inquiry_serial_no = string_from_property(&properties[0])?;
    let inquiry_product_id = string_from_property(&properties[1])?;
    let inquiry_vendor_id = string_from_property(&properties[2])?;

    // We expect that the parent of the "blkdev" node is an "nvme" driver.
    let nvme_node = get_parent_node(&node, "nvme")?;

    let vendor_id =
        i64_from_property(&find_properties(&nvme_node, ["vendor-id"])?[0])?;

    // The model is generally equal to "inquiry-vendor-id" plus
    // "inquiry-product-id", separated by a space.
    //
    // However, libdevfs may emit a placeholder value for the
    // "inquiry-vendor-id", in which case it should be omitted.
    let model = match inquiry_vendor_id.as_str() {
        "" | "NVMe" => inquiry_product_id,
        _ => format!("{inquiry_vendor_id} {inquiry_product_id}"),
    };

    let device_id = DiskIdentity {
        vendor: format!("{:x}", vendor_id),
        serial: inquiry_serial_no,
        model,
    };

    // We expect that the parent of the "nvme" device is a "pcieb" driver.
    let pcieb_node = get_parent_node(&nvme_node, "pcieb")?;

    // The "pcieb" device needs to have a physical slot for us to understand
    // what type of disk it is.
    let slot = i64_from_property(
        &find_properties(&pcieb_node, ["physical-slot#"])?[0],
    )?;
    let Some(variant) = slot_to_disk_variant(slot) else {
        warn!(log, "Slot# {slot} is not recognized as a disk: {devfs_path}");
        return Err(Error::UnrecognizedSlot { slot } );
    };

    let disk = UnparsedDisk::new(
        PathBuf::from(&devfs_path),
        dev_path,
        slot,
        variant,
        device_id,
    );
    disks.insert(disk);
    Ok(())
}

// Performs a single walk of the device info tree, updating our view of hardware
// and sending notifications to any subscribers.
fn poll_device_tree(
    log: &Logger,
    inner: &Arc<Mutex<HardwareView>>,
    tx: &broadcast::Sender<HardwareUpdate>,
) -> Result<(), Error> {
    // Construct a view of hardware by walking the device tree.
    let polled_hw = HardwareSnapshot::new(log)?;

    // After inspecting the device tree, diff with the old view, and provide
    // necessary updates.
    let mut updates = vec![];
    {
        let mut inner = inner.lock().unwrap();
        inner.update_tofino(&polled_hw, &mut updates);
        inner.update_blkdev(&polled_hw, &mut updates);
        inner.baseboard = Some(polled_hw.baseboard);
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
        match poll_device_tree(&log, &inner, &tx) {
            // We've already warned about `NotAGimlet` by this point,
            // so let's not spam the logs.
            Ok(_) | Err(Error::NotAGimlet(_)) => (),
            Err(err) => {
                warn!(log, "Failed to query device tree: {err}");
            }
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
        match poll_device_tree(&log, &inner, &tx) {
            Ok(_) => (),
            // Allow non-gimlet devices to proceed with a "null" view of
            // hardware, otherwise they won't be able to start.
            Err(Error::NotAGimlet(root)) => {
                warn!(log, "Device is not a Gimlet ({root}), proceeding with null hardware view");
            }
            Err(err) => {
                return Err(format!("Failed to poll device tree: {err}"))
            }
        };

        let log2 = log.clone();
        let inner2 = inner.clone();
        let tx2 = tx.clone();
        let _worker = tokio::task::spawn(async move {
            hardware_tracking_task(log2, inner2, tx2).await
        });

        Ok(Self { log, inner, tx, _worker })
    }

    pub fn baseboard(&self) -> Baseboard {
        self.inner
            .lock()
            .unwrap()
            .baseboard
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Baseboard::unknown())
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
