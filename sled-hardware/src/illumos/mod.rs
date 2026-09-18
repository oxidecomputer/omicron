// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DiskFirmware;
use crate::ExternalDisks;
use crate::HardwareView;
use crate::TofinoSnapshot;
use crate::TofinoView;
use crate::disk_bay::{
    ClassifyError, NvmeFacts, ObservedBay, ObservedOccupant, classify_bay,
};
use crate::nvme_instance::NvmeInstance;
use crate::{DendriteAsic, DiskBay, DiskBayOccupant, SledMode, UnparsedDisk};
use gethostname::gethostname;
use illumos_devinfo::{DevInfo, Node, Property};
use libnvme::namespace::NamespaceDiscoveryLevel;
use libnvme::{Nvme, controller::Controller};
use sled_agent_types::disk::DiskIdentity;
use sled_hardware_types::{Baseboard, OxideSled, SledCpuFamily};
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use std::collections::{HashMap, HashSet};
use tokio::sync::watch;
use uuid::Uuid;

mod topo;

mod gpt;
mod partitions;
mod sysconf;

pub use partitions::{NvmeFormattingError, ensure_partition_layout};

const TOFINO_MONITOR: &'static str = "/opt/oxide/sled-agent/tofino-monitor";

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Failed to access devinfo: {0}")]
    DevInfo(anyhow::Error),

    #[error("Device does not appear to be an Oxide sled: {0}")]
    NotAnOxideSled(String),

    #[error("Invalid Utf8 path: {0}")]
    FromPathBuf(#[from] camino::FromPathBufError),

    #[error("Node {node} missing device property {name}")]
    MissingDeviceProperty { node: String, name: String },

    #[error("Invalid value for boot-storage-unit property: {0}")]
    InvalidBootStorageUnitValue(i64),

    #[error("Expected property {name} to have type {ty}")]
    UnexpectedPropertyType { name: String, ty: String },

    #[error("Failed to issue request to sysconf: {0}")]
    SysconfError(#[from] sysconf::Error),

    #[error("Failed to read disk bays from the hardware topology")]
    Topology(#[from] topo::TopoError),

    #[error("Could not describe a disk bay from the topology and libnvme")]
    Bay(#[from] ClassifyError),

    #[error("Disk {identity:?} appears in both {first} and {second}")]
    DuplicateDiskIdentity {
        identity: DiskIdentity,
        first: String,
        second: String,
    },

    #[error("Failed to init nvme handle: {0}")]
    NvmeHandleInit(#[from] libnvme::NvmeInitError),

    #[error("libnvme error: {0}")]
    Nvme(#[from] libnvme::NvmeError),

    #[error("libnvme controller error: {0}")]
    NvmeController(#[from] libnvme::controller::NvmeControllerError),

    #[error("libnvme controller info error: {0}")]
    NvmeInfo(#[from] libnvme::controller_info::NvmeInfoError),

    #[error("Unable to grab NVMe Controller lock")]
    NvmeControllerLocked,

    #[error("Failed to get NVMe Controller's firmware log page: {0}")]
    FirmwareLogPage(#[from] libnvme::firmware::FirmwareLogPageError),
}

/// Return true if the host system is an Oxide Sled.
pub fn is_oxide_sled() -> anyhow::Result<bool> {
    let mut device_info = DevInfo::new()?;
    let mut node_walker = device_info.walk_node();
    let Some(root) = node_walker.next().transpose()? else {
        anyhow::bail!("No nodes in device tree");
    };
    Ok(OxideSled::try_from_root_node_name(&root.node_name()).is_some())
}

impl TofinoSnapshot {
    fn new() -> Self {
        Self { exists: false, available: false }
    }
}

// Which BSU (i.e., host flash rom) slot did we boot from?
#[derive(Debug, Clone, Copy)]
enum BootStorageUnit {
    A,
    B,
}

impl TryFrom<i64> for BootStorageUnit {
    type Error = Error;

    fn try_from(raw: i64) -> Result<Self, Self::Error> {
        match raw {
            0x41 => Ok(Self::A),
            0x42 => Ok(Self::B),
            _ => Err(Error::InvalidBootStorageUnitValue(raw)),
        }
    }
}

// A snapshot of information about the underlying hardware
#[derive(Debug, Clone)]
struct HardwareSnapshot {
    tofino: TofinoSnapshot,
    disks: HashMap<DiskIdentity, UnparsedDisk>,
    disk_bays: Vec<DiskBay>,
}

impl HardwareSnapshot {
    // Walk the device tree to capture a view of the current hardware.
    fn new(
        log: &Logger,
        external_disks: &ExternalDisks,
    ) -> Result<Self, Error> {
        let mut device_info =
            DevInfo::new_force_load().map_err(Error::DevInfo)?;

        let mut node_walker = device_info.walk_node();

        // First, check the root node. If we aren't running on an Oxide sled,
        // bail.
        let Some(root) =
            node_walker.next().transpose().map_err(Error::DevInfo)?
        else {
            return Err(Error::DevInfo(anyhow::anyhow!(
                "No nodes in device tree"
            )));
        };
        let root_node = root.node_name();
        let Some(sled_type) = OxideSled::try_from_root_node_name(&root_node)
        else {
            return Err(Error::NotAnOxideSled(root_node));
        };

        let properties = find_properties(&root, ["boot-storage-unit"])?;
        let boot_storage_unit =
            BootStorageUnit::try_from(i64_from_property(&properties[0])?)?;

        // Monitor for the Tofino device and driver.
        let tofino = get_tofino_snapshot(log, &mut device_info);

        // Find the disks. The hardware topology says which bays exist and
        // what is behind each one; libnvme fills in what only the controller
        // knows. If either cannot answer, this snapshot cannot say which
        // disks exist, and the previous view stands until the next poll.
        let mut disks = HashMap::new();
        let mut disk_bays = Vec::new();
        match external_disks {
            ExternalDisks::DetectPhysical => {
                let observed = topo::read_disk_bays(log)?;
                let nvme = Nvme::new()?;
                let mut bay_of_disk: HashMap<DiskIdentity, String> =
                    HashMap::new();
                for bay in &observed {
                    let (described, disk) = poll_bay(
                        log,
                        &nvme,
                        sled_type,
                        boot_storage_unit,
                        bay,
                    )?;
                    if let Some(disk) = disk {
                        let identity = disk.identity().clone();
                        if let Some(first) = bay_of_disk
                            .insert(identity.clone(), bay.location.clone())
                        {
                            return Err(Error::DuplicateDiskIdentity {
                                identity,
                                first,
                                second: bay.location.clone(),
                            });
                        }
                        disks.insert(identity, disk);
                    }
                    disk_bays.push(described);
                }
            }

            ExternalDisks::Hardcoded { vdevs: _, disks: hardcoded_disks } => {
                // These stand in for real hardware, so give each one a bay.
                for disk in hardcoded_disks {
                    disk_bays.push(DiskBay {
                        location: disk.location().to_string(),
                        kind: disk.variant(),
                        occupant: DiskBayOccupant::Disk {
                            identity: disk.identity().clone(),
                        },
                    });
                    disks.insert(disk.identity().clone(), disk.clone());
                }
            }
        }

        Ok(Self { tofino, disks, disk_bays })
    }
}

impl HardwareView {
    // TODO-cleanup: We construct a mostly-empty `HardwareView` and rely on our
    // caller (`HardwareManager::new()`) filling in several of these fields with
    // correct values by calling `poll_device_tree()` _before_ it ever exposes a
    // `HardwareView` to its consumers. This isn't ideal (we leave a window open
    // where we return a mostly-empty view) but is not trivial to rework.
    fn new(
        baseboard: Baseboard,
        tofino: TofinoView,
        cpu_family: SledCpuFamily,
    ) -> Result<Self, Error> {
        Ok(Self {
            tofino,
            disks: HashMap::new(),
            disk_bays: Vec::new(),
            baseboard,
            online_processor_count: sysconf::online_processor_count()?,
            usable_physical_pages: sysconf::usable_physical_pages()?,
            usable_physical_ram_bytes: sysconf::usable_physical_ram_bytes()?,
            cpu_family,
        })
    }
}

fn pcie_slot_is_boot_disk(
    sled: OxideSled,
    pcie_slot: i64,
    boot_storage_unit: BootStorageUnit,
) -> bool {
    let slots = sled.bootdisk_pcie_slots();
    match boot_storage_unit {
        BootStorageUnit::A => slots[0] == pcie_slot,
        BootStorageUnit::B => slots[1] == pcie_slot,
    }
}

fn get_tofino_snapshot(log: &Logger, devinfo: &mut DevInfo) -> TofinoSnapshot {
    let (exists, available) = match tofino::get_tofino_from_devinfo(devinfo) {
        Ok(None) => (false, false),
        Ok(Some(node)) => (node.has_asic(), node.is_available()),
        Err(e) => {
            error!(log, "failed to get tofino state: {e:?}");
            (false, false)
        }
    };
    if exists {
        debug!(
            log,
            "Found tofino node, with asic {}available",
            if available { "" } else { "un" }
        );
    }
    TofinoSnapshot { exists, available }
}

/// Convert a property to a `u32` if possible, passing through an `i64`.
///
/// Returns an error if either:
///
/// - The actual devinfo property isn't an integer type.
/// - The value does not fit in a `u32`.
fn u32_from_property(prop: &Property<'_>) -> Result<u32, Error> {
    i64_from_property(prop).and_then(|val| {
        u32::try_from(val).map_err(|_| Error::UnexpectedPropertyType {
            name: prop.name(),
            ty: "u32".to_string(),
        })
    })
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

/// Describes one bay the topology reported and, when it holds an NVMe disk
/// with a namespace, the disk to manage.
fn poll_bay(
    log: &Logger,
    nvme: &Nvme,
    sled: OxideSled,
    boot_storage_unit: BootStorageUnit,
    bay: &ObservedBay,
) -> Result<(DiskBay, Option<UnparsedDisk>), Error> {
    let facts = match &bay.occupant {
        ObservedOccupant::Nvme { instance, namespaces, .. } => {
            if namespaces.len() > 1 {
                // Only the first namespace becomes a disk. See
                // https://github.com/oxidecomputer/omicron/issues/5241.
                warn!(
                    log,
                    "NVMe controller has more than one namespace; \
                     only the first is used";
                    "location" => &bay.location,
                    "nvme_instance" => %instance,
                    "namespaces" => namespaces.len(),
                );
            }
            Some(nvme_facts(log, nvme, *instance)?)
        }
        ObservedOccupant::Empty | ObservedOccupant::Other { .. } => None,
    };
    let is_boot_disk =
        pcie_slot_is_boot_disk(sled, bay.pcie_slot, boot_storage_unit);
    Ok(classify_bay(bay, facts.as_ref(), is_boot_disk)?)
}

/// Asks libnvme for what the topology does not record about a controller:
/// its PCI vendor id, how many namespaces are active, and its firmware
/// slots.
fn nvme_facts(
    log: &Logger,
    nvme: &Nvme,
    instance: NvmeInstance,
) -> Result<NvmeFacts, Error> {
    let controller = Controller::init_by_instance(nvme, instance.as_i32())?;
    let pci_vid = controller.get_info()?.pci_vid()?;

    let mut active_namespaces = 0;
    for namespace in
        controller.namespace_discovery(NamespaceDiscoveryLevel::Active)?
    {
        namespace?;
        active_namespaces += 1;
    }

    let controller_lock = match controller.try_read_lock() {
        libnvme::controller::TryLockResult::Ok(locked) => locked,
        // We should only hit this if something in the system has locked the
        // controller in question for writing.
        libnvme::controller::TryLockResult::Locked(_) => {
            warn!(
                log,
                "NVMe Controller is already locked so we will try again
                in the next hardware snapshot"
            );
            return Err(Error::NvmeControllerLocked);
        }
        libnvme::controller::TryLockResult::Err(err) => {
            return Err(Error::from(err));
        }
    };
    let firmware_log_page = controller_lock.get_firmware_log_page()?;
    let firmware = DiskFirmware::new(
        firmware_log_page.active_slot,
        firmware_log_page.next_active_slot,
        firmware_log_page.slot1_is_read_only,
        firmware_log_page.number_of_slots,
        firmware_log_page.slot_iter().map(|s| s.map(str::to_string)).collect(),
    );

    Ok(NvmeFacts { pci_vid, active_namespaces, firmware })
}

// Poll just enough of the device info tree to get the Baseboard. We really
// don't want to deal with an `Option` upstream.
fn learn_baseboard(log: &Logger) -> Result<Baseboard, Error> {
    let mut device_info = DevInfo::new_force_load().map_err(Error::DevInfo)?;

    let mut node_walker = device_info.walk_node();

    // First, check the root node. If we aren't running on an Oxide sled,
    // bail.
    let Some(root) = node_walker.next().transpose().map_err(Error::DevInfo)?
    else {
        return Err(Error::DevInfo(anyhow::anyhow!("No nodes in device tree")));
    };
    let root_node = root.node_name();
    let Some(sled_type) = OxideSled::try_from_root_node_name(&root_node) else {
        return Ok(get_pc_baseboard(log, root_node.as_str()));
    };

    let properties = find_properties(
        &root,
        ["baseboard-identifier", "baseboard-model", "baseboard-revision"],
    )?;

    let baseboard = Baseboard::new_oxide_sled(
        sled_type,
        string_from_property(&properties[0])?,
        string_from_property(&properties[1])?,
        u32_from_property(&properties[2])?,
    );

    info!(log, "Found baseboard for Oxide hardware"; "baseboard" => %baseboard);

    Ok(baseboard)
}

// Performs a single walk of the device info tree, updating the view of hardware
// kept in the `hardware_view_tx` watch channel if anything has changed.
fn poll_device_tree(
    log: &Logger,
    hardware_view_tx: &watch::Sender<HardwareView>,
    external_disks: &ExternalDisks,
) -> Result<(), Error> {
    // Construct a view of hardware by walking the device tree.
    let polled_hw = HardwareSnapshot::new(log, external_disks);
    let polled_hw = match polled_hw {
        Ok(polled_hw) => polled_hw,

        Err(e) => {
            if let Error::NotAnOxideSled(_) = &e {
                hardware_view_tx.send_if_modified(|inner| {
                    let mut did_modify = false;

                    // For platforms that don't support the HardwareSnapshot
                    // functionality, sled-agent can be supplied a fixed list of
                    // UnparsedDisks. Add those to the HardwareSnapshot here if
                    // they are missing (which they will be for non-sleds).
                    if let ExternalDisks::Hardcoded { vdevs: _, disks } =
                        external_disks
                    {
                        for disk in disks {
                            let identity = disk.identity();
                            if !inner.disks.contains_key(identity) {
                                inner
                                    .disks
                                    .insert(identity.clone(), disk.clone());
                                did_modify = true;
                            }
                        }
                    }

                    did_modify
                });
            }

            return Err(e);
        }
    };

    let HardwareSnapshot {
        tofino: polled_tofino,
        disks: polled_disks,
        disk_bays: polled_bays,
    } = polled_hw;

    // Check for any changes since the last view.
    let mut did_modify_tofino = false;
    let mut did_modify_disks = false;
    hardware_view_tx.send_if_modified(|inner| {
        did_modify_tofino = match &mut inner.tofino {
            TofinoView::Real(inner_snapshot) => {
                if *inner_snapshot == polled_tofino {
                    false
                } else {
                    *inner_snapshot = polled_tofino;
                    true
                }
            }
            TofinoView::Stub { .. } => false,
        };

        did_modify_disks =
            if inner.disks == polled_disks && inner.disk_bays == polled_bays {
                false
            } else {
                inner.disks = polled_disks.clone();
                inner.disk_bays = polled_bays.clone();
                true
            };

        did_modify_tofino || did_modify_disks
    });

    info!(
        log, "Completed poll of device tree";
        "did_modify_tofino" => did_modify_tofino,
        "did_modify_disks" => did_modify_disks,
    );
    if did_modify_tofino {
        info!(log, "Updated tofino"; "tofino" => ?polled_tofino);
    }
    if did_modify_disks {
        info!(
            log, "Updated disks";
            "disks" => ?polled_disks,
            "disk_bays" => ?polled_bays,
        );
    }

    Ok(())
}

// Using an external process, watch for the disappearance of a tofino device.
fn monitor_tofino(
    log: slog::Logger,
    hardware_view_tx: watch::Sender<HardwareView>,
) {
    match std::fs::exists(TOFINO_MONITOR) {
        Err(_) | Ok(false) => {
            error!(&log, "tofino monitor tool not found at {}", TOFINO_MONITOR);
            return;
        }
        _ => {}
    };

    loop {
        let mut child = match std::process::Command::new(TOFINO_MONITOR).spawn()
        {
            Ok(c) => {
                debug!(
                    &log,
                    "launched tofino monitor process as pid {}",
                    c.id()
                );
                c
            }
            Err(e) => {
                error!(&log, "failed to launch tofino monitor: {e:?}");
                return;
            }
        };

        // This child process should only exit if it has seen a tofino ASIC and
        // then received a "tofino removed" event from the kernel.  If that
        // happens, it should exit cleanly with an exit code of 0.  If the child
        // exits for any other reason, we don't know what it means, and can only
        // log the event for posterity.  In either case, we send a message to
        // the hardware monitor task indicating that the Tofino is gone, and it
        // will perform any necessary cleanup.  This cleanup will include
        // shutting down the switch zone if, for some reason, it still exists.
        match child.wait() {
            Err(e) => error!(&log, "failed to collect exit status: {e:?}"),
            Ok(s) => info!(&log, "child exited with code: {:?}", s.code()),
        }
        hardware_view_tx.send_if_modified(|inner| {
            match &mut inner.tofino {
                TofinoView::Real(snapshot) => {
                    if snapshot.available {
                        snapshot.available = false;
                        true // we did modify the value
                    } else {
                        // tofino already unavailable; no changes
                        false
                    }
                }
                TofinoView::Stub { .. } => false,
            }
        });

        // By the time we get here, the switch zone should be gone and the
        // device tree cleaned up.  Still, it doesn't hurt to give things a
        // moment to stabilize before restarting the monitor.
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

/// Return a `Baseboard` for non-Oxide hardware
///
/// The device tree is not populated with `Baseboard` information on non-Oxide
/// hardware. Therefore we must construct one. For backwards compatibility
/// with the Canada region we continue to use the hostname as the serial number
/// in the common case. However, first we check the smbios type1 table to see
/// if we are running in a4x2. If so we use the configured information from
/// the smbios. This allows us to match the generated certificates used for
/// sprockets so that trust quorum works in a4x2.
fn get_pc_baseboard(log: &Logger, root_node: &str) -> Baseboard {
    let pc_baseboard = read_smbios(log).unwrap_or_else(|| {
        Baseboard::new_pc(
            gethostname()
                .into_string()
                .unwrap_or_else(|_| Uuid::new_v4().simple().to_string()),
            root_node.to_string(),
        )
    });
    info!(log, "Generated {} baseboard {:?}", root_node, pc_baseboard);
    pc_baseboard
}

/// Read the smbios type 1 information. If the manufacturer is "a4x2" then construct
/// and return a `Baseboard`. Otherwise, return None.
fn read_smbios(log: &Logger) -> Option<Baseboard> {
    let output = std::process::Command::new("/usr/sbin/smbios")
        .args(["-t", "1"])
        .output()
        .ok()?;

    parse_smbios_output(log, String::from_utf8(output.stdout).ok()?)
}

/// Parse the smbios input returning a `Baseboard` if the manufacturer is `a4x2`
/// and parsing completes successfully.
fn parse_smbios_output(log: &Logger, output: String) -> Option<Baseboard> {
    let mut iter = output.lines().skip(3);

    let log = log.new(o!("component" => "HardwareManager: parse_smbios"));

    // Parse manufacturer
    let mut manufacturer_iter = iter.next()?.split(":");
    if manufacturer_iter.next()?.trim() != "Manufacturer" {
        info!(log, "Missing manufacturer key: skipping");
        return None;
    }
    if manufacturer_iter.next()?.trim() != "a4x2" {
        info!(log, "Manufacturer is not a4x2: skipping");
        return None;
    }

    // Parse Product
    let mut product_iter = iter.next()?.split(":");
    if product_iter.next()?.trim() != "Product" {
        info!(log, "Missing product key: skipping");
        return None;
    }
    let product = product_iter.next()?.trim().to_string();

    // Parse Serial Number
    let mut serial_iter = iter.nth(1)?.split(":");
    if serial_iter.next()?.trim() != "Serial Number" {
        info!(log, "Missing serial number key: skipping");
        return None;
    }
    let serial_number = serial_iter.next()?.trim().to_string();

    Some(Baseboard::new_pc(serial_number, product))
}

fn hardware_tracking_task(
    log: Logger,
    hardware_view_tx: watch::Sender<HardwareView>,
    external_disks: ExternalDisks,
) {
    loop {
        match poll_device_tree(&log, &hardware_view_tx, &external_disks) {
            // We've already warned about `NotAnOxideSled` by this point,
            // so let's not spam the logs.
            Ok(_) | Err(Error::NotAnOxideSled(_)) => (),
            Err(err) => {
                warn!(log, "Failed to query device tree: {err}");
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

/// A representation of the underlying hardware.
///
/// This is a wrapper around a watch channel, mostly for historical reasons. We
/// provide various methods that delegate down to the inner value. For callers
/// that need real access to a watch channel (e.g., to monitor for `changed()`
/// events), use [`HardwareManager::subscribe()`].
#[derive(Clone)]
pub struct HardwareManager {
    hardware_view_rx: watch::Receiver<HardwareView>,
}

impl HardwareManager {
    /// Creates a new representation of the underlying hardware, and initializes
    /// a task which periodically updates that representation.
    ///
    /// Arguments:
    /// - `sled_mode`: The sled's mode of operation (auto detect or force gimlet/scrimlet).
    /// - `nonsled_observed_disks`: For non-sleds, inject these disks into
    ///    HardwareSnapshot objects.
    pub fn new(
        log: &Logger,
        sled_mode: SledMode,
        external_disks: ExternalDisks,
    ) -> Result<Self, String> {
        let log = log.new(o!("component" => "HardwareManager"));
        info!(log, "Creating HardwareManager");
        let cpu_family = crate::detect_cpu_family(&log);

        let baseboard = learn_baseboard(&log).map_err(|err| {
            format!("Failed to learn baseboard via device tree: {err}")
        })?;

        let hw =
            match sled_mode {
                // Treat as a possible scrimlet and setup to scan for real Tofino device.
                SledMode::Auto
                | SledMode::Scrimlet { asic: DendriteAsic::TofinoAsic } => {
                    HardwareView::new(
                        baseboard,
                        TofinoView::Real(TofinoSnapshot::new()),
                        cpu_family,
                    )
                }

                // Treat sled as gimlet and ignore any attached Tofino device.
                SledMode::Sled => HardwareView::new(
                    baseboard,
                    TofinoView::Stub { active: false },
                    cpu_family,
                ),

                // Treat as scrimlet and use the stub Tofino device.
                SledMode::Scrimlet { asic: DendriteAsic::TofinoStub } => {
                    HardwareView::new(
                        baseboard,
                        TofinoView::Stub { active: true },
                        cpu_family,
                    )
                }

                // Treat as scrimlet (w/ SoftNPU) and use the stub Tofino device.
                // TODO-correctness:
                // I'm not sure whether or not we should be treating softnpu
                // as a stub or treating it as a different HardwareView variant,
                // so this might change.
                SledMode::Scrimlet {
                    asic:
                        DendriteAsic::SoftNpuZone
                        | DendriteAsic::SoftNpuPropolisDevice,
                } => HardwareView::new(
                    baseboard,
                    TofinoView::Stub { active: true },
                    cpu_family,
                ),
            }
            .map_err(|e| e.to_string())?;
        let (hardware_view_tx, hardware_view_rx) = watch::channel(hw);

        // Force the device tree to be polled at least once before returning.
        // This mitigates issues where the Sled Agent could try to propagate
        // an "empty" view of hardware to other consumers before the first
        // query.
        match poll_device_tree(&log, &hardware_view_tx, &external_disks) {
            Ok(_) => (),
            // Allow non-sled devices to proceed with a "null" view of
            // hardware, otherwise they won't be able to start.
            Err(Error::NotAnOxideSled(root)) => {
                warn!(
                    log,
                    "Device is not an Oxide sled ({root}), proceeding with null hardware view"
                );
            }
            Err(err) => {
                return Err(format!("Failed to poll device tree: {err}"));
            }
        };

        // We poll the device tree to detect new tofinos and disks.  This
        // polling also detects devices that have gone away, but we need to
        // respond to a tofino disappearance more quickly than a regular polling
        // interval will allow.  To that end, we fire off a task that maintains
        // a device contract with the kernel to handle those disappearances.
        let log2 = log.clone();
        let tx2 = hardware_view_tx.clone();
        std::thread::spawn(move || {
            monitor_tofino(log2, tx2);
        });

        std::thread::spawn(move || {
            hardware_tracking_task(log, hardware_view_tx, external_disks);
        });

        Ok(Self { hardware_view_rx })
    }

    pub fn baseboard(&self) -> Baseboard {
        self.hardware_view_rx.borrow().baseboard()
    }

    pub fn cpu_family(&self) -> SledCpuFamily {
        self.hardware_view_rx.borrow().cpu_family()
    }

    pub fn online_processor_count(&self) -> u32 {
        self.hardware_view_rx.borrow().online_processor_count()
    }

    pub fn usable_physical_pages(&self) -> u64 {
        self.hardware_view_rx.borrow().usable_physical_pages()
    }

    pub fn usable_physical_ram_bytes(&self) -> u64 {
        self.hardware_view_rx.borrow().usable_physical_ram_bytes()
    }

    pub fn disks(&self) -> HashMap<DiskIdentity, UnparsedDisk> {
        self.hardware_view_rx.borrow().disks()
    }

    pub fn is_scrimlet(&self) -> bool {
        self.hardware_view_rx.borrow().is_scrimlet()
    }

    pub fn subscribe(&self) -> watch::Receiver<HardwareView> {
        self.hardware_view_rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::illumos::parse_smbios_output;
    use omicron_test_utils::dev::test_setup_log;

    #[test]
    fn parse_smbios_valid() {
        let logctx = test_setup_log("parse_smbios_valid");

        let output = "\
ID    SIZE TYPE
1     372  SMB_TYPE_SYSTEM (type 1) (system information)

  Manufacturer: a4x2
  Product: PPP-PPPPPPP
  Version: 0
  Serial Number: 0000000000

  UUID: faaad566-debf-d311-01a6-9c6b00871b75
  UUID (Endian-corrected): 66d5aafa-bfde-11d3-01a6-9c6b00871b75
  Wake-Up Event: 0x6 (power switch)
  SKU Number: To be filled by O.E.M.
  Family: To be filled by O.E.M.
"
        .to_string();

        let baseboard = parse_smbios_output(&logctx.log, output).unwrap();
        assert_eq!(baseboard.model(), "PPP-PPPPPPP");
        assert_eq!(baseboard.identifier(), "0000000000");
        logctx.cleanup_successful();
    }

    #[test]
    fn parse_smbios_invalid() {
        let logctx = test_setup_log("parse_smbios_invalid");

        let output = "\
ID    SIZE TYPE
1     372  SMB_TYPE_SYSTEM (type 1) (system information)

dfadfasdfasdfdf  Manufacturer: a4x2
  Product: PPP-PPPPPPP
  Version: 0
  Serial Number: 0000000000
"
        .to_string();

        assert!(parse_smbios_output(&logctx.log, output).is_none());
        logctx.cleanup_successful();
    }
}
