// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DiskFirmware;
use crate::DiskHealthMap;
use crate::ExternalDisks;
use crate::HardwareView;
use crate::TofinoSnapshot;
use crate::TofinoView;
use crate::disk_bay::{
    BayDescription, DescribeError, NvmeFacts, ObservedBay, PolledBay,
    PolledDisk, collect_polled_bays, describe_bay_from_topology, resolve_bays,
};
use crate::nvme_instance::NvmeInstance;
use crate::{DendriteAsic, DiskBay, DiskBayOccupant, SledMode, UnparsedDisk};
use chrono::{DateTime, Utc};
use gethostname::gethostname;
use iddqd::IdOrdMap;
use illumos_devinfo::{DevInfo, Node, Property};
use libnvme::health::{HealthLogPage, HealthLogPageError};
use libnvme::namespace::NamespaceDiscoveryLevel;
use libnvme::{Nvme, controller::Controller};
use libtopo::{Snapshot, TopoHdl};
use sled_agent_types::disk::DiskIdentity;
use sled_agent_types::inventory::{
    NvmeCriticalWarnings, NvmeEnduranceGroupWarnings, NvmeHealth,
    NvmeHealthError, NvmeThermalManagement,
};
use sled_hardware_types::{Baseboard, OxideSled, SledCpuFamily};
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

mod topo;

mod gpt;
mod partitions;
mod softnpu;
mod sysconf;

pub use partitions::{NvmeFormattingError, ensure_partition_layout};
pub use softnpu::find_softnpu_device;

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

    #[error(transparent)]
    Topology(#[from] topo::TopoError),

    #[error("Failed to init nvme handle")]
    NvmeHandleInit(#[from] libnvme::NvmeInitError),

    #[error("libnvme error")]
    Nvme(#[from] libnvme::NvmeError),

    #[error("libnvme controller error")]
    NvmeController(#[from] libnvme::controller::NvmeControllerError),

    #[error("libnvme controller info error")]
    NvmeInfo(#[from] libnvme::controller_info::NvmeInfoError),

    #[error("Unable to grab NVMe Controller lock")]
    NvmeControllerLocked,

    #[error("Failed to get NVMe Controller's firmware log page")]
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
#[derive(Debug)]
struct HardwareSnapshot {
    tofino: TofinoSnapshot,
    /// The bays which have been observed by this poll. Note that transient
    /// errors, or errors with individual bays, may cause partial information
    /// to be observed. Therefore, we translate this "point-in-time" view into
    /// a "usable" view of disks later, in `resolve_bays`.
    ///
    /// See the `disk_bay` module docs for more details.
    ///
    /// `Err` only if disk bays could not be read at all this time.
    bays: Result<IdOrdMap<PolledBay>, Error>,
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

        // Each poll takes a single topology snapshot, which is used for all
        // data gathered from libtopo in that poll. A topo handle allows only
        // one snapshot (see
        // <https://github.com/oxidecomputer/libtopo/issues/14>), so we open a
        // new handle for each poll.
        let topo_hdl;
        let topo_snap = match TopoHdl::open() {
            Ok(hdl) => {
                topo_hdl = hdl;
                topo_hdl.snapshot()
            }
            Err(err) => Err(err),
        };

        let bays = match external_disks {
            ExternalDisks::DetectPhysical => match topo_snap {
                Ok(ref snap) => {
                    poll_bays(log, snap, sled_type, boot_storage_unit)
                }
                Err(err) => Err(topo::TopoError::from(err).into()),
            },

            // These stand in for real hardware, so give each one a bay.
            ExternalDisks::Hardcoded { vdevs: _, disks } => {
                let now = Utc::now();
                let bays = disks.iter().map(|disk| PolledBay::Described {
                    bay: DiskBay {
                        location: disk.location().clone(),
                        kind: disk.variant(),
                        occupant: DiskBayOccupant::Disk {
                            identity: disk.identity().clone(),
                        },
                    },
                    disk: Some(Box::new(PolledDisk {
                        disk: disk.clone(),
                        health: Ok(NvmeHealth::new_fake(now)),
                    })),
                });
                Ok(collect_polled_bays(log, bays))
            }
        };

        Ok(Self { tofino, bays })
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
            disk_bays: IdOrdMap::new(),
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

/// Reads every bay from the hardware topology and describes each one.
fn poll_bays(
    log: &Logger,
    snap: &Snapshot<'_>,
    sled: OxideSled,
    boot_storage_unit: BootStorageUnit,
) -> Result<IdOrdMap<PolledBay>, Error> {
    let observed = topo::read_disk_bays(log, snap)?;
    let nvme = Nvme::new()?;
    let bays = observed
        .iter()
        .map(|bay| poll_bay(log, &nvme, sled, boot_storage_unit, bay));
    Ok(collect_polled_bays(log, bays))
}

/// Describes one bay reported by the topology snapshot, and the NVMe disk
/// in that bay (if it contains one).
///
/// Returns `PolledBay::Unreadable` if the topology or libnvme could not
/// describe the bay.
fn poll_bay(
    log: &Logger,
    nvme: &Nvme,
    sled: OxideSled,
    boot_storage_unit: BootStorageUnit,
    bay: &ObservedBay,
) -> PolledBay {
    let unreadable = |error: String| PolledBay::Unreadable {
        location: bay.location.clone(),
        kind: bay.kind,
        driver: bay.binding_driver.clone(),
        error,
    };
    let could_not_describe = |err: DescribeError| {
        let err = InlineErrorChain::new(&err);
        warn!(
            log,
            "Could not describe disk bay";
            "location" => %bay.location,
            &err,
        );
        unreadable(err.to_string())
    };

    let nvme_bay = match describe_bay_from_topology(bay) {
        Ok(BayDescription::Complete(bay)) => {
            return PolledBay::Described { bay, disk: None };
        }
        Ok(BayDescription::NeedsNvmeFacts(nvme_bay)) => nvme_bay,
        Err(err) => return could_not_describe(err),
    };

    let instance = nvme_bay.instance();
    let (facts, health) = match nvme_facts(nvme, instance) {
        Ok(facts) => facts,
        Err(err) => {
            let err = InlineErrorChain::new(&err);
            warn!(
                log,
                "Failed to query NVMe controller";
                "location" => %bay.location,
                "nvme_instance" => %instance,
                &err,
            );
            return unreadable(format!(
                "failed to query NVMe controller {instance}: {err}"
            ));
        }
    };
    let is_boot_disk =
        pcie_slot_is_boot_disk(sled, nvme_bay.pcie_slot(), boot_storage_unit);
    match nvme_bay.combine_with_nvme_facts(&facts, is_boot_disk) {
        Ok((bay, disk)) => {
            let disk = disk.map(|disk| {
                let health = health.map_err(|err| {
                    let err = InlineErrorChain::new(&err);
                    warn!(
                        log,
                        "Failed to read NVMe health log page";
                        "location" => %bay.location,
                        "nvme_instance" => %instance,
                        &err,
                    );
                    NvmeHealthError { message: err.to_string() }
                });
                Box::new(PolledDisk { disk, health })
            });
            PolledBay::Described { bay, disk }
        }
        Err(err) => could_not_describe(err),
    }
}

/// Asks libnvme for the controller details that the topology does not report:
/// its PCI vendor id, how many namespaces it has that are not ignored by the
/// nvme driver, and its firmware slots.
///
/// Also reads the controller's SMART / Health Information log page. A failure
/// to read it is returned alongside the other details, rather than in place
/// of them.
fn nvme_facts(
    nvme: &Nvme,
    instance: NvmeInstance,
) -> Result<(NvmeFacts, Result<NvmeHealth, HealthLogPageError>), Error> {
    let controller = Controller::init_by_instance(nvme, instance.as_i32())?;
    let pci_vid = controller.get_info()?.pci_vid()?;

    let mut usable_namespaces = 0;
    for namespace in
        controller.namespace_discovery(NamespaceDiscoveryLevel::NotIgnored)?
    {
        namespace?;
        usable_namespaces += 1;
    }

    let controller_lock = match controller.try_read_lock() {
        libnvme::controller::TryLockResult::Ok(locked) => locked,
        // We should only hit this if something in the system has locked the
        // controller in question for writing.
        libnvme::controller::TryLockResult::Locked(_) => {
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

    let health = controller_lock
        .get_health_log_page()
        .map(|page| nvme_health_from_log_page(page, Utc::now()));

    Ok((NvmeFacts { pci_vid, usable_namespaces, firmware }, health))
}

fn nvme_health_from_log_page(
    page: HealthLogPage,
    time_collected: DateTime<Utc>,
) -> NvmeHealth {
    // No controller can plausibly reach a count this large, so there is
    // nothing better to do with one than clamp it.
    let saturate = |count: u128| u64::try_from(count).unwrap_or(u64::MAX);

    let HealthLogPage {
        critical_warnings,
        endurance_group_warnings,
        temperature,
        available_spare_percent,
        available_spare_threshold_percent,
        life_used_percent,
        data_read_bytes,
        data_written_bytes,
        host_read_commands,
        host_write_commands,
        controller_busy_minutes,
        power_cycles,
        power_on_hours,
        unsafe_shutdowns,
        media_errors,
        error_log_entries,
        warning_temperature_minutes,
        critical_temperature_minutes,
        temperature_sensors,
        thermal_management,
    } = page;

    NvmeHealth {
        time_collected,
        critical_warnings: NvmeCriticalWarnings {
            available_spare_low: critical_warnings.available_spare_low,
            temperature: critical_warnings.temperature,
            reliability_degraded: critical_warnings.reliability_degraded,
            read_only: critical_warnings.read_only,
            volatile_memory_backup_failed: critical_warnings
                .volatile_memory_backup_failed,
            persistent_memory_region_read_only: critical_warnings
                .persistent_memory_region_read_only,
            indeterminate_personality_state: critical_warnings
                .indeterminate_personality_state,
        },
        endurance_group_warnings: endurance_group_warnings.map(|warnings| {
            NvmeEnduranceGroupWarnings {
                available_spare_low: warnings.available_spare_low,
                reliability_degraded: warnings.reliability_degraded,
                read_only: warnings.read_only,
            }
        }),
        temperature_kelvin: temperature.map(|t| t.kelvin()),
        available_spare_percent,
        available_spare_threshold_percent,
        life_used_percent,
        data_read_bytes: saturate(data_read_bytes),
        data_written_bytes: saturate(data_written_bytes),
        host_read_commands: saturate(host_read_commands),
        host_write_commands: saturate(host_write_commands),
        controller_busy_minutes: saturate(controller_busy_minutes),
        power_cycles: saturate(power_cycles),
        power_on_hours: saturate(power_on_hours),
        unsafe_shutdowns: saturate(unsafe_shutdowns),
        media_errors: saturate(media_errors),
        error_log_entries: saturate(error_log_entries),
        warning_temperature_minutes,
        critical_temperature_minutes,
        temperature_sensors_kelvin: temperature_sensors
            .map(|sensor| sensor.map(|t| t.kelvin())),
        thermal_management: thermal_management.map(|thermal| {
            NvmeThermalManagement {
                temp1_transitions: thermal.temp1_transitions,
                temp2_transitions: thermal.temp2_transitions,
                temp1_seconds: thermal.temp1_seconds,
                temp2_seconds: thermal.temp2_seconds,
            }
        }),
    }
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
//
// The disk health readings in `disk_health_tx` are replaced on every poll,
// with only what this poll read.
fn poll_device_tree(
    log: &Logger,
    hardware_view_tx: &watch::Sender<HardwareView>,
    disk_health_tx: &watch::Sender<Arc<DiskHealthMap>>,
    external_disks: &ExternalDisks,
) -> Result<(), Error> {
    // Construct a view of hardware by walking the device tree.
    let polled_hw = HardwareSnapshot::new(log, external_disks);
    let polled_hw = match polled_hw {
        Ok(polled_hw) => polled_hw,

        Err(e) => {
            // No hardware was read, but hardcoded disks are not hardware.
            let disk_health = match external_disks {
                ExternalDisks::Hardcoded { vdevs: _, disks } => {
                    let now = Utc::now();
                    disks
                        .iter()
                        .map(|disk| {
                            let health = Ok(NvmeHealth::new_fake(now));
                            (disk.identity().clone(), health)
                        })
                        .collect()
                }
                ExternalDisks::DetectPhysical => DiskHealthMap::new(),
            };
            disk_health_tx.send_replace(Arc::new(disk_health));

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

    let HardwareSnapshot { tofino: polled_tofino, bays: polled_bays } =
        polled_hw;

    let polled_bays = polled_bays.map_err(|err| {
        warn!(log, "Failed to read disk bays"; InlineErrorChain::new(&err));
        InlineErrorChain::new(&err).to_string()
    });
    // Disks in bays this poll could not describe, or did not find, stay in
    // the view.
    let (polled_disks, polled_bays, polled_disk_health) = {
        let previous = hardware_view_tx.borrow();
        resolve_bays(log, polled_bays, &previous.disk_bays, &previous.disks)
    };

    // Health is published before the disks it describes, so that a reader
    // which looks up disks and then their health finds a reading for any
    // disk this poll added.
    disk_health_tx.send_replace(Arc::new(polled_disk_health));

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
    disk_health_tx: watch::Sender<Arc<DiskHealthMap>>,
    external_disks: ExternalDisks,
) {
    loop {
        match poll_device_tree(
            &log,
            &hardware_view_tx,
            &disk_health_tx,
            &external_disks,
        ) {
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
    disk_health_rx: watch::Receiver<Arc<DiskHealthMap>>,
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
        let (disk_health_tx, disk_health_rx) = watch::channel(Arc::default());

        // Force the device tree to be polled at least once before returning.
        // This mitigates issues where the Sled Agent could try to propagate
        // an "empty" view of hardware to other consumers before the first
        // query.
        match poll_device_tree(
            &log,
            &hardware_view_tx,
            &disk_health_tx,
            &external_disks,
        ) {
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
            hardware_tracking_task(
                log,
                hardware_view_tx,
                disk_health_tx,
                external_disks,
            );
        });

        Ok(Self { hardware_view_rx, disk_health_rx })
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

    pub fn disk_bays(&self) -> IdOrdMap<DiskBay> {
        self.hardware_view_rx.borrow().disk_bays().clone()
    }

    pub fn is_scrimlet(&self) -> bool {
        self.hardware_view_rx.borrow().is_scrimlet()
    }

    pub fn subscribe(&self) -> watch::Receiver<HardwareView> {
        self.hardware_view_rx.clone()
    }

    /// Returns the disk health readings from the latest hardware poll.
    ///
    /// Unlike [`HardwareManager::subscribe`], this changes on nearly every
    /// poll, so it is not useful to wait on.
    pub fn subscribe_disk_health(&self) -> watch::Receiver<Arc<DiskHealthMap>> {
        self.disk_health_rx.clone()
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

    #[test]
    fn nvme_health_from_log_page_converts_fields() {
        use crate::illumos::nvme_health_from_log_page;
        use chrono::Utc;
        use libnvme::health::{
            CriticalWarnings, EnduranceGroupWarnings, HealthLogPage,
            ThermalManagement,
        };
        use libnvme::temperature::Temperature;
        use sled_agent_types::inventory::{
            NvmeCriticalWarnings, NvmeEnduranceGroupWarnings,
        };

        let kelvin = |k| Temperature::from_kelvin(k);
        let mut temperature_sensors = [None; 8];
        temperature_sensors[0] = kelvin(321);
        temperature_sensors[7] = kelvin(328);
        let page = HealthLogPage {
            critical_warnings: CriticalWarnings {
                available_spare_low: true,
                temperature: false,
                reliability_degraded: true,
                read_only: false,
                volatile_memory_backup_failed: Some(true),
                persistent_memory_region_read_only: Some(false),
                indeterminate_personality_state: None,
            },
            endurance_group_warnings: Some(EnduranceGroupWarnings {
                available_spare_low: false,
                reliability_degraded: true,
                read_only: false,
            }),
            temperature: kelvin(320),
            available_spare_percent: 99,
            available_spare_threshold_percent: 10,
            life_used_percent: 3,
            data_read_bytes: 512_000,
            // Too large for a u64.
            data_written_bytes: u128::from(u64::MAX) + 1,
            host_read_commands: 1,
            host_write_commands: 2,
            controller_busy_minutes: 3,
            power_cycles: 4,
            power_on_hours: 5,
            unsafe_shutdowns: 6,
            media_errors: 7,
            error_log_entries: 8,
            warning_temperature_minutes: Some(9),
            critical_temperature_minutes: None,
            temperature_sensors,
            thermal_management: Some(ThermalManagement {
                temp1_transitions: 10,
                temp2_transitions: 11,
                temp1_seconds: 12,
                temp2_seconds: 13,
            }),
        };

        let time_collected = Utc::now();
        let health = nvme_health_from_log_page(page, time_collected);

        assert_eq!(health.time_collected, time_collected);
        assert_eq!(
            health.critical_warnings,
            NvmeCriticalWarnings {
                available_spare_low: true,
                temperature: false,
                reliability_degraded: true,
                read_only: false,
                volatile_memory_backup_failed: Some(true),
                persistent_memory_region_read_only: Some(false),
                indeterminate_personality_state: None,
            }
        );
        assert_eq!(
            health.endurance_group_warnings,
            Some(NvmeEnduranceGroupWarnings {
                available_spare_low: false,
                reliability_degraded: true,
                read_only: false,
            })
        );
        assert_eq!(health.temperature_kelvin, Some(320));
        assert_eq!(health.available_spare_percent, 99);
        assert_eq!(health.available_spare_threshold_percent, 10);
        assert_eq!(health.life_used_percent, 3);
        assert_eq!(health.data_read_bytes, 512_000);
        assert_eq!(health.data_written_bytes, u64::MAX);
        assert_eq!(health.host_read_commands, 1);
        assert_eq!(health.host_write_commands, 2);
        assert_eq!(health.controller_busy_minutes, 3);
        assert_eq!(health.power_cycles, 4);
        assert_eq!(health.power_on_hours, 5);
        assert_eq!(health.unsafe_shutdowns, 6);
        assert_eq!(health.media_errors, 7);
        assert_eq!(health.error_log_entries, 8);
        assert_eq!(health.warning_temperature_minutes, Some(9));
        assert_eq!(health.critical_temperature_minutes, None);
        assert_eq!(
            health.temperature_sensors_kelvin,
            [Some(321), None, None, None, None, None, None, Some(328)]
        );
        let thermal = health.thermal_management.unwrap();
        assert_eq!(thermal.temp1_transitions, 10);
        assert_eq!(thermal.temp2_transitions, 11);
        assert_eq!(thermal.temp1_seconds, 12);
        assert_eq!(thermal.temp2_seconds, 13);
    }
}
