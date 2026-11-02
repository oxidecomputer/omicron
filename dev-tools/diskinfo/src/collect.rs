// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect disk inventory from devinfo, libnvme and libefi, the same sources
//! sled-agent uses (`sled-hardware/src/illumos/mod.rs`), and locations from
//! libtopo as `nvmeadm list -L` does.

use crate::types::{DiskEntry, Firmware, LbaFormat, Probe};
use crate::{probe, topo};
use anyhow::{Context, Result, bail};
use illumos_devinfo::{DevInfo, DevLinkType, DevLinks, Node};
use kstat_rs::Ctl;
use libnvme::Nvme;
use libnvme::controller::{Controller, TryLockResult};
use libnvme::health::HealthLogPage;
use libnvme::namespace::NamespaceDiscoveryLevel;
use libnvme::wdc_devmgmt::DevMgmtLog;
use libnvme::wdc_eol::EolLog;
use std::collections::BTreeMap;

/// What we learn from an NVMe controller's `blkdev` child.
struct Blkdev {
    disk_path: Option<String>,
    vid: Option<String>,
    pid: Option<String>,
    partitioned: bool,
    probe: Option<Probe>,
}

pub fn collect() -> Result<Vec<DiskEntry>> {
    let mut devinfo = DevInfo::new().context("devinfo snapshot")?;

    let kstat = Ctl::new()
        .map_err(|e| {
            eprintln!("warning: not-ready probe unavailable: kstat: {e}")
        })
        .ok();

    // A controller can exist without a blkdev (e.g. namespace detached), so
    // gather blkdevs keyed by their parent's instance, then walk controllers.
    let mut blkdevs = BTreeMap::new();
    for node in devinfo.walk_driver("blkdev") {
        let node = node?;
        match blkdev_info(&node, kstat.as_ref()) {
            Ok(Some((instance, b))) => {
                blkdevs.insert(instance, b);
            }
            Ok(None) => {}
            Err(e) => eprintln!("warning: {}: {e:#}", node.node_name()),
        }
    }

    let mut controllers = Vec::new();
    for node in devinfo.walk_driver("nvme") {
        let node = node?;
        let Some(instance) = node.instance() else {
            continue;
        };
        controllers.push(instance);
    }

    let mut locations = topo::nvme_locations().unwrap_or_else(|e| {
        eprintln!("warning: location information unavailable: {e:#}");
        Default::default()
    });

    let nvme = Nvme::new()?;
    let mut disks = Vec::new();
    for c in controllers {
        let instance = format!("nvme{c}");
        let ctrl = match controller_info(&nvme, c) {
            Ok(ctrl) => ctrl,
            Err(e) => {
                eprintln!("warning: {instance}: {e:#}");
                continue;
            }
        };
        let b = blkdevs.remove(&c);
        let probe = b.as_ref().and_then(|b| b.probe.clone());

        disks.push(DiskEntry {
            instance,
            model: ctrl.model,
            serial: ctrl.serial,
            firmware: ctrl.firmware,
            capacity_bytes: ctrl.capacity_bytes,
            lba_format: ctrl.lba_format,
            disk_path: b.as_ref().and_then(|b| b.disk_path.clone()),
            vid: b.as_ref().and_then(|b| b.vid.clone()),
            pid: b.as_ref().and_then(|b| b.pid.clone()),
            location: locations.remove(&c),
            partitioned: b.as_ref().is_some_and(|b| b.partitioned),
            log_pages: ctrl.log_pages,
            health: ctrl.health,
            probe,
            wdc_devmgmt: ctrl.wdc_devmgmt,
            wdc_eol: ctrl.wdc_eol,
        });
    }

    Ok(disks)
}

/// What we learn from libnvme about a controller.
struct ControllerInfo {
    model: String,
    serial: String,
    firmware: Firmware,
    capacity_bytes: Option<u64>,
    lba_format: Option<LbaFormat>,
    log_pages: Vec<String>,
    health: Option<HealthLogPage>,
    wdc_devmgmt: Option<DevMgmtLog>,
    wdc_eol: Option<EolLog>,
}

fn controller_info(nvme: &Nvme, instance: i32) -> Result<ControllerInfo> {
    let ctrl = Controller::init_by_instance(nvme, instance)?;
    let info = ctrl.get_info()?;

    let (capacity_bytes, lba_format) = first_active_namespace(&ctrl)?.unzip();

    // Only fetch the log pages the controller implements.
    let log_pages: Vec<String> = match ctrl.implemented_log_pages() {
        Ok(pages) => pages.into_iter().map(|p| p.name).collect(),
        Err(e) => {
            eprintln!("warning: nvme{instance}: log page discovery: {e}");
            Vec::new()
        }
    };
    let has = |name: &str| log_pages.iter().any(|p| p == name);
    let (want_firmware, want_health, want_devmgmt, want_eol) =
        (has("firmware"), has("health"), has("wdc/devmgmt"), has("wdc/eol"));

    // As sled-agent does, take a non-blocking read lock so we neither block
    // on nor race with a writer (e.g. a format or firmware update).
    let mut next_firmware = None;
    let mut health = None;
    let mut wdc_devmgmt = None;
    let mut wdc_eol = None;
    if want_firmware || want_health || want_devmgmt || want_eol {
        match ctrl.try_read_lock() {
            TryLockResult::Ok(locked) => {
                if want_firmware {
                    match locked.get_firmware_log_page() {
                        Ok(page) => {
                            next_firmware =
                                page.next_version().map(str::to_owned)
                        }
                        Err(e) => eprintln!(
                            "warning: nvme{instance}: firmware log: {e}"
                        ),
                    }
                }
                if want_health {
                    match locked.get_health_log_page() {
                        Ok(page) => health = Some(page),
                        Err(e) => eprintln!(
                            "warning: nvme{instance}: health log: {e}"
                        ),
                    }
                }
                if want_devmgmt {
                    match locked.get_wdc_devmgmt_log_page() {
                        Ok(log) => wdc_devmgmt = Some(log),
                        Err(e) => eprintln!(
                            "warning: nvme{instance}: wdc/devmgmt log: {e}"
                        ),
                    }
                }
                if want_eol {
                    match locked.get_wdc_eol_log_page() {
                        Ok(log) => wdc_eol = Some(log),
                        Err(e) => eprintln!(
                            "warning: nvme{instance}: wdc/eol log: {e}"
                        ),
                    }
                }
            }
            TryLockResult::Locked(_) => {
                eprintln!("warning: nvme{instance}: controller is locked");
            }
            TryLockResult::Err(e) => {
                eprintln!("warning: nvme{instance}: read lock: {e}");
            }
        }
    }

    Ok(ControllerInfo {
        model: info.model().into_owned(),
        serial: info.serial().into_owned(),
        firmware: Firmware {
            active: info.fwrev().into_owned(),
            next: next_firmware,
        },
        capacity_bytes,
        lba_format,
        log_pages,
        health,
        wdc_devmgmt,
        wdc_eol,
    })
}

/// Size in bytes and current LBA format of the first active namespace.
fn first_active_namespace(
    ctrl: &Controller<'_>,
) -> Result<Option<(u64, LbaFormat)>> {
    let Some(ns) =
        ctrl.namespace_discovery(NamespaceDiscoveryLevel::Active)?.next()
    else {
        return Ok(None);
    };
    let info = ns?.get_info()?;
    let fmt = info.current_format()?;
    Ok(Some((
        info.size_bytes()?,
        LbaFormat {
            data_size_bytes: fmt.data_size(),
            metadata_size_bytes: fmt.meta_size(),
        },
    )))
}

/// Returns the parent NVMe controller's instance and what we can learn from
/// a `blkdev` node.
fn blkdev_info(
    node: &Node<'_>,
    kstat: Option<&Ctl>,
) -> Result<Option<(i32, Blkdev)>> {
    let Some(parent) = node.parent()? else {
        return Ok(None);
    };
    if parent.driver_name().as_deref() != Some("nvme") {
        return Ok(None);
    }
    let Some(instance) = parent.instance() else {
        return Ok(None);
    };

    let props = node.string_props();
    let vid = props.get("inquiry-vendor-id").cloned();
    let pid = props.get("inquiry-product-id").cloned();

    let disk_path = whole_disk_link(node)?;

    let devfs_path = node.devfs_path()?;
    let raw = format!("/devices{devfs_path}:wd,raw");

    let probe = kstat.and_then(|ctl| {
        probe::probe(ctl, instance, &raw)
            .map_err(|e| {
                eprintln!("warning: nvme{instance}: not-ready probe: {e:#}")
            })
            .ok()
    });

    // As sled-agent does, read the GPT from the raw whole-disk minor.
    let partitioned = libefi_illumos::Gpt::read(&raw).is_ok();

    Ok(Some((instance, Blkdev { disk_path, vid, pid, partitioned, probe })))
}

/// The `/dev/dsk/cXtYd0` name for a blkdev's whole-disk ("wd") minor.
fn whole_disk_link(node: &Node<'_>) -> Result<Option<String>> {
    for m in node.minors() {
        let m = m?;
        if m.name() != "wd" {
            continue;
        }
        let links = DevLinks::new(false)?;
        let devfs_path = m.devfs_path()?;
        let name = links
            .links_for_path(&devfs_path)?
            .into_iter()
            .filter(|l| l.linktype() == DevLinkType::Primary)
            .filter_map(|l| {
                l.path().file_name().map(|f| f.to_string_lossy().into_owned())
            })
            .find(|f| f.ends_with("d0"));
        return match name {
            Some(name) => Ok(Some(name)),
            None => bail!("no /dev link for {devfs_path}"),
        };
    }
    Ok(None)
}
