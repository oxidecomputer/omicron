// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Output types for a disk.  The log page types come from libnvme.

use libnvme::health::HealthLogPage;
use libnvme::wdc_devmgmt::DevMgmtLog;
use libnvme::wdc_eol::EolLog;
use serde::{Deserialize, Serialize};

/// A single NVMe disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskEntry {
    /// Controller instance, e.g. `nvme0`.
    pub instance: String,
    pub model: String,
    pub serial: String,
    pub firmware: Firmware,
    /// Size of the first active namespace.  `None` if there is none.
    pub capacity_bytes: Option<u64>,
    /// Current LBA format of the first active namespace.
    pub lba_format: Option<LbaFormat>,
    /// Whole-disk device name, e.g. `c1t00A0750158FCA3FAd0`.
    pub disk_path: Option<String>,
    pub vid: Option<String>,
    pub pid: Option<String>,
    /// Physical location label, e.g. `M.2 East` or `N3`.
    pub location: Option<String>,
    /// Whether the disk has a partition table.
    pub partitioned: bool,
    /// Log pages the controller implements, as `nvmeadm list-logpages`
    /// reports them.  Only these are fetched.
    pub log_pages: Vec<String>,
    /// SMART/Health log page.  `None` if it couldn't be read.
    pub health: Option<HealthLogPage>,
    /// Result of opening the disk while watching `nvm_ns_notrdy`.  `None` if
    /// there is no blkdev to open or the probe couldn't run.
    pub probe: Option<Probe>,
    /// WDC Device Manageability log page.  Only on drives that implement
    /// `wdc/devmgmt` (the Ultrastar DC SN840).
    pub wdc_devmgmt: Option<DevMgmtLog>,
    /// WDC EOL log page.  Only on drives that implement `wdc/eol` (the
    /// Ultrastar DC SN840).
    pub wdc_eol: Option<EolLog>,
}

/// Firmware versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Firmware {
    /// Version running now, from Identify Controller.
    pub active: String,
    /// Version the drive will run after its next reset, if that differs from
    /// `active`.  `None` if it won't change or the firmware log page couldn't
    /// be read.
    pub next: Option<String>,
}

/// An LBA format.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LbaFormat {
    pub data_size_bytes: u64,
    pub metadata_size_bytes: u32,
}

/// Whether opening the disk caused "Namespace Not Ready" failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Probe {
    /// Why the open failed, if it did (typically ENXIO for a not-ready
    /// namespace).
    pub open_error: Option<String>,
    /// `nvme:<instance>:device:nvm_ns_notrdy` before the open.
    pub nvm_ns_notrdy_before: u64,
    /// `nvme:<instance>:device:nvm_ns_notrdy` after the open.
    pub nvm_ns_notrdy_after: u64,
}
