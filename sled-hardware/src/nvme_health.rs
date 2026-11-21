// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! What the hardware poll reads from an NVMe controller's SMART / Health
//! Information log page.

use chrono::{DateTime, Utc};
use sled_agent_types::inventory::NvmeHealth;

/// The Critical Warning field of an NVMe SMART / Health Information log
/// page. Each is `true` if that warning is raised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NvmeCriticalWarnings {
    /// Available spare capacity has fallen below the threshold.
    pub available_spare_low: bool,
    /// Temperature is outside the threshold.
    pub temperature: bool,
    /// Reliability is degraded due to media errors.
    pub reliability_degraded: bool,
    /// Media has been placed in read-only mode.
    pub read_only: bool,
    /// Volatile memory backup device has failed. `None` if the controller
    /// has no volatile write cache.
    pub volatile_memory_backup_failed: Option<bool>,
    /// Persistent Memory Region has become read-only or unreliable. `None`
    /// if the controller's NVMe version is before 1.4.
    pub persistent_memory_region_read_only: Option<bool>,
    /// A change to a personality's settings did not complete and could not
    /// be reverted. `None` if the controller's NVMe version is before 2.3.
    pub indeterminate_personality_state: Option<bool>,
}

/// The Endurance Group Critical Warning Summary field of an NVMe SMART /
/// Health Information log page. Each is `true` if that warning is raised for
/// one or more Endurance Groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NvmeEnduranceGroupWarnings {
    /// Available spare capacity has fallen below the threshold.
    pub available_spare_low: bool,
    /// Reliability is degraded due to media or internal errors.
    pub reliability_degraded: bool,
    /// Namespaces have been placed in read-only mode.
    pub read_only: bool,
}

/// The thermal management statistics of an NVMe SMART / Health Information
/// log page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NvmeThermalManagement {
    /// Transitions into thermal management temperature 1.
    pub temp1_transitions: u32,
    /// Transitions into thermal management temperature 2.
    pub temp2_transitions: u32,
    /// Seconds in thermal management temperature 1.
    pub temp1_seconds: u32,
    /// Seconds in thermal management temperature 2.
    pub temp2_seconds: u32,
}

/// A reading of the SMART / Health Information log page of an NVMe
/// controller.
///
/// The controller reports its counters as 128-bit values. A count too large
/// for a `u64` is recorded as `u64::MAX`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NvmeHealthLogPage {
    /// When sled-agent read this log page from the controller.
    pub time_collected: DateTime<Utc>,
    pub critical_warnings: NvmeCriticalWarnings,
    /// `None` if the controller's NVMe version is before 1.4.
    pub endurance_group_warnings: Option<NvmeEnduranceGroupWarnings>,
    /// Composite temperature. `None` if the controller did not report one.
    pub temperature_kelvin: Option<u16>,
    /// Normalized percentage of remaining spare capacity.
    pub available_spare_percent: u8,
    /// The percentage below which `available_spare_percent` raises a
    /// critical warning.
    pub available_spare_threshold_percent: u8,
    /// Vendor estimate of the percentage of rated life used; may exceed 100.
    pub life_used_percent: u8,
    /// Bytes read by the host. The controller counts in units of 512,000
    /// bytes, rounded up.
    pub data_read_bytes: u64,
    /// Bytes written by the host, counted as `data_read_bytes` is.
    pub data_written_bytes: u64,
    pub host_read_commands: u64,
    pub host_write_commands: u64,
    /// Minutes the controller was busy with I/O commands.
    pub controller_busy_minutes: u64,
    pub power_cycles: u64,
    pub power_on_hours: u64,
    pub unsafe_shutdowns: u64,
    /// Occurrences of the controller detecting an unrecovered data integrity
    /// error.
    pub media_errors: u64,
    /// Entries in the controller's Error Information log over its life.
    pub error_log_entries: u64,
    /// Minutes above the warning composite temperature threshold. `None` if
    /// the controller's NVMe version is before 1.2, or it reports no warning
    /// threshold.
    pub warning_temperature_minutes: Option<u32>,
    /// Minutes above the critical composite temperature threshold. `None` if
    /// the controller's NVMe version is before 1.2, or it reports no critical
    /// threshold.
    pub critical_temperature_minutes: Option<u32>,
    /// Temperature sensors 1 through 8, in order. A sensor is `None` if it
    /// isn't implemented, or the controller's NVMe version is before 1.2.
    pub temperature_sensors_kelvin: [Option<u16>; 8],
    /// `None` if the controller's NVMe version is before 1.3.
    pub thermal_management: Option<NvmeThermalManagement>,
}

impl NvmeHealthLogPage {
    /// Returns the health of an unused disk with no problems, for disks that
    /// are not backed by a real NVMe controller.
    pub fn new_fake(time_collected: DateTime<Utc>) -> NvmeHealthLogPage {
        NvmeHealthLogPage {
            time_collected,
            critical_warnings: NvmeCriticalWarnings {
                available_spare_low: false,
                temperature: false,
                reliability_degraded: false,
                read_only: false,
                volatile_memory_backup_failed: None,
                persistent_memory_region_read_only: None,
                indeterminate_personality_state: None,
            },
            endurance_group_warnings: None,
            temperature_kelvin: None,
            available_spare_percent: 100,
            available_spare_threshold_percent: 10,
            life_used_percent: 0,
            data_read_bytes: 0,
            data_written_bytes: 0,
            host_read_commands: 0,
            host_write_commands: 0,
            controller_busy_minutes: 0,
            power_cycles: 0,
            power_on_hours: 0,
            unsafe_shutdowns: 0,
            media_errors: 0,
            error_log_entries: 0,
            warning_temperature_minutes: None,
            critical_temperature_minutes: None,
            temperature_sensors_kelvin: [None; 8],
            thermal_management: None,
        }
    }

    /// The part of this reading that sled-agent reports in inventory.
    pub fn inventory_health(&self) -> NvmeHealth {
        NvmeHealth {
            time_collected: self.time_collected,
            reliability_degraded: self.critical_warnings.reliability_degraded,
            read_only: self.critical_warnings.read_only,
        }
    }
}
