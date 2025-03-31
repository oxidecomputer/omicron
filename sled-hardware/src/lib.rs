// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{Logger, info};

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        mod illumos;
        pub use illumos::*;
    } else {
        mod non_illumos;
        pub use non_illumos::*;
    }
}

pub mod cleanup;
pub mod disk;
pub use disk::*;
pub mod underlay;

/// Provides information from the underlying hardware about updates
/// which may require action on behalf of the Sled Agent.
///
/// These updates should generally be "non-opinionated" - the higher
/// layers of the sled agent can make the call to ignore these updates
/// or not.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum HardwareUpdate {
    TofinoDeviceChange,
    TofinoLoaded,
    TofinoUnloaded,
    DiskAdded(UnparsedDisk),
    DiskRemoved(UnparsedDisk),
    DiskUpdated(UnparsedDisk),
}

// The type of networking 'ASIC' the Dendrite service is expected to manage
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum DendriteAsic {
    TofinoAsic,
    TofinoStub,
    SoftNpuZone,
    SoftNpuPropolisDevice,
}

impl std::fmt::Display for DendriteAsic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DendriteAsic::TofinoAsic => "tofino_asic",
                DendriteAsic::TofinoStub => "tofino_stub",
                DendriteAsic::SoftNpuZone => "soft_npu_zone",
                DendriteAsic::SoftNpuPropolisDevice =>
                    "soft_npu_propolis_device",
            }
        )
    }
}

/// Configuration for forcing a sled to run as a Scrimlet or Gimlet
#[derive(Copy, Clone, Debug)]
pub enum SledMode {
    /// Automatically detect whether to run as a Gimlet or Scrimlet (w/ real Tofino ASIC)
    Auto,
    /// Force sled to run as a Gimlet
    Gimlet,
    /// Force sled to run as a Scrimlet
    Scrimlet { asic: DendriteAsic },
}

/// Accounting for high watermark memory usage for various system purposes
#[derive(Clone)]
pub struct MemoryReservations {
    log: Logger,
    hardware_manager: HardwareManager,
    /// The amount of memory expected to be used if "control plane" services all
    /// running on this sled. "control plane" here refers to services that have
    /// roughly fixed memory use given differing sled hardware configurations.
    /// DNS (internal, external), Nexus, Cockroach, or ClickHouse are all
    /// examples of "control plane" here.
    ///
    /// This is a pessimistic overestimate; it is unlikely
    /// (and one might say undesirable) that all such services are colocated on
    /// a sled, and (as described in RFD 413) the budgeting for each service's
    /// RAM must include headroom for those services potentially forking and
    /// bursting required swap or resident pages.
    //
    // XXX: This is really something we should be told by Neuxs, perhaps after
    // starting with this conservative estimate to get the sled started.
    control_plane_earmark_bytes: u64,
    // XXX: Crucible involves some amount of memory in support of the volumes it
    // manages. We should collect zpool size and estimate the memory that would
    // be used if all available storage was dedicated to Crucible volumes. For
    // now this is part of the control plane earmark.
}

impl MemoryReservations {
    pub fn new(
        log: Logger,
        hardware_manager: HardwareManager,
        control_plane_earmark_mib: Option<u32>,
    ) -> MemoryReservations {
        const MIB: u64 = 1024 * 1024;
        let control_plane_earmark_bytes =
            u64::from(control_plane_earmark_mib.unwrap_or(0)) * MIB;

        Self { log, hardware_manager, control_plane_earmark_bytes }
    }

    /// Compute the amount of physical memory that could be set aside for the
    /// VMM reservoir.
    ///
    /// The actual VMM reservoir will be smaller than this amount, and is either
    /// a fixed amount of memory specified by `ReservoirMode::Size` or
    /// a percentage of this amount specified by `ReservoirMode::Percentage`.
    pub fn vmm_eligible(&self) -> u64 {
        let hardware_physical_ram_bytes =
            self.hardware_manager.usable_physical_ram_bytes();
        // Don't like hardcoding a struct size from the host OS here like
        // this, maybe we shuffle some bits around before merging.. On the
        // other hand, the last time page_t changed was illumos-gate commit
        // a5652762e5 from 2006.
        const PAGE_T_SIZE: u64 = 120;
        let max_page_t_bytes =
            self.hardware_manager.usable_physical_pages() * PAGE_T_SIZE;

        let vmm_eligible = hardware_physical_ram_bytes
            - max_page_t_bytes
            - self.control_plane_earmark_bytes;

        info!(
            self.log,
            "Calculated {vmm_eligible} bytes for VMM \
            reservoir: {hardware_physical_ram_bytes} physical RAM bytes \
            - {max_page_t_bytes} bytes for maximum page_t structures \
            - {} bytes held back for services on the sled",
            self.control_plane_earmark_bytes
        );

        vmm_eligible
    }
}
