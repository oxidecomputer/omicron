// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
