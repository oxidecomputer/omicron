// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::StartupOptions;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct HostStartupOptions {
    pub phase2_recovery_mode: bool,
    pub kbm: bool,
    pub bootrd: bool,
    pub prom: bool,
    pub kmdb: bool,
    pub kmdb_boot: bool,
    pub boot_ramdisk: bool,
    pub boot_net: bool,
    pub verbose: bool,
}

impl From<HostStartupOptions> for StartupOptions {
    fn from(mgs_opt: HostStartupOptions) -> Self {
        let mut opt = StartupOptions::empty();
        opt.set(
            StartupOptions::PHASE2_RECOVERY_MODE,
            mgs_opt.phase2_recovery_mode,
        );
        opt.set(StartupOptions::STARTUP_KBM, mgs_opt.kbm);
        opt.set(StartupOptions::STARTUP_BOOTRD, mgs_opt.bootrd);
        opt.set(StartupOptions::STARTUP_PROM, mgs_opt.prom);
        opt.set(StartupOptions::STARTUP_KMDB, mgs_opt.kmdb);
        opt.set(StartupOptions::STARTUP_KMDB_BOOT, mgs_opt.kmdb_boot);
        opt.set(StartupOptions::STARTUP_BOOT_RAMDISK, mgs_opt.boot_ramdisk);
        opt.set(StartupOptions::STARTUP_BOOT_NET, mgs_opt.boot_net);
        opt.set(StartupOptions::STARTUP_VERBOSE, mgs_opt.verbose);
        opt
    }
}

impl From<StartupOptions> for HostStartupOptions {
    fn from(opt: StartupOptions) -> Self {
        Self {
            phase2_recovery_mode: opt
                .contains(StartupOptions::PHASE2_RECOVERY_MODE),
            kbm: opt.contains(StartupOptions::STARTUP_KBM),
            bootrd: opt.contains(StartupOptions::STARTUP_BOOTRD),
            prom: opt.contains(StartupOptions::STARTUP_PROM),
            kmdb: opt.contains(StartupOptions::STARTUP_KMDB),
            kmdb_boot: opt.contains(StartupOptions::STARTUP_KMDB_BOOT),
            boot_ramdisk: opt.contains(StartupOptions::STARTUP_BOOT_RAMDISK),
            boot_net: opt.contains(StartupOptions::STARTUP_BOOT_NET),
            verbose: opt.contains(StartupOptions::STARTUP_VERBOSE),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ComponentFirmwareHashStatus {
    /// The hash is not available; the client must issue a separate request to
    /// begin calculating the hash.
    HashNotCalculated,
    /// The hash is currently being calculated; the client should sleep briefly
    /// then check again.
    ///
    /// We expect this operation to take a handful of seconds in practice.
    HashInProgress,
    /// The hash of the given firmware slot.
    Hashed { sha256: [u8; 32] },
}
