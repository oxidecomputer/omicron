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
    // XXX: This is really something we should be told by Nexus, perhaps after
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
            "Calculated eligible VMM reservoir size";
            "vmm_eligible" => %vmm_eligible,
            "physical_ram_bytes" => %hardware_physical_ram_bytes,
            "max_page_t_bytes" => %max_page_t_bytes,
            "control_plane_earmark_bytes" => %self.control_plane_earmark_bytes,
        );

        vmm_eligible
    }
}

/// Detects the current sled's CPU family using the CPUID instruction.
///
/// TODO: Ideally we would call into libtopo and pass along the information
/// identified there. See
/// <https://github.com/oxidecomputer/omicron/issues/8732>.
///
/// Everything here is duplicative with CPU identification done by the kernel.
/// You'll even find a very similar (but much more comprehensive) AMD family
/// mapping at `amd_revmap` in `usr/src/uts/intel/os/cpuid_subr.c`. But
/// sled-agent does not yet know about libtopo, getting topo snapshots, walking
/// them, or any of that, so the parsing is performed again here.
#[cfg(target_arch = "x86_64")]
pub fn detect_cpu_family(log: &Logger) -> sled_hardware_types::CpuFamily {
    use core::arch::x86_64::__cpuid_count;
    use sled_hardware_types::CpuFamily;

    // Read leaf 0 to figure out the processor's vendor and whether leaf 1
    // (which contains family, model, and stepping information) is available.
    let leaf_0 = unsafe { __cpuid_count(0, 0) };

    info!(log, "read CPUID leaf 0 to detect CPU vendor"; "values" => ?leaf_0);

    // If leaf 1 is unavailable, there's no way to figure out what family this
    // processor belongs to.
    if leaf_0.eax < 1 {
        return CpuFamily::Unknown;
    }

    // Check the vendor ID string in ebx/ecx/edx.
    match (leaf_0.ebx, leaf_0.ecx, leaf_0.edx) {
        // "AuthenticAMD"; see AMD APM volume 3 (March 2024) section E.3.1.
        (0x68747541, 0x444D4163, 0x69746E65) => {}
        _ => return CpuFamily::Unknown,
    }

    // Feature detection after this point is AMD-specific - if we find ourselves
    // supporting other CPU vendors we'll want to split this out accordingly.

    // Per AMD APM volume 3 (March 2024) section E.3.2, the processor family
    // number is computed as follows:
    //
    // - Read bits 11:8 of leaf 1 eax to get the "base" family value. If this
    //   value is less than 0xF, the family value is equal to the base family
    //   value.
    // - If the base family value is 0xF, eax[27:20] contains the "extended"
    //   family value, and the actual family value is the sum of the base and
    //   the extended values.
    let leaf_1 = unsafe { __cpuid_count(1, 0) };
    let mut family = (leaf_1.eax & 0x00000F00) >> 8;
    if family == 0xF {
        family += (leaf_1.eax & 0x0FF00000) >> 20;
    }

    // Also from the APM volume 3 section E.3.2, the processor model number is
    // computed as follows:
    //
    // - Read bits 7:4 of leaf 1 eax to get the "base" model value.
    // - If the "base" family value is less than 0xF, the "base" model stands.
    //   Otherwise, four additional bits of the model come from eax[19:16].
    //
    // If the computed family number is 0xF or greater, that implies the "base"
    // family was 0xF or greater as well.
    let mut model = (leaf_1.eax & 0x000000F0) >> 4;
    if family >= 0xF {
        model |= (leaf_1.eax & 0x000F0000) >> 12;
    }

    info!(
        log,
        "read CPUID leaf 1 to detect CPU family";
        "leaf1.eax" => format_args!("{:#08x}", leaf_1.eax),
        "leaf1.ebx" => format_args!("{:#08x}", leaf_1.ebx),
        "leaf1.ecx" => format_args!("{:#08x}", leaf_1.ecx),
        "leaf1.edx" => format_args!("{:#08x}", leaf_1.edx),
        "parsed family" => format_args!("{family:#x}"),
        "parsed model" => format_args!("{model:#x}"),
    );

    // Match on the family/model ranges we've detected. Notably client parts are
    // reported as if they were their server counterparts; the feature parity is
    // close enough that guests probably won't run into issues. This lowers
    // friction for testing migrations where the control plane would need to
    // tell what hosts could be compatible with a VMM's CPU platform.
    //
    // TODO(?): Exhaustively check that client parts support all CPU features of
    // the corresponding Oxide CPU platform before doing this "as-if" reporting.
    // Lab systems built out of client parts may have hardware which support all
    // features in the corresponding instance CPU platform, but have individual
    // features disabled in the BIOS or by client part microcode. This can
    // result in funky situations, like an Oxide CPU platform advertising CPU
    // features that lab systems don't support. This is unlikely, but take
    // AVX512 as an example: users can often disable AVX512 entirely on Zen 5
    // BIOSes. In this case a VM on a 9000-series Ryzen will be told those
    // instructions are available only for the guest to get #UD at runtime.
    match family {
        0x19 if model <= 0x0F => {
            // This covers both Milan and Zen 3-based Threadrippers. I don't
            // have a 5000-series Threadripper on hand to test but I believe
            // they are feature-compatible.
            CpuFamily::AmdMilan
        }
        0x19 if model >= 0x10 && model <= 0x1F => {
            // This covers both Genoa and Zen 4-based Threadrippers. Again,
            // don't have a comparable Threadripper to test here.
            //
            // We intend to expose Turin and Milan as families a guest can
            // choose, skipping the Zen 4 EPYC parts. So, round this down to
            // Milan; if we're here it's a lab system and the alternative is
            // "unknown".
            CpuFamily::AmdMilan
        }
        0x19 if model >= 0x20 && model <= 0x2F => {
            // These are client Zen 3 parts aka Vermeer. Feature-wise, they are
            // missing INVLPGB from Milan, but are otherwise close, and we don't
            // expose INVLPGB to guests currently anyway.
            CpuFamily::AmdMilan
        }
        0x19 if model >= 0x60 && model <= 0x6F => {
            // These are client Zen 4 parts aka Raphael. Similar to the above
            // with Genoa and Vermeer, round these down to Milan in support of
            // lab clusters instead of calling them unknown.
            CpuFamily::AmdMilan
        }
        0x1A if model <= 0x0F => CpuFamily::AmdTurin,
        0x1A if model >= 0x10 && model <= 0x1F => {
            // These are Turin Dense. From a CPU feature perspective they're
            // equivalently capable to Turin, but they are physically distinct
            // and sled operators should be able to see that.
            CpuFamily::AmdTurinDense
        }
        0x1A if model >= 0x40 && model <= 0x4F => {
            // These are client Zen 5 parts aka Granite Ridge. Won't be in a
            // rack, but plausibly in a lab cluster. Like other non-server
            // parts, these don't have INVLPGB, which we don't expose to guests.
            // They should otherwise be a sufficient stand-in for Turin.
            CpuFamily::AmdTurin
        }
        // Remaining family/model ranges in known families are likely mobile
        // parts and intentionally rolled up into "Unknown." There, it's harder
        // to predict what features out of the corresponding CPU platform would
        // actually be present. It's also less likely that someone has a laptop
        // or APU as part of a development cluster!
        //
        // Other families are, of course, unknown.
        _ => CpuFamily::Unknown,
    }
}
