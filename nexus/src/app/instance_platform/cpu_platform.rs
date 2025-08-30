// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use raw_cpuid::{
    ApmInfo, CpuIdDump, CpuIdResult, CpuIdWriter,
    ExtendedFeatureIdentification2, ExtendedFeatures,
    ExtendedProcessorFeatureIdentifiers, ExtendedState, ExtendedStateInfo,
    ExtendedTopologyLevel, FeatureInfo, L1CacheTlbInfo, L2And3CacheTlbInfo,
    MonitorMwaitInfo, PerformanceOptimizationInfo,
    ProcessorCapacityAndFeatureInfo, ProcessorTopologyInfo, SvmFeatures,
    ThermalPowerInfo, Tlb1gbPageInfo, Vendor, VendorInfo,
};
use sled_agent_client::types::CpuidEntry;

/// Check if `target` describes a processor that agrees with `base` on
/// architectural behaviors defined in CPUID leaves.
///
/// Arguably this should live in a crate outside Omicron which is used by both
/// Omicron and Propolis. Perhaps the Oxide fork of `rust-cpuid`. It's here to
/// sketch the logic and expected to move later.
///
/// NOTE: This does *not currently check ISA extensions or other feature
/// compatibility*. It assumes that the CPUID profiles are already known to be
/// feature-compatibile ahead of time. Instead, this is to check details like
/// "`clflush` operates on the same number of words".
#[allow(dead_code)]
pub fn functionally_same(base: CpuIdDump, target: CpuIdDump) -> bool {
    let base = raw_cpuid::CpuId::with_cpuid_reader(base);
    let target = raw_cpuid::CpuId::with_cpuid_reader(target);

    match (base.get_feature_info(), target.get_feature_info()) {
        (Some(base_info), Some(target_info)) => {
            let base_clflush_size = base_info.cflush_cache_line_size();
            let target_clflush_size = target_info.cflush_cache_line_size();
            if base_clflush_size != target_clflush_size {
                return false;
            }
        }
        (Some(_), None) | (None, Some(_)) | (None, None) => {
            // TODO: Might be able to tolerate these cases in practice, but
            // realistically we should never be here.
            return false;
        }
    }
    match (
        base.get_processor_capacity_feature_info(),
        target.get_processor_capacity_feature_info(),
    ) {
        (Some(base_info), Some(target_info)) => {
            if base_info.physical_address_bits()
                < target_info.physical_address_bits()
            {
                return false;
            }

            if base_info.linear_address_bits()
                < target_info.linear_address_bits()
            {
                return false;
            }

            // TODO: this probably could be a `<` relationship like above, but
            // I'm not so familiar here and am being stricter because of it.
            if base_info.guest_physical_address_bits()
                != target_info.guest_physical_address_bits()
            {
                return false;
            }

            if base_info.invlpgb_max_pages() < target_info.invlpgb_max_pages() {
                return false;
            }

            // TODO: really having a max RDPRU ID of anything more than 0 is
            // sketchy...
            if base_info.max_rdpru_id() < target_info.max_rdpru_id() {
                return false;
            }
        }
        _ => {
            // TODO: Probably can tolerate `target` not having this leaf, but we
            // won't be here in practice.
            return false;
        }
    }

    // Disagreements in this leaf likely only result in suboptimal performance,
    // rather than architectural misunderstanding. A permissive comparison would
    // overlook differences here.
    match (
        base.get_performance_optimization_info(),
        target.get_performance_optimization_info(),
    ) {
        (Some(base_info), Some(target_info)) => {
            if base_info.has_movu() != target_info.has_movu() {
                return false;
            }

            // TODO: this could be more precise; if the base has fp256 and the
            // target has fp128, that's probably fine. Likewise, if the base has
            // fp512 and the target has a smaller width, claiming the FPU
            // datapath is narrower than it really is, is probably fine.
            if base_info.has_fp256() != target_info.has_fp256() {
                return false;
            }
        }
        _ => {
            // Specific cases here may be acceptable, but for expediency (and
            // because we don't intend to support vCPUs whose profiles would not
            // have this leaf), just bail here.
            return false;
        }
    }

    // Bits checked here describe architectural behavior. If they differ, the
    // base CPU will behave differently than the target wants to see.
    //
    // It may be okay in some cases to allow these to differ, but take a
    // conservative approach until we need otherwise.
    match (
        base.get_extended_feature_identification_2(),
        target.get_extended_feature_identification_2(),
    ) {
        (Some(base_info), Some(target_info)) => {
            if base_info.has_no_nested_data_bp()
                != target_info.has_no_nested_data_bp()
            {
                return false;
            }

            if base_info.has_lfence_always_serializing()
                != target_info.has_lfence_always_serializing()
            {
                return false;
            }

            if base_info.has_null_select_clears_base()
                != target_info.has_null_select_clears_base()
            {
                return false;
            }
        }
        _ => {
            return false;
        }
    }

    // TODO: really not sure if we should include things like cache
    // hierarchy/core topology information here. Misrepresenting the actual
    // system can result in cache-sized buffers being sized incorrectly (or at
    // least suboptimally), but as long as cache sizes grow rather than shrink
    // it may only be "performance is not as good as it could be" rather than a
    // more deleterious outcome.

    true
}

/// The Platonic ideal Milan. This is what we would "like" to define as "The
/// Milan vCPU platform" absent any other constraints. This is a slightly
/// slimmer version of the Milan platform defined in RFD 314, with
/// justifications there.
///
/// Notably, this avoids describing individual processor SKUs' characteristics,
/// where possible.  This CPUID configuration as-is is untested; guests may not
/// boot, this may be too reductive, etc.
fn milan_ideal() -> CpuIdDump {
    let mut cpuid = raw_cpuid::CpuId::with_cpuid_reader(CpuIdDump::new());
    let leaf = VendorInfo::amd();
    cpuid.set_vendor_info(Some(leaf)).expect("can set leaf 0");
    cpuid
        .set_extended_function_info(Some(leaf))
        .expect("can set leaf 8000_0000h");

    let mut leaf = FeatureInfo::new(Vendor::Amd);

    // Set up EAX: Family 19h model 1h.
    leaf.set_extended_family_id(0x00); // why is this like this one of these two lines should go away
    leaf.set_extended_family_id(0xA);
    leaf.set_base_family_id(0x0F);
    leaf.set_base_model_id(0x01);
    leaf.set_stepping_id(0x01);

    // Set up EBX
    leaf.set_brand_index(0);
    leaf.set_cflush_cache_line_size(8); // 8 quadwords (64 bytes)
    // This and max logical processor ID are populated dynamically.
    leaf.set_initial_local_apic_id(0);
    leaf.set_max_logical_processor_ids(0);

    // Set up ECX
    leaf.set_sse3(true);
    leaf.set_pclmulqdq(true);
    leaf.set_ds_area(false);
    leaf.set_monitor_mwait(false);

    leaf.set_cpl(false);
    leaf.set_vmx(false);
    leaf.set_smx(false);
    leaf.set_eist(false);

    leaf.set_tm2(false);
    leaf.set_ssse3(true);
    leaf.set_cnxtid(false);
    // bit 11 is reserved

    leaf.set_fma(true);
    leaf.set_cmpxchg16b(true);
    // bit 14 is reserved
    leaf.set_pdcm(false);

    //bit 16 is reserved
    leaf.set_pcid(false);
    leaf.set_dca(false);
    leaf.set_sse41(true);

    leaf.set_sse42(true);
    leaf.set_x2apic(false);
    leaf.set_movbe(true);
    leaf.set_popcnt(true);

    leaf.set_tsc_deadline(false);
    leaf.set_aesni(true);
    leaf.set_xsave(true);
    leaf.set_oxsave(false); // Managed dynamically in practice

    leaf.set_avx(true);
    leaf.set_f16c(true);
    leaf.set_rdrand(true);
    // This CPUID profile will be presented to hypervisor guests
    leaf.set_hypervisor(true);

    // Set up EDX
    leaf.set_fpu(true);
    leaf.set_vme(true);
    leaf.set_de(true);
    leaf.set_pse(true);

    leaf.set_tsc(true);
    leaf.set_msr(true);
    leaf.set_pae(true);
    leaf.set_mce(true);

    leaf.set_cmpxchg8b(true);
    leaf.set_apic(true);
    // bit 10 is reserved
    leaf.set_sysenter_sysexit(true);

    leaf.set_mtrr(true);
    leaf.set_pge(true);
    leaf.set_mca(true);
    leaf.set_cmov(true);

    leaf.set_pat(true);
    leaf.set_pse36(true);
    // bit 18 is reserved
    leaf.set_clflush(true);

    // bit 20 is reserved
    // bit 21 is reserved
    // bit 22 is reserved
    leaf.set_mmx(true);

    leaf.set_fxsave_fxstor(true);
    leaf.set_sse(true);
    leaf.set_sse2(true);
    // bit 27 is reserved

    // managed dynamically in practice
    leaf.set_htt(false);
    // bits 29-31 are not used here.

    cpuid.set_feature_info(Some(leaf)).expect("can set leaf 1");

    // Leaf 2, 3, 4: all skipped on AMD

    // Leaf 5: Monitor and MWait. All zero here.
    cpuid
        .set_monitor_mwait_info(Some(MonitorMwaitInfo::empty()))
        .expect("can set leaf 5");

    // Leaf 6: Power management and some feature bits.
    //
    // Power management is all zeroed.
    let mut leaf = ThermalPowerInfo::empty();
    leaf.set_arat(true);
    leaf.set_hw_coord_feedback(false);

    cpuid.set_thermal_power_info(Some(leaf)).expect("can set leaf 6");

    // Leaf 7: Extended features
    let mut leaf = ExtendedFeatures::new();
    leaf.set_fsgsbase(true);
    leaf.set_tsc_adjust_msr(false);
    leaf.set_sgx(false);
    leaf.set_bmi1(true);

    leaf.set_hle(false);
    leaf.set_avx2(true);
    leaf.set_fdp(false);
    leaf.set_smep(true);

    leaf.set_bmi2(true);
    leaf.set_rep_movsb_stosb(true); // Also known as "ERMS".
    leaf.set_invpcid(false);
    // Bit 11 is reserved on AMD

    // PQM (bit 12) is clear here. TODO: no nice helper to set false yet.
    // Bit 13 is reserved on AMD
    // Bit 14 is reserved on AMD
    // Bit 15 is reserved on AMD

    leaf.set_avx512f(false);
    leaf.set_avx512dq(false);
    leaf.set_rdseed(true);
    leaf.set_adx(true);

    leaf.set_smap(true);
    leaf.set_avx512_ifma(false);
    // Bit 22 is reserved on AMD
    leaf.set_clflushopt(true);

    leaf.set_clwb(true);
    // Bit 25 is reserved on AMD
    // Bit 26 is reserved on AMD
    // Bit 27 is reserved on AMD

    leaf.set_avx512cd(false);
    leaf.set_sha(true);
    leaf.set_avx512bw(false);
    leaf.set_avx512vl(false);

    // Set up leaf 7 ECX

    // Bit 0 is reserved on AMD
    leaf.set_avx512vbmi(false);
    leaf.set_umip(false);
    leaf.set_pku(false);

    leaf.set_ospke(false);
    // Bit 5 is reserved on AMD
    leaf.set_avx512vbmi2(false);
    leaf.set_cet_ss(false);

    leaf.set_gfni(false); // Not in Milan
    leaf.set_vaes(true);
    leaf.set_vpclmulqdq(true);
    leaf.set_avx512vnni(false);

    leaf.set_avx512bitalg(false);
    // Bit 13 is reserved on AMD
    leaf.set_avx512vpopcntdq(false);
    // Bit 15 is reserved on AMD

    // Bits 16 through 31 are either reserved or zero on Milan.

    // Set up leaf 7 EDX
    leaf.set_fsrm(true);
    cpuid.set_extended_feature_info(Some(leaf)).expect("can set leaf 7");

    // Hide extended topology info (leaf Bh)
    cpuid.set_extended_topology_info(None).expect("can set leaf 8");

    // TODO: kind of gross to have to pass an empty `CpuIdDump` here...
    let mut state = ExtendedStateInfo::empty(CpuIdDump::new());
    state.set_xcr0_supports_legacy_x87(true);
    state.set_xcr0_supports_sse_128(true);
    state.set_xcr0_supports_avx_256(true);
    // Managed dynamically in practice.
    state.set_xsave_area_size_enabled_features(0x340);
    state.set_xsave_area_size_supported_features(0x340);

    state.set_xsaveopt(true);
    state.set_xsavec(true);
    state.set_xgetbv(true);
    state.set_xsave_size(0x340);

    let mut leaves = state.into_leaves().to_vec();
    let mut ymm_state = ExtendedState::empty();
    ymm_state.set_size(0x100);
    ymm_state.set_offset(0x240);
    leaves.push(Some(ymm_state.into_leaf()));

    cpuid.set_extended_state_info(Some(&leaves[..])).expect("can set leaf Dh");

    let mut leaf = ExtendedProcessorFeatureIdentifiers::empty(Vendor::Amd);
    // This is the same as the leaf 1 EAX configured earlier.
    leaf.set_extended_signature(0x00A00F11);

    // Set up EBX
    leaf.set_pkg_type(0x4);

    // Set up ECX
    leaf.set_lahf_sahf(true);
    leaf.set_cmp_legacy(false);
    leaf.set_svm(false);
    leaf.set_ext_apic_space(false);

    leaf.set_alt_mov_cr8(true);
    leaf.set_lzcnt(true);
    leaf.set_sse4a(true);
    leaf.set_misaligned_sse_mode(true);

    leaf.set_prefetchw(true);
    leaf.set_osvw(false); // May be set in hardware, hopefully can hide hardware errata from guests
    leaf.set_ibs(false);
    leaf.set_xop(false);

    leaf.set_skinit(false);
    leaf.set_wdt(false);
    // Bit 15 is reserved here.
    leaf.set_lwp(false);

    leaf.set_fma4(false); // Not on Milan

    // Bits 17-19 are reserved

    // Bit 20 is reserved
    // Bit 21 is reserved, formerly TBM
    leaf.set_topology_extensions(true);
    leaf.set_perf_cntr_extensions(true);

    leaf.set_nb_perf_cntr_extensions(false);
    // Bit 25 is reserved
    leaf.set_data_access_bkpt_extension(true);
    leaf.set_perf_tsc(false);

    leaf.set_perf_cntr_llc_extensions(false);
    leaf.set_monitorx_mwaitx(false);
    leaf.set_addr_mask_extension(true);
    // Bit 31 is reserved

    // Set up EDX
    leaf.set_syscall_sysret(true);
    leaf.set_execute_disable(true);
    leaf.set_mmx_extensions(true);
    leaf.set_fast_fxsave_fxstor(true);
    leaf.set_1gib_pages(true);
    leaf.set_rdtscp(true);
    leaf.set_64bit_mode(true);

    cpuid
        .set_extended_processor_and_feature_identifiers(Some(leaf))
        .expect("can set leaf 8000_0001h");

    // Leaves 8000_0002 through 8000_0005
    cpuid
        .set_processor_brand_string(Some(b"AMD EPYC 7003-like Processor"))
        .expect("can set vCPU brand string");

    // Hide L1 cache+TLB info (leaf 8000_0005h)
    cpuid.set_l1_cache_and_tlb_info(None).expect("can set leaf 8000_0005h");

    // Hide L2 and L3 cache+TLB info (leaf 8000_0006h)
    cpuid.set_l2_l3_cache_and_tlb_info(None).expect("can set leaf 8000_0006h");

    // Set up advanced power management info (leaf 8000_0007h)
    let mut leaf = ApmInfo::empty();
    leaf.set_invariant_tsc(true);
    cpuid
        .set_advanced_power_mgmt_info(Some(leaf))
        .expect("can set leaf 8000_0007h");

    // Set up processor capacity info (leaf 8000_0008h)
    let mut leaf = ProcessorCapacityAndFeatureInfo::empty();

    // Set up leaf 8000_0008 EAX
    leaf.set_physical_address_bits(0x30);
    leaf.set_linear_address_bits(0x30);
    leaf.set_guest_physical_address_bits(0);

    // St up leaf 8000_0008 EBX
    leaf.set_cl_zero(true);
    leaf.set_restore_fp_error_ptrs(true);
    leaf.set_wbnoinvd(true);

    // Populated dynamically in practice.
    leaf.set_num_phys_threads(1);
    leaf.set_apic_id_size(0);
    leaf.set_perf_tsc_size(0);

    leaf.set_invlpgb_max_pages(0);
    leaf.set_max_rdpru_id(0);

    cpuid
        .set_processor_capacity_feature_info(Some(leaf))
        .expect("can set leaf 8000_0008h");

    // Leaf 8000_000Ah is zeroed out for guests.
    cpuid
        .set_svm_info(Some(SvmFeatures::empty()))
        .expect("can set leaf 8000_000Ah");

    // Hide TLB information for 1GiB pages (leaf 8000_0019h)
    cpuid.set_tlb_1gb_page_info(None).expect("can set leaf 8000_0019h");

    // Set up processor optimization info (leaf 8000_001Ah)
    let mut leaf = PerformanceOptimizationInfo::empty();
    leaf.set_movu(true); // TODO: BREAKING
    leaf.set_fp256(true); // TODO: BREAKINGISH?
    cpuid
        .set_performance_optimization_info(Some(leaf))
        .expect("can set leaf 8000_001Ah");

    // Leaf 8000_001B and 8000_001C are handled after all other leaves.

    // Hide extended cache topology as well (Leaf 8000_001D)
    cpuid.set_extended_cache_parameters(None).expect("can set leaf 8000_001Dh");

    let mut leaf = ProcessorTopologyInfo::empty();
    leaf.set_threads_per_core(2);
    cpuid
        .set_processor_topology_info(Some(leaf))
        .expect("can set leaf 8000_001Eh");

    cpuid.set_memory_encryption_info(None).expect("can set leaf 8000_001Fh");

    let mut leaf = ExtendedFeatureIdentification2::empty();
    leaf.set_no_nested_data_bp(true);
    leaf.set_lfence_always_serializing(true);
    leaf.set_null_select_clears_base(true);
    cpuid
        .set_extended_feature_identification_2(Some(leaf))
        .expect("can set leaf 8000_0021h");

    let mut dump = cpuid.into_source();

    // There are a few leaves that are not yet defined in `raw-cpuid` but we
    // commit to being zero. In practice, *omitted* leaves with an explicit
    // CPUID specification will be zero, but setting them to zero here avoids
    // all doubt.

    // First, instruction-based sampling (IBS) is hidden from guests for now
    // (note `set_ibs(false)` above)
    dump.set_leaf(0x8000_001B, Some(CpuIdResult::empty()));
    // Lightweight profiling (LWP) is not supported by Milan, and not advertised
    // to guests.  (note `set_lwp(false)` above)
    dump.set_leaf(0x8000_001C, Some(CpuIdResult::empty()));
    // SEV is not supported in guests (note `set_sev(false)` above)
    dump.set_leaf(0x8000_001F, Some(CpuIdResult::empty()));

    dump
}

pub fn milan_rfd314() -> Vec<CpuidEntry> {
    // This is the Milan we'd "want" to expose, absent any other constraints.
    let baseline = milan_ideal();

    let mut cpuid = raw_cpuid::CpuId::with_cpuid_reader(baseline);

    let mut leaf = cpuid
        .get_extended_feature_info()
        .expect("baseline Milan defines leaf 1");

    // RFD 314 describes the circumstances around RDSEED, but it is not
    // currently available.
    leaf.set_rdseed(false);

    cpuid.set_extended_feature_info(Some(leaf)).expect("can set leaf 7h");

    // Set up extended topology info (leaf Bh)
    let mut levels = Vec::new();

    let mut topo_level1 = ExtendedTopologyLevel::empty();
    // EAX
    topo_level1.set_shift_right_for_next_apic_id(1);
    // EBX
    topo_level1.set_processors(2);
    // ECX
    topo_level1.set_level_number(0);
    // This level describes SMT. If there's no SMT enabled (single-core VM?)
    // then this level should not be present, probably?
    topo_level1.set_level_type(1);

    levels.push(topo_level1);

    let mut topo_level2 = ExtendedTopologyLevel::empty();
    // ECX
    topo_level2.set_level_number(1);
    topo_level2.set_level_type(2);

    levels.push(topo_level2);

    let mut topo_level3 = ExtendedTopologyLevel::empty();
    // ECX
    topo_level3.set_level_number(2);
    // Level type 0 indicates this level is invalid. This level is included only
    // to be explicit about where the topology ends.
    topo_level3.set_level_type(0);

    levels.push(topo_level3);
    cpuid
        .set_extended_topology_info(Some(levels.as_slice()))
        .expect("can set leaf 8000_0021h");

    let mut leaf = cpuid
        .get_extended_processor_and_feature_identifiers()
        .expect("baseline Milan defines leaf 8000_0001");
    // RFD 314 describes these leaf 8000_0001 wrinkles.
    //
    // Extended APIC space support was originally provided to guests because the
    // host supports it and it was passed through. The extended space is not
    // supported in Bhyve, but we leave it set here to not change it from under
    // guests.
    //
    // Bhyve now supports all six performance counters, so we could set the perf
    // counter extension bit here, but again it is left as-is to not change
    // CPUID from under a guest.
    //
    // RDTSCP requires some Bhyve and Propolis work to support, so it is masked
    // off for now.
    leaf.set_ext_apic_space(false); // TODO: I thought this was set. is it not? check PR
    leaf.set_perf_cntr_extensions(false);
    leaf.set_rdtscp(false);

    cpuid
        .set_extended_processor_and_feature_identifiers(Some(leaf))
        .expect("can set leaf 8000_0001h");

    // VMs on Milan currently get brand string and cache topology information from the host
    // processor, so replicate it to minimize changes for now.

    // Leaves 8000_0002 through 8000_0005
    cpuid
        .set_processor_brand_string(Some(b"AMD EPYC 7713P 64-Core Processor"))
        .expect("can set vCPU brand string");

    // Set up L1 cache+TLB info (leaf 8000_0005h)
    let mut leaf = L1CacheTlbInfo::empty();

    leaf.set_itlb_2m_4m_size(0x40);
    leaf.set_itlb_2m_4m_associativity(0xff);
    leaf.set_dtlb_2m_4m_size(0x40);
    leaf.set_dtlb_2m_4m_associativity(0xff);

    leaf.set_itlb_4k_size(0x40);
    leaf.set_itlb_4k_associativity(0xff);
    leaf.set_dtlb_4k_size(0x40);
    leaf.set_dtlb_4k_associativity(0xff);

    leaf.set_dcache_line_size(0x40);
    leaf.set_dcache_lines_per_tag(0x01);
    leaf.set_dcache_associativity(0x08);
    leaf.set_dcache_size(0x20);

    leaf.set_icache_line_size(0x40);
    leaf.set_icache_lines_per_tag(0x01);
    leaf.set_icache_associativity(0x08);
    leaf.set_icache_size(0x20);

    cpuid
        .set_l1_cache_and_tlb_info(Some(leaf))
        .expect("can set leaf 8000_0005h");

    // Set up L2 and L3 cache+TLB info (leaf 8000_0006h)
    let mut leaf = L2And3CacheTlbInfo::empty();

    // Set up leaf 8000_0006h EAX
    leaf.set_itlb_2m_4m_size(0x200);
    leaf.set_itlb_2m_4m_associativity(0x2);
    leaf.set_dtlb_2m_4m_size(0x800);
    leaf.set_dtlb_2m_4m_associativity(0x4);

    // Set up leaf 8000_0006h EBX
    leaf.set_itlb_4k_size(0x200);
    leaf.set_itlb_4k_associativity(0x4);
    leaf.set_dtlb_4k_size(0x800);
    leaf.set_dtlb_4k_associativity(0x6);

    // Set up leaf 8000_0006h ECX
    leaf.set_l2cache_line_size(0x40);
    leaf.set_l2cache_lines_per_tag(0x1);
    leaf.set_l2cache_associativity(0x6);
    leaf.set_l2cache_size(0x0200);

    // Set up leaf 8000_0006h EDX
    leaf.set_l3cache_line_size(0x40);
    leaf.set_l3cache_lines_per_tag(0x1);
    leaf.set_l3cache_associativity(0x9);
    leaf.set_l3cache_size(0x0200);

    cpuid
        .set_l2_l3_cache_and_tlb_info(Some(leaf))
        .expect("can set leaf 8000_0006h");

    // Set up TLB information for 1GiB pages (leaf 8000_0019h)
    let mut leaf = Tlb1gbPageInfo::empty();
    leaf.set_dtlb_l1_1gb_associativity(0xF);
    leaf.set_dtlb_l1_1gb_size(0x40);
    leaf.set_itlb_l1_1gb_associativity(0xF);
    leaf.set_itlb_l1_1gb_size(0x40);
    leaf.set_dtlb_l2_1gb_associativity(0xF);
    leaf.set_dtlb_l2_1gb_size(0x40);
    leaf.set_itlb_l2_1gb_associativity(0);
    leaf.set_itlb_l2_1gb_size(0);
    cpuid.set_tlb_1gb_page_info(Some(leaf)).expect("can set leaf 8000_0019h");

    // Set up extended cache hierarchy info (leaf 8000_001Dh)
    let mut levels = Vec::new();
    levels.push(CpuIdResult {
        eax: 0x00000121,
        ebx: 0x01C0003F,
        ecx: 0x0000003F,
        edx: 0x00000000,
    });
    levels.push(CpuIdResult {
        eax: 0x00000122,
        ebx: 0x01C0003F,
        ecx: 0x0000003F,
        edx: 0x00000000,
    });
    levels.push(CpuIdResult {
        eax: 0x00000143,
        ebx: 0x01C0003F,
        ecx: 0x000003FF,
        edx: 0x00000002,
    });
    levels.push(CpuIdResult {
        eax: 0x00000163,
        ebx: 0x03C0003F,
        ecx: 0x00007FFF,
        edx: 0x00000001,
    });
    cpuid
        .set_extended_cache_parameters(Some(levels.as_slice()))
        .expect("can set leaf 8000_001Dh");

    dump_to_cpuid_entries(cpuid.into_source())
}

fn dump_to_cpuid_entries(dump: CpuIdDump) -> Vec<CpuidEntry> {
    let mut entries = Vec::new();

    for (leaf, subleaf, regs) in dump.into_iter() {
        entries.push(CpuidEntry {
            leaf: leaf,
            subleaf: subleaf,
            eax: regs.eax,
            ebx: regs.ebx,
            ecx: regs.ecx,
            edx: regs.edx,
        });
    }

    // Entry order does not actually matter. Sort here because it's fast (~30-35 leaves) and
    // looking at the vec in logs or on the wire *so* much nicer.
    entries.sort_by(|left, right| {
        let by_leaf = left.leaf.cmp(&right.leaf);
        if by_leaf == std::cmp::Ordering::Equal {
            left.subleaf.cmp(&right.subleaf)
        } else {
            by_leaf
        }
    });

    entries
}

#[test]
fn milan_rfd314_is_as_described() {
    macro_rules! cpuid_leaf {
        ($leaf:literal, $eax:literal, $ebx:literal, $ecx:literal, $edx:literal) => {
            CpuidEntry {
                leaf: $leaf,
                subleaf: None,
                eax: $eax,
                ebx: $ebx,
                ecx: $ecx,
                edx: $edx,
            }
        };
    }

    macro_rules! cpuid_subleaf {
        ($leaf:literal, $sl:literal, $eax:literal, $ebx:literal, $ecx:literal, $edx:literal) => {
            CpuidEntry {
                leaf: $leaf,
                subleaf: Some($sl),
                eax: $eax,
                ebx: $ebx,
                ecx: $ecx,
                edx: $edx,
            }
        };
    }

    // This CPUID leaf blob is a collection of the leaves described in RFD 314.
    // RFD 314 is the source of truth for what bits are set here and why.
    // `milan_rfd314()` constructs what ought to be an *identical* set of bits,
    // but in a manner more amenable to machine validation that pairs of CPU
    // platforms are (or are not!) compatible, be they virtual (guest) CPUs or,
    // later, physical (host) CPUs.
    //
    // This is present only to validate initial CPU platforms work and as a link
    // between 314 and the present day. Actual guest CPU platforms may differ as
    // we enable additional guest functionality in the future; this is not a
    // source of truth for actual guest platforms.
    const MILAN_CPUID: [CpuidEntry; 33] = [
        cpuid_leaf!(0x0, 0x0000000D, 0x68747541, 0x444D4163, 0x69746E65),
        cpuid_leaf!(0x1, 0x00A00F11, 0x00000800, 0xF6D83203, 0x078BFBFF),
        cpuid_leaf!(0x5, 0x00000000, 0x00000000, 0x00000000, 0x00000000),
        cpuid_leaf!(0x6, 0x00000004, 0x00000000, 0x00000000, 0x00000000),
        cpuid_subleaf!(
            0x7, 0x0, 0x00000000, 0x219803A9, 0x00000600, 0x00000010
        ),
        cpuid_subleaf!(
            0x7, 0x1, 0x00000000, 0x00000000, 0x00000000, 0x00000000
        ),
        cpuid_subleaf!(
            0xB, 0x0, 0x00000001, 0x00000002, 0x00000100, 0x00000000
        ),
        cpuid_subleaf!(
            0xB, 0x1, 0x00000000, 0x00000000, 0x00000201, 0x00000000
        ),
        cpuid_subleaf!(
            0xB, 0x2, 0x00000000, 0x00000000, 0x00000002, 0x00000000
        ),
        cpuid_subleaf!(
            0xD, 0x0, 0x00000007, 0x00000340, 0x00000340, 0x00000000
        ),
        cpuid_subleaf!(
            0xD, 0x1, 0x00000007, 0x00000340, 0x00000000, 0x00000000
        ),
        cpuid_subleaf!(
            0xD, 0x2, 0x00000100, 0x00000240, 0x00000000, 0x00000000
        ),
        cpuid_leaf!(0x80000000, 0x80000021, 0x68747541, 0x444D4163, 0x69746E65),
        cpuid_leaf!(0x80000001, 0x00A00F11, 0x40000000, 0x444001F1, 0x27D3FBFF),
        cpuid_leaf!(0x80000002, 0x20444D41, 0x43595045, 0x31373720, 0x36205033),
        cpuid_leaf!(0x80000003, 0x6F432D34, 0x50206572, 0x65636F72, 0x726F7373),
        cpuid_leaf!(0x80000004, 0x20202020, 0x20202020, 0x20202020, 0x00202020),
        cpuid_leaf!(0x80000005, 0xFF40FF40, 0xFF40FF40, 0x20080140, 0x20080140),
        cpuid_leaf!(0x80000006, 0x48002200, 0x68004200, 0x02006140, 0x08009140),
        cpuid_leaf!(0x80000007, 0x00000000, 0x00000000, 0x00000000, 0x00000100),
        cpuid_leaf!(0x80000008, 0x00003030, 0x00000205, 0x00000000, 0x00000000),
        cpuid_leaf!(0x8000000A, 0x00000000, 0x00000000, 0x00000000, 0x00000000),
        cpuid_leaf!(0x80000019, 0xF040F040, 0xF0400000, 0x00000000, 0x00000000),
        cpuid_leaf!(0x8000001A, 0x00000006, 0x00000000, 0x00000000, 0x00000000),
        cpuid_leaf!(0x8000001B, 0x00000000, 0x00000000, 0x00000000, 0x00000000),
        cpuid_leaf!(0x8000001C, 0x00000000, 0x00000000, 0x00000000, 0x00000000),
        cpuid_subleaf!(
            0x8000001D, 0x0, 0x00000121, 0x01C0003F, 0x0000003F, 0x00000000
        ),
        cpuid_subleaf!(
            0x8000001D, 0x1, 0x00000122, 0x01C0003F, 0x0000003F, 0x00000000
        ),
        cpuid_subleaf!(
            0x8000001D, 0x2, 0x00000143, 0x01C0003F, 0x000003FF, 0x00000002
        ),
        cpuid_subleaf!(
            0x8000001D, 0x3, 0x00000163, 0x03C0003F, 0x00007FFF, 0x00000001
        ),
        cpuid_leaf!(0x8000001E, 0x00000000, 0x00000100, 0x00000000, 0x00000000),
        cpuid_leaf!(0x8000001F, 0x00000000, 0x00000000, 0x00000000, 0x00000000),
        cpuid_leaf!(0x80000021, 0x00000045, 0x00000000, 0x00000000, 0x00000000),
    ];

    let computed = milan_rfd314();

    // `milan_rfd314` sorts by leaf/subleaf, so everything *should* be in the
    // same order.. just a question if it's all the same:
    for (l, r) in MILAN_CPUID.iter().zip(computed.as_slice().iter()) {
        eprintln!("comparing {:#08x}.{:?}", l.leaf, l.subleaf);
        assert_eq!(
            l, r,
            "leaf {:#08x} (subleaf? {:?}) did not match",
            l.leaf, l.subleaf
        );
    }
}
