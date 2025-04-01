// The C-style type names used here are to be closer to the header definition,
// but run afoul of Rust style.
#![allow(non_camel_case_types)]

use std::ffi::c_void;

type uchar_t = u16;
type ushort_t = u16;
type uint_t = u32;
type ulong_t = u32;
type u_offset_t = u64;
type pfn_t = ulong_t;

/// Only defined in support of `page_t` below.
#[derive(Copy, Clone)]
#[repr(C)]
struct kcondvar_t {
    _opaque: ushort_t,
}

/// Only defined in support of `page_t` below.
#[derive(Copy, Clone)]
#[repr(C)]
struct kmutex_t {
    _opaque: *const c_void,
}

/// `page_t` is a fairly internal structure to illumos, but we use its size in
/// estimation of expected host OS allocations over in `sled-agent`.
///
/// It is public to be used in `header-check`, but `doc(hidden)` as it's really
/// intended to be used only for `page_t_size()`. There is no point where the
/// kernel offers users a `page_t`, nor where a user should be offering the
/// kernel a `page_t`.
#[derive(Clone, Copy)]
#[repr(C)]
#[doc(hidden)]
pub struct page {
    // pub field to produce at least one field offset test. We don't need to use
    // the field itself, but ctest2 will produce an `offset_of` macro even if
    // it's unused, in which case we'll get an annoying warning.
    pub p_offset: u_offset_t,
    p_vnode: *const c_void,
    p_vpmref: uint_t,
    p_hash: *const c_void,
    p_vpnext: *const c_void,
    p_vpprev: *const c_void,
    p_next: *const c_void,
    p_prev: *const c_void,
    p_lckcnt: ushort_t,
    p_cowcnt: ushort_t,
    p_cv: kcondvar_t,
    p_io_cv: kcondvar_t,
    p_iolock_state: uchar_t,
    p_szc: uchar_t,
    p_fsdata: uchar_t,
    p_state: uchar_t,
    p_nrm: uchar_t,
    p_embed: uchar_t,
    p_index: uchar_t,
    p_toxic: uchar_t,
    p_mapping: *const c_void,
    p_pagenum: pfn_t,
    p_share: uint_t,
    p_sharepad: uint_t,
    p_mlentry: uint_t,
    p_ilock: kmutex_t,
}

pub const fn page_t_size() -> usize {
    std::mem::size_of::<page>()
}
