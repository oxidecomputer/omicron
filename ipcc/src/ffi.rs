// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::ffi::{c_char, c_int, c_uint};

/// Opaque libipcc handle
#[repr(C)]
pub(crate) struct libipcc_handle_t {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

/// Indicates that there was no error. Used as the initialized value when
/// calling into libipcc.
pub(crate) const LIBIPCC_ERR_OK: libipcc_err_t = 0;

/// Indicates that there was a memory allocation error. The system error
/// contains the specific errno.
pub(crate) const LIBIPCC_ERR_NO_MEM: libipcc_err_t = 1;

/// One of the function parameters does not pass validation. There will be more
/// detail available via libipcc_errmsg().
pub(crate) const LIBIPCC_ERR_INVALID_PARAM: libipcc_err_t = 2;

/// An internal error occurred. There will be more detail available via
/// libipcc_errmsg() and libipcc_syserr().
pub(crate) const LIBIPCC_ERR_INTERNAL: libipcc_err_t = 3;

/// The requested lookup key was not known to the SP.
pub(crate) const LIBIPCC_ERR_KEY_UNKNOWN: libipcc_err_t = 4;

/// The value for the requested lookup key was too large for the
/// supplied buffer.
pub(crate) const LIBIPCC_ERR_KEY_BUFTOOSMALL: libipcc_err_t = 5;

/// An attempt to write to a key failed because the key is read-only.
pub(crate) const LIBIPCC_ERR_KEY_READONLY: libipcc_err_t = 6;

/// An attempt to write to a key failed because the passed value is too
/// long.
pub(crate) const LIBIPCC_ERR_KEY_VALTOOLONG: libipcc_err_t = 7;

/// Compression or decompression failed. If appropriate, libipcc_syserr() will
/// return the Z_ error from zlib.
pub(crate) const LIBIPCC_ERR_KEY_ZERR: libipcc_err_t = 8;
pub(crate) type libipcc_err_t = c_uint;

/// Maxium length of an error message retrieved by libipcc_errmsg().
pub(crate) const LIBIPCC_ERR_LEN: usize = 1024;

/// Flags that can be passed to libipcc when looking up a key. Today this is
/// used for looking up a compressed key, however nothing in the public API of
/// this crate takes advantage of this.
pub(crate) type libipcc_key_flag_t = ::std::os::raw::c_uint;

/// Opaque rot_resp_t handle
#[repr(C)]
pub(crate) struct libipcc_rot_resp_t {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

/// These aren't strictly part of libipcc but are used as part of message calculations
/// for RoT
pub(crate) const IPCC_MIN_MESSAGE_SIZE: usize = 19;
pub(crate) const IPCC_MAX_MESSAGE_SIZE: usize = 4123;
pub(crate) const IPCC_MAX_DATA_SIZE: usize =
    IPCC_MAX_MESSAGE_SIZE - IPCC_MIN_MESSAGE_SIZE;

#[link(name = "ipcc")]
extern "C" {
    pub(crate) fn libipcc_init(
        lihp: *mut *mut libipcc_handle_t,
        libipcc_errp: *mut libipcc_err_t,
        syserrp: *mut c_int,
        errmsg: *const c_char,
        errlen: usize,
    ) -> bool;
    pub(crate) fn libipcc_fini(lih: *mut libipcc_handle_t);
    pub(crate) fn libipcc_err(lih: *mut libipcc_handle_t) -> libipcc_err_t;
    pub(crate) fn libipcc_syserr(lih: *mut libipcc_handle_t) -> c_int;
    pub(crate) fn libipcc_errmsg(lih: *mut libipcc_handle_t) -> *const c_char;
    pub(crate) fn libipcc_keylookup(
        lih: *mut libipcc_handle_t,
        key: u8,
        bufp: *mut *mut u8,
        lenp: *mut usize,
        flags: libipcc_key_flag_t,
    ) -> bool;

    pub(crate) fn libipcc_rot_send(
        lih: *mut libipcc_handle_t,
        request: *const u8,
        len: usize,
        rotrp: *mut *mut libipcc_rot_resp_t,
    ) -> bool;
    pub(crate) fn libipcc_rot_resp_get(
        rotrp: *mut libipcc_rot_resp_t,
        lenp: *mut usize,
    ) -> *const u8;
    pub(crate) fn libipcc_rot_resp_free(rotrp: *mut libipcc_rot_resp_t);
}
