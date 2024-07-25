// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{
    ffi::{c_int, CStr, CString},
    ptr,
};

use crate::IpccError;
use crate::{ffi::*, IpccErrorInner};

pub struct IpccHandle(*mut libipcc_handle_t);

impl Drop for IpccHandle {
    fn drop(&mut self) {
        unsafe {
            libipcc_fini(self.0);
        }
    }
}
fn ipcc_fatal_error<C: Into<String>>(
    context: C,
    lerr: libipcc_err_t,
    syserr: c_int,
    errmsg: CString,
) -> IpccError {
    let context = context.into();
    let syserr = if syserr == 0 {
        "no system errno".to_string()
    } else {
        std::io::Error::from_raw_os_error(syserr).to_string()
    };
    let inner = IpccErrorInner {
        context,
        errmsg: errmsg.to_string_lossy().into_owned(),
        syserr,
    };
    match lerr {
        LIBIPCC_ERR_OK => panic!("called fatal on LIBIPCC_ERR_OK"),
        LIBIPCC_ERR_NO_MEM => IpccError::NoMem(inner),
        LIBIPCC_ERR_INVALID_PARAM => IpccError::InvalidParam(inner),
        LIBIPCC_ERR_INTERNAL => IpccError::Internal(inner),
        LIBIPCC_ERR_KEY_UNKNOWN => IpccError::KeyUnknown(inner),
        LIBIPCC_ERR_KEY_BUFTOOSMALL => IpccError::KeyBufTooSmall(inner),
        LIBIPCC_ERR_KEY_READONLY => IpccError::KeyReadonly(inner),
        LIBIPCC_ERR_KEY_VALTOOLONG => IpccError::KeyValTooLong(inner),
        LIBIPCC_ERR_KEY_ZERR => IpccError::KeyZerr(inner),
        _ => IpccError::UnknownErr(inner),
    }
}

impl IpccHandle {
    pub fn new() -> Result<Self, IpccError> {
        let mut ipcc_handle: *mut libipcc_handle_t = ptr::null_mut();
        // We subtract 1 from the length of the inital vector since CString::new
        // will append a nul for us.
        // Safety: Unwrapped because we guarantee that the supplied bytes
        // contain no 0 bytes up front.
        let errmsg = CString::new(vec![1; LIBIPCC_ERR_LEN - 1]).unwrap();
        let errmsg_len = errmsg.as_bytes().len();
        let errmsg_ptr = errmsg.into_raw();
        let mut lerr = LIBIPCC_ERR_OK;
        let mut syserr = 0;
        if !unsafe {
            libipcc_init(
                &mut ipcc_handle,
                &mut lerr,
                &mut syserr,
                errmsg_ptr,
                errmsg_len,
            )
        } {
            // Safety: CString::from_raw retakes ownership of a CString
            // transferred to C via CString::into_raw. We are calling into_raw()
            // above so it is safe to turn this back into it's owned variant.
            let errmsg = unsafe { CString::from_raw(errmsg_ptr) };
            return Err(ipcc_fatal_error(
                "Could not init libipcc handle",
                lerr,
                syserr,
                errmsg,
            ));
        }

        Ok(IpccHandle(ipcc_handle))
    }

    fn fatal<C: Into<String>>(&self, context: C) -> IpccError {
        let lerr = unsafe { libipcc_err(self.0) };
        let errmsg = unsafe { libipcc_errmsg(self.0) };
        // Safety: CStr::from_ptr is documented as safe if:
        //   1. The pointer contains a valid null terminator at the end of
        //      the string
        //   2. The pointer is valid for reads of bytes up to and including
        //      the null terminator
        //   3. The memory referenced by the return CStr is not mutated for
        //      the duration of lifetime 'a
        //
        // (1) is true because this crate initializes space for an error message
        // via CString::new which adds a terminator on our behalf.
        // (2) should be guaranteed by libipcc itself since it is writing error
        // messages into the CString backed buffer that we gave it.
        // (3) We aren't currently mutating the memory referenced by the
        // CStr, and we are creating an owned copy of the data immediately so
        // that it can outlive the lifetime of the libipcc handle if needed.
        let errmsg = unsafe { CStr::from_ptr(errmsg) }.to_owned();
        let syserr = unsafe { libipcc_syserr(self.0) };
        ipcc_fatal_error(context, lerr, syserr, errmsg)
    }

    pub(crate) fn key_lookup(
        &self,
        key: u8,
        buf: &mut [u8],
    ) -> Result<usize, IpccError> {
        let mut lenp = buf.len();

        if !unsafe {
            libipcc_keylookup(self.0, key, &mut buf.as_mut_ptr(), &mut lenp, 0)
        } {
            return Err(self.fatal(format!("lookup of key {key} failed")));
        }

        Ok(lenp)
    }

    pub(crate) fn rot_request(
        &self,
        req: &[u8],
        resp: &mut [u8],
    ) -> Result<usize, IpccError> {
        let mut ipcc_rot_resp: *mut libipcc_rot_resp_t = ptr::null_mut();
        if !unsafe {
            libipcc_rot_send(
                self.0,
                req.as_ptr(),
                req.len(),
                &mut ipcc_rot_resp,
            )
        } {
            return Err(self.fatal("rot_send failed"));
        }

        let mut lenp = resp.len();

        let data = unsafe { libipcc_rot_resp_get(ipcc_rot_resp, &mut lenp) };

        let slice = unsafe { core::slice::from_raw_parts(data, lenp) };
        resp[..lenp].copy_from_slice(&slice);

        unsafe {
            libipcc_rot_resp_free(ipcc_rot_resp);
        }
        Ok(lenp)
    }
}
