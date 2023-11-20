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
        let syserr = unsafe { libc::strerror(syserr) };
        unsafe { CStr::from_ptr(syserr) }.to_string_lossy().into_owned()
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
        LIBIPCC_ERR_KEY_UNKNOWN => IpccError::Internal(inner),
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
        let errmsg = CString::new(vec![1; 1024]).unwrap();
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
}
