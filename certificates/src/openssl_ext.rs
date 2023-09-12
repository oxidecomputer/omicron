// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extensions to the `openssl` and `openssl-sys` crates that have not yet been
//! published upstream.

use foreign_types::ForeignTypeRef;
use openssl::error::ErrorStack;
use openssl::x509::X509Ref;
use openssl_sys::X509 as RawX509;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_uint;
use std::ffi::CStr;
use std::ptr;

extern "C" {
    // `X509_check_host()` is only exported by `openssl-sys` if the `bindgen`
    // feature is enabled
    // (https://github.com/sfackler/rust-openssl/issues/2041). For now, we'll
    // define the function ourselves; it was added in OpenSSL 1.0.2 and did not
    // change in OpenSSL 3.0, and we do not need to support anything older.
    fn X509_check_host(
        cert: *mut RawX509,
        name: *const c_char,
        namelen: usize,
        flags: c_uint,
        peername: *mut *mut c_char,
    ) -> c_int;
}

pub(crate) trait X509Ext {
    fn valid_for_hostname(&self, name: &CStr) -> Result<bool, ErrorStack>;
}

impl X509Ext for X509Ref {
    fn valid_for_hostname(&self, name: &CStr) -> Result<bool, ErrorStack> {
        // Safety: We know `self` is a valid X509Ref and `hostname` is a valid C
        // string. We pass a hostname length of 0 which instructs OpenSSL to use
        // `strlen()` to check its length. We do not pass any flags and do not
        // want the cert name returned to us.
        let rc = unsafe {
            X509_check_host(self.as_ptr(), name.as_ptr(), 0, 0, ptr::null_mut())
        };

        match rc {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(ErrorStack::get()),
        }
    }
}
