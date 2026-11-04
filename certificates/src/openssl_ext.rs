// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extensions to the `openssl` and `openssl-sys` crates that have not yet been
//! published upstream.

use foreign_types::ForeignTypeRef;
use openssl::error::ErrorStack;
use openssl::nid::Nid;
use openssl::x509::X509Ref;
use openssl_sys::X509 as RawX509;
use std::ffi::CStr;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_uint;
use std::ptr;

unsafe extern "C" {
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
    /// Check whether this cert is valid for the provided `hostname`.
    fn valid_for_hostname(&self, hostname: &CStr) -> Result<bool, ErrorStack>;

    /// Return a description of the hostname(s) of this cert: either the Subject
    /// Alternate Names, if present, or the Common Name if not.
    fn hostname_description(&self) -> Result<String, ErrorStack>;

    /// Returns the extended key usage bitmask from
    /// `X509_get_extended_key_usage()` if this cert has an EKU extension.
    fn extended_key_usage(&self) -> Option<u32>;
}

impl X509Ext for X509Ref {
    fn valid_for_hostname(&self, hostname: &CStr) -> Result<bool, ErrorStack> {
        // Safety: We know `self` is a valid X509Ref and `hostname` is a valid C
        // string. We pass a hostname length of 0 which instructs OpenSSL to use
        // `strlen()` to check its length. We do not pass any flags and do not
        // want the cert name returned to us.
        let rc = unsafe {
            X509_check_host(
                self.as_ptr(),
                hostname.as_ptr(),
                0,
                0,
                ptr::null_mut(),
            )
        };

        match rc {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(ErrorStack::get()),
        }
    }

    fn hostname_description(&self) -> Result<String, ErrorStack> {
        // Most expected case: we have SANs.
        if let Some(stack) = self.subject_alt_names() {
            let mut dns_names = Vec::new();
            for item in &stack {
                if let Some(name) = item.dnsname() {
                    dns_names.push(name);
                }
            }

            return if dns_names.is_empty() {
                Ok("SANs present but no DNS names found".to_string())
            } else {
                Ok(format!("SANs: {}", dns_names.join(", ")))
            };
        }

        // Less expected: no SANs, so we check the CN.
        let subject_name = self.subject_name();
        let mut common_names = subject_name
            .entries_by_nid(Nid::COMMONNAME)
            .map(|entry| entry.data().as_utf8());

        // Check for very unexpected contents: zero or multiple CNs.
        let Some(common_name) = common_names.next() else {
            return Ok("Neither SANs nor CN present".to_string());
        };
        let common_name = common_name?;
        if let Some(next_common_name) = common_names.next() {
            let next_common_name = next_common_name?;
            let mut all_cns = format!("{}, {}", common_name, next_common_name);
            for cn in common_names {
                let cn = cn?;
                all_cns.push_str(", ");
                all_cns.push_str(&cn);
            }
            return Ok(format!("Multiple CNs: {all_cns}"));
        }

        Ok(format!("CN: {common_name}"))
    }

    fn extended_key_usage(&self) -> Option<u32> {
        let extension_flags =
            unsafe { openssl_sys::X509_get_extension_flags(self.as_ptr()) };

        if extension_flags & openssl_sys::EXFLAG_XKUSAGE == 0 {
            return None;
        }

        let eku =
            unsafe { openssl_sys::X509_get_extended_key_usage(self.as_ptr()) };

        Some(eku)
    }
}
