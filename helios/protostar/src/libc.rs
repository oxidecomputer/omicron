// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Miscellaneous FFI wrapper functions for libc

use helios_fusion::interfaces::libc::Libc;

#[derive(Default)]
pub struct RealLibc {}

impl Libc for RealLibc {
    fn sysconf(&self, arg: i32) -> std::io::Result<i64> {
        let res = unsafe { libc::sysconf(arg) };
        if res == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(res)
    }
}
