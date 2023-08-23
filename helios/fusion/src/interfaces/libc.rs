// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface to the libc API

pub trait Libc {
    /// sysconf(3c)
    fn sysconf(&self, arg: i32) -> std::io::Result<i64>;
}
