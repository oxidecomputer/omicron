// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Access to sysconf-related info.

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("sysconf failed accessing {arg}: {e}")]
    Sysconf { arg: String, e: std::io::Error },

    #[error("Integer conversion error: {0}")]
    Integer(#[from] std::num::TryFromIntError),
}

fn sysconf(argname: &str, arg: libc::c_int) -> Result<u64, Error> {
    let r = unsafe { libc::sysconf(arg) };
    if r == -1 {
        return Err(Error::Sysconf {
            arg: argname.to_string(),
            e: std::io::Error::last_os_error(),
        });
    }
    Ok(r.try_into()?)
}

/// Returns the number of online processors on this sled.
pub fn online_processor_count() -> Result<u32, Error> {
    // Although the value returned by sysconf is an i64, we parse
    // the value as a u32.
    //
    // A value greater than u32::MAX (or a negative value) would return
    // an error here.
    Ok(u32::try_from(sysconf(
        "online processor count",
        libc::_SC_NPROCESSORS_ONLN,
    )?)?)
}

/// Returns the amount of RAM on this sled, in bytes.
pub fn usable_physical_ram_bytes() -> Result<u64, Error> {
    let phys_pages = sysconf("physical pages", libc::_SC_PHYS_PAGES)?;
    let page_size = sysconf("physical page size", libc::_SC_PAGESIZE)?;
    Ok(phys_pages * page_size)
}
