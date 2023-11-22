// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for interacting with disk control operations (`man dkio`).

use libc::c_int;
use libc::c_uint;
use std::io;
use std::ptr;

pub fn flush_write_cache(fd: i32) -> Result<(), io::Error> {
    // Safety: We are issuing the `DKIOCFLUSHWRITECACHE` ioctl which is
    // documented to take a an argument of `NULL` when called from user-mode.
    // Due to the definition of `ioctl`, we have to turbofish the `null_mut()`
    // call to _something_; any raw C type is fine (we don't want a Rust fat
    // pointer type).
    let rc = unsafe {
        libc::ioctl(fd, DKIOCFLUSHWRITECACHE as _, ptr::null_mut::<i32>())
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaInfoExtended {
    pub media_type: MediaType,
    pub logical_block_size: u32,
    /// Capacity is in units of logical blocks.
    pub capacity: u64,
    pub physical_block_size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaType {
    Unknown,
    MoErasable,
    MoWriteOnce,
    AsMo,
    Cdrom,
    Cdr,
    Cdrw,
    Dvdrom,
    Dvdr,
    Dvdram,
    FixedDisk,
    Floppy,
    Zip,
    Jaz,
}

impl MediaInfoExtended {
    pub fn from_fd(fd: i32) -> Result<Self, io::Error> {
        let mut minfo = dk_minfo_ext::default();

        // Safety: We are issuing the `DKIOCGMEDIAINFOEXT` ioctl which is
        // documented to take a struct of type `dk_minfo_ext`. Assuming our type
        // definitions below (which are a combination of man page details and
        // headers) are correct, this call is safe.
        let rc =
            unsafe { libc::ioctl(fd, DKIOCGMEDIAINFOEXT as _, &mut minfo) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        let media_type = match minfo.dki_media_type {
            DK_MO_ERASABLE => MediaType::MoErasable,
            DK_MO_WRITEONCE => MediaType::MoWriteOnce,
            DK_AS_MO => MediaType::AsMo,
            DK_CDROM => MediaType::Cdrom,
            DK_CDR => MediaType::Cdr,
            DK_CDRW => MediaType::Cdrw,
            DK_DVDROM => MediaType::Dvdrom,
            DK_DVDR => MediaType::Dvdr,
            DK_DVDRAM => MediaType::Dvdram,
            DK_FIXED_DISK => MediaType::FixedDisk,
            DK_FLOPPY => MediaType::Floppy,
            DK_ZIP => MediaType::Zip,
            DK_JAZ => MediaType::Jaz,
            _ => MediaType::Unknown,
        };

        Ok(Self {
            media_type,
            logical_block_size: minfo.dki_lbsize,
            capacity: minfo.dki_capacity,
            physical_block_size: minfo.dki_pbsize,
        })
    }
}

// Types and constants from `man dkio` under `DKIOCGMEDIAINFOEXT`

#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
#[allow(non_camel_case_types)]
struct dk_minfo_ext {
    dki_media_type: c_uint,
    dki_lbsize: c_uint,
    dki_capacity: diskaddr_t,
    dki_pbsize: c_uint,
}

// We map all unknown values to `Unknown`, including `DK_UNKNOWN`, so we prefix
// it with `_` to show it's not forgotten but not used.
const _DK_UNKNOWN: c_uint = 0x00; /* Media inserted - type unknown */

const DK_MO_ERASABLE: c_uint = 0x03; /* MO Erasable */
const DK_MO_WRITEONCE: c_uint = 0x04; /* MO Write once */
const DK_AS_MO: c_uint = 0x05; /* AS MO */
const DK_CDROM: c_uint = 0x08; /* CDROM */
const DK_CDR: c_uint = 0x09; /* CD-R */
const DK_CDRW: c_uint = 0x0A; /* CD-RW */
const DK_DVDROM: c_uint = 0x10; /* DVD-ROM */
const DK_DVDR: c_uint = 0x11; /* DVD-R */
const DK_DVDRAM: c_uint = 0x12; /* DVD_RAM or DVD-RW */

const DK_FIXED_DISK: c_uint = 0x10001; /* Fixed disk SCSI or otherwise */
const DK_FLOPPY: c_uint = 0x10002; /* Floppy media */
const DK_ZIP: c_uint = 0x10003; /* IOMEGA ZIP media */
const DK_JAZ: c_uint = 0x10004; /* IOMEGA JAZ media */

// Related types and constants from `/usr/include/sys/types.h`

#[allow(non_camel_case_types)]
type diskaddr_t = libc::c_ulonglong;

// Related types and constants from `/usr/include/sys/dkio.h`

const DKIOC: c_int = 0x04 << 8;
const DKIOCGMEDIAINFOEXT: c_int = DKIOC | 48;
const DKIOCFLUSHWRITECACHE: c_int = DKIOC | 34;
