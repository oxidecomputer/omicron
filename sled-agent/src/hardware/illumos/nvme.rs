// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! ioctls for querying NVMe devices
//!
//! For more context on the structures referenced in this file, refer to:
//! <https://github.com/illumos/illumos-gate/blob/98f586d71279849001034981c5906e9e3901d56f/usr/src/uts/common/sys/nvme.h>

use std::fs::OpenOptions;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

/// Minimal subset of the NVMe ID Controller data structure used to identify a WDC device.
#[derive(Debug, Clone)]
pub struct ControllerId {
    pub vendor_id: u16,
    pub subsystem_vendor_id: u16,
    pub serial: String,
    pub model: String,
    pub firmware_rev: String,
    pub oui: [u8; 3],
    pub unique_controller_id: u16,
    pub nvme_version: String,
    pub fru_guid: String,
}

pub(crate) const NVME_ID_CTRL_BUFSIZE: usize = 4096;

impl From<&[u8]> for ControllerId {
    fn from(buf: &[u8]) -> ControllerId {
        assert!(buf.len() >= NVME_ID_CTRL_BUFSIZE);
        let vendor_id = u16::from_ne_bytes(buf[..2].try_into().unwrap());
        let subsystem_vendor_id =
            u16::from_ne_bytes(buf[2..4].try_into().unwrap());
        let serial = String::from_utf8_lossy(&buf[4..24]).trim().to_string();
        let model = String::from_utf8_lossy(&buf[24..64]).trim().to_string();
        let firmware_rev =
            String::from_utf8_lossy(&buf[64..72]).trim().to_string();
        let oui = buf[73..76].try_into().unwrap();
        let unique_controller_id =
            u16::from_ne_bytes(buf[78..80].try_into().unwrap());
        let version = u32::from_ne_bytes(buf[80..84].try_into().unwrap());
        let major = (version & 0xFFFF_0000) >> 16;
        let minor = (version & 0x0000_FF00) >> 8;
        let tertiary = version & 0x0000_00FF;
        let nvme_version = format!("{}.{}.{}", major, minor, tertiary);
        let fru_guid = buf[112..128]
            .iter()
            .map(|x| format!("{:x}", x))
            .collect::<Vec<_>>()
            .join("");
        ControllerId {
            vendor_id,
            subsystem_vendor_id,
            serial,
            model,
            firmware_rev,
            oui,
            unique_controller_id,
            nvme_version,
            fru_guid,
        }
    }
}

/// FFI-safe version of the `nvme_ioctl_t` used to communicate with the `nvme` driver
#[repr(C)]
#[derive(Debug)]
struct Ioctl {
    // Length of `n_buf`
    n_len: libc::size_t,
    // Data buffer for command
    n_buf: usize,
    // The command flag or other data
    n_arg: u64,
}

// Ioctl command definitions
const NVME_IOC: i32 =
    ('N' as i32) << 24 | ('V' as i32) << 16 | ('M' as i32) << 8;
const NVME_IOC_IDENTIFY_CTRL: i32 = NVME_IOC | 1;
const NVME_IDENTIFY_CTRL: u64 = 1;

fn ioctl(
    dev: &PathBuf,
    cmd: i32,
    buffer: &mut [u8],
    arg: u64,
) -> Result<(), io::Error> {
    let mut ioc =
        Ioctl { n_len: buffer.len(), n_buf: buffer.as_ptr() as _, n_arg: arg };
    let file = OpenOptions::new().read(true).write(true).open(&dev)?;
    let rv = unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            cmd,
            (&mut ioc) as *const _ as *mut libc::c_void,
        )
    };
    if rv == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Run the NVMe Identify Controller command for the given device.
pub(crate) fn identify_controller(
    dev: &PathBuf,
) -> Result<ControllerId, io::Error> {
    let mut buffer = vec![0u8; NVME_ID_CTRL_BUFSIZE];
    ioctl(dev, NVME_IOC_IDENTIFY_CTRL, &mut buffer, NVME_IDENTIFY_CTRL)?;
    Ok(ControllerId::from(&buffer[..]))
}
