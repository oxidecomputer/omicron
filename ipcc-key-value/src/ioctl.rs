// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! IPCC `ioctl` interface.
//!
//! This module is tightly-coupled to the host OS image, and should only be used
//! by services bundled with it (e.g., sled-agent and installinator).

use crate::InstallinatorImageId;
use crate::InstallinatorImageIdError;
use crate::IpccKeyLookupError;
use crate::PingError;
use crate::Pong;
use libc::c_int;
use std::fs::File;
use std::io;
use std::os::unix::prelude::AsRawFd;

pub struct Ipcc {
    file: File,
}

impl Ipcc {
    pub fn open() -> io::Result<Self> {
        let file = File::options().read(true).write(true).open(IPCC_DEV)?;
        Ok(Self { file })
    }

    pub fn ping(&self) -> Result<Pong, PingError> {
        const EXPECTED_REPLY: &[u8] = b"pong";

        let mut buf = [0; EXPECTED_REPLY.len()];
        let n = self.key_lookup(IpccKey::Ping, &mut buf)?;
        let buf = &buf[..n];

        if buf == EXPECTED_REPLY {
            Ok(Pong)
        } else {
            Err(PingError::UnexpectedReply(buf.to_vec()))
        }
    }

    pub fn installinator_image_id(
        &self,
    ) -> Result<InstallinatorImageId, InstallinatorImageIdError> {
        let mut buf = [0; InstallinatorImageId::CBOR_SERIALIZED_SIZE];
        let n = self.key_lookup(IpccKey::InstallinatorImageId, &mut buf)?;
        let id = InstallinatorImageId::deserialize(&buf[..n])
            .map_err(InstallinatorImageIdError::DeserializationFailed)?;
        Ok(id)
    }

    fn key_lookup(
        &self,
        key: IpccKey,
        buf: &mut [u8],
    ) -> Result<usize, IpccKeyLookupError> {
        let mut kl = IpccKeyLookup {
            key: key as u8,
            buflen: u16::try_from(buf.len()).unwrap_or(u16::MAX),
            result: 0,
            datalen: 0,
            buf: buf.as_mut_ptr(),
        };

        let result = unsafe {
            libc::ioctl(
                self.file.as_raw_fd(),
                IPCC_KEYLOOKUP,
                &mut kl as *mut IpccKeyLookup,
            )
        };

        if result != 0 {
            let error = io::Error::last_os_error();
            return Err(IpccKeyLookupError::IoctlFailed { error });
        }

        match kl.result {
            IPCC_KEYLOOKUP_SUCCESS => Ok(usize::from(kl.datalen)),
            IPCC_KEYLOOKUP_UNKNOWN_KEY => {
                Err(IpccKeyLookupError::UnknownKey { key: format!("{key:?}") })
            }
            IPCC_KEYLOOKUP_NO_VALUE => Err(IpccKeyLookupError::NoValueForKey),
            IPCC_KEYLOOKUP_BUFFER_TOO_SMALL => {
                Err(IpccKeyLookupError::BufferTooSmallForValue)
            }
            _ => Err(IpccKeyLookupError::UnknownResultValue(kl.result)),
        }
    }
}

// --------------------------------------------------------------------
// IPCC keys; the source of truth for these is RFD 316 + the
// `host-sp-messages` crate in hubris.
// --------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum IpccKey {
    Ping = 0,
    InstallinatorImageId = 1,
}

// --------------------------------------------------------------------
// Constants and structures from stlouis `usr/src/uts/oxide/sys/ipcc.h`
// --------------------------------------------------------------------

const IPCC_DEV: &str = "/dev/ipcc";

const IPCC_IOC: c_int =
    ((b'i' as c_int) << 24) | ((b'c' as c_int) << 16) | ((b'c' as c_int) << 8);

const IPCC_KEYLOOKUP: c_int = IPCC_IOC | 4;

const IPCC_KEYLOOKUP_SUCCESS: u8 = 0;
const IPCC_KEYLOOKUP_UNKNOWN_KEY: u8 = 1;
const IPCC_KEYLOOKUP_NO_VALUE: u8 = 2;
const IPCC_KEYLOOKUP_BUFFER_TOO_SMALL: u8 = 3;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct IpccKeyLookup {
    key: u8,
    buflen: u16,
    result: u8,
    datalen: u16,
    buf: *mut u8,
}
