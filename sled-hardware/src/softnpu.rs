// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! 9P wire format used to identify the propolis SoftNPU device.
//!
//! Propolis exposes SoftNPU to a guest as a virtio 9p device whose server
//! answers Tversion with the version string `9P2000.P4`. The PCI ids match
//! any other virtio 9p device, so the version exchange is the discriminator.

/// Version string served by the propolis SoftNPU 9p handler.
pub const SOFTNPU_9P_VERSION: &str = "9P2000.P4";

/// Maximum message size offered in Tversion.
pub const MSIZE: u32 = 8192;

const TVERSION: u8 = 100;
const RVERSION: u8 = 101;
const NOTAG: u16 = 0xffff;

#[derive(Debug, thiserror::Error)]
pub enum SoftNpuDetectError {
    #[error("failed to walk device tree: {0}")]
    DevInfo(anyhow::Error),

    #[error("{path}: device busy")]
    Busy { path: String },

    #[error("{path}: {err}")]
    Io {
        path: String,
        #[source]
        err: std::io::Error,
    },

    #[error("{path}: malformed Rversion: {reason}")]
    Protocol { path: String, reason: String },
}

/// Encode a Tversion message.
///
/// Layout: size[4] type[1] tag[2] msize[4] version[s], little endian.
pub fn encode_tversion(version: &str) -> Vec<u8> {
    let version = version.as_bytes();
    let size = (4 + 1 + 2 + 4 + 2 + version.len()) as u32;
    let mut msg = Vec::with_capacity(size as usize);
    msg.extend_from_slice(&size.to_le_bytes());
    msg.push(TVERSION);
    msg.extend_from_slice(&NOTAG.to_le_bytes());
    msg.extend_from_slice(&MSIZE.to_le_bytes());
    msg.extend_from_slice(&(version.len() as u16).to_le_bytes());
    msg.extend_from_slice(version);
    msg
}

/// Decode the version string from an Rversion message.
///
/// Layout: size[4] type[1] tag[2] msize[4] version[s], little endian.
pub fn decode_rversion(msg: &[u8]) -> Result<String, String> {
    const HEADER_LEN: usize = 4 + 1 + 2 + 4 + 2;
    if msg.len() < HEADER_LEN {
        return Err(format!("short reply ({} bytes)", msg.len()));
    }
    if msg[4] != RVERSION {
        return Err(format!("unexpected message type {}", msg[4]));
    }
    let len = u16::from_le_bytes([msg[11], msg[12]]) as usize;
    let version = msg
        .get(HEADER_LEN..HEADER_LEN + len)
        .ok_or_else(|| "truncated version string".to_string())?;
    Ok(String::from_utf8_lossy(version).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rversion(version: &[u8]) -> Vec<u8> {
        let size = (4 + 1 + 2 + 4 + 2 + version.len()) as u32;
        let mut msg = Vec::new();
        msg.extend_from_slice(&size.to_le_bytes());
        msg.push(RVERSION);
        msg.extend_from_slice(&NOTAG.to_le_bytes());
        msg.extend_from_slice(&MSIZE.to_le_bytes());
        msg.extend_from_slice(&(version.len() as u16).to_le_bytes());
        msg.extend_from_slice(version);
        msg
    }

    #[test]
    fn tversion_layout() {
        let msg = encode_tversion(SOFTNPU_9P_VERSION);
        assert_eq!(msg.len(), 13 + SOFTNPU_9P_VERSION.len());
        assert_eq!(u32::from_le_bytes(msg[0..4].try_into().unwrap()), 22);
        assert_eq!(msg[4], TVERSION);
        assert_eq!(&msg[13..], SOFTNPU_9P_VERSION.as_bytes());
    }

    #[test]
    fn rversion_roundtrip() {
        let msg = rversion(SOFTNPU_9P_VERSION.as_bytes());
        assert_eq!(decode_rversion(&msg).unwrap(), SOFTNPU_9P_VERSION);
        let msg = rversion(b"9P2000.L");
        assert_eq!(decode_rversion(&msg).unwrap(), "9P2000.L");
    }

    #[test]
    fn rversion_malformed() {
        assert!(decode_rversion(&[]).is_err());
        assert!(decode_rversion(&[0; 12]).is_err());

        let mut msg = rversion(SOFTNPU_9P_VERSION.as_bytes());
        msg[4] = TVERSION;
        assert!(decode_rversion(&msg).is_err());

        let mut msg = rversion(SOFTNPU_9P_VERSION.as_bytes());
        msg.truncate(msg.len() - 1);
        assert!(decode_rversion(&msg).is_err());
    }
}
