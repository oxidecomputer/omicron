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

/// Maximum message size offered in Tversion. The server must answer with a
/// value no larger than this.
const MSIZE: u32 = 8192;
const TVERSION: u8 = 100;
const RVERSION: u8 = 101;
const NOTAG: u16 = 0xffff;

// Field offsets in a version message, laid out as
// size[4] type[1] tag[2] msize[4] version[s], little endian. The string
// field is a 2-byte length followed by the bytes.
const TYPE_OFFSET: usize = 4;
const TAG_OFFSET: usize = 5;
const MSIZE_OFFSET: usize = 7;
const VERSION_LEN_OFFSET: usize = 11;
const VERSION_OFFSET: usize = 13;

/// Total size of a version message carrying `version`.
fn msg_size(version: &[u8]) -> usize {
    VERSION_OFFSET + version.len()
}

/// Encode a Tversion message.
pub fn encode_tversion(version: &str) -> Vec<u8> {
    let version = version.as_bytes();
    let size: u32 =
        msg_size(version).try_into().expect("version message fits in u32");
    let len: u16 =
        version.len().try_into().expect("version string fits in u16");
    let mut msg = Vec::with_capacity(msg_size(version));
    msg.extend_from_slice(&size.to_le_bytes());
    msg.push(TVERSION);
    msg.extend_from_slice(&NOTAG.to_le_bytes());
    msg.extend_from_slice(&MSIZE.to_le_bytes());
    msg.extend_from_slice(&len.to_le_bytes());
    msg.extend_from_slice(version);
    msg
}

/// Decode the version string from an Rversion message. The reply must carry
/// NOTAG and an msize no larger than the one offered in Tversion.
pub fn decode_rversion(msg: &[u8]) -> Result<String, String> {
    if msg.len() < VERSION_OFFSET {
        return Err(format!("short reply ({} bytes)", msg.len()));
    }
    if msg[TYPE_OFFSET] != RVERSION {
        return Err(format!("unexpected message type {}", msg[TYPE_OFFSET]));
    }
    let tag = u16::from_le_bytes([msg[TAG_OFFSET], msg[TAG_OFFSET + 1]]);
    if tag != NOTAG {
        return Err(format!("unexpected tag {tag:#x}"));
    }
    let msize = u32::from_le_bytes(
        msg[MSIZE_OFFSET..MSIZE_OFFSET + 4].try_into().unwrap(),
    );
    if msize == 0 || msize > MSIZE {
        return Err(format!("invalid msize {msize}"));
    }
    let len = usize::from(u16::from_le_bytes([
        msg[VERSION_LEN_OFFSET],
        msg[VERSION_LEN_OFFSET + 1],
    ]));
    let version = msg
        .get(VERSION_OFFSET..VERSION_OFFSET + len)
        .ok_or_else(|| "truncated version string".to_string())?;
    Ok(String::from_utf8_lossy(version).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rversion(version: &[u8], tag: u16, msize: u32) -> Vec<u8> {
        let mut msg = encode_tversion(std::str::from_utf8(version).unwrap());
        msg[TYPE_OFFSET] = RVERSION;
        msg[TAG_OFFSET..TAG_OFFSET + 2].copy_from_slice(&tag.to_le_bytes());
        msg[MSIZE_OFFSET..MSIZE_OFFSET + 4]
            .copy_from_slice(&msize.to_le_bytes());
        msg
    }

    #[test]
    fn tversion_layout() {
        let msg = encode_tversion(SOFTNPU_9P_VERSION);
        assert_eq!(msg.len(), msg_size(SOFTNPU_9P_VERSION.as_bytes()));
        assert_eq!(
            u32::from_le_bytes(msg[0..4].try_into().unwrap()),
            msg.len() as u32
        );
        assert_eq!(msg[TYPE_OFFSET], TVERSION);
        assert_eq!(&msg[VERSION_OFFSET..], SOFTNPU_9P_VERSION.as_bytes());
    }

    #[test]
    fn rversion_roundtrip() {
        let msg = rversion(SOFTNPU_9P_VERSION.as_bytes(), NOTAG, MSIZE);
        assert_eq!(decode_rversion(&msg).unwrap(), SOFTNPU_9P_VERSION);
        let msg = rversion(b"9P2000.L", NOTAG, MSIZE / 2);
        assert_eq!(decode_rversion(&msg).unwrap(), "9P2000.L");
    }

    #[test]
    fn rversion_malformed() {
        assert!(decode_rversion(&[]).is_err());
        assert!(decode_rversion(&[0; VERSION_OFFSET - 1]).is_err());

        let mut msg = rversion(SOFTNPU_9P_VERSION.as_bytes(), NOTAG, MSIZE);
        msg[TYPE_OFFSET] = TVERSION;
        assert!(decode_rversion(&msg).is_err());

        let mut msg = rversion(SOFTNPU_9P_VERSION.as_bytes(), NOTAG, MSIZE);
        msg.truncate(msg.len() - 1);
        assert!(decode_rversion(&msg).is_err());
    }

    #[test]
    fn rversion_bad_tag_or_msize() {
        let msg = rversion(SOFTNPU_9P_VERSION.as_bytes(), 1, MSIZE);
        assert!(decode_rversion(&msg).is_err());
        let msg = rversion(SOFTNPU_9P_VERSION.as_bytes(), NOTAG, MSIZE + 1);
        assert!(decode_rversion(&msg).is_err());
        let msg = rversion(SOFTNPU_9P_VERSION.as_bytes(), NOTAG, 0);
        assert!(decode_rversion(&msg).is_err());
    }
}
