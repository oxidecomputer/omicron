// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Detection of the propolis SoftNPU 9p device.

use crate::SwitchDetectError;
use crate::softnpu::{SOFTNPU_9P_VERSION, decode_rversion, encode_tversion};
use illumos_devinfo::{DevInfo, Node};
use slog::{Logger, debug, info};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::time::Duration;

const VIRTIO_VENDOR_ID: i32 = 0x1af4;
// Transitional and modern virtio 9p PCI device ids.
const VIRTIO_9P_DEVICE_IDS: [i32; 2] = [0x1009, 0x1049];
const NINEP_MINOR: &str = "9p";
const OPEN_ATTEMPTS: usize = 3;
const OPEN_RETRY_DELAY: Duration = Duration::from_millis(500);
/// A vio9p read returns one whole 9P message or EOVERFLOW without
/// consuming it when the buffer is smaller than the message's own size
/// field. The reply will always fit here.
const REPLY_BUF_LEN: usize = 65536;

/// Returns whether the propolis SoftNPU 9p device is attached. The Tofino
/// ASIC is detected by the hardware monitor.
///
/// Each virtio 9p node is opened exclusively and sent a Tversion. Only the
/// SoftNPU handler answers `9P2000.P4`; another version is another device.
/// Busy, unopenable, or malformed replies are errors.
pub fn find_softnpu_device(log: &Logger) -> Result<bool, SwitchDetectError> {
    let mut devinfo =
        DevInfo::new_force_load().map_err(SwitchDetectError::DevInfo)?;
    for node in devinfo.walk_node() {
        let node = node.map_err(SwitchDetectError::DevInfo)?;
        if probe_node(log, &node)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Returns whether `node` is the SoftNPU 9p device.
fn probe_node(
    log: &Logger,
    node: &Node<'_>,
) -> Result<bool, SwitchDetectError> {
    if !is_virtio_9p(node)? {
        return Ok(false);
    }
    let Some(path) = ninep_minor_path(node)? else {
        debug!(
            log,
            "virtio 9p node has no {NINEP_MINOR} minor";
            "node" => node.node_name(),
        );
        return Ok(false);
    };
    let version = probe_version(log, &path)?;
    if version == SOFTNPU_9P_VERSION {
        info!(log, "found SoftNPU 9p device"; "path" => path);
        Ok(true)
    } else {
        debug!(
            log,
            "virtio 9p device is not SoftNPU";
            "path" => path,
            "version" => version,
        );
        Ok(false)
    }
}

fn is_virtio_9p(node: &Node<'_>) -> Result<bool, SwitchDetectError> {
    let mut vendor = None;
    let mut device = None;
    for prop in node.props() {
        let prop = prop.map_err(SwitchDetectError::DevInfo)?;
        match prop.name().as_str() {
            "vendor-id" => vendor = prop.as_i32(),
            "device-id" => device = prop.as_i32(),
            _ => {}
        }
    }
    Ok(vendor == Some(VIRTIO_VENDOR_ID)
        && device.is_some_and(|d| VIRTIO_9P_DEVICE_IDS.contains(&d)))
}

fn ninep_minor_path(
    node: &Node<'_>,
) -> Result<Option<String>, SwitchDetectError> {
    for minor in node.minors() {
        let minor = minor.map_err(SwitchDetectError::DevInfo)?;
        if minor.name() == NINEP_MINOR {
            let path =
                minor.devfs_path().map_err(SwitchDetectError::DevInfo)?;
            return Ok(Some(format!("/devices{path}")));
        }
    }
    Ok(None)
}

/// One Tversion/Rversion exchange over the vio9p character device, returning
/// the version the device answered with.
///
/// The driver permits a single exclusive open, so EBUSY means another
/// consumer such as scadm currently holds the device; retry briefly.
fn probe_version(
    log: &Logger,
    path: &str,
) -> Result<String, SwitchDetectError> {
    for attempt in 1..=OPEN_ATTEMPTS {
        info!(
            log,
            "probing virtio 9p device for SoftNPU";
            "path" => path,
            "attempt" => attempt,
        );
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXCL)
            .open(path)
        {
            Ok(file) => file,
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                if attempt < OPEN_ATTEMPTS {
                    std::thread::sleep(OPEN_RETRY_DELAY);
                }
                continue;
            }
            Err(err) => {
                return Err(SwitchDetectError::Open {
                    path: path.to_string(),
                    err,
                });
            }
        };
        file.write_all(&encode_tversion(SOFTNPU_9P_VERSION)).map_err(
            |err| SwitchDetectError::Write { path: path.to_string(), err },
        )?;
        let mut buf = vec![0u8; REPLY_BUF_LEN];
        let n = file.read(&mut buf).map_err(|err| SwitchDetectError::Read {
            path: path.to_string(),
            err,
        })?;
        return decode_rversion(&buf[..n]).map_err(|reason| {
            SwitchDetectError::Protocol { path: path.to_string(), reason }
        });
    }
    Err(SwitchDetectError::Busy {
        path: path.to_string(),
        attempts: OPEN_ATTEMPTS,
    })
}
