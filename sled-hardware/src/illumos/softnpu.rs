// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Detection of the propolis SoftNPU 9p device.

use crate::SwitchDetectError;
use crate::softnpu::{SOFTNPU_9P_VERSION, decode_rversion, encode_tversion};
use illumos_devinfo::{DevInfo, Node};
use slog::{Logger, debug, info, warn};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::time::Duration;

const VIRTIO_VENDOR_ID: i32 = 0x1af4;
// Transitional and modern virtio 9p PCI device ids.
const VIRTIO_9P_DEVICE_IDS: [i32; 2] = [0x1009, 0x1049];
const NINEP_MINOR: &str = "9p";
const OPEN_ATTEMPTS: usize = 3;
const OPEN_RETRY_DELAY: Duration = Duration::from_millis(500);
const REPLY_BUF_LEN: usize = 65536;

enum Probe {
    Version(String),
    Busy,
}

/// Returns the devfs path of the SoftNPU 9p device when one is attached.
///
/// Every virtio 9p node with an attached driver is opened exclusively and
/// asked for its 9P version. Only the propolis SoftNPU handler answers with
/// `9P2000.P4`. A device that stays busy across retries, such as a mounted
/// 9p filesystem, is logged and skipped.
pub(super) fn find_softnpu_device(
    log: &Logger,
    devinfo: &mut DevInfo,
) -> Result<Option<String>, SwitchDetectError> {
    let mut walker = devinfo.walk_node();
    while let Some(node) =
        walker.next().transpose().map_err(SwitchDetectError::DevInfo)?
    {
        if !is_virtio_9p(&node)? {
            continue;
        }
        let Some(path) = ninep_minor_path(&node)? else {
            debug!(
                log,
                "virtio 9p node has no {NINEP_MINOR} minor";
                "node" => node.node_name(),
            );
            continue;
        };
        match probe_version(&path)? {
            Probe::Version(version) if version == SOFTNPU_9P_VERSION => {
                info!(log, "found SoftNPU 9p device"; "path" => &path);
                return Ok(Some(path));
            }
            Probe::Version(version) => {
                debug!(
                    log,
                    "virtio 9p device is not SoftNPU";
                    "path" => path,
                    "version" => version,
                );
            }
            Probe::Busy => {
                warn!(log, "virtio 9p device busy; skipping"; "path" => path);
            }
        }
    }
    Ok(None)
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

/// One Tversion/Rversion exchange over the vio9p character device.
///
/// The driver permits a single exclusive open, so EBUSY means another
/// consumer such as scadm or a 9p mount currently holds the device.
fn probe_version(path: &str) -> Result<Probe, SwitchDetectError> {
    for attempt in 1..=OPEN_ATTEMPTS {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXCL)
            .open(path)
        {
            Ok(file) => {
                return exchange_version(path, file).map(Probe::Version);
            }
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                if attempt < OPEN_ATTEMPTS {
                    std::thread::sleep(OPEN_RETRY_DELAY);
                }
            }
            Err(err) => {
                return Err(SwitchDetectError::Io {
                    path: path.to_string(),
                    err,
                });
            }
        }
    }
    Ok(Probe::Busy)
}

fn exchange_version(
    path: &str,
    mut file: File,
) -> Result<String, SwitchDetectError> {
    let io = |err| SwitchDetectError::Io { path: path.to_string(), err };
    file.write_all(&encode_tversion(SOFTNPU_9P_VERSION)).map_err(io)?;
    let mut buf = vec![0u8; REPLY_BUF_LEN];
    let n = file.read(&mut buf).map_err(io)?;
    decode_rversion(&buf[..n]).map_err(|reason| SwitchDetectError::Protocol {
        path: path.to_string(),
        reason,
    })
}
