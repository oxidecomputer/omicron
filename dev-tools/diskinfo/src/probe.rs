// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Detect an NVMe namespace that is "Not Ready" (generic status 0x82).
//!
//! Such a drive can look fine to `nvmeadm` and in its SMART/Health log page,
//! while every read fails (oxidecomputer/meta#1142).  The nvme driver counts
//! those failures in the `nvme:<instance>:device:nvm_ns_notrdy` kstat.
//!
//! Opening a blkdev always re-reads the disk label (`bd_open` ->
//! `cmlb_validate`, which forces a revalidation), so a plain open is a
//! reliable way to make the drive do some I/O.  We read the counter before and
//! after the open: if it went up, the namespace is not ready.

use crate::types::Probe;
use anyhow::{Context, Result, bail};
use kstat_rs::{Ctl, Data, NamedData};
use std::fs::File;

/// Read `nvme:<instance>:device:nvm_ns_notrdy`.
fn nvm_ns_notrdy(ctl: &Ctl, instance: i32) -> Result<u64> {
    let mut ks = ctl
        .filter(Some("nvme"), Some(instance), Some("device"))
        .next()
        .with_context(|| format!("no kstat nvme:{instance}:device"))?;
    let Data::Named(named) = ctl.read(&mut ks)? else {
        bail!("kstat nvme:{instance}:device is not a named kstat");
    };
    named
        .iter()
        .find(|n| n.name == "nvm_ns_notrdy")
        .and_then(|n| match n.value {
            NamedData::UInt64(v) => Some(v),
            _ => None,
        })
        .with_context(|| {
            format!("no uint64 nvm_ns_notrdy in nvme:{instance}:device")
        })
}

/// Open `raw_path`, which must be a blkdev of NVMe controller `instance`, and
/// report whether that caused "Namespace Not Ready" failures.
pub fn probe(ctl: &Ctl, instance: i32, raw_path: &str) -> Result<Probe> {
    let before = nvm_ns_notrdy(ctl, instance)?;
    let open_error = File::open(raw_path).err().map(|e| e.to_string());
    let after = nvm_ns_notrdy(ctl, instance)?;

    Ok(Probe {
        open_error,
        nvm_ns_notrdy_before: before,
        nvm_ns_notrdy_after: after,
    })
}
