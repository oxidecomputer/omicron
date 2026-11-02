// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Print an inventory of the NVMe disks on this system as JSON.
//!
//! This is a native replacement for rkdeploy's `disk_command()`, which shells
//! out to `nvmeadm list`, `diskinfo -P`, `prtvtoc` and `nvmeadm get-logpage
//! health` and stitches their output together.

#[cfg(target_os = "illumos")]
mod collect;
#[cfg(target_os = "illumos")]
mod probe;
#[cfg(target_os = "illumos")]
mod topo;
#[cfg(target_os = "illumos")]
mod types;

#[cfg(target_os = "illumos")]
fn main() -> anyhow::Result<()> {
    let disks = collect::collect()?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &disks)?;
    println!();
    Ok(())
}

#[cfg(not(target_os = "illumos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("diskinfo is only supported on illumos")
}
