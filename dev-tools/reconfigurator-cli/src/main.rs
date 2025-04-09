// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconfigurator CLI entrypoint.
//!
//! This is a small shim over `lib.rs`, and is structured this way so that other
//! crates can depend on reconfigurator-cli as a library.

use clap::Parser;
use reconfigurator_cli::CmdReconfiguratorSim;

fn main() -> anyhow::Result<()> {
    let cmd = CmdReconfiguratorSim::parse();
    cmd.exec()
}
