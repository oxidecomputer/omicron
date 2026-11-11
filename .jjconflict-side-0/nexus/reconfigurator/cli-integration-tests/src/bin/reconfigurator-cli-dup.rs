// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A copy of reconfigurator-cli's `main.rs`.
//!
//! This is a workaround for the fact that Cargo only lets integration tests use
//! binaries defined in the same crate. We'd like two sets of integration tests
//! against reconfigurator-cli: quicker ones that live in that crate, and slower
//! ones that depend on Nexus and live here.
//!
//! The tests don't have to use reconfigurator-cli as a binary. They could also
//! use it as a library, but doing that properly would require stdout and stderr
//! to be redirected to in-memory buffers. This small binary works around that.

use clap::Parser;
use reconfigurator_cli::CmdReconfiguratorSim;

fn main() -> anyhow::Result<()> {
    let cmd = CmdReconfiguratorSim::parse();
    cmd.exec()
}
