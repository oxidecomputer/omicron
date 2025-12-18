// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron debugger (omdb) - binary entrypoint
//!
//! This is a small shim over `lib.rs`, and is structured this way so that other
//! crates can depend on omicron-omdb as a library.

use clap::Parser;
use omicron_omdb::Omdb;

fn main() -> Result<(), anyhow::Error> {
    sigpipe::reset();
    oxide_tokio_rt::run(async {
        let cmd = Omdb::parse();
        cmd.exec().await
    })
}
