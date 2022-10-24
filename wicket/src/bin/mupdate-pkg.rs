// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for creating an rack release update as a tarfile.

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use wicket::update::RackUpdateSpec;

#[derive(Debug, Parser)]
struct Args {
    /// Path of input toml file that gets parsed into a [`RackUpdateSpec`]
    #[arg(short, long)]
    spec: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let s = std::fs::read_to_string(&args.spec)?;
    let spec: RackUpdateSpec = toml::from_str(&s)?;
    let path = spec.create_archive()?;
    println!("Created Release Update: {}", path.display());

    Ok(())
}
