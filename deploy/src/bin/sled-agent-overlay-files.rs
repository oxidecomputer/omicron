// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This binary is used to generate files unique to the sled agent running on
//! each server. Specifically, the unique files we care about are key shares
//! used for the trust quourm here. We generate a shared secret then split it,
//! distributing each share to the appropriate server.

use omicron_sled_agent::bootstrap::trust_quorum::{
    RackSecret, ShareDistribution,
};

use anyhow::{anyhow, Result};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled-agent-overlay-files",
    about = "Generate server unique files for deployment"
)]
struct Args {
    /// The rack secret threshold
    #[structopt(short, long)]
    threshold: usize,

    /// A directory per server where the files are output
    #[structopt(short, long)]
    directories: Vec<PathBuf>,
}

// Generate a rack secret and allocate a ShareDistribution to each deployment
// server folder.
fn overlay_secret_shares(
    threshold: usize,
    server_dirs: &[PathBuf],
) -> Result<()> {
    let total_shares = server_dirs.len();
    if total_shares < 2 {
        println!(
            "Skipping secret share distribution: only one server \
             available."
        );
        return Ok(());
    }
    let secret = RackSecret::new();
    let (shares, verifier) = secret
        .split(threshold, total_shares)
        .map_err(|e| anyhow!("Failed to split rack secret: {:?}", e))?;
    for (share, server_dir) in shares.into_iter().zip(server_dirs) {
        ShareDistribution {
            threshold,
            total_shares,
            verifier: verifier.clone(),
            share,
        }
        .write(&server_dir)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::from_args_safe().map_err(|err| anyhow!(err))?;
    overlay_secret_shares(args.threshold, &args.directories)?;
    Ok(())
}
