// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This binary is used to generate files unique to the sled agent running on
//! each server. Specifically, the unique files we care about are key shares
//! used for the trust quourm here. We generate a shared secret then split it,
//! distributing each share to the appropriate server.

use anyhow::{Context, Result};
use clap::Parser;
use sp_sim::config::GimletConfig;
use sp_sim::config::SpCommonConfig;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(
    name = "sled-agent-overlay-files",
    about = "Generate server unique files for deployment"
)]
struct Args {
    /// A directory per server where the files are output
    #[clap(short, long, action)]
    directories: Vec<PathBuf>,
}

// Generate a config file for a simulated SP in each deployment server folder.
fn overlay_sp_configs(server_dirs: &[PathBuf]) -> Result<()> {
    // We will eventually need to flesh out more of this config; for now,
    // it's sufficient to only generate an SP that emulates a RoT.
    let mut config = GimletConfig {
        common: SpCommonConfig {
            multicast_addr: None,
            bind_addrs: None,
            serial_number: [0; 16],
            manufacturing_root_cert_seed: [0; 32],
            device_id_cert_seed: [0; 32],
        },
        components: Vec::new(),
    };

    // Our lazy device ID generation fails if we overflow a u8.
    assert!(server_dirs.len() <= 255, "expand simulated SP ID generation");

    for server_dir in server_dirs {
        config.common.serial_number[0] += 1;
        config.common.device_id_cert_seed[0] += 1;

        let bytes = toml::ser::to_vec(&config).unwrap();
        let path = server_dir.join("config-sp.toml");
        std::fs::write(&path, bytes)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::try_parse()?;
    overlay_sp_configs(&args.directories)?;
    Ok(())
}
