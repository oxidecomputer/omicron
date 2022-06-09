// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This binary is used to generate files unique to the sled agent running on
//! each server. Specifically, the unique files we care about are key shares
//! used for the trust quourm here. We generate a shared secret then split it,
//! distributing each share to the appropriate server.

use anyhow::{anyhow, Context, Result};
use omicron_sled_agent::sp::SimSpConfig;
use sp_sim::config::GimletConfig;
use sp_sim::config::SpCommonConfig;
use sprockets_rot::common::certificates::SerialNumber;
use sprockets_rot::salty;
use sprockets_rot::RotConfig;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled-agent-overlay-files",
    about = "Generate server unique files for deployment"
)]
struct Args {
    /// A directory per server where the files are output
    #[structopt(short, long)]
    directories: Vec<PathBuf>,
}

// Generate a config file for a simulated SP in each deployment server folder.
fn overlay_sp_configs(server_dirs: &[PathBuf]) -> Result<()> {
    let local_sp_configs = (1..=(server_dirs.len() as u32))
        .map(|id| {
            let id = id.to_be_bytes();
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
            let len = config.common.serial_number.len();
            config.common.serial_number[len - 4..].copy_from_slice(&id);
            let len = config.common.device_id_cert_seed.len();
            config.common.device_id_cert_seed[len - 4..].copy_from_slice(&id);
            config
        })
        .collect::<Vec<_>>();

    let trust_quorum_members = local_sp_configs
        .iter()
        .map(|config| {
            let config = &config.common;
            let manufacturing_keypair =
                salty::Keypair::from(&config.manufacturing_root_cert_seed);
            let device_id_keypair =
                salty::Keypair::from(&config.device_id_cert_seed);
            let serial_number = SerialNumber(config.serial_number);
            let config = RotConfig::bootstrap_for_testing(
                &manufacturing_keypair,
                device_id_keypair,
                serial_number,
            );
            config.certificates.device_id
        })
        .collect::<Vec<_>>();

    for (server_dir, local_sp) in
        server_dirs.iter().zip(local_sp_configs.into_iter())
    {
        let config = SimSpConfig {
            local_sp,
            trust_quorum_members: trust_quorum_members.clone(),
        };

        let bytes = toml::ser::to_vec(&config).unwrap();
        let path = server_dir.join("config-sp.toml");
        std::fs::write(&path, bytes)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::from_args_safe().map_err(|err| anyhow!(err))?;
    overlay_sp_configs(&args.directories)?;
    Ok(())
}
