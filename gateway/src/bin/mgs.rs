// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run gateway, the management gateway service

use anyhow::{Context, anyhow};
use camino::Utf8PathBuf;
use clap::Parser;
use futures::StreamExt;
use omicron_common::cmd::{CmdError, fatal};
use omicron_gateway::{Config, MgsArguments, start_server};
use signal_hook::consts::signal;
use signal_hook_tokio::Signals;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[clap(name = "gateway", about = "See README.adoc for more information")]
enum Args {
    /// Start an MGS server
    Run {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: Utf8PathBuf,

        /// Read server ID and address(es) for dropshot server from our SMF
        /// properties (only valid when running as a service on illumos)
        #[clap(long)]
        id_and_address_from_smf: bool,

        /// Server ID
        #[clap(
            short,
            long,
            action,
            conflicts_with = "id_and_address_from_smf",
            required_unless_present = "id_and_address_from_smf"
        )]
        id: Option<Uuid>,

        /// Address for dropshot server
        #[clap(
            short,
            long,
            action,
            conflicts_with = "id_and_address_from_smf",
            required_unless_present = "id_and_address_from_smf"
        )]
        address: Option<SocketAddrV6>,
    },
}

#[derive(Debug)]
struct ConfigProperties {
    id: Uuid,
    addresses: Vec<SocketAddrV6>,
    rack_id: Option<Uuid>,
}

fn main() {
    if let Err(cmd_error) = oxide_tokio_rt::run(do_run()) {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Run {
            config_file_path,
            id_and_address_from_smf,
            id,
            address,
        } => {
            let config = Config::from_file(&config_file_path)
                .map_err(anyhow::Error::new)
                .map_err(CmdError::Failure)?;

            let mut signals = Signals::new([signal::SIGUSR1])
                .context("failed to set up signal handler")
                .map_err(CmdError::Failure)?;

            let (id, addresses, rack_id) = if id_and_address_from_smf {
                let config = read_smf_config()?;
                (config.id, config.addresses, config.rack_id)
            } else {
                // Does it matter if `rack_id` is always `None` in this case?
                let rack_id = None;
                // Clap ensures the first two fields are present if
                // `id_and_address_from_smf` is false, so we can safely unwrap.
                (id.unwrap(), vec![address.unwrap()], rack_id)
            };
            let args = MgsArguments { id, addresses, rack_id };
            let mut server = start_server(config, args)
                .await
                .map_err(|e| CmdError::Failure(anyhow!(e)))?;

            loop {
                tokio::select! {
                    signal = signals.next() => match signal {
                        Some(signal::SIGUSR1) => {
                            let new_config = read_smf_config()?;
                            if new_config.id != id {
                                return Err(CmdError::Failure(anyhow!("cannot change server ID on refresh")));
                            }
                            server.set_rack_id(new_config.rack_id);
                            server
                                .adjust_dropshot_addresses(&new_config.addresses)
                                .await
                                .map_err(|err| CmdError::Failure(anyhow!("config refresh failed: {err}")))?;
                        }
                        // We only register `SIGUSR1` and never close the
                        // handle, so we never expect `None` or any other
                        // signal.
                        _ => unreachable!("invalid signal: {signal:?}"),
                    },
                    result = server.wait_for_finish() => {
                        return result.map_err(|err| CmdError::Failure(anyhow!(err)))
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "illumos")]
fn read_smf_config() -> Result<ConfigProperties, CmdError> {
    fn scf_to_cmd_err(err: illumos_utils::scf::ScfError) -> CmdError {
        CmdError::Failure(anyhow!(err))
    }

    use illumos_utils::scf::ScfHandle;

    // Name of our config property group; must match our SMF manifest.xml.
    const CONFIG_PG: &str = "config";

    // Name of the property within CONFIG_PG for our server ID.
    const PROP_ID: &str = "id";

    // Name of the property within CONFIG_PG for our server addresses.
    const PROP_ADDR: &str = "address";

    // Name of the property within CONFIG_PG for our rack ID.
    const PROP_RACK_ID: &str = "rack_id";

    let scf = ScfHandle::new().map_err(scf_to_cmd_err)?;
    let instance = scf.self_instance().map_err(scf_to_cmd_err)?;
    let snapshot = instance.running_snapshot().map_err(scf_to_cmd_err)?;
    let config = snapshot.property_group(CONFIG_PG).map_err(scf_to_cmd_err)?;

    let prop_id = config.value_as_string(PROP_ID).map_err(scf_to_cmd_err)?;

    let prop_id = Uuid::try_parse(&prop_id)
        .with_context(|| {
            format!(
                "failed to parse `{CONFIG_PG}/{PROP_ID}` ({prop_id:?}) as a \
                UUID"
            )
        })
        .map_err(CmdError::Failure)?;

    let prop_rack_id =
        config.value_as_string(PROP_RACK_ID).map_err(scf_to_cmd_err)?;

    let rack_id = if prop_rack_id == "unknown" {
        None
    } else {
        Some(
            Uuid::try_parse(&prop_rack_id)
                .with_context(|| {
                    format!(
                        "failed to parse `{CONFIG_PG}/{PROP_RACK_ID}` \
                        ({prop_rack_id:?}) as a UUID"
                    )
                })
                .map_err(CmdError::Failure)?,
        )
    };

    let prop_addr =
        config.values_as_strings(PROP_ADDR).map_err(scf_to_cmd_err)?;

    let mut addresses = Vec::with_capacity(prop_addr.len());

    for addr in prop_addr {
        addresses.push(
            addr.parse()
                .with_context(|| {
                    format!(
                        "failed to parse `{CONFIG_PG}/{PROP_ADDR}` ({addr:?}) \
                        as a socket address"
                    )
                })
                .map_err(CmdError::Failure)?,
        );
    }

    if addresses.is_empty() {
        Err(CmdError::Failure(anyhow!(
            "no addresses specified by `{CONFIG_PG}/{PROP_ADDR}`"
        )))
    } else {
        Ok(ConfigProperties { id: prop_id, addresses, rack_id })
    }
}

#[cfg(not(target_os = "illumos"))]
fn read_smf_config() -> Result<ConfigProperties, CmdError> {
    Err(CmdError::Failure(anyhow!(
        "SMF configuration only available on illumos"
    )))
}
