// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run gateway, the management gateway service

use clap::Parser;
use futures::StreamExt;
use omicron_common::cmd::{fatal, CmdError};
use omicron_gateway::{run_openapi, start_server, Config, MgsArguments};
use signal_hook::consts::signal;
use signal_hook_tokio::Signals;
use std::net::SocketAddrV6;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[clap(name = "gateway", about = "See README.adoc for more information")]
enum Args {
    /// Print the external OpenAPI Spec document and exit
    Openapi,

    /// Start an MGS server
    Run {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: PathBuf,

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
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Openapi => run_openapi().map_err(CmdError::Failure),
        Args::Run {
            config_file_path,
            id_and_address_from_smf,
            id,
            address,
        } => {
            let config = Config::from_file(&config_file_path).map_err(|e| {
                CmdError::Failure(format!(
                    "failed to parse {}: {}",
                    config_file_path.display(),
                    e
                ))
            })?;

            let mut signals =
                Signals::new(&[signal::SIGUSR1]).map_err(|e| {
                    CmdError::Failure(format!(
                        "failed to set up signal handler: {e}"
                    ))
                })?;

            let (id, addresses) = if id_and_address_from_smf {
                let config = read_smf_config()?;
                (config.id, config.addresses)
            } else {
                // Clap ensures these are present if `id_and_address_from_smf`
                // is false, so we can safely unwrap.
                (id.unwrap(), vec![address.unwrap()])
            };
            let args = MgsArguments { id, addresses };
            let mut server =
                start_server(config, args).await.map_err(CmdError::Failure)?;

            loop {
                tokio::select! {
                    signal = signals.next() => match signal {
                        Some(signal::SIGUSR1) => {
                            let new_config = read_smf_config()?;
                            if new_config.id != id {
                                return Err(CmdError::Failure(
                                    "cannot change server ID on refresh"
                                        .to_string()
                                ));
                            }
                            server
                                .adjust_dropshot_addresses(&new_config.addresses)
                                .await
                                .map_err(|err| CmdError::Failure(
                                    format!("config refresh failed: {err}")
                                ))?;
                        }
                        // We only register `SIGUSR1` and never close the
                        // handle, so we never expect `None` or any other
                        // signal.
                        _ => unreachable!("invalid signal: {signal:?}"),
                    },
                    result = server.wait_for_finish() => {
                        return result.map_err(CmdError::Failure)
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "illumos")]
fn read_smf_config() -> Result<ConfigProperties, CmdError> {
    use crucible_smf::{Scf, ScfError};

    // Name of our config property group; must match our SMF manifest.xml.
    const CONFIG_PG: &str = "config";

    // Name of the property within CONFIG_PG for our server ID.
    const PROP_ID: &str = "id";

    // Name of the property within CONFIG_PG for our server addresses.
    const PROP_ADDR: &str = "address";

    // This function is pretty boilerplate-y; we can reduce it by using this
    // error type to help us construct a `CmdError::Failure(_)` string. It
    // assumes (for the purposes of error messages) any property being fetched
    // lives under the `CONFIG_PG` property group.
    #[derive(Debug, thiserror::Error)]
    enum Error {
        #[error("failed to create scf handle: {0}")]
        ScfHandle(ScfError),
        #[error("failed to get self smf instance: {0}")]
        SelfInstance(ScfError),
        #[error("failed to get self running snapshot: {0}")]
        RunningSnapshot(ScfError),
        #[error("failed to get propertygroup `{CONFIG_PG}`: {0}")]
        GetPg(ScfError),
        #[error("missing propertygroup `{CONFIG_PG}`")]
        MissingPg,
        #[error("failed to get property `{CONFIG_PG}/{prop}`: {err}")]
        GetProperty { prop: &'static str, err: ScfError },
        #[error("missing property `{CONFIG_PG}/{prop}`")]
        MissingProperty { prop: &'static str },
        #[error("failed to get value for `{CONFIG_PG}/{prop}`: {err}")]
        GetValue { prop: &'static str, err: ScfError },
        #[error("failed to get values for `{CONFIG_PG}/{prop}`: {err}")]
        GetValues { prop: &'static str, err: ScfError },
        #[error("failed to get value for `{CONFIG_PG}/{prop}`")]
        MissingValue { prop: &'static str },
        #[error("failed to get `{CONFIG_PG}/{prop} as a string: {err}")]
        ValueAsString { prop: &'static str, err: ScfError },
    }

    impl From<Error> for CmdError {
        fn from(err: Error) -> Self {
            Self::Failure(err.to_string())
        }
    }

    let scf = Scf::new().map_err(Error::ScfHandle)?;
    let instance = scf.get_self_instance().map_err(Error::SelfInstance)?;
    let snapshot =
        instance.get_running_snapshot().map_err(Error::RunningSnapshot)?;

    let config = snapshot
        .get_pg("config")
        .map_err(Error::GetPg)?
        .ok_or(Error::MissingPg)?;

    let prop_id = config
        .get_property(PROP_ID)
        .map_err(|err| Error::GetProperty { prop: PROP_ID, err })?
        .ok_or_else(|| Error::MissingProperty { prop: PROP_ID })?
        .value()
        .map_err(|err| Error::GetValue { prop: PROP_ID, err })?
        .ok_or(Error::MissingValue { prop: PROP_ID })?
        .as_string()
        .map_err(|err| Error::ValueAsString { prop: PROP_ID, err })?;

    let prop_id = Uuid::try_parse(&prop_id).map_err(|err| {
        CmdError::Failure(format!(
            "failed to parse `{CONFIG_PG}/{PROP_ID}` ({prop_id:?}) as a UUID: {err}"
        ))
    })?;

    let prop_addr = config
        .get_property(PROP_ADDR)
        .map_err(|err| Error::GetProperty { prop: PROP_ADDR, err })?
        .ok_or_else(|| Error::MissingProperty { prop: PROP_ADDR })?;

    let mut addresses = Vec::new();

    for value in prop_addr
        .values()
        .map_err(|err| Error::GetValues { prop: PROP_ADDR, err })?
    {
        let addr = value
            .map_err(|err| Error::GetValue { prop: PROP_ADDR, err })?
            .as_string()
            .map_err(|err| Error::ValueAsString { prop: PROP_ADDR, err })?;

        addresses.push(addr.parse().map_err(|err| CmdError::Failure(format!(
            "failed to parse `{CONFIG_PG}/{PROP_ADDR}` ({prop_id:?}) as a socket address: {err}"
        )))?);
    }

    if addresses.is_empty() {
        Err(CmdError::Failure(format!(
            "no addresses specified by `{CONFIG_PG}/{PROP_ADDR}`"
        )))
    } else {
        Ok(ConfigProperties { id: prop_id, addresses })
    }
}

#[cfg(not(target_os = "illumos"))]
fn read_smf_config() -> Result<ConfigProperties, CmdError> {
    Err(CmdError::Failure(
        "SMF configuration only available on illumos".to_string(),
    ))
}
