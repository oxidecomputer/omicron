// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use camino::Utf8PathBuf;
use dropshot::HttpError;
use illumos_utils::{ExecutionError, output_to_exec_error};
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use std::ffi::OsStr;
use std::io;
use std::net::SocketAddrV6;
use tokio::process::Command;

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum ClickhouseCliError {
    #[error("failed to run `clickhouse {subcommand}`")]
    Run {
        description: &'static str,
        subcommand: String,
        #[source]
        err: io::Error,
    },
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
}

impl From<ClickhouseCliError> for HttpError {
    fn from(err: ClickhouseCliError) -> Self {
        match err {
            ClickhouseCliError::Run { .. }
            | ClickhouseCliError::ExecutionError(_) => {
                let message = InlineErrorChain::new(&err).to_string();
                HttpError {
                    status_code: http::StatusCode::INTERNAL_SERVER_ERROR,
                    error_code: Some(String::from("Internal")),
                    external_message: message.clone(),
                    internal_message: message,
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ClickhouseCli {
    /// Path to where the clickhouse binary is located
    pub binary_path: Utf8PathBuf,
    /// Address on where the clickhouse keeper is listening on
    pub listen_address: SocketAddrV6,
}

impl ClickhouseCli {
    pub fn new (binary_path: Utf8PathBuf, listen_address: SocketAddrV6) -> Self {
        Self {binary_path, listen_address}
    }

    pub async fn lgif(&self) -> Result<String, ClickhouseCliError> {
        self.keeper_client_non_interactive(
            ["lgif"].into_iter(),
            "Retrieve logically grouped information file",
        )
        .await
    }

    async fn keeper_client_non_interactive<'a, I>(
        &self,
        subcommand_args: I,
        subcommand_description: &'static str,
    ) -> Result<String, ClickhouseCliError>
    where
        I: Iterator<Item = &'a str>,
    {
        let mut command = Command::new(&self.binary_path);
        command
            .arg("keeper-client")
            .arg("--host")
            .arg(&format!("[{}]", self.listen_address.ip()))
            .arg("--port")
            .arg(&format!("{}", self.listen_address.port()))
            .arg("--query");

        let args: Vec<&'a str> = subcommand_args.collect();
        let query = args.join(" ");
        command.arg(query);

        let output = command.output().await.map_err(|err| {
            let args: Vec<&OsStr> = command.as_std().get_args().collect();
            let args_parsed: Vec<String> = args
                .iter()
                .map(|&os_str| os_str.to_string_lossy().into_owned())
                .collect();
            let args_str = args_parsed.join(" ");
            ClickhouseCliError::Run {
                description: subcommand_description,
                subcommand: args_str,
                err,
            }
        })?;

        if !output.status.success() {
            return Err(output_to_exec_error(command.as_std(), &output).into());
        }

        // TODO: Actually parse this
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}
