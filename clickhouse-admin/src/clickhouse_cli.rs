// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use camino::Utf8PathBuf;
use clickhouse_admin_types::{Lgif, RaftConfig};
use dropshot::HttpError;
use illumos_utils::{output_to_exec_error, ExecutionError};
use slog::Logger;
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
    #[error("failed to parse command output")]
    Parse {
        description: &'static str,
        stdout: String,
        stderr: String,
        #[source]
        err: anyhow::Error,
    },
}

impl From<ClickhouseCliError> for HttpError {
    fn from(err: ClickhouseCliError) -> Self {
        match err {
            ClickhouseCliError::Run { .. }
            | ClickhouseCliError::Parse { .. }
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
    pub log: Option<Logger>,
}

impl ClickhouseCli {
    pub fn new(binary_path: Utf8PathBuf, listen_address: SocketAddrV6) -> Self {
        Self { binary_path, listen_address, log: None }
    }

    pub fn with_log(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    pub async fn lgif(&self) -> Result<Lgif, ClickhouseCliError> {
        self.keeper_client_non_interactive(
            "lgif",
            "Retrieve logically grouped information file",
            Lgif::parse,
            self.log.clone().unwrap(),
        )
        .await
    }

    pub async fn raft_config(&self) -> Result<RaftConfig, ClickhouseCliError> {
        self.keeper_client_non_interactive(
            "get /keeper/config",
            "Retrieve raft configuration information",
            RaftConfig::parse,
            self.log.clone().unwrap(),
        )
        .await
    }

    async fn keeper_client_non_interactive<F, T>(
        &self,
        query: &str,
        subcommand_description: &'static str,
        parse: F,
        log: Logger,
    ) -> Result<T, ClickhouseCliError>
    where
        F: FnOnce(&Logger, &[u8]) -> Result<T>,
    {
        let mut command = Command::new(&self.binary_path);
        command
            .arg("keeper-client")
            .arg("--host")
            .arg(&format!("[{}]", self.listen_address.ip()))
            .arg("--port")
            .arg(&format!("{}", self.listen_address.port()))
            .arg("--query")
            .arg(query);

        let output = command.output().await.map_err(|err| {
            let err_args: Vec<&OsStr> = command.as_std().get_args().collect();
            let err_args_parsed: Vec<String> = err_args
                .iter()
                .map(|&os_str| os_str.to_string_lossy().into_owned())
                .collect();
            let err_args_str = err_args_parsed.join(" ");
            ClickhouseCliError::Run {
                description: subcommand_description,
                subcommand: err_args_str,
                err,
            }
        })?;

        if !output.status.success() {
            return Err(output_to_exec_error(command.as_std(), &output).into());
        }

        parse(&log, &output.stdout).map_err(|err| ClickhouseCliError::Parse {
            description: subcommand_description,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stdout).to_string(),
            err,
        })
    }
}
