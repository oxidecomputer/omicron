// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrappers around illumos-specific commands.

use dropshot::HttpError;
use slog_error_chain::InlineErrorChain;
#[allow(unused)]
use std::sync::atomic::{AtomicBool, Ordering};

pub mod addrobj;
#[cfg(target_os = "illumos")]
pub mod contract;
pub mod coreadm;
pub mod destructor;
pub mod dkio;
pub mod dladm;
pub mod dumpadm;
pub mod fstyp;
pub mod ipadm;
pub mod libc;
pub mod link;
pub mod opte;
pub mod route;
pub mod running_zone;
pub mod scf;
pub mod smf_helper;
pub mod svc;
pub mod svcadm;
pub mod vmm_reservoir;
pub mod zfs;
pub mod zone;
pub mod zpool;

pub mod fakes;

pub const PFEXEC: &str = "/usr/bin/pfexec";
pub const ZONEADM: &str = "/usr/sbin/zoneadm";

#[derive(Debug)]
pub struct CommandFailureInfo {
    command: String,
    status: std::process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl std::fmt::Display for CommandFailureInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Command [{}] executed and failed with status: {}",
            self.command, self.status
        )?;
        write!(f, "  stdout: {}", self.stdout)?;
        write!(f, "  stderr: {}", self.stderr)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Failed to start execution of [{command}]: {err}")]
    ExecutionStart { command: String, err: std::io::Error },

    #[error("{0}")]
    CommandFailure(Box<CommandFailureInfo>),

    #[error("contract error: {msg}: {err}")]
    ContractFailure { msg: String, err: std::io::Error },

    #[error("Failed to parse command output")]
    ParseFailure(String),

    #[error("Zone is not running")]
    NotRunning,
}

impl From<ExecutionError> for HttpError {
    fn from(err: ExecutionError) -> Self {
        let message = InlineErrorChain::new(&err).to_string();
        HttpError {
            status_code: dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
            error_code: Some(String::from("Internal")),
            external_message: message.clone(),
            internal_message: message,
            headers: None,
        }
    }
}

fn command_to_string(command: &std::process::Command) -> String {
    command
        .get_args()
        .map(|s| s.to_string_lossy().into())
        .collect::<Vec<String>>()
        .join(" ")
}

pub fn output_to_exec_error(
    command: &std::process::Command,
    output: &std::process::Output,
) -> ExecutionError {
    ExecutionError::CommandFailure(Box::new(CommandFailureInfo {
        command: command
            .get_args()
            .map(|s| s.to_string_lossy().into())
            .collect::<Vec<String>>()
            .join(" "),
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }))
}

// Helper function for starting the process and checking the
// exit code result.
pub fn execute(
    command: &mut std::process::Command,
) -> Result<std::process::Output, ExecutionError> {
    let output =
        command.output().map_err(|err| ExecutionError::ExecutionStart {
            command: command_to_string(command),
            err,
        })?;

    if !output.status.success() {
        return Err(output_to_exec_error(command, &output));
    }

    Ok(output)
}

pub async fn execute_async(
    command: &mut tokio::process::Command,
) -> Result<std::process::Output, ExecutionError> {
    let output = command.output().await.map_err(|err| {
        ExecutionError::ExecutionStart {
            command: command_to_string(command.as_std()),
            err,
        }
    })?;

    if !output.status.success() {
        return Err(output_to_exec_error(command.as_std(), &output));
    }

    Ok(output)
}
