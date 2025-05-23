// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Wrapper for command execution with timeout.

use std::{
    process::{Command, ExitStatus},
    time::Duration,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncReadExt;

#[cfg(target_os = "illumos")]
use crate::contract::ContractError;

#[cfg(not(target_os = "illumos"))]
use crate::contract_stub::ContractError;

const DLADM: &str = "/usr/sbin/dladm";
const IPADM: &str = "/usr/sbin/ipadm";
const KSTAT: &str = "/usr/bin/kstat";
const NVMEADM: &str = "/usr/sbin/nvmeadm";
const PFEXEC: &str = "/usr/bin/pfexec";
const PFILES: &str = "/usr/bin/pfiles";
const PSTACK: &str = "/usr/bin/pstack";
const PARGS: &str = "/usr/bin/pargs";
const SVCS: &str = "/usr/bin/svcs";
const UPTIME: &str = "/usr/bin/uptime";
const ZFS: &str = "/usr/sbin/zfs";
const ZONEADM: &str = "/usr/sbin/zoneadm";
const ZPOOL: &str = "/usr/sbin/zpool";

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub trait SledDiagnosticsCommandHttpOutput {
    fn get_output(self) -> SledDiagnosticsQueryOutput;
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SledDiagnosticsQueryOutput {
    Success {
        /// The command and its arguments.
        command: String,
        /// Any stdout/stderr produced by the command.
        stdio: String,
        /// The exit status of the command. This will be the exit code (if any)
        /// and exit reason such as from a signal.
        exit_status: String,
        /// The exit code if one was present when the command exited.
        exit_code: Option<i32>,
    },
    Failure {
        /// The reason the command failed to execute.
        error: String,
    },
}

#[derive(Error, Debug)]
pub enum SledDiagnosticsCmdError {
    #[error("libcontract error: {0}")]
    Contract(#[from] ContractError),
    #[error("Failed to duplicate pipe for command [{command}]: {error}")]
    Dup { command: String, error: std::io::Error },
    #[error("Failed to proccess output for command [{command}]: {error}")]
    Output { command: String, error: std::io::Error },
    #[error(
        "Failed to convert pipe for command [{command}] to owned fd: {error}"
    )]
    OwnedFd { command: String, error: std::io::Error },
    #[error("Failed to create pipe for command [{command}]: {error}")]
    Pipe { command: String, error: std::io::Error },
    #[error("Failed to spawn command [{command}]: {error}")]
    Spawn { command: String, error: std::io::Error },
    #[error("Failed to execute command [{command}] in {duration:?}")]
    Timeout { command: String, duration: Duration },
    #[error("Failed to wait on command [{command}]: {error}")]
    Wait { command: String, error: std::io::Error },
}

#[derive(Debug)]
pub struct SledDiagnosticsCmdOutput {
    pub command: String,
    pub stdio: String,
    pub exit_status: ExitStatus,
}

impl SledDiagnosticsCommandHttpOutput
    for Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>
{
    fn get_output(self) -> SledDiagnosticsQueryOutput {
        match self {
            Ok(output) => SledDiagnosticsQueryOutput::Success {
                command: output.command,
                stdio: output.stdio,
                exit_status: output.exit_status.to_string(),
                exit_code: output.exit_status.code(),
            },
            Err(error) => {
                SledDiagnosticsQueryOutput::Failure { error: error.to_string() }
            }
        }
    }
}

/// Takes a given `Command` and returns a lossy representation of the program
// and its arguments.
fn command_to_string(command: &Command) -> String {
    use std::fmt::Write;

    // Grab the command itself
    let mut res: String = command.get_program().to_string_lossy().into();
    // Grab the command arguments
    for arg in command.get_args() {
        let arg = arg.to_string_lossy();
        write!(&mut res, " {arg}").expect("write! to strings never fails");
    }
    res
}

/// Spawn a command asynchronously and collect its output by interleaving stdout
/// and stderr as they occur.
async fn execute(
    cmd: Command,
) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    let cmd_string = command_to_string(&cmd);
    let (sender, mut rx) = tokio::net::unix::pipe::pipe().map_err(|e| {
        SledDiagnosticsCmdError::Pipe { command: cmd_string.clone(), error: e }
    })?;
    let pipe = sender.into_nonblocking_fd().map_err(|e| {
        SledDiagnosticsCmdError::OwnedFd {
            command: cmd_string.clone(),
            error: e,
        }
    })?;
    let pipe_dup = pipe.try_clone().map_err(|e| {
        SledDiagnosticsCmdError::Dup { command: cmd_string.clone(), error: e }
    })?;

    // TODO MTZ: We may eventually want to reuse some of the process contract
    // bits from illumos_utils to wrap the command in.
    let mut cmd = tokio::process::Command::from(cmd);
    cmd.kill_on_drop(true);
    cmd.stdout(pipe);
    cmd.stderr(pipe_dup);

    let mut child = cmd.spawn().map_err(|e| {
        SledDiagnosticsCmdError::Spawn { command: cmd_string.clone(), error: e }
    })?;
    // NB: This drop call is load-bearing and prevents a deadlock. The command
    // struct holds onto the write half of the pipe preventing the read side
    // from ever closing. To prevent this we drop the command now that we have
    // spawned the process successfully.
    drop(cmd);

    let mut stdio = String::new();
    rx.read_to_string(&mut stdio).await.map_err(|e| {
        SledDiagnosticsCmdError::Output {
            command: cmd_string.clone(),
            error: e,
        }
    })?;

    let exit_status = child.wait().await.map_err(|e| {
        SledDiagnosticsCmdError::Wait { command: cmd_string.clone(), error: e }
    })?;

    Ok(SledDiagnosticsCmdOutput { command: cmd_string, stdio, exit_status })
}

/// Spawn a command that's allowed to execute within a given time limit.
pub async fn execute_command_with_timeout(
    command: Command,
    duration: Duration,
) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    let cmd_string = command_to_string(&command);
    let tokio_command = execute(command);
    match tokio::time::timeout(duration, tokio_command).await {
        Ok(res) => res,
        Err(_elapsed) => Err(SledDiagnosticsCmdError::Timeout {
            command: cmd_string,
            duration,
        }),
    }
}

pub fn zoneadm_list() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(ZONEADM).arg("list").arg("-cip");
    cmd
}

pub fn ipadm_show_interface() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(IPADM).arg("show-if");
    cmd
}

pub fn ipadm_show_addr() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(IPADM).arg("show-addr");
    cmd
}

pub fn ipadm_show_prop() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(IPADM).arg("show-prop");
    cmd
}

pub fn dladm_show_phys() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(DLADM).args(["show-phys", "-m"]);
    cmd
}

pub fn dladm_show_ether() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(DLADM).arg("show-ether");
    cmd
}

pub fn dladm_show_link() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(DLADM).arg("show-link");
    cmd
}

pub fn dladm_show_vnic() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(DLADM).arg("show-vnic");
    cmd
}

pub fn dladm_show_linkprop() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(DLADM).arg("show-linkprop");
    cmd
}

pub fn nvmeadm_list() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(NVMEADM).arg("list");
    cmd
}

pub fn pargs_process(pid: i32) -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(PARGS).arg("-ae").arg(pid.to_string());
    cmd
}

pub fn pstack_process(pid: i32) -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(PSTACK).arg(pid.to_string());
    cmd
}

pub fn pfiles_process(pid: i32) -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(PFILES).arg(pid.to_string());
    cmd
}

pub fn uptime() -> Command {
    let mut cmd = std::process::Command::new(UPTIME);
    cmd.env_clear();
    cmd
}

pub fn kstat_low_page() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(KSTAT).arg("-p").arg("unix::system_pages:low_mem_scan");
    cmd
}

pub fn svcs_enabled_but_not_running() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(SVCS).arg("-xZ");
    cmd
}

pub fn count_disks() -> Command {
    let mut cmd = std::process::Command::new("bash");
    cmd.env_clear().args([
        "-c",
        "(pfexec diskinfo -pH | tee | wc -l | xargs | grep -x '12' > /dev/null) \
            && echo 'OK: All expected disks found' \
            || echo 'WARN: Unexpected number of physical disks (expected 12)'",
    ]);
    cmd
}

pub fn zfs_list_unmounted() -> Command {
    let mut cmd = std::process::Command::new("bash");
    cmd.env_clear().args([
        "-c",
        "pfexec zfs list -r -o name,mounted | grep oxp | grep -v yes$ \
             && echo 'WARN: Found unmounted dataset(s)' \
             || echo 'OK: No unmounted datasets'",
    ]);
    cmd
}

pub fn count_crucibles() -> Command {
    let mut cmd = std::process::Command::new("bash");
    cmd.env_clear()
        .args([
            "-c",
            "(zoneadm list | grep crucible | grep -v pantry | tee | wc -l | xargs | grep -x '10' > /dev/null) \
            && echo 'OK: 10 Crucibles found' \
            || echo 'WARN: Unexpected number of crucible zones (expected 10)'"
        ]);
    cmd
}

pub fn identify_datasets_close_to_quota() -> Command {
    let mut cmd = std::process::Command::new("bash");
    cmd.env_clear()
        .args([
            "-c",
            "zfs list -Hp -o used,quota,name,avail,mountpoint | \
                egrep 'oxp|oxi' | \
                egrep -v 'none|crucible' | \
                awk '$2 > 0 && $1 / $2 >= 0.8 { any=1; print } END { exit !any }' \
            && echo 'WARN: Found near-quota datasets' \
            || echo 'OK: No near-quota datasets found'"
        ]);
    cmd
}

pub fn identify_datasets_with_less_than_300_gib_avail() -> Command {
    let mut cmd = std::process::Command::new("bash");
    cmd.env_clear().args([
        "-c",
        "zfs list -Hp -o used,quota,name,avail,mountpoint | \
                egrep 'oxp|oxi' | \
                egrep -v 'none|crucible' | \
                awk '$4 < (300 * (1024^3)) { any=1; print } END { exit !any }' \
            && echo 'WARN: Found low-space datasets' \
            || echo 'OK: No low-space datasets found'",
    ]);
    cmd
}

pub fn dimm_check() -> Command {
    let mut cmd = std::process::Command::new("bash");
    cmd.env_clear().args([
        "-c",
        "prtconf -m | \
                grep -v -e 1036271 -e 2084847 \
            && echo 'WARN: Unexpected quantity of system memory' \
            || echo 'OK: Found expected quantity of system memory'",
    ]);
    cmd
}

pub fn zfs_list() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear()
        .arg(ZFS)
        .arg("list")
        .arg("-o")
        .arg("name,used,avail,quota,reservation,mountpoint,mounted");
    cmd
}

pub fn zpool_status() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(ZPOOL).arg("status");
    cmd
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{process::Command, time::Duration};

    #[tokio::test]
    async fn test_long_running_command_is_aborted() {
        let mut command = Command::new("sleep");
        command.env_clear().arg("10");

        match execute_command_with_timeout(command, Duration::from_millis(500))
            .await
        {
            Err(SledDiagnosticsCmdError::Timeout { .. }) => (),
            _ => panic!("command should have timed out"),
        }
    }

    #[tokio::test]
    async fn test_command_stdout_is_correct() {
        let mut command = Command::new("echo");
        command.env_clear().arg("No Cables. No Assembly. Just Cloud.");

        let res = execute_command_with_timeout(command, Duration::from_secs(5))
            .await
            .unwrap();
        let expected_output =
            "No Cables. No Assembly. Just Cloud.\n".to_string();
        assert_eq!(expected_output, res.stdio);
    }

    #[tokio::test]
    async fn test_command_stderr_is_correct() {
        let mut command = Command::new("bash");
        command.env_clear().args(["-c", "echo oxide computer > /dev/stderr"]);

        let res = execute_command_with_timeout(command, Duration::from_secs(5))
            .await
            .unwrap();
        let expected_output = "oxide computer\n".to_string();
        assert_eq!(expected_output, res.stdio);
    }

    #[tokio::test]
    async fn test_command_stdout_stderr_are_interleaved() {
        let mut command = Command::new("bash");
        command.env_clear().args([
            "-c",
            "echo one > /dev/stdout \
            && echo two > /dev/stderr \
            && echo three > /dev/stdout",
        ]);

        let res = execute_command_with_timeout(command, Duration::from_secs(5))
            .await
            .unwrap();
        let expected_output = "one\ntwo\nthree\n".to_string();
        assert_eq!(expected_output, res.stdio);
    }
}
