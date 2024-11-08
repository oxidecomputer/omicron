use std::{process::Command, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use illumos_utils::{zone::IPADM, PFEXEC, ZONEADM};
use thiserror::Error;
use tokio::io::AsyncReadExt;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub trait SupportBundleCommandHttpOutput {
    fn get_output(self) -> String;
}

#[derive(Error, Debug)]
pub enum SupportBundleCmdError {
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
pub struct SupportBundleCmdOutput {
    pub command: String,
    pub stdio: String,
    pub exit_status: String,
}

impl std::fmt::Display for SupportBundleCmdOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Command executed [{}]:", self.command)?;
        writeln!(f, "    ==== stdio ====\n{}", self.stdio)?;
        writeln!(f, "    ==== exit status ====\n{}", self.exit_status)
    }
}

impl SupportBundleCommandHttpOutput
    for Result<SupportBundleCmdOutput, SupportBundleCmdError>
{
    fn get_output(self) -> String {
        match self {
            Ok(output) => format!("{output}"),
            Err(error) => format!("{error}"),
        }
    }
}

/// Takes a given `Command` and returns a lossy representation of the program
// and it's arguments.
fn command_to_string(command: &Command) -> String {
    // Grab the command arguments
    let mut res = command
        .get_args()
        .map(|a| a.to_string_lossy().into())
        .collect::<Vec<String>>();

    // Grab the command itself
    res.insert(0, command.get_program().to_string_lossy().into());
    res.join(" ")
}

/// Spawn a command asynchronously and collect it's output by interleaving
/// stdout and stderr as they occur.
async fn execute(
    cmd: Command,
) -> Result<SupportBundleCmdOutput, SupportBundleCmdError> {
    let cmd_string = command_to_string(&cmd);
    let (sender, mut rx) = tokio::net::unix::pipe::pipe().map_err(|e| {
        SupportBundleCmdError::Pipe { command: cmd_string.clone(), error: e }
    })?;
    let pipe = sender.into_nonblocking_fd().map_err(|e| {
        SupportBundleCmdError::OwnedFd { command: cmd_string.clone(), error: e }
    })?;
    let pipe_dup = pipe.try_clone().map_err(|e| {
        SupportBundleCmdError::Dup { command: cmd_string.clone(), error: e }
    })?;

    // TODO MTZ: We may eventually want to reuse some of the process contract
    // bits from illumos_utils to wrap the command in.
    let mut cmd = tokio::process::Command::from(cmd);
    cmd.kill_on_drop(true);
    cmd.stdout(pipe);
    cmd.stderr(pipe_dup);

    let mut child = cmd.spawn().map_err(|e| SupportBundleCmdError::Spawn {
        command: cmd_string.clone(),
        error: e,
    })?;
    // NB: This drop call is load-bearing and prevents a deadlock. The command
    // struct holds onto the write half of the pipe preventing the read side
    // from ever closing. To prevent this we drop the command now that we have
    // spawned the process successfully.
    drop(cmd);

    let mut stdio = String::new();
    rx.read_to_string(&mut stdio).await.map_err(|e| {
        SupportBundleCmdError::Output { command: cmd_string.clone(), error: e }
    })?;

    let exit_status =
        child.wait().await.map(|es| format!("{es}")).map_err(|e| {
            SupportBundleCmdError::Wait {
                command: cmd_string.clone(),
                error: e,
            }
        })?;

    Ok(SupportBundleCmdOutput { command: cmd_string, stdio, exit_status })
}

/// Spawn a command that's allowed to execute within a given time limit.
async fn execute_command_with_timeout(
    command: Command,
    duration: Duration,
) -> Result<SupportBundleCmdOutput, SupportBundleCmdError> {
    let cmd_string = command_to_string(&command);
    let tokio_command = execute(command);
    match tokio::time::timeout(duration, tokio_command).await {
        Ok(res) => res,
        Err(_elapsed) => Err(SupportBundleCmdError::Timeout {
            command: cmd_string,
            duration,
        }),
    }
}

fn zoneadm_list() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(ZONEADM).arg("list").arg("-cip");
    cmd
}

fn ipadm_show_interface() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(IPADM).arg("show-if");
    cmd
}

fn ipadm_show_addr() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(IPADM).arg("show-addr");
    cmd
}

fn ipadm_show_prop() -> Command {
    let mut cmd = std::process::Command::new(PFEXEC);
    cmd.env_clear().arg(IPADM).arg("show-prop");
    cmd
}

/*
 * Public API
 */

/// List all zones on a sled.
pub async fn zoneadm_info(
) -> Result<SupportBundleCmdOutput, SupportBundleCmdError> {
    execute_command_with_timeout(zoneadm_list(), DEFAULT_TIMEOUT).await
}

/// Retrieve various `ipadm` command output for the system.
pub async fn ipadm_info(
) -> Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>> {
    [ipadm_show_interface(), ipadm_show_addr(), ipadm_show_prop()]
        .into_iter()
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>>>()
        .await
}

#[cfg(test)]
mod test {
    use std::{process::Command, time::Duration};

    use crate::support_bundle::*;

    #[tokio::test]
    async fn test_long_running_command_is_aborted() {
        let mut command = Command::new("sleep");
        command.env_clear().arg("10");

        match execute_command_with_timeout(command, Duration::from_millis(500))
            .await
        {
            Err(SupportBundleCmdError::Timeout { .. }) => (),
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
        command.env_clear().args(&["-c", "echo oxide computer > /dev/stderr"]);

        let res = execute_command_with_timeout(command, Duration::from_secs(5))
            .await
            .unwrap();
        let expected_output = "oxide computer\n".to_string();
        assert_eq!(expected_output, res.stdio);
    }

    #[tokio::test]
    async fn test_command_stdout_stderr_are_interleaved() {
        let mut command = Command::new("bash");
        command.env_clear().args(&[
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
