//! Wrappers around illumos-specific commands.

pub mod dladm;
pub mod svc;
pub mod zfs;
pub mod zone;
pub mod zpool;

const PFEXEC: &str = "/usr/bin/pfexec";

#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    #[error("Failed to start execution of process: {0}")]
    ExecutionStart(std::io::Error),

    #[error(
        "Command executed and failed with status: {status}. Output: {stderr}"
    )]
    CommandFailure { status: std::process::ExitStatus, stderr: String },
}

// Helper function for starting the process and checking the
// exit code result.
fn execute(
    command: &mut std::process::Command,
) -> Result<std::process::Output, ExecutionError> {
    let output = command
        .env_clear()
        .output()
        .map_err(|e| ExecutionError::ExecutionStart(e))?;

    if !output.status.success() {
        return Err(ExecutionError::CommandFailure {
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }

    Ok(output)
}
