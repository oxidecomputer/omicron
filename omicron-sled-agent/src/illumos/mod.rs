//! Wrappers around illumos-specific commands.

pub mod dladm;
pub mod svc;
pub mod zfs;
pub mod zone;

use omicron_common::error::ApiError;

const PFEXEC: &str = "/usr/bin/pfexec";

// Helper function for starting the process and checking the
// exit code result.
fn execute(
    command: &mut std::process::Command,
) -> Result<std::process::Output, ApiError> {
    let output =
        command.env_clear().output().map_err(|e| ApiError::InternalError {
            message: format!("Failed to execute {:?}: {}", command, e),
        })?;

    if !output.status.success() {
        return Err(ApiError::InternalError {
            message: format!(
                "Command {:?} executed and failed: {}",
                command,
                String::from_utf8_lossy(&output.stderr)
            ),
        });
    }

    Ok(output)
}
