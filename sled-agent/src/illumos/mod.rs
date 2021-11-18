//! Wrappers around illumos-specific commands.

pub mod dladm;
pub mod svc;
pub mod zfs;
pub mod zone;

use omicron_common::api::external::Error;

const PFEXEC: &str = "/usr/bin/pfexec";

// Helper function for starting the process and checking the
// exit code result.
fn execute(
    command: &mut std::process::Command,
) -> Result<std::process::Output, Error> {
    let output = command.output().map_err(|e| Error::InternalError {
        internal_message: format!("Failed to execute {:?}: {}", command, e),
    })?;

    if !output.status.success() {
        return Err(Error::InternalError {
            internal_message: format!(
                "Command {:?} executed and failed: {}",
                command,
                String::from_utf8_lossy(&output.stderr)
            ),
        });
    }

    Ok(output)
}
