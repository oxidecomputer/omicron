//! Wrappers around illumos-specific commands.

mod dladm;
mod svc;
mod zfs;
mod zone;

pub use self::dladm::{Dladm, VNIC_PREFIX};
pub use self::svc::wait_for_service;
pub use self::zone::{Zones, ZONE_PREFIX};
pub use self::zfs::{Zfs, ZONE_ZFS_POOL};

use omicron_common::error::ApiError;

const PFEXEC: &str = "/usr/bin/pfexec";

// Helper function for starting the process and checking the
// exit code result.
fn execute(
    command: &mut std::process::Command,
) -> Result<std::process::Output, ApiError> {
    let output = command.output().map_err(|e| ApiError::InternalError {
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
