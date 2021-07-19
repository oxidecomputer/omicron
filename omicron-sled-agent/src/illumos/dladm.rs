//! Utilities for poking at data links.

use crate::illumos::{execute, PFEXEC};
use omicron_common::error::ApiError;

pub const VNIC_PREFIX: &str = "vnic_propolis";

const DLADM: &str = "/usr/sbin/dladm";

/// Wraps commands for interacting with data links.
pub struct Dladm {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Dladm {
    /// Returns the name of the first observed physical data link.
    pub fn find_physical() -> Result<String, ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "show-phys", "-p", "-o", "LINK"]);
        let output = execute(cmd)?;
        Ok(String::from_utf8(output.stdout)
            .map_err(|e| ApiError::InternalError {
                message: format!("Cannot parse dladm output as UTF-8: {}", e),
            })?
            .lines()
            // TODO: This is arbitrary, but we're currently grabbing the first
            // physical device. Should we have a more sophisticated method for
            // selection?
            .next()
            .map(|s| s.trim())
            .ok_or_else(|| ApiError::InternalError {
                message: "No physical devices found".to_string(),
            })?
            .to_string())
    }

    /// Creates a new VNIC atop a physical device.
    pub fn create_vnic(
        physical: &str,
        vnic_name: &str,
    ) -> Result<(), ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd =
            command.args(&[DLADM, "create-vnic", "-l", physical, vnic_name]);
        execute(cmd)?;
        Ok(())
    }

    /// Returns all VNICs that may be managed by the Sled Agent.
    pub fn get_vnics() -> Result<Vec<String>, ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "show-vnic", "-p", "-o", "LINK"]);
        let output = execute(cmd)?;

        let vnics = String::from_utf8(output.stdout)
            .map_err(|e| ApiError::InternalError {
                message: format!(
                    "Failed to parse UTF-8 from dladm output: {}",
                    e
                ),
            })?
            .lines()
            .filter(|vnic| vnic.starts_with(VNIC_PREFIX))
            .map(|s| s.to_owned())
            .collect();
        Ok(vnics)
    }

    /// Remove a vnic from the sled.
    pub fn delete_vnic(name: &str) -> Result<(), ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "delete-vnic", name]);
        execute(cmd)?;
        Ok(())
    }
}
