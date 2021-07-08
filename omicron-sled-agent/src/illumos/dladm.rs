//! Utilities for poking at data links.

use ipnet::IpNet;
use omicron_common::error::ApiError;
use std::net::IpAddr;
use std::str::FromStr;

use crate::illumos::{execute, PFEXEC};

pub const VNIC_PREFIX: &str = "vnic_propolis";

/// Wraps commands for interacting with data links.
pub struct Dladm {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Dladm {
    /// Returns the name of the first observed physical data link.
    pub fn find_physical() -> Result<String, ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&["dladm", "show-phys", "-o", "LINK"]);
        let output = execute(cmd)?;
        Ok(String::from_utf8(output.stdout)
            .map_err(|e| ApiError::InternalError {
                message: format!("Cannot parse dladm output as UTF-8: {}", e),
            })?
            .lines()
            // TODO: This is arbitrary, but we're currently grabbing the first
            // physical device. Should we have a more sophisticated method for
            // selection?
            .find(|s| *s != "LINK")
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
            command.args(&["dladm", "create-vnic", "-l", physical, vnic_name]);
        execute(cmd)?;
        Ok(())
    }

    /// Returns all VNICs that may be managed by the Sled Agent.
    pub fn get_vnics() -> Result<Vec<String>, ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&["dladm", "show-vnic", "-p", "-o", "LINK"]);
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
        let cmd = command.args(&["dladm", "delete-vnic", name]);
        execute(cmd)?;
        Ok(())
    }
}

/// Returns the default gateway accessible to the calling zone.
// TODO: We could use this, invoking:
//      $ route add default <result of this function>
// In the non-GZ to give it a connection to the GZ's gateway.
#[allow(dead_code)]
pub fn get_default_gateway() -> Result<IpAddr, ApiError> {
    let mut command = std::process::Command::new("route");
    let cmd = command.args(&["-n", "get", "default"]);
    let output = execute(cmd)?;

    let addr = String::from_utf8(output.stdout)
        .map_err(|e| ApiError::InternalError {
            message: format!("Failed to parse UTF-8 from route output: {}", e),
        })?
        .lines()
        .find_map(|s| {
            Some(s.trim().strip_prefix("gateway:")?.trim().to_string())
        })
        .ok_or_else(|| ApiError::InternalError {
            message: "Route command succeeded, but did not contain gateway"
                .to_string(),
        })?;

    IpAddr::from_str(&addr).map_err(|e| ApiError::InternalError {
        message: format!("Failed to parse IP address from output: {}", e),
    })
}

/// Returns the IP address (plus subnet mask) of a physical device.
#[allow(dead_code)]
pub fn get_ip_address(phys: &str) -> Result<IpNet, ApiError> {
    let mut command = std::process::Command::new("ipadm");
    let cmd =
        command.args(&["show-addr", &format!("{}/", phys), "-p", "-o", "ADDR"]);
    let output = execute(cmd)?;
    let out = String::from_utf8(output.stdout)
        .map_err(|e| ApiError::InternalError {
            message: format!("Failed to parse UTF-8 from route output: {}", e),
        })?
        .trim()
        .to_string();

    println!("Output from ipadm: {}", out);
    out.parse().map_err(|e| ApiError::InternalError {
        message: format!("Failed to parse ipadm output as IP address: {}", e),
    })
}
