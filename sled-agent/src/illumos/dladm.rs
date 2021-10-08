//! Utilities for poking at data links.

use crate::common::vlan::VlanID;
use crate::illumos::{execute, PFEXEC};
use omicron_common::api::external::Error;
use omicron_common::api::external::MacAddr;

pub const VNIC_PREFIX: &str = "vnic_propolis";
pub const DLADM: &str = "/usr/sbin/dladm";

/// The name of a physical datalink.
#[derive(Debug, Clone)]
pub struct PhysicalLink(pub String);

/// Wraps commands for interacting with data links.
pub struct Dladm {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Dladm {
    /// Returns the name of the first observed physical data link.
    pub fn find_physical() -> Result<PhysicalLink, Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "show-phys", "-p", "-o", "LINK"]);
        let output = execute(cmd)?;
        let name = String::from_utf8(output.stdout)
            .map_err(|e| Error::InternalError {
                message: format!("Cannot parse dladm output as UTF-8: {}", e),
            })?
            .lines()
            // TODO: This is arbitrary, but we're currently grabbing the first
            // physical device. Should we have a more sophisticated method for
            // selection?
            .next()
            .map(|s| s.trim())
            .ok_or_else(|| Error::InternalError {
                message: "No physical devices found".to_string(),
            })?
            .to_string();
        Ok(PhysicalLink(name))
    }

    /// Creates a new VNIC atop a physical device.
    ///
    /// * `physical`: The physical link on top of which a device will be
    /// created.
    /// * `vnic_name`: Exact name of the VNIC to be created.
    /// * `mac`: An optional unicast MAC address for the newly created NIC.
    /// * `vlan`: An optional VLAN ID for VLAN tagging.
    pub fn create_vnic(
        physical: &PhysicalLink,
        vnic_name: &str,
        mac: Option<MacAddr>,
        vlan: Option<VlanID>,
    ) -> Result<(), Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let mut args = vec![
            DLADM.to_string(),
            "create-vnic".to_string(),
            "-t".to_string(),
            "-l".to_string(),
            physical.0.to_string(),
        ];

        if let Some(mac) = mac {
            args.push("-m".to_string());
            args.push(mac.0.to_string());
        }

        if let Some(vlan) = vlan {
            args.push("-v".to_string());
            args.push(vlan.to_string());
        }

        args.push(vnic_name.to_string());
        let cmd = command.args(&args);
        execute(cmd)?;
        Ok(())
    }

    /// Returns all VNICs that may be managed by the Sled Agent.
    pub fn get_vnics() -> Result<Vec<String>, Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "show-vnic", "-p", "-o", "LINK"]);
        let output = execute(cmd)?;

        let vnics = String::from_utf8(output.stdout)
            .map_err(|e| Error::InternalError {
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
    pub fn delete_vnic(name: &str) -> Result<(), Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "delete-vnic", name]);
        execute(cmd)?;
        Ok(())
    }
}
