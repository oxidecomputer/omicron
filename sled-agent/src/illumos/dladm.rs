// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for poking at data links.

use crate::common::vlan::VlanID;
use crate::illumos::vnic::VnicKind;
use crate::illumos::{execute, ExecutionError, PFEXEC};
use omicron_common::api::external::MacAddr;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

pub const VNIC_PREFIX: &str = "ox";
pub const VNIC_PREFIX_CONTROL: &str = "oxControl";

/// Prefix used to name VNICs over xde devices / OPTE ports.
// TODO-correctness: Remove this when `xde` devices can be directly used beneath
// Viona, and thus plumbed directly to guests.
pub const VNIC_PREFIX_GUEST: &str = "vopte";

pub const DLADM: &str = "/usr/sbin/dladm";

/// Errors returned from [`Dladm::find_physical`].
#[derive(thiserror::Error, Debug)]
pub enum FindPhysicalLinkError {
    #[error("Failed to find physical link: {0}")]
    Execution(#[from] ExecutionError),

    #[error("No Physical Link devices found")]
    NoPhysicalLinkFound,
}

/// Errors returned from [`Dladm::get_mac`].
#[derive(thiserror::Error, Debug)]
pub enum GetMacError {
    #[error("Mac Address cannot be looked up; Link not found: {0:?}")]
    NotFound(PhysicalLink),

    #[error("Failed to get MAC address: {0}")]
    Execution(#[from] ExecutionError),

    #[error("Failed to parse MAC: {0}")]
    ParseMac(#[from] macaddr::ParseError),
}

/// Errors returned from [`Dladm::create_vnic`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to create VNIC {name} on link {link:?}: {err}")]
pub struct CreateVnicError {
    name: String,
    link: PhysicalLink,
    #[source]
    err: ExecutionError,
}

/// Errors returned from [`Dladm::get_vnics`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to get vnics: {err}")]
pub struct GetVnicError {
    #[source]
    err: ExecutionError,
}

/// Errors returned from [`Dladm::delete_vnic`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to delete vnic {name}: {err}")]
pub struct DeleteVnicError {
    name: String,
    #[source]
    err: ExecutionError,
}

/// The name of a physical datalink.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct PhysicalLink(pub String);

/// Wraps commands for interacting with data links.
pub struct Dladm {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Dladm {
    /// Returns the name of the first observed physical data link.
    pub fn find_physical() -> Result<PhysicalLink, FindPhysicalLinkError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "show-phys", "-p", "-o", "LINK"]);
        let output = execute(cmd)?;
        let name = String::from_utf8_lossy(&output.stdout)
            .lines()
            // TODO: This is arbitrary, but we're currently grabbing the first
            // physical device. Should we have a more sophisticated method for
            // selection?
            .next()
            .map(|s| s.trim())
            .ok_or_else(|| FindPhysicalLinkError::NoPhysicalLinkFound)?
            .to_string();
        Ok(PhysicalLink(name))
    }

    /// Returns the MAC address of a physical link.
    pub fn get_mac(link: PhysicalLink) -> Result<MacAddr, GetMacError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            DLADM,
            "show-phys",
            "-m",
            "-p",
            "-o",
            "ADDRESS",
            &link.0,
        ]);
        let output = execute(cmd)?;
        let name = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .map(|s| s.trim())
            .ok_or_else(|| GetMacError::NotFound(link))?
            .to_string();

        // Ensure the MAC address is zero-padded, so it may be parsed as a
        // MacAddr. This converts segments like ":a" to ":0a".
        let name = name
            .split(':')
            .map(|segment| format!("{:0>2}", segment))
            .collect::<Vec<String>>()
            .join(":");
        let mac = MacAddr::from_str(&name)?;
        Ok(mac)
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
    ) -> Result<(), CreateVnicError> {
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
        execute(cmd).map_err(|err| CreateVnicError {
            name: vnic_name.to_string(),
            link: physical.clone(),
            err,
        })?;
        Ok(())
    }

    /// Returns VNICs that may be managed by the Sled Agent of a particular
    /// kind.
    pub fn get_vnics(kind: VnicKind) -> Result<Vec<String>, GetVnicError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "show-vnic", "-p", "-o", "LINK"]);
        let output = execute(cmd).map_err(|err| GetVnicError { err })?;

        let vnics = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|name| {
                // Ensure this is a kind of VNIC that the sled agent could be
                // responsible for.
                match VnicKind::from_name(name) {
                    Some(vnic_kind) => {
                        // Ensure matches the caller-specified VnicKind
                        if vnic_kind == kind {
                            Some(name.to_owned())
                        } else {
                            None
                        }
                    }
                    // Always ignore this VNIC if it's not a valid VnicKind.
                    None => None,
                }
            })
            .collect();
        Ok(vnics)
    }

    /// Remove a vnic from the sled.
    pub fn delete_vnic(name: &str) -> Result<(), DeleteVnicError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[DLADM, "delete-vnic", name]);
        execute(cmd)
            .map_err(|err| DeleteVnicError { name: name.to_string(), err })?;
        Ok(())
    }
}
