// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing IP interfaces.

use crate::addrobj::{IPV6_LINK_LOCAL_ADDROBJ_NAME, IPV6_STATIC_ADDROBJ_NAME};
use crate::zone::IPADM;
use crate::{execute, ExecutionError, PFEXEC};
use std::net::Ipv6Addr;

/// Wraps commands for interacting with interfaces.
pub struct Ipadm {}

/// Expected error message contents when showing an addrobj that doesn't exist.
const ADDROBJ_NOT_FOUND_ERR: &str = "Address object not found";

/// Expected error message when an interface already exists.
const INTERFACE_ALREADY_EXISTS: &str = "Interface already exists";

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
impl Ipadm {
    /// Ensure that an IP interface exists on the provided datalink.
    pub fn ensure_ip_interface_exists(
        datalink: &str,
    ) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "create-if", "-t", datalink]);
        match execute(cmd) {
            Ok(_) => Ok(()),
            Err(ExecutionError::CommandFailure(info))
                if info.stderr.contains(INTERFACE_ALREADY_EXISTS) =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // Set MTU to 9000 on both IPv4 and IPv6
    pub fn set_interface_mtu(datalink: &str) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[
            IPADM,
            "set-ifprop",
            "-t",
            "-p",
            "mtu=9000",
            "-m",
            "ipv4",
            datalink,
        ]);
        execute(cmd)?;

        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[
            IPADM,
            "set-ifprop",
            "-t",
            "-p",
            "mtu=9000",
            "-m",
            "ipv6",
            datalink,
        ]);
        execute(cmd)?;
        Ok(())
    }

    pub fn create_static_and_autoconfigured_addrs(
        datalink: &str,
        listen_addr: &Ipv6Addr,
    ) -> Result<(), ExecutionError> {
        // Create auto-configured address on the IP interface if it doesn't already exist
        let addrobj = format!("{}/{}", datalink, IPV6_LINK_LOCAL_ADDROBJ_NAME);
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "show-addr", &addrobj]);
        match execute(cmd) {
            Err(ExecutionError::CommandFailure(info))
                if info.stderr.contains(ADDROBJ_NOT_FOUND_ERR) =>
            {
                let mut cmd = std::process::Command::new(PFEXEC);
                let cmd = cmd.args(&[
                    IPADM,
                    "create-addr",
                    "-t",
                    "-T",
                    "addrconf",
                    &addrobj,
                ]);
                execute(cmd)?;
            }
            Err(other) => return Err(other),
            Ok(_) => (),
        };

        // Create static address on the IP interface if it doesn't already exist
        let addrobj = format!("{}/{}", datalink, IPV6_STATIC_ADDROBJ_NAME);
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "show-addr", &addrobj]);
        match execute(cmd) {
            Err(ExecutionError::CommandFailure(info))
                if info.stderr.contains(ADDROBJ_NOT_FOUND_ERR) =>
            {
                let mut cmd = std::process::Command::new(PFEXEC);
                let cmd = cmd.args(&[
                    IPADM,
                    "create-addr",
                    "-t",
                    "-T",
                    "static",
                    "-a",
                    &listen_addr.to_string(),
                    &addrobj,
                ]);
                execute(cmd).map(|_| ())
            }
            Err(other) => Err(other),
            Ok(_) => Ok(()),
        }
    }

    // Create gateway on the IP interface if it doesn't already exist
    pub fn create_opte_gateway(
        opte_iface: &String,
    ) -> Result<(), ExecutionError> {
        let addrobj = format!("{}/public", opte_iface);
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "show-addr", &addrobj]);
        match execute(cmd) {
            Err(_) => {
                let mut cmd = std::process::Command::new(PFEXEC);
                let cmd = cmd.args(&[
                    IPADM,
                    "create-addr",
                    "-t",
                    "-T",
                    "dhcp",
                    &addrobj,
                ]);
                execute(cmd)?;
            }
            Ok(_) => (),
        };
        Ok(())
    }
}
