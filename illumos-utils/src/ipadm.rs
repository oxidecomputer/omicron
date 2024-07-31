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
// The message changed to be consistent regardless of the state of the
// system in illumos 16677. It is now always `ERR1` below. Prior to that, it
// would most often be `ERR2` but could sometimes be blank or `ERR1`.
const ADDROBJ_NOT_FOUND_ERR1: &str = "address: Object not found";
const ADDROBJ_NOT_FOUND_ERR2: &str = "Address object not found";

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

    pub fn addrobj_addr(
        addrobj: &str,
    ) -> Result<Option<String>, ExecutionError> {
        // Note that additional privileges are not required to list address
        // objects, and so there is no `pfexec` here.
        let mut cmd = std::process::Command::new(IPADM);
        let cmd = cmd.args(&["show-addr", "-po", "addr", addrobj]);
        match execute(cmd) {
            Err(ExecutionError::CommandFailure(info))
                if [ADDROBJ_NOT_FOUND_ERR1, ADDROBJ_NOT_FOUND_ERR2]
                    .iter()
                    .any(|&ss| info.stderr.contains(ss)) =>
            {
                // The address object does not exist.
                Ok(None)
            }
            Err(e) => Err(e),
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                Ok(stdout.trim().lines().next().map(|s| s.to_owned()))
            }
        }
    }

    pub fn addrobj_exists(addrobj: &str) -> Result<bool, ExecutionError> {
        Ok(Self::addrobj_addr(addrobj)?.is_some())
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
        // Create auto-configured address on the IP interface if it doesn't
        // already exist
        let addrobj = format!("{}/{}", datalink, IPV6_LINK_LOCAL_ADDROBJ_NAME);
        if !Self::addrobj_exists(&addrobj)? {
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

        // Create static address on the IP interface if it doesn't already exist
        let addrobj = format!("{}/{}", datalink, IPV6_STATIC_ADDROBJ_NAME);
        if !Self::addrobj_exists(&addrobj)? {
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
            execute(cmd)?;
        }

        Ok(())
    }

    // Create gateway on the IP interface if it doesn't already exist
    pub fn create_opte_gateway(
        opte_iface: &String,
    ) -> Result<(), ExecutionError> {
        let addrobj = format!("{}/public", opte_iface);
        if !Self::addrobj_exists(&addrobj)? {
            let mut cmd = std::process::Command::new(PFEXEC);
            let cmd =
                cmd.args(&[IPADM, "create-addr", "-t", "-T", "dhcp", &addrobj]);
            execute(cmd)?;
        }
        Ok(())
    }
}
