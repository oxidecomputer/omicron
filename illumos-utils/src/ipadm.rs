// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing IP interfaces.

use crate::zone::IPADM;
use crate::{execute, ExecutionError, PFEXEC};
use std::net::Ipv6Addr;

/// Wraps commands for interacting with interfaces.
pub struct Ipadm {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
impl Ipadm {
    // Remove current IP interface and create a new temporary one.
    pub fn set_temp_interface_for_datalink(
        datalink: &str,
    ) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "delete-if", datalink]);
        // First we remove IP interface if it already exists. If it doesn't
        // exist and the command returns an error we continue anyway as
        // the next step is to create it.
        let _ = execute(cmd);

        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "create-if", "-t", datalink]);
        execute(cmd)?;
        Ok(())
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
        let addrobj = format!("{}/ll", datalink);
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
                    "addrconf",
                    &addrobj,
                ]);
                execute(cmd)?;
            }
            Ok(_) => (),
        };

        // Create static address on the IP interface if it doesn't already exist
        let addrobj = format!("{}/omicron6", datalink);
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
                    "static",
                    "-a",
                    &listen_addr.to_string(),
                    &addrobj,
                ]);
                execute(cmd)?;
            }
            Ok(_) => (),
        };
        Ok(())
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
