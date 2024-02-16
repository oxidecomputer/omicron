// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing IP interfaces.

use crate::zone::IPADM;
use crate::{execute, inner, output_to_exec_error, ExecutionError, PFEXEC};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::process::Stdio;
use std::str::FromStr;

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

    // Retrieve OPTE IP from interface
    pub fn retrieve_opte_ip(
        opte_iface: &String,
    ) -> Result<Ipv4Addr, ExecutionError> {
        let addrobj = format!("{}/public", opte_iface);
        let mut cmd = std::process::Command::new(PFEXEC);
        let child_cmd = cmd
            .args(&[IPADM, "show-addr", "-p", "-o", "ADDR", &addrobj])
            .stdout(Stdio::piped());
        let mut child = child_cmd.spawn().map_err(|err| {
            ExecutionError::ExecutionStart {
                command: inner::to_string(child_cmd),
                err,
            }
        })?;

        let Some(child_stdio) = child.stdout.take() else {
            return Err(ExecutionError::EmptyOutput {
                command: inner::to_string(child_cmd),
            });
        };

        let child_stdio: Stdio = child_stdio.try_into().map_err(|_| {
            ExecutionError::EmptyOutput { command: inner::to_string(child_cmd) }
        })?;

        let mut cmd = std::process::Command::new("cut");
        let cut_cmd = cmd
            .args(&["-d", "/", "-f", "1"])
            .stdin(child_stdio)
            .stdout(Stdio::piped());
        let cut =
            cut_cmd.spawn().map_err(|err| ExecutionError::ExecutionStart {
                command: inner::to_string(cut_cmd),
                err,
            })?;

        let out = cut.wait_with_output().unwrap();
        if !out.status.success() {
            return Err(output_to_exec_error(cut_cmd, &out));
        };

        let opte_ip = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .map(|s| s.trim())
            .ok_or_else(|| output_to_exec_error(cut_cmd, &out))?
            .to_string();

        let addr = Ipv4Addr::from_str(&opte_ip).unwrap();
        Ok(addr)
    }
}
