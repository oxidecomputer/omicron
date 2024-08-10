// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating the routing tables.

use crate::zone::ROUTE;
use crate::{execute, inner, output_to_exec_error, ExecutionError, PFEXEC};
use libc::ESRCH;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Wraps commands for interacting with routing tables.
pub struct Route {}

#[derive(Debug, Clone, Copy)]
pub enum Gateway {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
impl Route {
    pub fn ensure_default_route_with_gateway(
        gateway: Gateway,
    ) -> Result<(), ExecutionError> {
        let inet;
        let gw;
        match gateway {
            Gateway::Ipv4(addr) => {
                inet = "-inet";
                gw = addr.to_string();
            }
            Gateway::Ipv6(addr) => {
                inet = "-inet6";
                gw = addr.to_string();
            }
        }
        // Add the desired route if it doesn't already exist
        let destination = "default";
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[ROUTE, "-n", "get", inet, destination, inet, &gw]);

        let out =
            cmd.output().map_err(|err| ExecutionError::ExecutionStart {
                command: inner::to_string(cmd),
                err,
            })?;
        match out.status.code() {
            Some(0) => (),
            // If the entry is not found in the table,
            // the exit status of the command will be 3 (ESRCH).
            // When that is the case, we'll add the route.
            Some(ESRCH) => {
                let mut cmd = std::process::Command::new(PFEXEC);
                let cmd =
                    cmd.args(&[ROUTE, "add", inet, destination, inet, &gw]);
                execute(cmd)?;
            }
            Some(_) | None => return Err(output_to_exec_error(cmd, &out)),
        };
        Ok(())
    }

    pub fn ensure_opte_route(
        gateway: &Ipv4Addr,
        iface: &String,
        opte_ip: &IpAddr,
    ) -> Result<(), ExecutionError> {
        // Add the desired route if it doesn't already exist
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[
            ROUTE,
            "-n",
            "get",
            "-host",
            &gateway.to_string(),
            &opte_ip.to_string(),
            "-interface",
            "-ifp",
            &iface.to_string(),
        ]);

        let out =
            cmd.output().map_err(|err| ExecutionError::ExecutionStart {
                command: inner::to_string(cmd),
                err,
            })?;
        match out.status.code() {
            Some(0) => (),
            // If the entry is not found in the table,
            // the exit status of the command will be 3 (ESRCH).
            // When that is the case, we'll add the route.
            Some(ESRCH) => {
                let mut cmd = std::process::Command::new(PFEXEC);
                let cmd = cmd.args(&[
                    ROUTE,
                    "add",
                    "-host",
                    &gateway.to_string(),
                    &opte_ip.to_string(),
                    "-interface",
                    "-ifp",
                    &iface.to_string(),
                ]);
                execute(cmd)?;
            }
            Some(_) | None => return Err(output_to_exec_error(cmd, &out)),
        };
        Ok(())
    }

    pub fn add_bootstrap_route(
        bootstrap_prefix: u16,
        gz_bootstrap_addr: Ipv6Addr,
        zone_vnic_name: &str,
    ) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[
            ROUTE,
            "add",
            "-inet6",
            &format!("{bootstrap_prefix:x}::/16"),
            &gz_bootstrap_addr.to_string(),
            "-ifp",
            zone_vnic_name,
        ]);
        execute(cmd)?;
        Ok(())
    }
}
