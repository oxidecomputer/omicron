// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating the routing tables.

use crate::zone::ROUTE;
use crate::{execute, inner, output_to_exec_error, ExecutionError, PFEXEC};
use std::net::Ipv6Addr;

/// Wraps commands for interacting with routing tables.
pub struct Route {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
impl Route {
    pub fn ensure_default_route_with_gateway(
        gateway: &Ipv6Addr,
    ) -> Result<(), ExecutionError> {
        // Add the desired route if it doesn't already exist
        let destination = "default";
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[
            ROUTE,
            "-n",
            "get",
            "-inet6",
            destination,
            "-inet6",
            &gateway.to_string(),
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
            Some(3) => {
                let mut cmd = std::process::Command::new(PFEXEC);
                let cmd = cmd.args(&[
                    ROUTE,
                    "add",
                    "-inet6",
                    destination,
                    "-inet6",
                    &gateway.to_string(),
                ]);
                execute(cmd)?;
            }
            Some(_) | None => return Err(output_to_exec_error(cmd, &out)),
        };
        Ok(())
    }
}
