// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating the routing tables.

use std::net::Ipv6Addr;
// TODO: Make sure this is the correct location when running the binary
use crate::zone::ROUTE;
use crate::{execute, ExecutionError, PFEXEC};

/// Wraps commands for interacting with routing tables.
pub struct Route {}

impl Route {
    pub fn ensure_default_route_with_gateway(
        gateway: &Ipv6Addr,
    ) -> Result<(), ExecutionError> {
        // Add the desired route if it doesn't already exist
        let destination = "default";
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[
            ROUTE,
            "get",
            "-inet6",
            destination,
            "-inet6",
            &gateway.to_string(),
        ]);
        match execute(cmd) {
            Err(_) => {
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
            Ok(_) => (),
        };
        Ok(())
    }
}
