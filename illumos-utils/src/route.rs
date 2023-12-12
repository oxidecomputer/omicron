// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating the routing tables.

use crate::zone::ROUTE;
use crate::{execute, ExecutionError, PFEXEC};
use std::net::Ipv6Addr;

/// Wraps commands for interacting with routing tables.
pub struct Route {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock, allow(dead_code))]
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
