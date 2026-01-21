// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating the routing tables.

use crate::zone::ROUTE;
use crate::{
    ExecutionError, PFEXEC, command_to_string, execute_async,
    output_to_exec_error,
};
use libc::ESRCH;
use omicron_common::address::{AZ_PREFIX, BOOTSTRAP_SUBNET_PREFIX, Ipv6Subnet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::process::Command;

/// Wraps commands for interacting with routing tables.
pub struct Route {}

#[derive(Debug, Clone, Copy)]
pub enum Gateway {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

impl Route {
    pub async fn ensure_default_route_with_gateway(
        gateway: Gateway,
    ) -> Result<(), ExecutionError> {
        Self::ensure_route_with_gateway("default", gateway).await
    }

    pub async fn ensure_underlay_route_with_gateway(
        gateway: Ipv6Addr,
    ) -> Result<(), ExecutionError> {
        // Route to the underlay AZ's /48 by deriving it from the gateway IP.
        let underlay_az: Ipv6Subnet<AZ_PREFIX> = Ipv6Subnet::new(gateway);
        let gateway = Gateway::Ipv6(gateway);
        Self::ensure_route_with_gateway(&underlay_az.to_string(), gateway).await
    }

    async fn ensure_route_with_gateway(
        destination: &str,
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
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[ROUTE, "-n", "get", inet, destination, inet, &gw]);

        let out = cmd.output().await.map_err(|err| {
            ExecutionError::ExecutionStart {
                command: command_to_string(cmd.as_std()),
                err,
            }
        })?;
        match out.status.code() {
            Some(0) => (),
            // If the entry is not found in the table,
            // the exit status of the command will be 3 (ESRCH).
            // When that is the case, we'll add the route.
            Some(ESRCH) => {
                let mut cmd = Command::new(PFEXEC);
                let cmd =
                    cmd.args(&[ROUTE, "add", inet, destination, inet, &gw]);
                execute_async(cmd).await?;
            }
            Some(_) | None => {
                return Err(output_to_exec_error(cmd.as_std(), &out));
            }
        };
        Ok(())
    }

    pub async fn ensure_opte_route(
        gateway: &Ipv4Addr,
        iface: &String,
        opte_ip: &IpAddr,
    ) -> Result<(), ExecutionError> {
        // Add the desired route if it doesn't already exist
        let mut cmd = Command::new(PFEXEC);
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

        let out = cmd.output().await.map_err(|err| {
            ExecutionError::ExecutionStart {
                command: command_to_string(cmd.as_std()),
                err,
            }
        })?;
        match out.status.code() {
            Some(0) => (),
            // If the entry is not found in the table,
            // the exit status of the command will be 3 (ESRCH).
            // When that is the case, we'll add the route.
            Some(ESRCH) => {
                let mut cmd = Command::new(PFEXEC);
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
                execute_async(cmd).await?;
            }
            Some(_) | None => {
                return Err(output_to_exec_error(cmd.as_std(), &out));
            }
        };
        Ok(())
    }

    pub async fn add_bootstrap_route(
        bootstrap_prefix: Ipv6Subnet<BOOTSTRAP_SUBNET_PREFIX>,
        gz_bootstrap_addr: Ipv6Addr,
        zone_vnic_name: &str,
    ) -> Result<(), ExecutionError> {
        Self::add_inet6_route(
            bootstrap_prefix,
            gz_bootstrap_addr,
            zone_vnic_name,
        )
        .await
    }

    pub async fn add_underlay_route(
        prefix: Ipv6Subnet<AZ_PREFIX>,
        gateway: Ipv6Addr,
        zone_vnic_name: &str,
    ) -> Result<(), ExecutionError> {
        Self::add_inet6_route(prefix, gateway, zone_vnic_name).await
    }

    async fn add_inet6_route<const N: u8>(
        prefix: Ipv6Subnet<N>,
        gateway: Ipv6Addr,
        zone_vnic_name: &str,
    ) -> Result<(), ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[
            ROUTE,
            "add",
            "-inet6",
            &prefix.to_string(),
            &gateway.to_string(),
            "-ifp",
            zone_vnic_name,
        ]);
        execute_async(cmd).await?;
        Ok(())
    }
}
