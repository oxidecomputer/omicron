// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for manipulating the routing tables.

use crate::zone::ROUTE;
use crate::zone::ZLOGIN;
use crate::{
    ExecutionError, PFEXEC, command_to_string, execute_async,
    output_to_exec_error,
};
use libc::ESRCH;
use omicron_common::address::{
    AZ_PREFIX_LENGTH, BOOTSTRAP_SUBNET_PREFIX_LENGTH, Ipv6Subnet,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::process::Command;

#[derive(Clone, Copy, Debug)]
enum RouteDestination {
    /// The "default" route, suitable for any gateway.
    ///
    /// This is only used in OPTE routing setup.
    Default,
    /// An AZ IPv6 /48 subnet. Used only for underlay routing setup.
    Subnet(Ipv6Subnet<AZ_PREFIX_LENGTH>),
}

impl core::fmt::Display for RouteDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteDestination::Default => f.write_str("default"),
            RouteDestination::Subnet(net) => net.fmt(f),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct GatewayRoute {
    destination: RouteDestination,
    address: IpAddr,
}

impl GatewayRoute {
    fn default_route(address: IpAddr) -> Self {
        Self { destination: RouteDestination::Default, address }
    }

    fn subnet_route(
        address: Ipv6Addr,
        subnet: Ipv6Subnet<AZ_PREFIX_LENGTH>,
    ) -> Self {
        Self {
            destination: RouteDestination::Subnet(subnet),
            address: IpAddr::V6(address),
        }
    }

    fn destination(&self) -> &RouteDestination {
        &self.destination
    }

    fn address(&self) -> &IpAddr {
        &self.address
    }
}

/// Wraps commands for interacting with routing tables.
pub struct Route {}

impl Route {
    /// Ensure there is an interface route to the underlay subnet on the
    /// provided link.
    pub async fn ensure_underlay_route_with_gateway(
        gateway: Ipv6Addr,
        datalink: &str,
    ) -> Result<(), ExecutionError> {
        // Route to the underlay AZ's /48 by deriving it from the gateway IP.
        let underlay_az = Ipv6Subnet::new(gateway);
        Self::ensure_route_with_gateway(
            None,
            GatewayRoute::subnet_route(gateway, underlay_az),
            datalink,
        )
        .await
    }

    /// Ensure there is an interface route for the `gateway_ip` to
    /// `destination` on the provided `datalink`.
    async fn ensure_route_with_gateway(
        zone: Option<&str>,
        gateway_route: GatewayRoute,
        datalink: &str,
    ) -> Result<(), ExecutionError> {
        let destination = gateway_route.destination().to_string();
        let inet;
        let gw;
        match gateway_route.address() {
            IpAddr::V4(addr) => {
                inet = "-inet";
                gw = addr.to_string();
            }
            IpAddr::V6(addr) => {
                inet = "-inet6";
                gw = addr.to_string();
            }
        }
        // Add the desired route if it doesn't already exist
        let mut cmd = Command::new(PFEXEC);
        if let Some(zone) = zone {
            cmd.args([ZLOGIN, zone]);
        }
        let cmd = cmd.args(&[
            ROUTE,
            "-n",
            "get",
            inet,
            &destination,
            inet,
            &gw,
            "-ifp",
            datalink,
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
                if let Some(zone) = zone {
                    cmd.args([ZLOGIN, zone]);
                }
                let cmd = cmd.args(&[
                    ROUTE,
                    "add",
                    inet,
                    &destination,
                    inet,
                    &gw,
                    "-ifp",
                    datalink,
                ]);
                execute_async(cmd).await?;
            }
            Some(_) | None => {
                return Err(output_to_exec_error(cmd.as_std(), &out));
            }
        };
        Ok(())
    }

    /// Configure an IPv4 route to the OPTE virtual gateway.
    ///
    /// # Details
    ///
    /// OPTE acts as the "virtual gateway" for all traffic from the private IP
    /// address. By design, we always configure OPTE with a /32 or /128 address,
    /// which means there are no other addresses "on-link", i.e., whose
    /// addresses can be resolved through ARP or NDP. OPTE itself, however, is
    /// on-link, and receives all traffic from the guest.
    ///
    /// But before that happens, the illumos kernel looks at an IP packet from
    /// the guest and has to decide where to route it. If the destination
    /// address is on-link, then the kernel will send an ARP or NDP request for
    /// that address, resolve it, and there we go. But like we said above, _no_
    /// addresses are on-link. So how does the kernel learn any routes?
    ///
    /// For IPv6, this all happens automagically through NDP. We can create an
    /// IPv6 link-local address with just the MAC address, and then send out
    /// Router Solicitations. OPTE will respond with Router Advertisements,
    /// advertising itself as a default router. The guest side will
    /// automatically learn to send all traffic to OPTE's virtual gateway
    /// address. (The actual address is _also_ learned through NDP. It's great.)
    ///
    /// For IPv4, things are harder. The mechanism for learning a default route
    /// is the DHCP Classless Static Route Option (#121, in RFC 3442). When the
    /// guest asks for a DHCP server and gets a lease back, that can include
    /// this information about the virtual gateway and route, similar to NDP.
    /// Unfortunately, the illumos `dhcpagent` doesn't understand this option.
    /// Therefore, we need to manually program this information using
    /// `route(8)`.
    ///
    /// This method adds a route to the single-host virtual gateway address
    /// provided in `gateway_ip`, and then ensures there's also a default route
    /// that sends all traffic from the guest out to the gateway.
    ///
    /// TODO-remove: We should pull all this shenanigans out when we resolve
    /// <https://github.com/oxidecomputer/stlouis/issues/326>. Doing so is tracked
    /// by <https://github.com/oxidecomputer/omicron/issues/2931>. At that point,
    /// the only thing we'll need to do is create the DHCP / addrconf `ipadm`
    /// addrobjs for V4 and / or V6, and then the protocols will do the rest.
    pub async fn configure_opte_virtual_gateway_ipv4_route(
        zone: Option<&str>,
        opte_port: &str,
        gateway_ip: &Ipv4Addr,
        private_ip: &Ipv4Addr,
    ) -> Result<(), ExecutionError> {
        Self::ensure_opte_route(zone, opte_port, gateway_ip, private_ip)
            .await?;
        Self::ensure_route_with_gateway(
            zone,
            GatewayRoute::default_route(IpAddr::V4(*gateway_ip)),
            opte_port,
        )
        .await
    }

    /// Ensure there is a host route from the private IP to the OPTE virtual
    /// gateway address, for the provided port.
    async fn ensure_opte_route(
        zone: Option<&str>,
        opte_port: &str,
        gateway_ip: &Ipv4Addr,
        private_ip: &Ipv4Addr,
    ) -> Result<(), ExecutionError> {
        // Add the desired route if it doesn't already exist
        let mut cmd = Command::new(PFEXEC);
        let gateway_ip = gateway_ip.to_string();
        let private_ip = private_ip.to_string();
        if let Some(zone) = zone {
            cmd.args([ZLOGIN, zone]);
        }
        let cmd = cmd.args(&[
            ROUTE,
            "-n",
            "get",
            "-inet",
            "-host",
            &gateway_ip,
            &private_ip,
            "-interface",
            "-ifp",
            opte_port,
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
                if let Some(zone) = zone {
                    cmd.args([ZLOGIN, zone]);
                }
                let cmd = cmd.args(&[
                    ROUTE,
                    "add",
                    "-inet",
                    "-host",
                    &gateway_ip,
                    &private_ip,
                    "-interface",
                    "-ifp",
                    opte_port,
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
        bootstrap_prefix: Ipv6Subnet<BOOTSTRAP_SUBNET_PREFIX_LENGTH>,
        gz_bootstrap_addr: Ipv6Addr,
        zone_vnic_name: &str,
    ) -> Result<(), ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[
            ROUTE,
            "add",
            "-inet6",
            &bootstrap_prefix.to_string(),
            &gz_bootstrap_addr.to_string(),
            "-ifp",
            zone_vnic_name,
        ]);
        execute_async(cmd).await?;
        Ok(())
    }
}
