// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for managing IP interfaces.

use crate::addrobj::{IPV6_LINK_LOCAL_ADDROBJ_NAME, IPV6_STATIC_ADDROBJ_NAME};
use crate::zone::IPADM;
use crate::{execute, ExecutionError, PFEXEC};
use oxnet::IpNet;
use std::net::{IpAddr, Ipv6Addr};

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

/// Expected error message when an addrobj already exists.
const ADDROBJ_ALREADY_EXISTS: &str = "Address object already exists";

pub enum AddrObjType {
    DHCP,
    AddrConf,
    Static(IpAddr),
}

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

    /// Create an address object with the provided parameters. If an object
    /// with the requested name already exists, return success. Note that in
    /// this case, the existing object is not checked to ensure it is
    /// consistent with the provided parameters.
    pub fn ensure_ip_addrobj_exists(
        addrobj: &str,
        addrtype: AddrObjType,
    ) -> Result<(), ExecutionError> {
        let mut cmd = std::process::Command::new(PFEXEC);
        let cmd = cmd.args(&[IPADM, "create-addr", "-t", "-T"]);
        let cmd = match addrtype {
            AddrObjType::DHCP => cmd.args(&["dhcp"]),
            AddrObjType::AddrConf => cmd.args(&["addrconf"]),
            AddrObjType::Static(addr) => {
                cmd.args(&["static", "-a", &addr.to_string()])
            }
        };
        let cmd = cmd.arg(&addrobj);
        match execute(cmd) {
            Ok(_) => Ok(()),
            Err(ExecutionError::CommandFailure(info))
                if info.stderr.contains(ADDROBJ_ALREADY_EXISTS) =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Remove any scope from an IPv6 address.
    /// e.g. fe80::8:20ff:fed0:8687%oxControlService1/10 ->
    ///      fe80::8:20ff:fed0:8687/10
    fn remove_addr_scope(input: &str) -> String {
        if let Some(pos) = input.find('%') {
            let (base, rest) = input.split_at(pos);
            if let Some(slash_pos) = rest.find('/') {
                format!("{}{}", base, &rest[slash_pos..])
            } else {
                base.to_string()
            }
        } else {
            input.to_string()
        }
    }

    /// Return the IP network associated with an address object, or None if
    /// there is no address object with this name.
    pub fn addrobj_addr(
        addrobj: &str,
    ) -> Result<Option<IpNet>, ExecutionError> {
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
                let out = std::str::from_utf8(&output.stdout).map_err(|e| {
                    let s = String::from_utf8_lossy(&output.stdout);
                    ExecutionError::ParseFailure(format!("{}: {}", e, s))
                })?;
                let lines: Vec<_> = out.trim().lines().collect();
                if lines.is_empty() {
                    return Ok(None);
                }
                match Self::remove_addr_scope(lines[0].trim()).parse() {
                    Ok(ipnet) => Ok(Some(ipnet)),
                    Err(e) => Err(ExecutionError::ParseFailure(format!(
                        "{}: {}",
                        lines[0].trim(),
                        e
                    ))),
                }
            }
        }
    }

    /// Determine if a named address object exists
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
        Self::ensure_ip_addrobj_exists(&addrobj, AddrObjType::AddrConf)?;

        // Create static address on the IP interface if it doesn't already exist
        let addrobj = format!("{}/{}", datalink, IPV6_STATIC_ADDROBJ_NAME);
        Self::ensure_ip_addrobj_exists(
            &addrobj,
            AddrObjType::Static((*listen_addr).into()),
        )?;
        Ok(())
    }

    // Create gateway on the IP interface if it doesn't already exist
    pub fn create_opte_gateway(
        opte_iface: &String,
    ) -> Result<(), ExecutionError> {
        let addrobj = format!("{}/public", opte_iface);
        Self::ensure_ip_addrobj_exists(&addrobj, AddrObjType::DHCP)?;
        Ok(())
    }
}
