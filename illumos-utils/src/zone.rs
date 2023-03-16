// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for interacting with Zones running Propolis.

use anyhow::anyhow;
use ipnetwork::IpNetwork;
use slog::info;
use slog::Logger;
use std::net::{IpAddr, Ipv6Addr};

use crate::addrobj::AddrObject;
use crate::dladm::{EtherstubVnic, VNIC_PREFIX_BOOTSTRAP, VNIC_PREFIX_CONTROL};
use crate::zfs::ZONE_ZFS_DATASET_MOUNTPOINT;
use crate::{execute, PFEXEC};
use omicron_common::address::SLED_PREFIX;

const DLADM: &str = "/usr/sbin/dladm";
pub const IPADM: &str = "/usr/sbin/ipadm";
pub const SVCADM: &str = "/usr/sbin/svcadm";
pub const SVCCFG: &str = "/usr/sbin/svccfg";
pub const ZLOGIN: &str = "/usr/sbin/zlogin";

// TODO: These could become enums
pub const ZONE_PREFIX: &str = "oxz_";
pub const PROPOLIS_ZONE_PREFIX: &str = "oxz_propolis-server_";

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Zone execution error: {0}")]
    Execution(#[from] crate::ExecutionError),

    #[error(transparent)]
    AddrObject(#[from] crate::addrobj::ParseError),

    #[error("Address not found: {addrobj}")]
    AddressNotFound { addrobj: AddrObject },
}

/// Operations issued via [`zone::Adm`].
#[derive(Debug, Clone)]
pub enum Operation {
    Boot,
    Configure,
    Delete,
    Halt,
    Install,
    List,
    Uninstall,
}

/// Errors from issuing [`zone::Adm`] commands.
#[derive(thiserror::Error, Debug)]
#[error("Failed to execute zoneadm command '{op:?}' for zone '{zone}': {err}")]
pub struct AdmError {
    op: Operation,
    zone: String,
    #[source]
    err: zone::ZoneError,
}

/// Errors which may be encountered when deleting addresses.
#[derive(thiserror::Error, Debug)]
#[error("Failed to delete address '{addrobj}' in zone '{zone}': {err}")]
pub struct DeleteAddressError {
    zone: String,
    addrobj: AddrObject,
    #[source]
    err: crate::ExecutionError,
}

/// Errors from [`Zones::get_control_interface`].
/// Error which may be returned accessing the control interface of a zone.
#[derive(thiserror::Error, Debug)]
pub enum GetControlInterfaceError {
    #[error("Failed to query zone '{zone}' for control interface: {err}")]
    Execution {
        zone: String,
        #[source]
        err: crate::ExecutionError,
    },

    #[error("VNIC starting with 'oxControl' not found in {zone}")]
    NotFound { zone: String },
}

/// Errors from [`Zones::get_bootstrap_interface`].
/// Error which may be returned accessing the bootstrap interface of a zone.
#[derive(thiserror::Error, Debug)]
pub enum GetBootstrapInterfaceError {
    #[error("Failed to query zone '{zone}' for control interface: {err}")]
    Execution {
        zone: String,
        #[source]
        err: crate::ExecutionError,
    },

    #[error("VNIC starting with 'oxBootstrap' not found in {zone}")]
    NotFound { zone: String },

    #[error(
        "VNIC starting with 'oxBootstrap' found in non-switch zone: {zone}"
    )]
    Unexpected { zone: String },
}

/// Errors which may be encountered ensuring addresses.
#[derive(thiserror::Error, Debug)]
#[error(
    "Failed to create address {request:?} with name {name} in {zone}: {err}"
)]
pub struct EnsureAddressError {
    zone: String,
    request: AddressRequest,
    name: AddrObject,
    #[source]
    err: anyhow::Error,
}

/// Errors from [`Zones::ensure_has_global_zone_v6_address`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to create address {address} with name {name} in the GZ on {link:?}: {err}. Note to developers: {extra_note}")]
pub struct EnsureGzAddressError {
    address: IpAddr,
    link: String,
    name: String,
    #[source]
    err: anyhow::Error,
    extra_note: String,
}

/// Describes the type of addresses which may be requested from a zone.
#[derive(Copy, Clone, Debug)]
// TODO-cleanup: Remove, along with moving to IPv6 addressing everywhere.
// See https://github.com/oxidecomputer/omicron/issues/889.
#[allow(dead_code)]
pub enum AddressRequest {
    Dhcp,
    Static(IpNetwork),
}

impl AddressRequest {
    /// Convenience function for creating an `AddressRequest` from a static IP.
    pub fn new_static(ip: IpAddr, prefix: Option<u8>) -> Self {
        let prefix = prefix.unwrap_or_else(|| match ip {
            IpAddr::V4(_) => 24,
            IpAddr::V6(_) => SLED_PREFIX,
        });
        let addr = IpNetwork::new(ip, prefix).unwrap();
        AddressRequest::Static(addr)
    }
}

/// Wraps commands for interacting with Zones.
pub struct Zones {}

#[cfg_attr(any(test, feature = "testing"), mockall::automock, allow(dead_code))]
impl Zones {
    /// Ensures a zone is halted before both uninstalling and deleting it.
    ///
    /// Returns the state the zone was in before it was removed, or None if the
    /// zone did not exist.
    pub async fn halt_and_remove(
        name: &str,
    ) -> Result<Option<zone::State>, AdmError> {
        match Self::find(name).await? {
            None => Ok(None),
            Some(zone) => {
                let state = zone.state();
                let (halt, uninstall) = match state {
                    // For states where we could be running, attempt to halt.
                    zone::State::Running | zone::State::Ready => (true, true),
                    // For zones where we never performed installation, simply
                    // delete the zone - uninstallation is invalid.
                    zone::State::Configured => (false, false),
                    // For most zone states, perform uninstallation.
                    _ => (false, true),
                };

                if halt {
                    zone::Adm::new(name).halt().await.map_err(|err| {
                        AdmError {
                            op: Operation::Halt,
                            zone: name.to_string(),
                            err,
                        }
                    })?;
                }
                if uninstall {
                    zone::Adm::new(name)
                        .uninstall(/* force= */ true)
                        .await
                        .map_err(|err| AdmError {
                            op: Operation::Uninstall,
                            zone: name.to_string(),
                            err,
                        })?;
                }
                zone::Config::new(name)
                    .delete(/* force= */ true)
                    .run()
                    .await
                    .map_err(|err| AdmError {
                    op: Operation::Delete,
                    zone: name.to_string(),
                    err,
                })?;
                Ok(Some(state))
            }
        }
    }

    /// Halt and remove the zone, logging the state in which the zone was found.
    pub async fn halt_and_remove_logged(
        log: &Logger,
        name: &str,
    ) -> Result<(), AdmError> {
        if let Some(state) = Self::halt_and_remove(name).await? {
            info!(
                log,
                "halt_and_remove_logged: Previous zone state: {:?}", state
            );
        }
        Ok(())
    }

    /// Installs a zone with the provided arguments.
    ///
    /// - If a zone with the name `zone_name` exists and is currently running,
    /// we return immediately.
    /// - Otherwise, the zone is deleted.
    #[allow(clippy::too_many_arguments)]
    pub async fn install_omicron_zone(
        log: &Logger,
        zone_name: &str,
        zone_image: &std::path::Path,
        datasets: &[zone::Dataset],
        filesystems: &[zone::Fs],
        devices: &[zone::Device],
        vnics: Vec<String>,
        limit_priv: Vec<String>,
    ) -> Result<(), AdmError> {
        if let Some(zone) = Self::find(zone_name).await? {
            info!(
                log,
                "install_omicron_zone: Found zone: {} in state {:?}",
                zone.name(),
                zone.state()
            );
            if zone.state() == zone::State::Running {
                // TODO: Admittedly, the zone still might be messed up. However,
                // for now, we assume that "running" means "good to go".
                return Ok(());
            } else {
                info!(
                    log,
                    "Invalid state; uninstalling and deleting zone {}",
                    zone_name
                );
                Zones::halt_and_remove_logged(log, zone.name()).await?;
            }
        }

        info!(log, "Configuring new Omicron zone: {}", zone_name);
        let mut cfg = zone::Config::create(
            zone_name,
            // overwrite=
            true,
            zone::CreationOptions::Blank,
        );
        let path = format!("{}/{}", ZONE_ZFS_DATASET_MOUNTPOINT, zone_name);
        cfg.get_global()
            .set_brand("omicron1")
            .set_path(&path)
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        if !limit_priv.is_empty() {
            let limit_priv = std::collections::BTreeSet::from_iter(limit_priv);
            cfg.get_global().set_limitpriv(limit_priv);
        }

        for dataset in datasets {
            cfg.add_dataset(&dataset);
        }
        for filesystem in filesystems {
            cfg.add_fs(&filesystem);
        }
        for device in devices {
            cfg.add_device(device);
        }
        for vnic in &vnics {
            cfg.add_net(&zone::Net {
                physical: vnic.to_string(),
                ..Default::default()
            });
        }
        cfg.run().await.map_err(|err| AdmError {
            op: Operation::Configure,
            zone: zone_name.to_string(),
            err,
        })?;

        info!(log, "Installing Omicron zone: {}", zone_name);

        zone::Adm::new(zone_name)
            .install(&[zone_image.as_ref()])
            .await
            .map_err(|err| AdmError {
                op: Operation::Install,
                zone: zone_name.to_string(),
                err,
            })?;
        Ok(())
    }

    /// Boots a zone (named `name`).
    pub async fn boot(name: &str) -> Result<(), AdmError> {
        zone::Adm::new(name).boot().await.map_err(|err| AdmError {
            op: Operation::Boot,
            zone: name.to_string(),
            err,
        })?;
        Ok(())
    }

    /// Returns all zones that may be managed by the Sled Agent.
    ///
    /// These zones must have names starting with [`ZONE_PREFIX`].
    pub async fn get() -> Result<Vec<zone::Zone>, AdmError> {
        Ok(zone::Adm::list()
            .await
            .map_err(|err| AdmError {
                op: Operation::List,
                zone: "<all>".to_string(),
                err,
            })?
            .into_iter()
            .filter(|z| z.name().starts_with(ZONE_PREFIX))
            .collect())
    }

    /// Finds a zone with a specified name.
    ///
    /// Can only return zones that start with [`ZONE_PREFIX`], as they
    /// are managed by the Sled Agent.
    pub async fn find(name: &str) -> Result<Option<zone::Zone>, AdmError> {
        Ok(Self::get().await?.into_iter().find(|zone| zone.name() == name))
    }

    /// Returns the name of the VNIC used to communicate with the control plane.
    pub fn get_control_interface(
        zone: &str,
    ) -> Result<String, GetControlInterfaceError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            DLADM,
            "show-vnic",
            "-p",
            "-o",
            "LINK",
        ]);
        let output = execute(cmd).map_err(|err| {
            GetControlInterfaceError::Execution { zone: zone.to_string(), err }
        })?;
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|name| {
                if name.starts_with(VNIC_PREFIX_CONTROL) {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .ok_or(GetControlInterfaceError::NotFound {
                zone: zone.to_string(),
            })
    }

    /// Returns the name of the VNIC used to communicate with the bootstrap network.
    pub fn get_bootstrap_interface(
        zone: &str,
    ) -> Result<Option<String>, GetBootstrapInterfaceError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            DLADM,
            "show-vnic",
            "-p",
            "-o",
            "LINK",
        ]);
        let output = execute(cmd).map_err(|err| {
            GetBootstrapInterfaceError::Execution {
                zone: zone.to_string(),
                err,
            }
        })?;
        let vnic =
            String::from_utf8_lossy(&output.stdout).lines().find_map(|name| {
                if name.starts_with(VNIC_PREFIX_BOOTSTRAP) {
                    Some(name.to_string())
                } else {
                    None
                }
            });

        if zone == "oxz_switch" && vnic.is_none() {
            Err(GetBootstrapInterfaceError::NotFound { zone: zone.to_string() })
        } else if zone != "oxz_switch" && vnic.is_some() {
            Err(GetBootstrapInterfaceError::Unexpected {
                zone: zone.to_string(),
            })
        } else {
            Ok(vnic)
        }
    }

    /// Ensures that an IP address on an interface matches the requested value.
    ///
    /// - If the address exists, ensure it has the desired value.
    /// - If the address does not exist, create it.
    ///
    /// This address may be optionally within a zone `zone`.
    /// If `None` is supplied, the address is queried from the Global Zone.
    #[allow(clippy::needless_lifetimes)]
    pub fn ensure_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, EnsureAddressError> {
        |zone, addrobj, addrtype| -> Result<IpNetwork, anyhow::Error> {
            match Self::get_address(zone, addrobj) {
                Ok(addr) => {
                    if let AddressRequest::Static(expected_addr) = addrtype {
                        // If the address is static, we need to validate that it
                        // matches the value we asked for.
                        if addr != expected_addr {
                            // If the address doesn't match, try removing the old
                            // value before using the new one.
                            Self::delete_address(zone, addrobj)
                                .map_err(|e| anyhow!(e))?;
                            return Self::create_address(
                                zone, addrobj, addrtype,
                            )
                            .map_err(|e| anyhow!(e));
                        }
                    }
                    Ok(addr)
                }
                Err(_) => Self::create_address(zone, addrobj, addrtype)
                    .map_err(|e| anyhow!(e)),
            }
        }(zone, addrobj, addrtype)
        .map_err(|err| EnsureAddressError {
            zone: zone.unwrap_or("global").to_string(),
            request: addrtype,
            name: addrobj.clone(),
            err,
        })
    }

    /// Gets the IP address of an interface.
    ///
    /// This address may optionally be within a zone named `zone`.
    /// If `None` is supplied, the address is queried from the Global Zone.
    #[allow(clippy::needless_lifetimes)]
    fn get_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<IpNetwork, Error> {
        let mut command = std::process::Command::new(PFEXEC);

        let mut args = vec![];
        if let Some(zone) = zone {
            args.push(ZLOGIN);
            args.push(zone);
        };
        let addrobj_str = addrobj.to_string();
        args.extend(&[IPADM, "show-addr", "-p", "-o", "ADDR", &addrobj_str]);

        let cmd = command.args(args);
        let output = execute(cmd)?;
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|s| s.parse().ok())
            .ok_or(Error::AddressNotFound { addrobj: addrobj.clone() })
    }

    /// Returns Ok(()) if `addrobj` has a corresponding link-local IPv6 address.
    ///
    /// Zone may either be `Some(zone)` for a non-global zone, or `None` to
    /// run the command in the Global zone.
    #[allow(clippy::needless_lifetimes)]
    fn has_link_local_v6_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), Error> {
        let mut command = std::process::Command::new(PFEXEC);

        let prefix =
            if let Some(zone) = zone { vec![ZLOGIN, zone] } else { vec![] };

        let interface = format!("{}", addrobj);
        let show_addr_args =
            &[IPADM, "show-addr", "-p", "-o", "TYPE", &interface];

        let args = prefix.iter().chain(show_addr_args);
        let cmd = command.args(args);
        let output = execute(cmd)?;
        if let Some(_) = String::from_utf8_lossy(&output.stdout)
            .lines()
            .find(|s| s.trim() == "addrconf")
        {
            return Ok(());
        }
        Err(Error::AddressNotFound { addrobj: addrobj.clone() })
    }

    // Attempts to create the requested address.
    //
    // Does NOT check if the address already exists.
    #[allow(clippy::needless_lifetimes)]
    fn create_address_internal<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<(), crate::ExecutionError> {
        let mut command = std::process::Command::new(PFEXEC);
        let mut args = vec![];
        if let Some(zone) = zone {
            args.push(ZLOGIN.to_string());
            args.push(zone.to_string());
        };

        args.extend(
            vec![IPADM, "create-addr", "-t", "-T"]
                .into_iter()
                .map(String::from),
        );

        match addrtype {
            AddressRequest::Dhcp => {
                args.push("dhcp".to_string());
            }
            AddressRequest::Static(addr) => {
                args.push("static".to_string());
                args.push("-a".to_string());
                args.push(addr.to_string());
            }
        }
        args.push(addrobj.to_string());

        let cmd = command.args(args);
        execute(cmd)?;
        Ok(())
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn delete_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), DeleteAddressError> {
        let mut command = std::process::Command::new(PFEXEC);
        let mut args = vec![];
        if let Some(zone) = zone {
            args.push(ZLOGIN.to_string());
            args.push(zone.to_string());
        };

        args.push(IPADM.to_string());
        args.push("delete-addr".to_string());
        args.push(addrobj.to_string());

        let cmd = command.args(args);
        execute(cmd).map_err(|err| DeleteAddressError {
            zone: zone.unwrap_or("global").to_string(),
            addrobj: addrobj.clone(),
            err,
        })?;
        Ok(())
    }

    /// Ensures a link-local IPv6 exists with the name provided in `addrobj`.
    ///
    /// A link-local address is necessary for allocating a static address on an
    /// interface on illumos.
    ///
    /// For more context, see:
    /// <https://ry.goodwu.net/tinkering/a-day-in-the-life-of-an-ipv6-address-on-illumos/>
    #[allow(clippy::needless_lifetimes)]
    pub fn ensure_has_link_local_v6_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), crate::ExecutionError> {
        if let Ok(()) = Self::has_link_local_v6_address(zone, &addrobj) {
            return Ok(());
        }

        // No link-local address was found, attempt to make one.
        let mut command = std::process::Command::new(PFEXEC);

        let prefix =
            if let Some(zone) = zone { vec![ZLOGIN, zone] } else { vec![] };

        let create_addr_args = &[
            IPADM,
            "create-addr",
            "-t",
            "-T",
            "addrconf",
            &addrobj.to_string(),
        ];
        let args = prefix.iter().chain(create_addr_args);

        let cmd = command.args(args);
        execute(cmd)?;
        Ok(())
    }

    // TODO(https://github.com/oxidecomputer/omicron/issues/821): We
    // should remove this function when Sled Agents are provided IPv6 addresses
    // from RSS.
    pub fn ensure_has_global_zone_v6_address(
        link: EtherstubVnic,
        address: Ipv6Addr,
        name: &str,
    ) -> Result<(), EnsureGzAddressError> {
        // Call the guts of this function within a closure to make it easier
        // to wrap the error with appropriate context.
        |link: EtherstubVnic, address, name| -> Result<(), anyhow::Error> {
            let gz_link_local_addrobj = AddrObject::link_local(&link.0)
                .map_err(|err| anyhow!(err))?;
            Self::ensure_has_link_local_v6_address(
                None,
                &gz_link_local_addrobj,
            )
            .map_err(|err| anyhow!(err))?;

            // Ensure that a static IPv6 address has been allocated to the
            // Global Zone. Without this, we don't have a way to route to IP
            // addresses that we want to create in the non-GZ. Note that we
            // use a `/64` prefix, as all addresses allocated for services on
            // this sled itself are within the underlay or bootstrap prefix.
            // Anything else must be routed through Sidecar.
            Self::ensure_address(
                None,
                &gz_link_local_addrobj
                    .on_same_interface(name)
                    .map_err(|err| anyhow!(err))?,
                AddressRequest::new_static(
                    IpAddr::V6(address),
                    Some(omicron_common::address::SLED_PREFIX),
                ),
            )
            .map_err(|err| anyhow!(err))?;
            Ok(())
        }(link.clone(), address, name)
        .map_err(|err| EnsureGzAddressError {
            address: IpAddr::V6(address),
            link: link.0.clone(),
            name: name.to_string(),
            err,
            extra_note:
                r#"As of https://github.com/oxidecomputer/omicron/pull/1066, we are changing the
                physical device on which Global Zone addresses are allocated.

                Before this PR, we allocated addresses and VNICs directly on a physical link.
                After this PR, we are allocating them on etherstubs.

                As a result, however, if your machine previously ran Omicron, it
                may have addresses on the physical link which we'd like to
                allocate from the etherstub instead.

                This can be fixed with the following commands:

                $ pfexec ipadm delete-addr <your-link>/bootstrap6
                $ pfexec ipadm delete-addr <your-link>/sled6
                $ pfexec ipadm delete-addr <your-link>/internaldns"#.to_string()
        })?;
        Ok(())
    }

    // Creates an IP address within a Zone.
    #[allow(clippy::needless_lifetimes)]
    fn create_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, Error> {
        // Do any prep work before allocating the address.
        //
        // Currently, this only happens when allocating IPv6 addresses in the
        // non-global zone - to access these addresses, we must first set up
        // an arbitrary IPv6 address within the Global Zone.
        if let Some(zone) = zone {
            match addrtype {
                AddressRequest::Dhcp => {}
                AddressRequest::Static(addr) => {
                    if addr.is_ipv6() {
                        // Finally, actually ensure that the v6 address we want
                        // exists within the zone.
                        let link_local_addrobj =
                            addrobj.link_local_on_same_interface()?;
                        Self::ensure_has_link_local_v6_address(
                            Some(zone),
                            &link_local_addrobj,
                        )?;
                    }
                }
            }
        };

        // Actually perform address allocation.
        Self::create_address_internal(zone, addrobj, addrtype)?;

        Self::get_address(zone, addrobj)
    }
}
