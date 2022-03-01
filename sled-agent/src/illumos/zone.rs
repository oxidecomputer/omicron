// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for interacting with Zones running Propolis.

use ipnetwork::IpNetwork;
use slog::Logger;
use std::net::IpAddr;

use crate::illumos::addrobj::AddrObject;
use crate::illumos::dladm::{Dladm, VNIC_PREFIX_CONTROL};
use crate::illumos::zfs::ZONE_ZFS_DATASET_MOUNTPOINT;
use crate::illumos::{execute, PFEXEC};

const DLADM: &str = "/usr/sbin/dladm";
const IPADM: &str = "/usr/sbin/ipadm";
pub const SVCADM: &str = "/usr/sbin/svcadm";
pub const SVCCFG: &str = "/usr/sbin/svccfg";
pub const ZLOGIN: &str = "/usr/sbin/zlogin";

// TODO: These could become enums
pub const ZONE_PREFIX: &str = "oxz_";
pub const PROPOLIS_ZONE_PREFIX: &str = "oxz_propolis-server_";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    // TODO: These could be grouped into an "operation" error with an enum
    // variant, if we want...
    #[error("Cannot halt zone: {0}")]
    Halt(zone::ZoneError),

    #[error("Cannot uninstall zone: {0}")]
    Uninstall(zone::ZoneError),

    #[error("Cannot delete zone: {0}")]
    Delete(zone::ZoneError),

    #[error("Cannot install zone: {0}")]
    Install(zone::ZoneError),

    #[error("Cannot configure zone: {0}")]
    Configure(zone::ZoneError),

    #[error("Cannot clone zone: {0}")]
    Clone(zone::ZoneError),

    #[error("Cannot boot zone: {0}")]
    Boot(zone::ZoneError),

    #[error("Cannot list zones: {0}")]
    List(zone::ZoneError),

    #[error("Zone execution error: {0}")]
    Execution(#[from] crate::illumos::ExecutionError),

    #[error("Failed to parse output: {0}")]
    Parse(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    Dladm(#[from] crate::illumos::dladm::Error),

    #[error("Error accessing filesystem: {0}")]
    Filesystem(std::io::Error),

    #[error("Unexpected IP address: {0}")]
    Ip(IpNetwork),

    #[error("Value not found")]
    NotFound,
}

/// Describes the type of addresses which may be requested from a zone.
#[derive(Copy, Clone, Debug)]
pub enum AddressRequest {
    Dhcp,
    Static(IpNetwork),
}

impl AddressRequest {
    /// Convenience function for creating an `AddressRequest` from a static IP.
    pub fn new_static(ip: IpAddr, prefix: Option<u8>) -> Self {
        let prefix = prefix.unwrap_or_else(|| match ip {
            IpAddr::V4(_) => 24,
            IpAddr::V6(_) => 64,
        });
        let addr = IpNetwork::new(ip, prefix).unwrap();
        AddressRequest::Static(addr)
    }
}

/// Wraps commands for interacting with Zones.
pub struct Zones {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zones {
    /// Ensures a zone is halted before both uninstalling and deleting it.
    pub fn halt_and_remove(log: &Logger, name: &str) -> Result<(), Error> {
        if let Some(zone) = Self::find(name)? {
            info!(log, "halt_and_remove: Zone state: {:?}", zone.state());
            let (halt, uninstall) = match zone.state() {
                // For states where we could be running, attempt to halt.
                zone::State::Running | zone::State::Ready => (true, true),
                // For zones where we never performed installation, simply
                // delete the zone - uninstallation is invalid.
                zone::State::Configured => (false, false),
                // For most zone states, perform uninstallation.
                _ => (false, true),
            };

            if halt {
                zone::Adm::new(name).halt().map_err(Error::Halt)?;
            }
            if uninstall {
                zone::Adm::new(name)
                    .uninstall(/* force= */ true)
                    .map_err(Error::Uninstall)?;
            }
            zone::Config::new(name)
                .delete(/* force= */ true)
                .run()
                .map_err(Error::Delete)?;
        }
        Ok(())
    }

    pub fn install_omicron_zone(
        log: &Logger,
        zone_name: &str,
        zone_image: &std::path::Path,
        datasets: &[zone::Dataset],
        devices: &[zone::Device],
        vnics: Vec<String>,
    ) -> Result<(), Error> {
        if let Some(zone) = Self::find(zone_name)? {
            info!(
                log,
                "install_omicron_zone: Found zone: {} in state {:?}",
                zone.name(),
                zone.state()
            );
            if zone.state() == zone::State::Installed
                || zone.state() == zone::State::Running
            {
                // TODO: Admittedly, the zone still might be messed up. However,
                // for now, we assume that "installed" means "good to go".
                return Ok(());
            } else {
                info!(
                    log,
                    "Invalid state; uninstalling and deleting zone {}",
                    zone_name
                );
                Zones::halt_and_remove(log, zone.name())?;
            }
        }

        info!(log, "Configuring new Omicron zone: {}", zone_name);
        let mut cfg = zone::Config::create(
            zone_name,
            /* overwrite= */ true,
            zone::CreationOptions::Blank,
        );
        let path = format!("{}/{}", ZONE_ZFS_DATASET_MOUNTPOINT, zone_name);
        cfg.get_global()
            .set_brand("omicron1")
            .set_path(&path)
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);

        for dataset in datasets {
            cfg.add_dataset(&dataset);
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
        cfg.run().map_err(Error::Configure)?;

        // TODO: This process takes a little while... Consider optimizing.
        info!(log, "Installing Omicron zone: {}", zone_name);

        zone::Adm::new(zone_name)
            .install(&[zone_image.as_ref()])
            .map_err(Error::Install)?;
        Ok(())
    }

    /// Boots a zone (named `name`).
    pub fn boot(name: &str) -> Result<(), Error> {
        zone::Adm::new(name).boot().map_err(Error::Boot)?;
        Ok(())
    }

    /// Returns all zones that may be managed by the Sled Agent.
    ///
    /// These zones must have names starting with [`ZONE_PREFIX`].
    pub fn get() -> Result<Vec<zone::Zone>, Error> {
        Ok(zone::Adm::list()
            .map_err(Error::List)?
            .into_iter()
            .filter(|z| z.name().starts_with(ZONE_PREFIX))
            .collect())
    }

    /// Finds a zone with a specified name.
    ///
    /// Can only return zones that start with [`ZONE_PREFIX`], as they
    /// are managed by the Sled Agent.
    pub fn find(name: &str) -> Result<Option<zone::Zone>, Error> {
        Ok(Self::get()?.into_iter().find(|zone| zone.name() == name))
    }

    /// Returns the name of the VNIC used to communicate with the control plane.
    pub fn get_control_interface(zone: &str) -> Result<String, Error> {
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
        let output = execute(cmd)?;
        String::from_utf8(output.stdout)
            .map_err(Error::Parse)?
            .lines()
            .find_map(|name| {
                if name.starts_with(VNIC_PREFIX_CONTROL) {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .ok_or(Error::NotFound)
    }

    /// Gets the address if one exists, creates one if one does not exist.
    #[allow(clippy::needless_lifetimes)]
    pub fn ensure_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, Error> {
        match Self::get_address(zone, addrobj) {
            Ok(addr) => {
                if let AddressRequest::Static(expected_addr) = addrtype {
                    if addr != expected_addr {
                        return Err(Error::Ip(addr));
                    }
                }
                Ok(addr)
            }
            Err(_) => Self::create_address(zone, addrobj, addrtype),
        }
    }

    /// Gets the IP address of an interface within a Zone.
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
        String::from_utf8(output.stdout)?
            .lines()
            .find_map(|s| s.parse().ok())
            .ok_or(Error::NotFound)
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

        let show_addr_args =
            &[IPADM, "show-addr", "-p", "-o", "TYPE", &addrobj.to_string()];

        let args = prefix.iter().chain(show_addr_args);
        let cmd = command.args(args);
        let output = execute(cmd)?;
        if let Some(_) = String::from_utf8(output.stdout)?
            .lines()
            .find(|s| s.trim() == "addrconf")
        {
            return Ok(());
        }
        Err(Error::NotFound)
    }

    // Attempts to create the requested address.
    //
    // Does NOT check if the address already exists.
    #[allow(clippy::needless_lifetimes)]
    fn create_address_internal<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<(), Error> {
        // No link-local address was found, attempt to make one.
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

    // Ensures a link local IPv6 exists for the object.
    //
    // This is necessary for allocating IPv6 addresses on illumos.
    //
    // For more context, see:
    // <https://ry.goodwu.net/tinkering/a-day-in-the-life-of-an-ipv6-address-on-illumos/>
    #[allow(clippy::needless_lifetimes)]
    fn ensure_has_link_local_v6_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), Error> {
        let link_local_addrobj = addrobj.on_same_interface("linklocal");

        if let Ok(()) =
            Self::has_link_local_v6_address(zone, &link_local_addrobj)
        {
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
            &link_local_addrobj.to_string(),
        ];
        let args = prefix.iter().chain(create_addr_args);

        let cmd = command.args(args);
        execute(cmd)?;
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
                        // Ensure that addrconf has been set up in the Global
                        // Zone.
                        let gz_link_local_addrobj = AddrObject::new(
                            &Dladm::find_physical()?.0,
                            "linklocal",
                        );
                        Self::ensure_has_link_local_v6_address(
                            None,
                            &gz_link_local_addrobj,
                        )?;

                        // Ensure that a static IPv6 address has been allocated
                        // to the Global Zone. Without this, we don't have a way
                        // to route to IP addresses that we want to create in
                        // the non-GZ.
                        Self::ensure_address(
                            None,
                            &gz_link_local_addrobj.on_same_interface("v6route"),
                            AddressRequest::new_static(
                                "fd00:1234::".parse().unwrap(),
                                Some(16),
                            ),
                        )?;

                        // Finally, actually ensure that the v6 address we want
                        // exists within the zone.
                        Self::ensure_has_link_local_v6_address(
                            Some(zone),
                            addrobj,
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
