// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for interacting with Zones running Propolis.

use anyhow::anyhow;
use camino::Utf8Path;
use ipnetwork::IpNetwork;
use ipnetwork::IpNetworkError;
use slog::Logger;
use slog::info;
use std::net::{IpAddr, Ipv6Addr};
use tokio::process::Command;

use crate::ExecutionError;
use crate::addrobj::AddrObject;
use crate::dladm::{EtherstubVnic, VNIC_PREFIX_BOOTSTRAP, VNIC_PREFIX_CONTROL};
use crate::zpool::PathInPool;
use crate::{PFEXEC, execute_async};
use omicron_common::address::SLED_PREFIX;
use omicron_uuid_kinds::OmicronZoneUuid;

const DLADM: &str = "/usr/sbin/dladm";
pub const IPADM: &str = "/usr/sbin/ipadm";
pub const SVCADM: &str = "/usr/sbin/svcadm";
pub const SVCCFG: &str = "/usr/sbin/svccfg";
pub const ZLOGIN: &str = "/usr/sbin/zlogin";
pub const ROUTE: &str = "/usr/sbin/route";

// TODO: These could become enums
pub const ZONE_PREFIX: &str = "oxz_";
pub const PROPOLIS_ZONE_PREFIX: &str = "oxz_propolis-server_";

pub fn zone_name(prefix: &str, id: Option<OmicronZoneUuid>) -> String {
    if let Some(id) = id {
        format!("{ZONE_PREFIX}{}_{}", prefix, id)
    } else {
        format!("{ZONE_PREFIX}{}", prefix)
    }
}

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
    err: AdmErrorKind,
}

#[derive(thiserror::Error, Debug)]
pub enum AdmErrorKind {
    /// The zone is currently in a state in which it cannot be uninstalled.
    /// These states are generally transient, so this error is likely to be
    /// retryable.
    #[error("this operation cannot be performed in the '{:?}' state", .0)]
    InvalidState(zone::State),
    /// Another zoneadm error occurred.
    #[error(transparent)]
    Zoneadm(#[from] zone::ZoneError),
}

impl AdmError {
    pub fn is_invalid_state(&self) -> bool {
        matches!(self.err, AdmErrorKind::InvalidState(_))
    }
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

/// Errors which may be encountered getting addresses.
#[derive(thiserror::Error, Debug)]
#[error("Failed to get address for name {name} in {zone}: {err}")]
pub struct GetAddressError {
    zone: String,
    name: AddrObject,
    #[source]
    err: anyhow::Error,
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
#[error(
    "Failed to create address {address} with name {name} in the GZ on {link:?}: {err}. Note to developers: {extra_note}"
)]
pub struct EnsureGzAddressError {
    address: IpAddr,
    link: String,
    name: String,
    #[source]
    err: anyhow::Error,
    extra_note: String,
}

/// Errors which may be encountered getting addresses.
#[derive(thiserror::Error, Debug)]
#[error("Failed to get addresses with name {name} in {zone}: {err}")]
pub struct GetAddressesError {
    zone: String,
    name: AddrObject,
    #[source]
    err: anyhow::Error,
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

// Helper function to parse the output of `ipadm show-addr -o ADDR`, which might
// or might not contain an interface scope (which `ipnetwork` doesn't know how
// to parse).
fn parse_ip_network(s: &str) -> Result<IpNetwork, IpNetworkError> {
    // Does `s` appear to contain a scope identifier? If so, we want to trim it
    // out.
    if let Some(scope_start) = s.find('%') {
        let (ip, rest) = s.split_at(scope_start);

        // Is there a `/prefix` _after_ the scope? If so, we want to reconstruct
        // a string consisting of the leading `ip` and the trailing `/prefix`,
        // removing the `%scope` in the middle.
        if let Some(prefix_start) = rest.find('/') {
            let (_scope, prefix) = rest.split_at(prefix_start);
            let without_scope = format!("{ip}{prefix}");
            without_scope.parse()
        } else {
            // We found a `%` indicating a scope but no `/` after it; parse just
            // the IP address.
            ip.parse()
        }
    } else {
        // No `%` found; just try parsing `s` directly.
        s.parse()
    }
}

/// Wraps commands for interacting with Zones.
pub struct Zones(());

/// Describes the API for interfacing with Zones.
///
/// This is a trait so that it can be faked out for tests.
#[async_trait::async_trait]
pub trait Api: Send + Sync {
    async fn get(&self) -> Result<Vec<zone::Zone>, crate::zone::AdmError> {
        Ok(zone::Adm::list()
            .await
            .map_err(|err| AdmError {
                op: Operation::List,
                zone: "<all>".to_string(),
                err: err.into(),
            })?
            .into_iter()
            .filter(|z| z.name().starts_with(ZONE_PREFIX))
            .collect())
    }

    async fn find(
        &self,
        name: &str,
    ) -> Result<Option<zone::Zone>, crate::zone::AdmError> {
        Ok(self.get().await?.into_iter().find(|zone| zone.name() == name))
    }

    /// Installs a zone with the provided arguments.
    ///
    /// - If a zone with the name `zone_name` exists and is currently running,
    /// we return immediately.
    /// - Otherwise, the zone is deleted.
    #[allow(clippy::too_many_arguments)]
    async fn install_omicron_zone(
        &self,
        log: &Logger,
        zone_root_path: &PathInPool,
        zone_name: &str,
        zone_image: &Utf8Path,
        datasets: &[zone::Dataset],
        filesystems: &[zone::Fs],
        devices: &[zone::Device],
        links: Vec<String>,
        limit_priv: Vec<String>,
    ) -> Result<(), crate::zone::AdmError> {
        if let Some(zone) = self.find(zone_name).await? {
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
                self.halt_and_remove_logged(log, zone.name()).await?;
            }
        }

        info!(log, "Configuring new Omicron zone: {}", zone_name);
        let mut cfg = zone::Config::create(
            zone_name,
            // overwrite=
            true,
            zone::CreationOptions::Blank,
        );
        cfg.get_global()
            .set_brand("omicron1")
            .set_path(&zone_root_path.path)
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        if !limit_priv.is_empty() {
            let limit_priv = std::collections::BTreeSet::from_iter(limit_priv);
            cfg.get_global().set_limitpriv(limit_priv);
        }

        for dataset in datasets {
            cfg.add_dataset(dataset);
        }
        for filesystem in filesystems {
            cfg.add_fs(filesystem);
        }
        for device in devices {
            cfg.add_device(device);
        }
        for link in &links {
            cfg.add_net(&zone::Net {
                physical: link.to_string(),
                ..Default::default()
            });
        }
        cfg.run().await.map_err(|err| AdmError {
            op: Operation::Configure,
            zone: zone_name.to_string(),
            err: err.into(),
        })?;

        info!(log, "Installing Omicron zone: {}", zone_name);

        zone::Adm::new(zone_name)
            .install(&[
                zone_image.as_ref(),
                "/opt/oxide/overlay.tar.gz".as_ref(),
            ])
            .await
            .map_err(|err| AdmError {
                op: Operation::Install,
                zone: zone_name.to_string(),
                err: err.into(),
            })?;
        Ok(())
    }

    /// Boots a zone (named `name`).
    async fn boot(&self, name: &str) -> Result<(), crate::zone::AdmError> {
        zone::Adm::new(name).boot().await.map_err(|err| AdmError {
            op: Operation::Boot,
            zone: name.to_string(),
            err: err.into(),
        })?;
        Ok(())
    }

    /// Return the ID for a _running_ zone with the specified name.
    async fn id(
        &self,
        name: &str,
    ) -> Result<Option<i32>, crate::zone::AdmError> {
        // Safety: illumos defines `zoneid_t` as a typedef for an integer, i.e.,
        // an `i32`, so this unwrap should always be safe.
        match self.find(name).await?.map(|zn| zn.id()) {
            Some(Some(id)) => Ok(Some(id.try_into().unwrap())),
            Some(None) | None => Ok(None),
        }
    }

    async fn wait_for_service(
        &self,
        zone: Option<&str>,
        fmri: &str,
        log: Logger,
    ) -> Result<(), omicron_common::api::external::Error> {
        crate::svc::wait_for_service(zone, fmri, log).await
    }

    /// Ensures a zone is halted before both uninstalling and deleting it.
    ///
    /// Returns the state the zone was in before it was removed, or None if the
    /// zone did not exist.
    async fn halt_and_remove(
        &self,
        name: &str,
    ) -> Result<Option<zone::State>, AdmError> {
        match self.find(name).await? {
            None => Ok(None),
            Some(zone) => {
                let state = zone.state();
                let (halt, uninstall) = match state {
                    // For states where we could be running, attempt to halt.
                    zone::State::Running | zone::State::Ready => (true, true),
                    // For zones where we never performed installation, simply
                    // delete the zone - uninstallation is invalid.
                    zone::State::Configured => (false, false),
                    // Attempting to uninstall a zone in the "down" state will
                    // fail. Instead, the caller must wait until the zone
                    // transitions to "installed".
                    zone::State::Down | zone::State::ShuttingDown => {
                        return Err(AdmError {
                            op: Operation::Uninstall,
                            zone: name.to_string(),
                            err: AdmErrorKind::InvalidState(state),
                        });
                    }
                    // For most zone states, perform uninstallation.
                    _ => (false, true),
                };

                if halt {
                    zone::Adm::new(name).halt().await.map_err(|err| {
                        AdmError {
                            op: Operation::Halt,
                            zone: name.to_string(),
                            err: err.into(),
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
                            err: err.into(),
                        })?;
                }
                zone::Config::new(name)
                    .delete(/* force= */ true)
                    .run()
                    .await
                    .map_err(|err| AdmError {
                    op: Operation::Delete,
                    zone: name.to_string(),
                    err: err.into(),
                })?;
                Ok(Some(state))
            }
        }
    }

    /// Halt and remove the zone, logging the state in which the zone was found.
    async fn halt_and_remove_logged(
        &self,
        log: &Logger,
        name: &str,
    ) -> Result<(), AdmError> {
        if let Some(state) = self.halt_and_remove(name).await? {
            info!(
                log,
                "halt_and_remove_logged: Previous zone state: {:?}", state
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Api for Zones {}

impl Zones {
    /// Access the real zone API, which will invoke commands on the host OS.
    ///
    /// If you're interested in testing this interface, consider using
    /// [crate::fakes::zone::Zones] instead.
    pub fn real_api() -> Self {
        Self(())
    }

    /// Returns the name of the VNIC used to communicate with the control plane.
    pub async fn get_control_interface(
        zone: &str,
    ) -> Result<String, GetControlInterfaceError> {
        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            DLADM,
            "show-vnic",
            "-p",
            "-o",
            "LINK",
        ]);
        let output = execute_async(cmd).await.map_err(|err| {
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
    pub async fn get_bootstrap_interface(
        zone: &str,
    ) -> Result<Option<String>, GetBootstrapInterfaceError> {
        let mut command = Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            DLADM,
            "show-vnic",
            "-p",
            "-o",
            "LINK",
        ]);
        let output = execute_async(cmd).await.map_err(|err| {
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
    pub async fn ensure_address(
        zone: Option<&str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, EnsureAddressError> {
        Self::ensure_address_inner(zone, addrobj, addrtype).await.map_err(
            |err| EnsureAddressError {
                zone: zone.unwrap_or("global").to_string(),
                request: addrtype,
                name: addrobj.clone(),
                err,
            },
        )
    }

    async fn ensure_address_inner(
        zone: Option<&str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> anyhow::Result<IpNetwork> {
        match Self::get_address_impl(zone, addrobj).await {
            Ok(addr) => {
                if let AddressRequest::Static(expected_addr) = addrtype {
                    // If the address is static, we need to validate that it
                    // matches the value we asked for.
                    if addr != expected_addr {
                        // If the address doesn't match, try removing the old
                        // value before using the new one.
                        Self::delete_address(zone, addrobj)
                            .await
                            .map_err(|e| anyhow!(e))?;
                        return Self::create_address(zone, addrobj, addrtype)
                            .await
                            .map_err(|e| anyhow!(e));
                    }
                }
                Ok(addr)
            }
            Err(_) => Self::create_address(zone, addrobj, addrtype)
                .await
                .map_err(|e| anyhow!(e)),
        }
    }

    /// Gets the IP address of an interface.
    ///
    /// This `addrobj` may optionally be within a zone named `zone`.
    /// If `None` is supplied, the address is queried from the Global Zone.
    #[allow(clippy::needless_lifetimes)]
    pub async fn get_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<IpNetwork, GetAddressError> {
        Self::get_address_impl(zone, addrobj).await.map_err(|err| {
            GetAddressError {
                zone: zone.unwrap_or("global").to_string(),
                name: addrobj.clone(),
                err: anyhow!(err),
            }
        })
    }

    /// Gets the IP address of an interface.
    ///
    /// This address may optionally be within a zone named `zone`.
    /// If `None` is supplied, the address is queried from the Global Zone.
    #[allow(clippy::needless_lifetimes)]
    async fn get_address_impl<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<IpNetwork, Error> {
        let mut command = Command::new(PFEXEC);

        let mut args = vec![];
        if let Some(zone) = zone {
            args.push(ZLOGIN);
            args.push(zone);
        };
        let addrobj_str = addrobj.to_string();
        args.extend(&[IPADM, "show-addr", "-p", "-o", "ADDR", &addrobj_str]);

        let cmd = command.args(args);
        let output = execute_async(cmd).await?;
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|s| parse_ip_network(s).ok())
            .ok_or(Error::AddressNotFound { addrobj: addrobj.clone() })
    }

    /// Gets all IP address of an interface.
    ///
    /// This `addrobj` may optionally be within a zone named `zone`.
    /// If `None` is supplied, the address is queried from the Global Zone.
    #[allow(clippy::needless_lifetimes)]
    pub async fn get_all_addresses<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<Vec<IpNetwork>, GetAddressesError> {
        let mut command = Command::new(PFEXEC);

        let mut args = vec![];
        if let Some(zone) = zone {
            args.push(ZLOGIN);
            args.push(zone);
        };
        let addrobj_str = addrobj.to_string();
        args.extend(&[IPADM, "show-addr", "-p", "-o", "ADDR", &addrobj_str]);

        let cmd = command.args(args);
        let output =
            execute_async(cmd).await.map_err(|err| GetAddressesError {
                zone: zone.unwrap_or("global").to_string(),
                name: addrobj.clone(),
                err: err.into(),
            })?;
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|s| s.parse().ok())
            .collect::<Vec<_>>())
    }

    /// Returns Ok(()) if `addrobj` has a corresponding link-local IPv6 address.
    ///
    /// Zone may either be `Some(zone)` for a non-global zone, or `None` to
    /// run the command in the Global zone.
    #[allow(clippy::needless_lifetimes)]
    async fn has_link_local_v6_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), Error> {
        let mut command = Command::new(PFEXEC);

        let prefix =
            if let Some(zone) = zone { vec![ZLOGIN, zone] } else { vec![] };

        let interface = format!("{}", addrobj);
        let show_addr_args =
            &[IPADM, "show-addr", "-p", "-o", "TYPE", &interface];

        let args = prefix.iter().chain(show_addr_args);
        let cmd = command.args(args);
        let output = execute_async(cmd).await?;
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
    pub async fn create_address_internal<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
        addrtype: AddressRequest,
    ) -> Result<(), crate::ExecutionError> {
        let mut command = Command::new(PFEXEC);
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
        execute_async(cmd).await?;

        Ok(())
    }

    /// Delete an address object.
    ///
    /// This method attempts to be idempotent: deleting a nonexistent address
    /// object returns `Ok(())`.
    #[allow(clippy::needless_lifetimes)]
    pub async fn delete_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), DeleteAddressError> {
        // Expected output on stderr if we try to delete an address that doesn't
        // exist. (We look for this error and return `Ok(_)`, making this method
        // idempotent.)
        const OBJECT_NOT_FOUND: &str =
            "could not delete address: Object not found";

        let mut command = Command::new(PFEXEC);
        let mut args = vec![];
        if let Some(zone) = zone {
            args.push(ZLOGIN.to_string());
            args.push(zone.to_string());
        };

        args.push(IPADM.to_string());
        args.push("delete-addr".to_string());
        args.push(addrobj.to_string());

        let cmd = command.args(args);
        match execute_async(cmd).await {
            Ok(_) => Ok(()),
            Err(ExecutionError::CommandFailure(err))
                if err.stderr.contains(OBJECT_NOT_FOUND) =>
            {
                Ok(())
            }
            Err(err) => Err(DeleteAddressError {
                zone: zone.unwrap_or("global").to_string(),
                addrobj: addrobj.clone(),
                err,
            }),
        }
    }

    /// Ensures a link-local IPv6 exists with the name provided in `addrobj`.
    ///
    /// A link-local address is necessary for allocating a static address on an
    /// interface on illumos.
    ///
    /// For more context, see:
    /// <https://ry.goodwu.net/tinkering/a-day-in-the-life-of-an-ipv6-address-on-illumos/>
    #[allow(clippy::needless_lifetimes)]
    pub async fn ensure_has_link_local_v6_address<'a>(
        zone: Option<&'a str>,
        addrobj: &AddrObject,
    ) -> Result<(), crate::ExecutionError> {
        if let Ok(()) = Self::has_link_local_v6_address(zone, &addrobj).await {
            return Ok(());
        }

        // No link-local address was found, attempt to make one.
        let mut command = Command::new(PFEXEC);

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
        execute_async(cmd).await?;
        Ok(())
    }

    // TODO(https://github.com/oxidecomputer/omicron/issues/821): We
    // should remove this function when Sled Agents are provided IPv6 addresses
    // from RSS. Edit to this TODO: we still need this for the bootstrap network
    // (which exists pre-RSS), but we should remove all uses of it other than
    // the bootstrap agent.
    pub async fn ensure_has_global_zone_v6_address(
        link: EtherstubVnic,
        address: Ipv6Addr,
        name: &str,
    ) -> Result<(), EnsureGzAddressError> {
        // Call the guts of this function within a closure to make it easier
        // to wrap the error with appropriate context.
        #[allow(clippy::redundant_closure_call)]
        async |link: EtherstubVnic, address, name| -> Result<(), anyhow::Error> {
            let gz_link_local_addrobj = AddrObject::link_local(&link.0)
                .map_err(|err| anyhow!(err))?;
            Self::ensure_has_link_local_v6_address(
                None,
                &gz_link_local_addrobj,
            )
            .await
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
            .await
            .map_err(|err| anyhow!(err))?;
            Ok(())
        }(link.clone(), address, name)
        .await
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
    async fn create_address<'a>(
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
                        )
                        .await?;
                    }
                }
            }
        };

        // Actually perform address allocation.
        Self::create_address_internal(zone, addrobj, addrtype).await?;

        Self::get_address_impl(zone, addrobj).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ip_network() {
        for (s, ip, prefix) in [
            (
                "fdb0:a840:2504:355::1/64",
                "fdb0:a840:2504:355::1".parse::<IpAddr>().unwrap(),
                64,
            ),
            (
                "fe80::aa40:25ff:fe04:355%cxgbe0/10",
                "fe80::aa40:25ff:fe04:355".parse::<IpAddr>().unwrap(),
                10,
            ),
            (
                "fe80::aa40:25ff:fe04:355%cxgbe0",
                "fe80::aa40:25ff:fe04:355".parse::<IpAddr>().unwrap(),
                128,
            ),
        ] {
            let parsed = parse_ip_network(s).unwrap();
            assert_eq!(parsed.ip(), ip);
            assert_eq!(parsed.prefix(), prefix);
        }
    }

    // This test validates that we correctly detect an attempt to delete an
    // address that does not exist and return `Ok(())`.
    #[cfg(target_os = "illumos")]
    #[tokio::test]
    async fn delete_nonexistent_address() {
        // We'll pick a name that hopefully no system actually has...
        let addr = AddrObject::new("nonsense", "shouldnotexist").unwrap();
        match Zones::delete_address(None, &addr).await {
            Ok(()) => (),
            Err(err) => panic!(
                "unexpected error deleting nonexistent address: {}",
                slog_error_chain::InlineErrorChain::new(&err)
            ),
        }
    }
}
