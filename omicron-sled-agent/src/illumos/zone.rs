//! API for interacting with Zones running Propolis.

use ipnet::IpNet;
#[cfg(test)]
use mockall::automock;
use omicron_common::error::ApiError;
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

use crate::illumos::zfs::ZONE_ZFS_POOL_MOUNTPOINT;
use crate::illumos::{execute, PFEXEC};

const BASE_ZONE: &str = "propolis_base";
const PROPOLIS_SVC_DIRECTORY: &str = "/opt/oxide/propolis-server";

pub const ZONE_PREFIX: &str = "propolis_instance_";

fn get_zone(name: &str) -> Result<Option<zone::Zone>, ApiError> {
    Ok(zone::Adm::list()
        .map_err(|e| ApiError::InternalError {
            message: format!("Cannot list zones: {}", e),
        })?
        .into_iter()
        .find(|zone| zone.name() == name))
}

/// Wraps commands for interacting with Zones.
pub struct Zones {}

#[cfg_attr(test, automock, allow(dead_code))]
impl Zones {
    /// Ensures a zone is halted before both uninstalling and deleting it.
    pub fn halt_and_remove(name: &str) -> Result<(), ApiError> {
        zone::Adm::new(name).halt().map_err(|e| ApiError::InternalError {
            message: format!("Cannot halt zone {}: {}", name, e),
        })?;
        zone::Adm::new(name).uninstall(/* force= */ true).map_err(|e| {
            ApiError::InternalError {
                message: format!("Cannot uninstall {}: {}", name, e),
            }
        })?;
        zone::Config::new(name).delete(/* force= */ true).run().map_err(
            |e| ApiError::InternalError {
                message: format!("Cannot delete {}: {}", name, e),
            },
        )?;
        Ok(())
    }

    /// Creates a "base" zone for Propolis, from which other Propolis
    /// zones may quickly be cloned.
    pub fn create_base(log: &Logger) -> Result<(), ApiError> {
        let name = BASE_ZONE;

        info!(log, "Querying for prescence of zone: {}", name);
        if let Some(zone) = get_zone(name)? {
            info!(
                log,
                "Found zone: {} in state {:?}",
                zone.name(),
                zone.state()
            );
            if zone.state() == zone::State::Installed {
                // TODO: Admittedly, the zone still might be messed up. However,
                // for now, we assume that "installed" means "good to go".
                return Ok(());
            } else {
                info!(
                    log,
                    "Invalid state; uninstalling and deleting zone {}", name
                );
                Zones::halt_and_remove(zone.name())?;
            }
        }

        info!(log, "Creating new base zone: {}", name);
        let mut cfg = zone::Config::create(
            name,
            /* overwrite= */ true,
            zone::CreationOptions::Template("sparse".to_string()),
        );
        cfg.get_global()
            .set_path(format!("{}/{}", ZONE_ZFS_POOL_MOUNTPOINT, name))
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        cfg.add_fs(&zone::Fs {
            ty: "lofs".to_string(),
            dir: PROPOLIS_SVC_DIRECTORY.to_string(),
            special: PROPOLIS_SVC_DIRECTORY.to_string(),
            options: vec!["ro".to_string()],
            ..Default::default()
        });
        cfg.run().map_err(|e| ApiError::InternalError {
            message: format!("Failed to create base zone: {}", e),
        })?;

        // TODO: This process takes a little while... Consider optimizing.
        info!(log, "Installing base zone: {}", name);
        zone::Adm::new(name).install(&[]).map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to install base zone: {}", e),
            }
        })?;

        Ok(())
    }

    /// Sets the configuration for a Propolis zone.
    ///
    /// This zone will be cloned as a child of the "base propolis zone".
    pub fn configure_child_zone(
        log: &Logger,
        name: &str,
        vnic: &str,
    ) -> Result<(), ApiError> {
        info!(log, "Creating child zone: {}", name);
        let mut cfg = zone::Config::create(
            name,
            /* overwrite= */ true,
            zone::CreationOptions::Template("sparse".to_string()),
        );
        cfg.get_global()
            .set_path(format!("{}/{}", ZONE_ZFS_POOL_MOUNTPOINT, name))
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        cfg.add_fs(&zone::Fs {
            ty: "lofs".to_string(),
            dir: PROPOLIS_SVC_DIRECTORY.to_string(),
            special: PROPOLIS_SVC_DIRECTORY.to_string(),
            // TODO: Should be read-only! However, with that setting, we're
            // failing to mount the block device.
            options: vec!["rw".to_string()],
            //        options: vec!["ro".to_string()],
            ..Default::default()
        });
        cfg.add_net(&zone::Net {
            physical: vnic.to_string(),
            ..Default::default()
        });
        cfg.add_device(&zone::Device { name: "/dev/vmm/*".to_string() });
        cfg.add_device(&zone::Device { name: "/dev/vmmctl".to_string() });
        cfg.add_device(&zone::Device { name: "/dev/viona".to_string() });
        cfg.run().map_err(|e| ApiError::InternalError {
            message: format!("Failed to create child zone: {}", e),
        })?;

        Ok(())
    }

    /// Clones a zone (named `name`) from the base Propolis zone.
    pub fn clone_from_base(name: &str) -> Result<(), ApiError> {
        zone::Adm::new(name).clone(BASE_ZONE).map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to clone zone: {}", e),
            }
        })?;
        Ok(())
    }

    /// Boots a zone (named `name`).
    pub fn boot(name: &str) -> Result<(), ApiError> {
        zone::Adm::new(name).boot().map_err(|e| ApiError::InternalError {
            message: format!("Failed to boot zone: {}", e),
        })?;
        Ok(())
    }

    /// Returns all zones that may be managed by the Sled Agent.
    pub fn get() -> Result<Vec<zone::Zone>, ApiError> {
        Ok(zone::Adm::list()
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to list zones: {}", e),
            })?
            .into_iter()
            .filter(|z| z.name().starts_with(ZONE_PREFIX))
            .collect())
    }

    /// Creates a static IP address within a Zone.
    pub fn create_address(
        zone: &str,
        interface: &str,
    ) -> Result<IpNet, ApiError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zlogin",
            zone,
            "ipadm",
            "create-addr",
            "-t",
            "-T",
            "dhcp",
            interface,
        ]);
        execute(cmd)?;

        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zlogin",
            zone,
            "ipadm",
            "show-addr",
            "-p",
            "-o",
            "ADDR",
            interface,
        ]);
        let output = execute(cmd)?;
        String::from_utf8(output.stdout)
            .map_err(|e| ApiError::InternalError {
                message: format!("Cannot parse ipadm output as UTF-8: {}", e),
            })?
            .lines()
            .find_map(|s| s.parse().ok())
            .ok_or(ApiError::InternalError {
                message: format!(
                    "Casnnot find a valid IP address on {}",
                    interface
                ),
            })
    }

    /// Configures and initializes a Propolis server within the specified Zone.
    pub fn run_propolis(
        zone: &str,
        id: &Uuid,
        addr: &SocketAddr,
    ) -> Result<(), ApiError> {
        // Import the service manifest for Propolis.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zlogin",
            zone,
            "svccfg",
            "import",
            "/opt/oxide/propolis-server/pkg/manifest.xml",
        ]);
        execute(cmd)?;

        // Set the desired address of the Propolis server.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zlogin",
            zone,
            "svccfg",
            "-s",
            "system/illumos/propolis-server",
            "setprop",
            &format!("config/server_addr={}", addr),
        ]);
        execute(cmd)?;

        // Create a new Propolis service instance.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zlogin",
            zone,
            "svccfg",
            "-s",
            "svc:/system/illumos/propolis-server",
            "add",
            &format!("vm-{}", id),
        ]);
        execute(cmd)?;

        // Turn on the server.
        //
        // TODO(https://www.illumos.org/issues/13837): Ideally, this should call
        // ".synchronous()", but it doesn't, because of an SMF bug.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zlogin",
            zone,
            "svcadm",
            "enable",
            "-t",
            &format!("svc:/system/illumos/propolis-server:vm-{}", id),
        ]);
        execute(cmd)?;

        Ok(())
    }
}
