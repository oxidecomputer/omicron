//! API for interacting with Zones running Propolis.

use ipnetwork::IpNetwork;
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

use crate::illumos::dladm::VNIC_PREFIX_CONTROL;
use crate::illumos::zfs::ZONE_ZFS_DATASET_MOUNTPOINT;
use crate::illumos::{execute, PFEXEC};

const PROPOLIS_BASE_ZONE: &str = "propolis_base";
const STORAGE_BASE_ZONE: &str = "storage_base";
const PROPOLIS_SVC_DIRECTORY: &str = "/opt/oxide/propolis-server";
pub const CRUCIBLE_SVC_DIRECTORY: &str = "/opt/oxide/crucible-agent";

const DLADM: &str = "/usr/sbin/dladm";
const IPADM: &str = "/usr/sbin/ipadm";
const SVCADM: &str = "/usr/sbin/svcadm";
const SVCCFG: &str = "/usr/sbin/svccfg";
const ZLOGIN: &str = "/usr/sbin/zlogin";

// TODO: These could become enums
pub const ZONE_PREFIX: &str = "oxz_";
pub const PROPOLIS_ZONE_PREFIX: &str = "oxz_propolis_instance_";
pub const CRUCIBLE_ZONE_PREFIX: &str = "oxz_crucible_instance_";
pub const COCKROACH_ZONE_PREFIX: &str = "oxz_cockroach_instance_";

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

    #[error("Value not found")]
    NotFound,
}

/// Wraps commands for interacting with Zones.
pub struct Zones {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zones {
    /// Ensures a zone is halted before both uninstalling and deleting it.
    pub fn halt_and_remove(log: &Logger, name: &str) -> Result<(), Error> {
        if let Some(zone) = Self::find(name)? {
            info!(log, "halt_and_remove: Zone state: {:?}", zone.state());
            if zone.state() == zone::State::Running {
                zone::Adm::new(name).halt().map_err(|e| Error::Halt(e))?;
            }
            zone::Adm::new(name)
                .uninstall(/* force= */ true)
                .map_err(|e| Error::Uninstall(e))?;
            zone::Config::new(name)
                .delete(/* force= */ true)
                .run()
                .map_err(|e| Error::Delete(e))?;
        }
        Ok(())
    }

    fn create_base(
        name: &str,
        log: &Logger,
        filesystems: &[zone::Fs],
        devices: &[zone::Device],
    ) -> Result<(), Error> {
        info!(log, "Querying for prescence of zone: {}", name);
        if let Some(zone) = Self::find(name)? {
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
                Zones::halt_and_remove(log, zone.name())?;
            }
        }

        info!(log, "Creating new base zone: {}", name);
        let mut cfg = zone::Config::create(
            name,
            /* overwrite= */ true,
            zone::CreationOptions::Blank,
        );
        cfg.get_global()
            .set_brand("sparse")
            .set_path(format!("{}/{}", ZONE_ZFS_DATASET_MOUNTPOINT, name))
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        for fs in filesystems {
            cfg.add_fs(&fs);
        }
        for device in devices {
            cfg.add_device(device);
        }
        cfg.run().map_err(|e| Error::Configure(e))?;

        // TODO: This process takes a little while... Consider optimizing.
        info!(log, "Installing base zone: {}", name);
        zone::Adm::new(name).install(&[]).map_err(|e| Error::Install(e))?;

        Ok(())
    }

    /// Creates a "base" zone for Propolis, from which other Propolis
    /// zones may quickly be cloned.
    pub fn create_propolis_base(log: &Logger) -> Result<(), Error> {
        Zones::create_base(
            PROPOLIS_BASE_ZONE,
            log,
            &[zone::Fs {
                ty: "lofs".to_string(),
                dir: PROPOLIS_SVC_DIRECTORY.to_string(),
                special: PROPOLIS_SVC_DIRECTORY.to_string(),
                options: vec!["ro".to_string()],
                ..Default::default()
            }],
            &[
                zone::Device { name: "/dev/vmm/*".to_string() },
                zone::Device { name: "/dev/vmmctl".to_string() },
                zone::Device { name: "/dev/viona".to_string() },
            ],
        )
    }

    /// Creates a "base" zone for storage services, from which other
    /// zones may quickly be cloned.
    pub fn create_storage_base(log: &Logger) -> Result<(), Error> {
        Zones::create_base(STORAGE_BASE_ZONE, log, &[], &[])
    }

    /// Sets the configuration for a zone.
    pub fn configure_zone(
        log: &Logger,
        name: &str,
        filesystems: &[zone::Fs],
        devices: &[zone::Device],
        vnics: Vec<String>,
    ) -> Result<(), Error> {
        info!(log, "Configuring zone: {}", name);
        let mut cfg = zone::Config::create(
            name,
            /* overwrite= */ true,
            zone::CreationOptions::Blank,
        );
        cfg.get_global()
            .set_brand("sparse")
            .set_path(format!("{}/{}", ZONE_ZFS_DATASET_MOUNTPOINT, name))
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        for fs in filesystems {
            cfg.add_fs(&fs);
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
        cfg.run().map_err(|e| Error::Configure(e))?;
        Ok(())
    }

    /// Sets the configuration for a Propolis zone.
    ///
    /// This zone will be cloned as a child of the "base propolis zone".
    pub fn configure_propolis_zone(
        log: &Logger,
        name: &str,
        vnics: Vec<String>,
    ) -> Result<(), Error> {
        Zones::configure_zone(
            log,
            name,
            &[zone::Fs {
                ty: "lofs".to_string(),
                dir: PROPOLIS_SVC_DIRECTORY.to_string(),
                special: PROPOLIS_SVC_DIRECTORY.to_string(),
                options: vec!["ro".to_string()],
                ..Default::default()
            }],
            &[
                zone::Device { name: "/dev/vmm/*".to_string() },
                zone::Device { name: "/dev/vmmctl".to_string() },
                zone::Device { name: "/dev/viona".to_string() },
            ],
            vnics,
        )
    }

    /// Clones a zone (named `name`) from the base Propolis zone.
    fn clone_from_base(name: &str, base: &str) -> Result<(), Error> {
        zone::Adm::new(name).clone(base).map_err(|e| Error::Clone(e))?;
        Ok(())
    }

    /// Clones a zone (named `name`) from the base Propolis zone.
    pub fn clone_from_base_propolis(name: &str) -> Result<(), Error> {
        Zones::clone_from_base(name, PROPOLIS_BASE_ZONE)
    }

    /// Clones a zone (named `name`) from the base Crucible zone.
    pub fn clone_from_base_storage(name: &str) -> Result<(), Error> {
        Zones::clone_from_base(name, STORAGE_BASE_ZONE)
    }

    /// Boots a zone (named `name`).
    pub fn boot(name: &str) -> Result<(), Error> {
        zone::Adm::new(name).boot().map_err(|e| Error::Boot(e))?;
        Ok(())
    }

    /// Returns all zones that may be managed by the Sled Agent.
    ///
    /// These zones must have names starting with [`ZONE_PREFIX`].
    pub fn get() -> Result<Vec<zone::Zone>, Error> {
        Ok(zone::Adm::list()
            .map_err(|e| Error::List(e))?
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
            .map_err(|e| Error::Parse(e))?
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
    pub fn ensure_address(
        zone: &str,
        addrobj: &str,
    ) -> Result<IpNetwork, Error> {
        match Zones::get_address(zone, addrobj) {
            Ok(addr) => Ok(addr),
            Err(_) => Zones::create_address(zone, addrobj),
        }
    }

    /// Gets the IP address of an interface within a Zone.
    ///
    /// TODO: Use types to distinguish "addrobj" from "interface" objects.
    pub fn get_address(zone: &str, addrobj: &str) -> Result<IpNetwork, Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            IPADM,
            "show-addr",
            "-p",
            "-o",
            "ADDR",
            addrobj,
        ]);
        let output = execute(cmd)?;
        String::from_utf8(output.stdout)?
            .lines()
            .find_map(|s| s.parse().ok())
            .ok_or(Error::NotFound)
    }

    /// Creates an IP address within a Zone.
    pub fn create_address(
        zone: &str,
        addrobj: &str,
    ) -> Result<IpNetwork, Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            IPADM,
            "create-addr",
            "-t",
            "-T",
            "dhcp",
            addrobj,
        ]);
        execute(cmd)?;
        Self::get_address(zone, addrobj)
    }

    /// Configures and initializes a Propolis server within the specified Zone.
    pub fn run_propolis(
        zone: &str,
        id: &Uuid,
        addr: &SocketAddr,
    ) -> Result<(), Error> {
        // Import the service manifest for Propolis.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            SVCCFG,
            "import",
            "/opt/oxide/propolis-server/pkg/manifest.xml",
        ]);
        execute(cmd)?;

        // Set the desired address of the Propolis server.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            SVCCFG,
            "-s",
            "system/illumos/propolis-server",
            "setprop",
            &format!("config/server_addr={}", addr),
        ]);
        execute(cmd)?;

        // Create a new Propolis service instance.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            SVCCFG,
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
            ZLOGIN,
            zone,
            SVCADM,
            "enable",
            "-t",
            &format!("svc:/system/illumos/propolis-server:vm-{}", id),
        ]);
        execute(cmd)?;

        Ok(())
    }
}
