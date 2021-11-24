//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

//! API for interacting with Zones running Propolis.

use ipnetwork::IpNetwork;
use omicron_common::api::external::Error;
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

use crate::illumos::zfs::ZONE_ZFS_DATASET_MOUNTPOINT;
use crate::illumos::{execute, PFEXEC};

const BASE_ZONE: &str = "propolis_base";
const PROPOLIS_SVC_DIRECTORY: &str = "/opt/oxide/propolis-server";

const IPADM: &str = "/usr/sbin/ipadm";
const SVCADM: &str = "/usr/sbin/svcadm";
const SVCCFG: &str = "/usr/sbin/svccfg";
const ZLOGIN: &str = "/usr/sbin/zlogin";

pub const ZONE_PREFIX: &str = "propolis_instance_";

fn get_zone(name: &str) -> Result<Option<zone::Zone>, Error> {
    Ok(zone::Adm::list()
        .map_err(|e| Error::InternalError {
            internal_message: format!("Cannot list zones: {}", e),
        })?
        .into_iter()
        .find(|zone| zone.name() == name))
}

/// Wraps commands for interacting with Zones.
pub struct Zones {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zones {
    /// Ensures a zone is halted before both uninstalling and deleting it.
    pub fn halt_and_remove(log: &Logger, name: &str) -> Result<(), Error> {
        if let Some(zone) = get_zone(name)? {
            info!(log, "halt_and_remove: Zone state: {:?}", zone.state());
            if zone.state() == zone::State::Running {
                zone::Adm::new(name).halt().map_err(|e| {
                    Error::InternalError {
                        internal_message: format!(
                            "Cannot halt zone {}: {}",
                            name, e
                        ),
                    }
                })?;
            }
            zone::Adm::new(name).uninstall(/* force= */ true).map_err(|e| {
                Error::InternalError {
                    internal_message: format!(
                        "Cannot uninstall {}: {}",
                        name, e
                    ),
                }
            })?;
            zone::Config::new(name).delete(/* force= */ true).run().map_err(
                |e| Error::InternalError {
                    internal_message: format!("Cannot delete {}: {}", name, e),
                },
            )?;
        }
        Ok(())
    }

    /// Seed the SMF files within a zone.
    ///
    /// This is a performance optimization, which manages to shave
    /// several seconds off of zone boot time.
    ///
    /// Background:
    /// - https://illumos.org/man/5/smf_bootstrap
    ///
    /// When a zone starts, a service called `manifest-import` uses
    /// `svccfg` to import XML-based config files into a SQLite DB.
    /// This database - stored at `/etc/svc/repository.db` - is used
    /// by SMF as a representation of known services.
    ///
    /// This process is, unfortunately, SLOW. It involves serially
    /// parsing a many XML files from storage, and loading them into
    /// this database file. The invocation below is effectively
    /// what would happen when the zone is first booted.
    ///
    /// By doing this operation for "base zones", first-time setup takes
    /// slightly longer, but all subsequent zones boot much faster.
    ///
    /// NOTE: This process could be optimized further by creating
    /// this file as part of the image building process - see
    /// `seed_smf` within https://github.com/illumos/image-builder.
    pub fn seed_smf(
        log: &Logger,
        mountpoint: &std::path::Path,
    ) -> Result<(), Error> {
        let tmpdir = tempfile::tempdir().map_err(|e| {
            Error::internal_error(&format!("Tempdir err: {}", e))
        })?;
        let mountpoint = mountpoint.to_str().unwrap();

        let repo = format!("{}/repo.db", tmpdir.as_ref().to_string_lossy());
        let seed = format!("{}/lib/svc/seed/{}.db", mountpoint, "nonglobal");
        let manifests = format!("{}/lib/svc/manifest", mountpoint);
        let installto = format!("{}/etc/svc/repository.db", mountpoint);

        std::fs::copy(&seed, &repo).map_err(|e| {
            Error::internal_error(&format!(
                "Cannot copy seed DB to tempdir: {}",
                e
            ))
        })?;

        let mut env = std::collections::HashMap::new();
        let dtd = "/usr/share/lib/xml/dtd/service_bundle.dtd.1".to_string();
        env.insert("SVCCFG_DTD".to_string(), dtd);
        env.insert("SVCCFG_REPOSITORY".to_string(), repo.to_string());
        env.insert("SVCCFG_CHECKHASH".to_string(), "1".to_string());
        env.insert("PKG_INSTALL_ROOT".to_string(), mountpoint.to_string());

        info!(log, "Seeding SMF repository at {}", mountpoint);
        let mut cmd = std::process::Command::new(PFEXEC);
        let command = cmd.envs(env).args(&[SVCCFG, "import", &manifests]);
        execute(command)?;
        info!(log, "Seeding SMF repository at {} - Complete", mountpoint);

        std::fs::copy(&repo, &installto).map_err(|e| {
            Error::internal_error(&format!("Cannot copy SMF DB: {}", e))
        })?;

        Ok(())
    }

    /// Creates a "base" zone for Propolis, from which other Propolis
    /// zones may quickly be cloned.
    pub fn create_base(log: &Logger) -> Result<(), Error> {
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
                Zones::halt_and_remove(log, zone.name())?;
            }
        }

        info!(log, "Creating new base zone: {}", name);
        let mut cfg = zone::Config::create(
            name,
            /* overwrite= */ true,
            zone::CreationOptions::Blank,
        );
        let path = format!("{}/{}", ZONE_ZFS_DATASET_MOUNTPOINT, name);
        cfg.get_global()
            .set_brand("sparse")
            .set_path(&path)
            .set_autoboot(false)
            .set_ip_type(zone::IpType::Exclusive);
        cfg.add_fs(&zone::Fs {
            ty: "lofs".to_string(),
            dir: PROPOLIS_SVC_DIRECTORY.to_string(),
            special: PROPOLIS_SVC_DIRECTORY.to_string(),
            options: vec!["ro".to_string()],
            ..Default::default()
        });
        cfg.run().map_err(|e| Error::InternalError {
            internal_message: format!("Failed to create base zone: {}", e),
        })?;
        // TODO: This process takes a little while... Consider optimizing.
        info!(log, "Installing base zone: {}", name);
        zone::Adm::new(name).install(&[]).map_err(|e| {
            Error::InternalError {
                internal_message: format!("Failed to install base zone: {}", e),
            }
        })?;

        info!(log, "Seeding base zone: {}", name);
        let root = format!("{}/{}", path, "root");
        Self::seed_smf(&log, std::path::Path::new(&root))?;

        Ok(())
    }

    /// Sets the configuration for a Propolis zone.
    ///
    /// This zone will be cloned as a child of the "base propolis zone".
    pub fn configure_child_zone(
        log: &Logger,
        name: &str,
        vnics: Vec<String>,
    ) -> Result<(), Error> {
        info!(log, "Creating child zone: {}", name);
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
        cfg.add_fs(&zone::Fs {
            ty: "lofs".to_string(),
            dir: PROPOLIS_SVC_DIRECTORY.to_string(),
            special: PROPOLIS_SVC_DIRECTORY.to_string(),
            options: vec!["ro".to_string()],
            ..Default::default()
        });
        for vnic in &vnics {
            cfg.add_net(&zone::Net {
                physical: vnic.to_string(),
                ..Default::default()
            });
        }
        cfg.add_device(&zone::Device { name: "/dev/vmm/*".to_string() });
        cfg.add_device(&zone::Device { name: "/dev/vmmctl".to_string() });
        cfg.add_device(&zone::Device { name: "/dev/viona".to_string() });
        cfg.run().map_err(|e| Error::InternalError {
            internal_message: format!("Failed to create child zone: {}", e),
        })?;

        Ok(())
    }

    /// Clones a zone (named `name`) from the base Propolis zone.
    pub fn clone_from_base(name: &str) -> Result<(), Error> {
        zone::Adm::new(name).clone(BASE_ZONE).map_err(|e| {
            Error::InternalError {
                internal_message: format!("Failed to clone zone: {}", e),
            }
        })?;
        Ok(())
    }

    /// Boots a zone (named `name`).
    pub fn boot(name: &str) -> Result<(), Error> {
        zone::Adm::new(name).boot().map_err(|e| Error::InternalError {
            internal_message: format!("Failed to boot zone: {}", e),
        })?;
        Ok(())
    }

    /// Returns all zones that may be managed by the Sled Agent.
    pub fn get() -> Result<Vec<zone::Zone>, Error> {
        Ok(zone::Adm::list()
            .map_err(|e| Error::InternalError {
                internal_message: format!("Failed to list zones: {}", e),
            })?
            .into_iter()
            .filter(|z| z.name().starts_with(ZONE_PREFIX))
            .collect())
    }

    /// Creates an IP address within a Zone.
    pub fn create_address(
        zone: &str,
        interface: &str,
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
            interface,
        ]);
        execute(cmd)?;

        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZLOGIN,
            zone,
            IPADM,
            "show-addr",
            "-p",
            "-o",
            "ADDR",
            interface,
        ]);
        let output = execute(cmd)?;
        String::from_utf8(output.stdout)
            .map_err(|e| Error::InternalError {
                internal_message: format!(
                    "Cannot parse ipadm output as UTF-8: {}",
                    e
                ),
            })?
            .lines()
            .find_map(|s| s.parse().ok())
            .ok_or(Error::InternalError {
                internal_message: format!(
                    "Cannot find a valid IP address on {}",
                    interface
                ),
            })
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
