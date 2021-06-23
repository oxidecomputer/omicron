//! API for interacting with Zones.

use ipnet::IpNet;
use omicron_common::error::ApiError;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use slog::Logger;
use uuid::Uuid;

const PFEXEC: &str = "/usr/bin/pfexec";

const BASE_ZONE: &str = "propolis_base";
const PROPOLIS_SVC_DIRECTORY: &str = "/opt/oxide/propolis-server";

const ZONE_ZFS_POOL_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_POOL: &str = "rpool/zone";

// Helper function for starting the process and checking the
// exit code result.
fn execute(command: &mut std::process::Command) -> Result<std::process::Output, ApiError> {
    let output = command
        .output()
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to execute {:?}: {}", command, e),
            }
        })?;

    if !output.status.success() {
        return Err(ApiError::InternalError {
            message: format!(
                "Command {:?} executed and failed: {}",
                command,
                String::from_utf8_lossy(&output.stderr)
            ),
        });
    }

    Ok(output)
}

pub fn ensure_zpool_exists(name: &str) -> Result<(), ApiError> {
    // If the zpool exists, we're done.
    let mut command = std::process::Command::new("zfs");
    let cmd = command.args(&["list", name]);
    if execute(cmd).is_ok() {
        return Ok(());
    }

    // If it doesn't exist, make it.
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command.args(&["create", "-o", &format!("mountpoint={}", ZONE_ZFS_POOL_MOUNTPOINT), name]);
    execute(cmd)?;
    Ok(())
}

fn get_zone(name: &str) -> Result<Option<zone::Zone>, ApiError> {
    Ok(zone::Adm::list().map_err(|e| {
        ApiError::InternalError {
            message: format!("Cannot list zones: {}", e),
        }
    })?.into_iter().find(|zone| {
        zone.name() == name
    }))
}

fn remove_zone(name: &str) -> Result<(), ApiError> {
    zone::Adm::new(name).uninstall(/* force= */ true).map_err(|e| {
        ApiError::InternalError {
            message: format!("Cannot uninstall {}: {}", name, e)
        }
    })?;
    zone::Config::new(name).delete(/* force= */ true).run().map_err(|e| {
        ApiError::InternalError {
            message: format!("Cannot delete {}: {}", name, e)
        }
    })?;
    Ok(())
}

pub fn find_physical_data_link() -> Result<String, ApiError> {
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command.args(&["dladm", "show-phys", "-o", "LINK"]);
    let output = execute(cmd)?;
    Ok(
        String::from_utf8(output.stdout)
            .map_err(|e| {
                ApiError::InternalError {
                    message: format!("Cannot parse dladm output as UTF-8: {}", e),
                }
            })?
            .lines()
            .filter(|s| *s != "LINK")
            // TODO: This is arbitrary, but we're currently grabbing the first
            // physical device. Should we have a more sophisticated method for
            // selection?
            .next()
            .ok_or_else(|| {
                ApiError::InternalError {
                    message: format!("No physical devices found"),
                }
            })?
            .to_string()
    )
}

pub fn create_vnic(physical: &str, vnic_name: &str) -> Result<(), ApiError> {
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command.args(&["dladm", "create-vnic", "-l", physical, vnic_name]);
    execute(cmd)?;
    Ok(())
}

/// Creates a "base" zone for Propolis, from which other Propolis
/// zones may quickly be cloned.
pub fn create_base_zone(log: &Logger) -> Result<(), ApiError> {
    let name = BASE_ZONE;

    info!(log, "Querying for prescence of zone: {}", name);
    if let Some(zone) = get_zone(name)? {
        info!(log, "Found zone: {} in state {:?}", zone.name(), zone.state());
        if zone.state() == zone::State::Installed {
            // TODO: Admittedly, the zone might be messed up. However, for
            // now, we assume that "installed" means "good to go".
            return Ok(());
        } else {
            info!(log, "Invalid state; uninstalling and deleting zone");
            remove_zone(zone.name())?;
        }
    }

    info!(log, "Creating new base zone: {}", name);
    let mut cfg = zone::Config::create(
        name,
        /* overwrite= */ true,
        zone::CreationOptions::Template("sparse".to_string())
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
    cfg.run().map_err(|e| {
        ApiError::InternalError {
            message: format!("Failed to create base zone: {}", e),
        }
    })?;

    // TODO: This process takes a little while...
    info!(log, "Installing base zone: {}", name);
    zone::Adm::new(name)
        .install(&[])
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to install base zone: {}", e),
            }
        })?;

    Ok(())
}

/// Sets the configuration for a Propolis zone.
///
/// This zone will be cloned as a child of the "base propolis zone".
pub fn configure_child_zone(log: &Logger, name: &str, vnic: &str) -> Result<(), ApiError> {
    info!(log, "Creating child zone: {}", name);
    let mut cfg = zone::Config::create(
        name,
        /* overwrite= */ true,
        zone::CreationOptions::Template("sparse".to_string())
    );
    cfg.get_global()
        .set_path(format!("{}/{}", ZONE_ZFS_POOL_MOUNTPOINT, name))
        .set_autoboot(false)
        .set_ip_type(zone::IpType::Exclusive);
    cfg.add_fs(&zone::Fs {
        ty: "lofs".to_string(),
        dir: PROPOLIS_SVC_DIRECTORY.to_string(),
        special: PROPOLIS_SVC_DIRECTORY.to_string(),
        // TODO: read-only! Without this, we're failing to mount the block
        // device.
        options: vec!["rw".to_string()],
//        options: vec!["ro".to_string()],
        ..Default::default()
    });
    cfg.add_net(&zone::Net {
        physical: vnic.to_string(),
        ..Default::default()
    });
    cfg.add_device(&zone::Device {
        name: "/dev/vmm/*".to_string(),
    });
    cfg.add_device(&zone::Device {
        name: "/dev/vmmctl".to_string(),
    });
    cfg.add_device(&zone::Device {
        name: "/dev/viona".to_string(),
    });
    cfg.run().map_err(|e| {
        ApiError::InternalError {
            message: format!("Failed to create child zone: {}", e),
        }
    })?;

    Ok(())
}

/// Clones a zone (named `name`) from the base Propolis zone.
pub fn clone_zone_from_base(name: &str) -> Result<(), ApiError> {
    zone::Adm::new(name)
        .clone(BASE_ZONE)
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to clone zone: {}", e),
            }
        })?;
    Ok(())
}

/// Boots a zone (names `name`).
pub fn boot_zone(name: &str) -> Result<(), ApiError> {
    zone::Adm::new(name)
        .boot()
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to boot zone: {}", e),
            }
        })?;
    Ok(())
}

/// Returns the default gateway accessible to the calling zone.
// TODO: We could use this, invoking:
//      $ route add default <result of this function>
// In the non-GZ to give it a connection to the internet, if we want.
pub fn get_default_gateway() -> Result<IpAddr, ApiError> {
    let mut command = std::process::Command::new("route");
    let cmd = command.args(&["-n", "get", "default"]);
    let output = execute(cmd)?;

    let addr = String::from_utf8(output.stdout)
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to parse UTF-8 from route output: {}", e),
            }
        })?
        .lines()
        .find_map(|s| {
            Some(s.trim().strip_prefix("gateway:")?.trim().to_string())
        })
        .ok_or_else(|| {
            ApiError::InternalError {
                message: format!("Route command succeeded, but did not contain gateway"),
            }
        })?;

    IpAddr::from_str(&addr)
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to parse IP address from output: {}", e),
            }
        })
}

/// Returns the IP address (plus subnet mask) of a physical device.
pub fn get_ip_address(phys: &str) -> Result<IpNet, ApiError> {
    let mut command = std::process::Command::new("ipadm");
    let cmd = command.args(&["show-addr", &format!("{}/", phys), "-p", "-o", "ADDR"]);
    let output = execute(cmd)?;
    let out = String::from_utf8(output.stdout)
        .map_err(|e| {
            ApiError::InternalError {
                message: format!("Failed to parse UTF-8 from route output: {}", e),
            }
        })?
        .trim()
        .to_string();

    println!("Output from ipadm: {}", out);
    Ok(
        out
            .parse()
            .map_err(|e| {
                ApiError::InternalError {
                    message: format!("Failed to parse ipadm output as IP address: {}", e),
                }
            })?
    )
}

/// Creates a static IP address within a Zone.
// XXX Example: ipadm create-addr -t -T static -a 192.168.1.5/24 vnic_prop0/v4
pub fn create_address(zone: &str, addr: &IpAddr, interface: &str) -> Result<(), ApiError> {
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command.args(&[
        "zlogin", zone, "ipadm", "create-addr", "-t", "-T", "static", "-a", &addr.to_string(), interface
    ]);
    execute(cmd)?;
    Ok(())
}

// TODO: Could we launch the SMF service, with a dependency on networking?
pub fn run_propolis(zone: &str, id: &Uuid, addr: &SocketAddr) -> Result<(), ApiError> {
    // svccfg import /opt/oxide/propolis-server/pkg/manifest.xml
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command
        .args(&[
            "zlogin",
            zone,
            "svccfg",
            "import",
            "/opt/oxide/propolis-server/pkg/manifest.xml",
        ]);
    execute(cmd)?;

    // svccfg -s system/illumos/propolis-server setprop config/server_addr=192.168.1.5:12400
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command
        .args(&[
            "zlogin",
            zone,
            "svccfg",
            "-s",
            "system/illumos/propolis-server",
            "setprop",
            &format!("config/server_addr={}", addr),
        ]);
    execute(cmd)?;

    // svccfg -s svc:/system/illumos/propolis-server add vm-<ID>
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command
        .args(&[
            "zlogin",
            zone,
            "svccfg",
            "-s",
            "svc:/system/illumos/propolis-server",
            "add",
            &format!("vm-{}", id),
        ]);
    execute(cmd)?;

    // svcadm enable -st svc:/system/illumos/propolis-server:vm-<ID>
    let mut command = std::process::Command::new(PFEXEC);
    let cmd = command
        .args(&[
            "zlogin",
            zone,
            "svcadm",
            "enable",
            "-st",
            &format!("svc:/system/illumos/propolis-server:vm-{}", id),
        ]);
    execute(cmd)?;

    Ok(())
}
