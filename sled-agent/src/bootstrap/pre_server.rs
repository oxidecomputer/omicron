// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality required to initialize the bootstrap agent even before it
//! starts running servers on the bootstrap network.

use super::maghemite;
use super::secret_retriever::LrtqOrHardcodedSecretRetriever;
use super::server::StartError;
use crate::config::Config;
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use camino::Utf8PathBuf;
use cancel_safe_futures::TryStreamExt;
use ddm_admin_client::Client as DdmAdminClient;
use futures::stream;
use futures::StreamExt;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm;
use illumos_utils::dladm::Dladm;
use illumos_utils::zfs;
use illumos_utils::zfs::Zfs;
use illumos_utils::zone;
use illumos_utils::zone::Zones;
use key_manager::KeyManager;
use key_manager::StorageKeyRequester;
use omicron_common::address::Ipv6Subnet;
use omicron_common::FileKv;
use sled_hardware::underlay;
use sled_hardware::DendriteAsic;
use sled_hardware::HardwareManager;
use sled_hardware::SledMode;
use slog::Drain;
use slog::Logger;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use tokio::task::JoinHandle;

pub(super) struct BootstrapManagers {
    pub(super) hardware_manager: HardwareManager,
    pub(super) storage_manager: StorageManager,
    pub(super) service_manager: ServiceManager,
}

pub(super) struct BootstrapAgentStartup {
    pub(super) config: Config,
    pub(super) global_zone_bootstrap_ip: Ipv6Addr,
    pub(super) ddm_admin_localhost_client: DdmAdminClient,
    pub(super) base_log: Logger,
    pub(super) startup_log: Logger,
    pub(super) managers: BootstrapManagers,
    pub(super) key_manager_handle: JoinHandle<()>,
}

impl BootstrapAgentStartup {
    pub(super) async fn run(config: Config) -> Result<Self, StartError> {
        let base_log = build_logger(&config)?;

        let log = base_log.new(o!("component" => "BootstrapAgentStartup"));

        // Perform several blocking startup tasks first; we move `config` and
        // `log` into this task, and on success, it gives them back to us.
        let (config, log, ddm_admin_localhost_client, startup_networking) =
            tokio::task::spawn_blocking(move || {
                enable_mg_ddm(&config, &log)?;
                ensure_zfs_key_directory_exists(&log)?;

                let startup_networking = BootstrapNetworking::setup(&config)?;

                // Start trying to notify ddmd of our bootstrap address so it can
                // advertise it to other sleds.
                let ddmd_client = DdmAdminClient::localhost(&log)
                    .map_err(StartError::CreateDdmAdminLocalhostClient)?;
                ddmd_client.advertise_prefix(Ipv6Subnet::new(
                    startup_networking.global_zone_bootstrap_ip,
                ));

                // Before we create the switch zone, we need to ensure that the
                // necessary ZFS and Zone resources are ready. All other zones
                // are created on U.2 drives.
                ensure_zfs_ramdisk_dataset()?;

                Ok::<_, StartError>((
                    config,
                    log,
                    ddmd_client,
                    startup_networking,
                ))
            })
            .await
            .unwrap()?;

        // Before we start monitoring for hardware, ensure we're running from a
        // predictable state.
        //
        // This means all VNICs, zones, etc.
        cleanup_all_old_global_state(&log).await?;

        // Ipv6 forwarding must be enabled to route traffic between zones,
        // including the switch zone which we may launch below if we find we're
        // actually running on a scrimlet.
        //
        // This should be a no-op if already enabled.
        BootstrapNetworking::enable_ipv6_forwarding().await?;

        // Spawn the `KeyManager` which is needed by the the StorageManager to
        // retrieve encryption keys.
        let (storage_key_requester, key_manager_handle) =
            spawn_key_manager_task(&base_log);

        let sled_mode = sled_mode_from_config(&config)?;

        // Start monitoring hardware. This is blocking so we use
        // `spawn_blocking`; similar to above, we move some things in and (on
        // success) it gives them back.
        let (base_log, log, hardware_manager) = {
            tokio::task::spawn_blocking(move || {
                info!(
                    log, "Starting hardware monitor";
                    "sled_mode" => ?sled_mode,
                );
                let hardware_manager =
                    HardwareManager::new(&base_log, sled_mode)
                        .map_err(StartError::StartHardwareManager)?;
                Ok::<_, StartError>((base_log, log, hardware_manager))
            })
            .await
            .unwrap()?
        };

        // Create a `StorageManager` and (possibly) synthetic disks.
        let storage_manager =
            StorageManager::new(&base_log, storage_key_requester).await;
        upsert_synthetic_zpools_if_needed(&log, &storage_manager, &config)
            .await;

        let global_zone_bootstrap_ip =
            startup_networking.global_zone_bootstrap_ip;

        let service_manager = ServiceManager::new_v2(
            &base_log,
            ddm_admin_localhost_client.clone(),
            startup_networking,
            sled_mode,
            config.skip_timesync,
            config.sidecar_revision.clone(),
            config.switch_zone_maghemite_links.clone(),
            storage_manager.resources().clone(),
        );

        Ok(Self {
            config,
            global_zone_bootstrap_ip,
            ddm_admin_localhost_client,
            base_log,
            startup_log: log,
            managers: BootstrapManagers {
                hardware_manager,
                storage_manager,
                service_manager,
            },
            key_manager_handle,
        })
    }
}

fn build_logger(config: &Config) -> Result<Logger, StartError> {
    let (drain, registration) = slog_dtrace::with_drain(
        config.log.to_logger("SledAgent").map_err(StartError::InitLogger)?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));

    match registration {
        slog_dtrace::ProbeRegistration::Success => {
            debug!(log, "registered DTrace probes");
            Ok(log)
        }
        slog_dtrace::ProbeRegistration::Failed(err) => {
            Err(StartError::RegisterDTraceProbes(err))
        }
    }
}

// Deletes all state which may be left-over from a previous execution of the
// Sled Agent.
//
// This may re-establish contact in the future, and re-construct a picture of
// the expected state of each service. However, at the moment, "starting from a
// known clean slate" is easier to work with.
async fn cleanup_all_old_global_state(log: &Logger) -> Result<(), StartError> {
    // Identify all existing zones which should be managed by the Sled
    // Agent.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/725):
    // Currently, we're removing these zones. In the future, we should
    // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
    // to leave the running Zones intact).
    let zones = Zones::get().await.map_err(StartError::ListZones)?;

    stream::iter(zones)
        .zip(stream::iter(std::iter::repeat(log.clone())))
        .map(Ok::<_, zone::AdmError>)
        // Use for_each_concurrent_then_try to delete as much as possible. We
        // only return one error though -- hopefully that's enough to signal to
        // the caller that this failed.
        .for_each_concurrent_then_try(None, |(zone, log)| async move {
            warn!(log, "Deleting existing zone"; "zone_name" => zone.name());
            Zones::halt_and_remove_logged(&log, zone.name()).await
        })
        .await
        .map_err(StartError::DeleteZone)?;

    // Identify all VNICs which should be managed by the Sled Agent.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/725)
    // Currently, we're removing these VNICs. In the future, we should
    // identify if they're being used by the aforementioned existing zones,
    // and track them once more.
    //
    // This should be accessible via:
    // $ dladm show-linkprop -c -p zone -o LINK,VALUE
    //
    // Note that we don't currently delete the VNICs in any particular
    // order. That should be OK, since we're definitely deleting the guest
    // VNICs before the xde devices, which is the main constraint.
    sled_hardware::cleanup::delete_omicron_vnics(&log)
        .await
        .map_err(StartError::DeleteOmicronVnics)?;

    // Also delete any extant xde devices. These should also eventually be
    // recovered / tracked, to avoid interruption of any guests that are
    // still running. That's currently irrelevant, since we're deleting the
    // zones anyway.
    //
    // This is also tracked by
    // https://github.com/oxidecomputer/omicron/issues/725.
    illumos_utils::opte::delete_all_xde_devices(&log)
        .map_err(StartError::DeleteXdeDevices)?;

    Ok(())
}

fn enable_mg_ddm(config: &Config, log: &Logger) -> Result<(), StartError> {
    let mg_addr_objs = underlay::find_nics(&config.data_links)
        .map_err(StartError::FindMaghemiteAddrObjs)?;
    if mg_addr_objs.is_empty() {
        return Err(StartError::NoUnderlayAddrObjs);
    }

    info!(log, "Starting mg-ddm service"; "addr-objs" => ?mg_addr_objs);
    maghemite::enable_mg_ddm_service_blocking(log.clone(), mg_addr_objs)?;

    Ok(())
}

fn ensure_zfs_key_directory_exists(log: &Logger) -> Result<(), StartError> {
    // We expect this directory to exist for Key Management
    // It's purposefully in the ramdisk and files only exist long enough
    // to create and mount encrypted datasets.
    info!(
        log, "Ensuring zfs key directory exists";
        "path" => sled_hardware::disk::KEYPATH_ROOT,
    );
    std::fs::create_dir_all(sled_hardware::disk::KEYPATH_ROOT).map_err(|err| {
        StartError::CreateZfsKeyDirectory {
            dir: sled_hardware::disk::KEYPATH_ROOT,
            err,
        }
    })
}

fn ensure_zfs_ramdisk_dataset() -> Result<(), StartError> {
    let zoned = true;
    let do_format = true;
    let encryption_details = None;
    let quota = None;
    Zfs::ensure_filesystem(
        zfs::ZONE_ZFS_RAMDISK_DATASET,
        zfs::Mountpoint::Path(Utf8PathBuf::from(
            zfs::ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT,
        )),
        zoned,
        do_format,
        encryption_details,
        quota,
    )
    .map_err(StartError::EnsureZfsRamdiskDataset)
}

async fn upsert_synthetic_zpools_if_needed(
    log: &Logger,
    storage_manager: &StorageManager,
    config: &Config,
) {
    if let Some(pools) = &config.zpools {
        for pool in pools {
            info!(
                log,
                "Upserting synthetic zpool to Storage Manager: {}",
                pool.to_string()
            );
            storage_manager.upsert_synthetic_disk(pool.clone()).await;
        }
    }
}

// Combine the `sled_mode` config with the build-time switch type to determine
// the actual sled mode.
fn sled_mode_from_config(config: &Config) -> Result<SledMode, StartError> {
    use crate::config::SledMode as SledModeConfig;
    let sled_mode = match config.sled_mode {
        SledModeConfig::Auto => {
            if !cfg!(feature = "switch-asic") {
                return Err(StartError::IncorrectBuildPackaging(
                    "sled-agent was not packaged with `switch-asic`",
                ));
            }
            SledMode::Auto
        }
        SledModeConfig::Gimlet => SledMode::Gimlet,
        SledModeConfig::Scrimlet => {
            let asic = if cfg!(feature = "switch-asic") {
                DendriteAsic::TofinoAsic
            } else if cfg!(feature = "switch-stub") {
                DendriteAsic::TofinoStub
            } else if cfg!(feature = "switch-softnpu") {
                DendriteAsic::SoftNpu
            } else {
                return Err(StartError::IncorrectBuildPackaging(
                    "sled-agent configured to run on scrimlet but wasn't \
                        packaged with switch zone",
                ));
            };
            SledMode::Scrimlet { asic }
        }
    };
    Ok(sled_mode)
}

fn spawn_key_manager_task(
    log: &Logger,
) -> (StorageKeyRequester, JoinHandle<()>) {
    let secret_retriever = LrtqOrHardcodedSecretRetriever::new();
    let (mut key_manager, storage_key_requester) =
        KeyManager::new(log, secret_retriever);

    let key_manager_handle =
        tokio::spawn(async move { key_manager.run().await });

    (storage_key_requester, key_manager_handle)
}

pub(crate) struct BootstrapNetworking {
    pub(crate) link_for_mac: dladm::PhysicalLink,
    pub(crate) bootstrap_etherstub: dladm::Etherstub,
    pub(crate) global_zone_bootstrap_ip: Ipv6Addr,
    pub(crate) global_zone_bootstrap_link_local_ip: Ipv6Addr,
    pub(crate) switch_zone_bootstrap_ip: Ipv6Addr,
    pub(crate) underlay_etherstub: dladm::Etherstub,
    pub(crate) underlay_etherstub_vnic: dladm::EtherstubVnic,
}

impl BootstrapNetworking {
    fn setup(config: &Config) -> Result<Self, StartError> {
        let link_for_mac = config.get_link().map_err(StartError::ConfigLink)?;
        let global_zone_bootstrap_ip = underlay::BootstrapInterface::GlobalZone
            .ip(&link_for_mac)
            .map_err(StartError::BootstrapLinkMac)?;

        let bootstrap_etherstub = Dladm::ensure_etherstub(
            dladm::BOOTSTRAP_ETHERSTUB_NAME,
        )
        .map_err(|err| StartError::EnsureEtherstubError {
            name: dladm::BOOTSTRAP_ETHERSTUB_NAME,
            err,
        })?;
        let bootstrap_etherstub_vnic =
            Dladm::ensure_etherstub_vnic(&bootstrap_etherstub)?;

        Zones::ensure_has_global_zone_v6_address(
            bootstrap_etherstub_vnic.clone(),
            global_zone_bootstrap_ip,
            "bootstrap6",
        )?;

        let global_zone_bootstrap_link_local_address = Zones::get_address(
            None,
            // AddrObject::link_local() can only fail if the interface name is
            // malformed, but we just got it from `Dladm`, so we know it's
            // valid.
            &AddrObject::link_local(&bootstrap_etherstub_vnic.0).unwrap(),
        )?;

        // Convert the `IpNetwork` down to just the IP address.
        let global_zone_bootstrap_link_local_ip =
            match global_zone_bootstrap_link_local_address.ip() {
                IpAddr::V4(_) => {
                    unreachable!("link local bootstrap address must be ipv6")
                }
                IpAddr::V6(addr) => addr,
            };

        // TODO-correctness: Creating the underlay IP and
        // etherstub/etherstub_vnic here is _slightly_ earlier than
        // BootstrapAgent did this setup (it waited until it was about to start
        // the HardwareMonitor), but I don't anything in between now and then
        // has an effect on these steps. Need to confirm.
        let switch_zone_bootstrap_ip = underlay::BootstrapInterface::SwitchZone
            .ip(&link_for_mac)
            .map_err(StartError::BootstrapLinkMac)?;

        let underlay_etherstub = Dladm::ensure_etherstub(
            dladm::UNDERLAY_ETHERSTUB_NAME,
        )
        .map_err(|err| StartError::EnsureEtherstubError {
            name: dladm::UNDERLAY_ETHERSTUB_NAME,
            err,
        })?;
        let underlay_etherstub_vnic =
            Dladm::ensure_etherstub_vnic(&underlay_etherstub)?;

        Ok(Self {
            link_for_mac,
            bootstrap_etherstub,
            global_zone_bootstrap_ip,
            global_zone_bootstrap_link_local_ip,
            switch_zone_bootstrap_ip,
            underlay_etherstub,
            underlay_etherstub_vnic,
        })
    }

    async fn enable_ipv6_forwarding() -> Result<(), StartError> {
        tokio::task::spawn_blocking(|| {
            let mut command = std::process::Command::new(illumos_utils::PFEXEC);
            let cmd = command.args(&[
                "/usr/sbin/routeadm",
                // Needed to access all zones, which are on the underlay.
                "-e",
                "ipv6-forwarding",
                "-u",
            ]);
            illumos_utils::execute(cmd)
                .map(|_output| ())
                .map_err(StartError::EnableIpv6Forwarding)
        })
        .await
        .unwrap()
    }
}
