// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functionality required to initialize the bootstrap agent even before it
//! starts running servers on the bootstrap network.

// Clippy doesn't like `StartError` due to
// https://github.com/oxidecomputer/usdt/issues/133; remove this once that issue
// is addressed.
#![allow(clippy::result_large_err)]

use super::maghemite;
use super::server::StartError;
use crate::config::Config;
use crate::config::SidecarRevision;
use crate::long_running_tasks::{
    spawn_all_longrunning_tasks, LongRunningTaskHandles,
};
use crate::services::ServiceManager;
use crate::services::TimeSyncConfig;
use crate::sled_agent::SledAgent;
use crate::storage_monitor::UnderlayAccess;
use camino::Utf8PathBuf;
use cancel_safe_futures::TryStreamExt;
use futures::stream;
use futures::StreamExt;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm;
use illumos_utils::dladm::Dladm;
use illumos_utils::zfs;
use illumos_utils::zfs::Zfs;
use illumos_utils::zone;
use illumos_utils::zone::Zones;
use omicron_common::address::Ipv6Subnet;
use omicron_common::FileKv;
use omicron_ddm_admin_client::Client as DdmAdminClient;
use sled_hardware::underlay;
use sled_hardware::DendriteAsic;
use sled_hardware::SledMode;
use sled_hardware_types::underlay::BootstrapInterface;
use slog::Drain;
use slog::Logger;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use tokio::sync::oneshot;

pub(super) struct BootstrapAgentStartup {
    pub(super) config: Config,
    pub(super) global_zone_bootstrap_ip: Ipv6Addr,
    pub(super) ddm_admin_localhost_client: DdmAdminClient,
    pub(super) base_log: Logger,
    pub(super) startup_log: Logger,
    pub(super) service_manager: ServiceManager,
    pub(super) long_running_task_handles: LongRunningTaskHandles,
    pub(super) sled_agent_started_tx: oneshot::Sender<SledAgent>,
    pub(super) underlay_available_tx: oneshot::Sender<UnderlayAccess>,
}

impl BootstrapAgentStartup {
    pub(super) async fn run(config: Config) -> Result<Self, StartError> {
        let base_log = build_logger(&config)?;

        // Ensure we have a thread that automatically reaps process contracts
        // when they become empty. See the comments in
        // illumos-utils/src/running_zone.rs for more detail.
        //
        // We're going to start monitoring for hardware below, which could
        // trigger launching the switch zone, and we need the contract reaper to
        // exist before entering any zones.
        illumos_utils::running_zone::ensure_contract_reaper(&base_log);

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

        // Are we a gimlet or scrimlet?
        let sled_mode = sled_mode_from_config(&config)?;

        // Spawn all important long running tasks that live for the lifetime of
        // the process and are used by both the bootstrap agent and sled agent
        let (
            long_running_task_handles,
            sled_agent_started_tx,
            service_manager_ready_tx,
            underlay_available_tx,
        ) = spawn_all_longrunning_tasks(
            &base_log,
            sled_mode,
            startup_networking.global_zone_bootstrap_ip,
            &config,
        )
        .await;

        let global_zone_bootstrap_ip =
            startup_networking.global_zone_bootstrap_ip;

        let time_sync = if let Some(true) = config.skip_timesync {
            TimeSyncConfig::Skip
        } else {
            TimeSyncConfig::Normal
        };

        let service_manager = ServiceManager::new(
            &base_log,
            ddm_admin_localhost_client.clone(),
            startup_networking,
            sled_mode,
            time_sync,
            config.sidecar_revision.clone(),
            config.switch_zone_maghemite_links.clone(),
            long_running_task_handles.storage_manager.clone(),
            long_running_task_handles.zone_bundler.clone(),
        );

        // Inform the hardware monitor that the service manager is ready
        // This is a onetime operation, and so we use a oneshot channel
        service_manager_ready_tx
            .send(service_manager.clone())
            .map_err(|_| ())
            .expect("Failed to send to StorageMonitor");

        Ok(Self {
            config,
            global_zone_bootstrap_ip,
            ddm_admin_localhost_client,
            base_log,
            startup_log: log,
            service_manager,
            long_running_task_handles,
            sled_agent_started_tx,
            underlay_available_tx,
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
    info!(log, "finding links {:?}", config.data_links);
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
        "path" => zfs::KEYPATH_ROOT,
    );
    std::fs::create_dir_all(zfs::KEYPATH_ROOT).map_err(|err| {
        StartError::CreateZfsKeyDirectory { dir: zfs::KEYPATH_ROOT, err }
    })
}

fn ensure_zfs_ramdisk_dataset() -> Result<(), StartError> {
    let zoned = false;
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
        None,
    )
    .map_err(StartError::EnsureZfsRamdiskDataset)
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
                match config.sidecar_revision {
                    SidecarRevision::SoftZone(_) => DendriteAsic::SoftNpuZone,
                    SidecarRevision::SoftPropolis(_) => {
                        DendriteAsic::SoftNpuPropolisDevice
                    }
                    _ => return Err(StartError::IncorrectBuildPackaging(
                        "sled-agent configured to run on softnpu zone but dosen't \
                         have a softnpu sidecar revision",
                    )),
                }
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

#[derive(Debug, Clone)]
pub(crate) struct BootstrapNetworking {
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
        let global_zone_bootstrap_ip = BootstrapInterface::GlobalZone
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

        let switch_zone_bootstrap_ip = BootstrapInterface::SwitchZone
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
