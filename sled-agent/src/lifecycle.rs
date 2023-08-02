// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO explanatory comment

#![allow(dead_code)]
// TODO remove

// `usdt::Error` is larger than clippy's "large err" threshold. Remove this
// allow if that changes (https://github.com/oxidecomputer/usdt/issues/133).
#![allow(clippy::result_large_err)]

use crate::bootstrap::maghemite;
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::config::Config;
use crate::config::ConfigError;
use camino::Utf8PathBuf;
use cancel_safe_futures::TryStreamExt;
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
use futures::stream::{self, StreamExt};
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::Etherstub;
use illumos_utils::dladm::EtherstubVnic;
use illumos_utils::dladm::PhysicalLink;
use illumos_utils::zfs;
use illumos_utils::zfs::Zfs;
use illumos_utils::zone;
use illumos_utils::zone::Zones;
use key_manager::{KeyManager, StorageKeyRequester};
use omicron_common::address::Ipv6Subnet;
use omicron_common::FileKv;
use sled_hardware::underlay;
use slog::Drain;
use slog::Logger;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use thiserror::Error;
use tokio::task::JoinHandle;

/// Describes errors which may occur while starting sled-agent.
///
/// All of these errors are fatal.
#[derive(Error, Debug)]
pub enum StartError {
    #[error("Failed to initialize logger")]
    InitLogger(#[source] io::Error),

    #[error("Failed to register DTrace probes")]
    RegisterDTraceProbes(#[source] usdt::Error),

    #[error("Failed to find address objects for maghemite")]
    FindMaghemiteAddrObjs(#[source] underlay::Error),

    #[error("underlay::find_nics() returned 0 address objects")]
    NoUnderlayAddrObjs,

    #[error("Failed to enable mg-ddm")]
    EnableMgDdm(#[from] maghemite::Error),

    #[error("Failed to create zfs key directory {dir:?}")]
    CreateZfsKeyDirectory {
        dir: &'static str,
        #[source]
        err: io::Error,
    },

    // TODO-completeness This error variant should go away (or change) when we
    // start using the IPCC-provided MAC address for the bootstrap network
    // (https://github.com/oxidecomputer/omicron/issues/2301), at least on real
    // gimlets. Maybe it stays around for non-gimlets?
    #[error("Failed to find link for bootstrap address generation")]
    ConfigLink(#[source] ConfigError),

    #[error("Failed to get MAC address of bootstrap link")]
    BootstrapLinkMac(#[source] dladm::GetMacError),

    #[error("Failed to ensure existence of etherstub {name:?}")]
    EnsureEtherstubError {
        name: &'static str,
        #[source]
        err: illumos_utils::ExecutionError,
    },

    #[error(transparent)]
    CreateVnicError(#[from] dladm::CreateVnicError),

    #[error(transparent)]
    EnsureGzAddressError(#[from] zone::EnsureGzAddressError),

    #[error(transparent)]
    GetAddressError(#[from] zone::GetAddressError),

    #[error("Failed to create DDM admin localhost client")]
    CreateDdmAdminLocalhostClient(#[source] DdmError),

    #[error("Failed to create ZFS ramdisk dataset")]
    EnsureZfsRamdiskDataset(#[source] zfs::EnsureFilesystemError),

    #[error("Failed to list zones")]
    ListZones(#[source] zone::AdmError),

    #[error("Failed to delete zone")]
    DeleteZone(#[source] zone::AdmError),

    #[error("Failed to delete omicron VNICs")]
    DeleteOmicronVnics(#[source] anyhow::Error),

    #[error("Failed to delete all XDE devices")]
    DeleteXdeDevices(#[source] illumos_utils::opte::Error),

    #[error("Failed to enable ipv6-forwarding")]
    EnableIpv6Forwarding(#[from] illumos_utils::ExecutionError),
}

pub struct SledAgent {}

impl SledAgent {
    pub async fn start(config: Config) -> Result<Self, StartError> {
        let base_log = build_logger(&config)?;

        let log = base_log.new(o!("component" => "SledAgent::start"));

        // Perform several blocking startup tasks first; we move `config` and
        // `log` into this task, and on success, it gives them back to us.
        let (config, log, startup_networking) =
            tokio::task::spawn_blocking(move || {
                enable_mg_ddm(&config, &log)?;
                ensure_zfs_key_directory_exists(&log)?;

                let startup_networking = StartupNetworking::setup(&config)?;

                // Start trying to notify ddmd of our bootstrap address so it can
                // advertise it to other sleds.
                let ddmd_client = DdmAdminClient::localhost(&log)
                    .map_err(StartError::CreateDdmAdminLocalhostClient)?;
                ddmd_client.advertise_prefix(Ipv6Subnet::new(
                    startup_networking.global_zone_bootstrap_ip,
                ));

                // Before we create the switch zone, we need to ensure that the
                // necessary ZFS and Zone resources are ready. All other zones are
                // created on U.2 drives.
                ensure_zfs_ramdisk_dataset()?;

                Ok::<_, StartError>((config, log, startup_networking))
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
        enable_ipv6_forwarding().await?;

        // Spawn the `KeyManager` which is needed by the the StorageManager to
        // retrieve encryption keys.
        let (storage_key_requester, key_manager_handle) =
            spawn_key_manager_task(&base_log);

        // Begin monitoring for hardware to handle tasks like initialization of
        // the switch zone.
        info!(log, "SledAgent monitoring for hardware");

        todo!()
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
    fs::create_dir_all(sled_hardware::disk::KEYPATH_ROOT).map_err(|err| {
        StartError::CreateZfsKeyDirectory {
            dir: sled_hardware::disk::KEYPATH_ROOT,
            err,
        }
    })
}

struct StartupNetworking {
    link_for_mac: PhysicalLink,
    bootstrap_etherstub: Etherstub,
    global_zone_bootstrap_ip: Ipv6Addr,
    global_zone_bootstrap_link_local_ip: Ipv6Addr,
    switch_zone_bootstrap_ip: Ipv6Addr,
    underlay_etherstub: Etherstub,
    underlay_etherstub_vnic: EtherstubVnic,
}

impl StartupNetworking {
    fn setup(config: &Config) -> Result<Self, StartError> {
        let link_for_mac = config.get_link().map_err(StartError::ConfigLink)?;
        let global_zone_bootstrap_ip = underlay::BootstrapInterface::GlobalZone
            .ip(&link_for_mac)
            .map_err(StartError::BootstrapLinkMac)?;

        let bootstrap_etherstub =
            ensure_etherstub(dladm::BOOTSTRAP_ETHERSTUB_NAME)?;
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

        let underlay_etherstub =
            ensure_etherstub(illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME)?;
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
}

fn ensure_etherstub(name: &'static str) -> Result<Etherstub, StartError> {
    Dladm::ensure_etherstub(name)
        .map_err(|err| StartError::EnsureEtherstubError { name, err })
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
