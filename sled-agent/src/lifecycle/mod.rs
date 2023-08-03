// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO explanatory comment

#![allow(dead_code)]
// TODO remove

// `usdt::Error` is larger than clippy's "large err" threshold. Remove this
// allow if that changes (https://github.com/oxidecomputer/usdt/issues/133).
#![allow(clippy::result_large_err)]

use crate::config::Config;
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use omicron_common::api::internal::shared::RackNetworkConfig;
use sled_hardware::HardwareManager;
use sled_hardware::HardwareUpdate;
use slog::Logger;
use std::net::Ipv6Addr;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

//mod pre_bootstore;
mod startup;

//use pre_bootstore::PreBootstore;
use self::startup::BootstoreJoinHandles;
use self::startup::SledAgentSetup;
use self::startup::StartError;

// Re-export this so `ServiceManager` can use it.
pub(crate) use self::startup::StartupNetworking;

pub struct SledAgent {
    sled_agent_main_task: JoinHandle<()>,
    bootstore_tasks: BootstoreJoinHandles,
}

impl SledAgent {
    pub async fn start(config: Config) -> Result<Self, StartError> {
        // Do all initial setup - if any step of this fails, we fail to start.
        let setup = SledAgentSetup::run(config).await?;

        // From this point on we will listen for hardware notifications and
        // potentially start the switch zone and be notified of new disks.
        let mut hardware_monitor = setup.hardware_manager.monitor();

        // Wait for our boot M.2 to show up (while handling any messages from
        // `hardware_monitor`).
        setup.wait_for_boot_m2(&mut hardware_monitor).await;

        // Wait for the bootstore to start (while handling any messages from
        // `hardware_monitor`). If this fails, we fail to start.
        let bootstore_tasks =
            setup.spawn_bootstore_tasks(&mut hardware_monitor).await?;

        // Do we have a StartSledAgentRequest stored in the ledger? If we fail
        // to read the ledger at all, we fail to start. While attempting to read
        // the ledger, continue handling any messages from `hardware_monitor`.
        let maybe_ledger = setup
            .read_persistent_sled_agent_request_from_ledger(
                &mut hardware_monitor,
            )
            .await?;

        // Spawn our version of `main()`; it runs until told to exit.
        // TODO-FIXME how do we tell it to exit?
        // TODO-FIXME wait until this task tell us to return to catch other
        // startup errors?
        let sled_agent_main_task = tokio::spawn(sled_agent_main(setup));

        Ok(Self { sled_agent_main_task, bootstore_tasks })
    }
}

async fn sled_agent_main(_setup: SledAgentSetup) {
    /*
    // Unpack all the handles and managers we already set up.
    let SledAgentSetup {
        config,
        ddm_admin_localhost_client,
        base_log,
        hardware_manager,
        storage_manager,
        service_manager,
        key_manager_handle,
    } = &setup;
    let mut hardware_monitor = hardware_manager.monitor();

    // Early lifecycle: We have not yet started the bootstore, and are waiting
    // for `hardware_manager` to tell us it has found our boot M.2. It may also
    // tell us we have a switch attached, in which case we start the switch
    // zone.
    let mut pre_bootstore = PreBootstore {
        hardware_monitor: &mut hardware_monitor,
        hardware_manager,
        service_manager,
        storage_manager,
        log: base_log.new(o!("component" => "PreBootstore")),
    };
    pre_bootstore.run().await;
    */
}

async fn handle_hardware_update(
    update: Result<HardwareUpdate, broadcast::error::RecvError>,
    hardware_manager: &HardwareManager,
    service_manager: &ServiceManager,
    storage_manager: &StorageManager,
    underlay_network: Option<(Ipv6Addr, Option<&RackNetworkConfig>)>,
    log: &Logger,
) {
    match update {
        Ok(update) => match update {
            HardwareUpdate::TofinoLoaded => {
                let baseboard = hardware_manager.baseboard();
                if let Err(e) = service_manager
                    .activate_switch(underlay_network, baseboard)
                    .await
                {
                    warn!(log, "Failed to activate switch: {e}");
                }
            }
            HardwareUpdate::TofinoUnloaded => {
                if let Err(e) = service_manager.deactivate_switch().await {
                    warn!(log, "Failed to deactivate switch: {e}");
                }
            }
            HardwareUpdate::TofinoDeviceChange => {
                // TODO-correctness What should we do here?
            }
            HardwareUpdate::DiskAdded(disk) => {
                storage_manager.upsert_disk(disk).await;
            }
            HardwareUpdate::DiskRemoved(disk) => {
                storage_manager.delete_disk(disk).await;
            }
        },
        Err(broadcast::error::RecvError::Lagged(count)) => {
            warn!(log, "Hardware monitor missed {count} messages");
            update_from_current_hardware_snapshot_if_needed(
                hardware_manager,
                service_manager,
                storage_manager,
                underlay_network,
                log,
            )
            .await;
        }
        Err(broadcast::error::RecvError::Closed) => {
            // The `HardwareManager` monitoring task is an infinite loop - the
            // only way for us to get `Closed` here is if it panicked, so we
            // will propagate such a panic.
            panic!("Hardware manager monitor task panicked");
        }
    }
}

async fn update_from_current_hardware_snapshot_if_needed(
    hardware_manager: &HardwareManager,
    service_manager: &ServiceManager,
    storage_manager: &StorageManager,
    underlay_network: Option<(Ipv6Addr, Option<&RackNetworkConfig>)>,
    log: &Logger,
) {
    info!(log, "Checking current full hardware snapshot");
    if hardware_manager.is_scrimlet_driver_loaded() {
        let baseboard = hardware_manager.baseboard();
        if let Err(e) =
            service_manager.activate_switch(underlay_network, baseboard).await
        {
            warn!(log, "Failed to activate switch: {e}");
        }
    } else {
        if let Err(e) = service_manager.deactivate_switch().await {
            warn!(log, "Failed to deactivate switch: {e}");
        }
    }

    storage_manager
        .ensure_using_exactly_these_disks(hardware_manager.disks())
        .await;
}
