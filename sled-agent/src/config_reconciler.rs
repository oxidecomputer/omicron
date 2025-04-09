// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler task to ensure the sled is configured to match the
//! most-recently-received `OmicronSledConfig` from Nexus.

// TODO-john remove
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures::future;
use futures::future::Either;
use id_map::Entry;
use id_map::IdMap;
use id_map::IdMappable as _;
use illumos_utils::dladm::EtherstubVnic;
use illumos_utils::zpool::PathInPool;
use illumos_utils::zpool::Zpool;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::DiskIdentity;
use sled_storage::config::MountConfig;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

mod datasets;
mod external_disks;
mod internal_disks;
mod ledger;
mod raw_disks;
mod zones;

use crate::metrics::MetricsRequestQueue;
use crate::services::ServiceManager;
use crate::services::TimeSyncConfig;

use self::datasets::DatasetMap;
use self::external_disks::ExternalDiskMap;
use self::internal_disks::InternalDisksTask;
use self::ledger::LedgerTask;
use self::ledger::LedgerTaskHandle;
use self::zones::ZoneMap;

pub(crate) use self::datasets::DatasetTask;
pub(crate) use self::datasets::DatasetTaskError;
pub(crate) use self::datasets::DatasetTaskHandle;
pub(crate) use self::internal_disks::InternalDisksReceiver;
pub(crate) use self::ledger::LedgerTaskError;
pub(crate) use self::zones::TimeSyncStatus;

#[derive(Debug)]
pub struct RawDisksSender {
    tx: watch::Sender<IdMap<RawDisk>>,
}

impl RawDisksSender {
    pub fn set_raw_disks<I>(&self, raw_disks: I)
    where
        I: Iterator<Item = RawDisk>,
    {
        let new_raw_disks = raw_disks.collect::<IdMap<_>>();
        self.tx.send_if_modified(|prev_raw_disks| {
            if *prev_raw_disks == new_raw_disks {
                false
            } else {
                *prev_raw_disks = new_raw_disks;
                true
            }
        });
    }

    pub fn add_or_update_raw_disk(&self, raw_disk: RawDisk) {
        self.tx.send_if_modified(|raw_disks| {
            match raw_disks.entry(raw_disk.id()) {
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(raw_disk);
                    true
                }
                Entry::Occupied(mut occupied_entry) => {
                    if *occupied_entry.get() == raw_disk {
                        false
                    } else {
                        occupied_entry.insert(raw_disk);
                        true
                    }
                }
            }
        });
    }

    pub fn remove_raw_disk(&self, identity: &DiskIdentity) {
        self.tx
            .send_if_modified(|raw_disks| raw_disks.remove(identity).is_some());
    }
}

#[derive(Debug, Clone)]
pub struct ReconcilerStateReceiver {
    rx: watch::Receiver<Arc<ReconcilerTaskState>>,
}

impl ReconcilerStateReceiver {
    pub(crate) fn current(&self) -> Arc<ReconcilerTaskState> {
        Arc::clone(&*self.rx.borrow())
    }

    pub(crate) fn current_and_update(&mut self) -> Arc<ReconcilerTaskState> {
        Arc::clone(&*self.rx.borrow_and_update())
    }

    pub(crate) async fn changed(&mut self) -> Result<(), RecvError> {
        self.rx.changed().await
    }
}

#[derive(Debug)]
pub struct ConfigReconcilerHandle {
    raw_disks_rx: watch::Receiver<IdMap<RawDisk>>,
    reconciler_state_rx: watch::Receiver<Arc<ReconcilerTaskState>>,
    internal_disks_rx: InternalDisksReceiver,
    current_config_rx: watch::Receiver<CurrentConfig>,
    dataset_task_handle: DatasetTaskHandle,
    ledger_task: LedgerTaskHandle,
    hold_while_waiting_for_sled_agent:
        Mutex<Option<ReconcilerTaskDependenciesHeldUntilSledAgentStarted>>,
    log: Logger,
}

#[derive(Debug)]
struct ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
    reconciler_state_tx: watch::Sender<Arc<ReconcilerTaskState>>,
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    log: Logger,
}

impl ConfigReconcilerHandle {
    pub(crate) fn new(
        key_requester: StorageKeyRequester,
        dataset_task_handle: DatasetTaskHandle,
        time_sync_config: TimeSyncConfig,
        base_log: &Logger,
    ) -> (Self, RawDisksSender) {
        let mount_config = Arc::clone(dataset_task_handle.mount_config());
        let (raw_disks_tx, raw_disks_rx) = watch::channel(IdMap::new());
        let internal_disks_rx = InternalDisksTask::spawn(
            Arc::clone(&mount_config),
            raw_disks_rx.clone(),
            base_log.new(slog::o!("component" => "InternalDisksTask")),
        );

        let (ledger_task, current_config_rx) = LedgerTask::spawn(
            internal_disks_rx.clone(),
            base_log.new(slog::o!("component" => "LedgerTask")),
        );

        let (reconciler_state_tx, reconciler_state_rx) =
            watch::channel(Arc::new(ReconcilerTaskState::new(mount_config)));

        let hold_while_waiting_for_sled_agent = Mutex::new(Some(
            ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
                reconciler_state_tx,
                key_requester,
                time_sync_config,
                log: base_log.new(slog::o!("component" => "ReconcilerTask")),
            },
        ));

        let log =
            base_log.new(slog::o!("component" => "ConfigReconcilerHandle"));

        (
            Self {
                raw_disks_rx,
                reconciler_state_rx,
                internal_disks_rx,
                current_config_rx,
                dataset_task_handle,
                ledger_task,
                hold_while_waiting_for_sled_agent,
                log,
            },
            RawDisksSender { tx: raw_disks_tx },
        )
    }

    pub(crate) fn internal_disks_rx(&self) -> &InternalDisksReceiver {
        &self.internal_disks_rx
    }

    pub(crate) fn reconciler_state_rx(&self) -> ReconcilerStateReceiver {
        ReconcilerStateReceiver { rx: self.reconciler_state_rx.clone() }
    }

    pub(crate) fn spawn_reconciliation_task(
        &self,
        service_manager: ServiceManager,
        metrics_queue: MetricsRequestQueue,
        underlay_vnic: EtherstubVnic,
    ) {
        let deps =
            match self.hold_while_waiting_for_sled_agent.lock().unwrap().take()
            {
                Some(deps) => deps,
                None => {
                    // We should only be called once in the startup path; if
                    // we're called multiple times, ignore all but the first
                    // call.
                    warn!(
                        self.log,
                        "spawn_reconciliation_task() called multiple times \
                         (ignored after first call)"
                    );
                    return;
                }
            };
        let ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
            reconciler_state_tx,
            key_requester,
            time_sync_config,
            log,
        } = deps;

        let reconciler_task = ReconcilerTask {
            state: reconciler_state_tx,
            current_config_rx: self.current_config_rx.clone(),
            raw_disks_rx: self.raw_disks_rx.clone(),
            time_sync_config,
            service_manager,
            key_requester,
            dataset_task_handle: self.dataset_task_handle.clone(),
            metrics_queue,
            underlay_vnic,
            log,
        };

        tokio::task::spawn(reconciler_task.run());
    }

    pub async fn set_new_config(
        &self,
        new_config: OmicronSledConfig,
    ) -> Result<(), LedgerTaskError> {
        self.ledger_task.set_new_config(new_config).await
    }

    pub fn current_ledgered_config(&self) -> Option<OmicronSledConfig> {
        match self.current_config_rx.borrow().clone() {
            CurrentConfig::WaitingForInternalDisks
            | CurrentConfig::WaitingForRackSetup => None,
            CurrentConfig::Ledgered(config) => Some(config),
        }
    }

    pub fn current_raw_disks_inventory(&self) -> Vec<InventoryDisk> {
        self.raw_disks_rx
            .borrow()
            .iter()
            .map(|disk| {
                let firmware = disk.firmware();
                InventoryDisk {
                    identity: disk.identity().clone(),
                    variant: disk.variant(),
                    slot: disk.slot(),
                    active_firmware_slot: firmware.active_slot(),
                    next_active_firmware_slot: firmware.next_active_slot(),
                    number_of_firmware_slots: firmware.number_of_slots(),
                    slot1_is_read_only: firmware.slot1_read_only(),
                    slot_firmware_versions: firmware.slots().to_vec(),
                }
            })
            .collect()
    }

    pub async fn current_zpool_and_dataset_inventory(
        &self,
    ) -> (Vec<InventoryZpool>, Vec<InventoryDataset>) {
        let state = Arc::clone(&*self.reconciler_state_rx.borrow());
        let zpool_names = state
            .all_managed_external_disk_pools()
            .cloned()
            .collect::<Vec<_>>();

        let inv_zpool_futures = zpool_names
            .iter()
            .map(|zpool_name| (zpool_name.id(), zpool_name.to_string()))
            .map(|(zpool_id, zpool_name)| async move {
                let name = zpool_name.clone();
                let result =
                    tokio::task::spawn_blocking(move || Zpool::get_info(&name))
                        .await
                        .expect("task did not panic");
                let info = match result {
                    Ok(info) => info,
                    Err(err) => {
                        warn!(
                            self.log, "Failed to access zpool info";
                            "zpool" => zpool_name,
                            InlineErrorChain::new(&err),
                        );
                        return None;
                    }
                };
                let total_size = match ByteCount::try_from(info.size()) {
                    Ok(n) => n,
                    Err(err) => {
                        warn!(
                            self.log, "Failed to parse zpool size";
                            "zpool" => zpool_name,
                            "raw_size" => info.size(),
                            InlineErrorChain::new(&err),
                        );
                        return None;
                    }
                };
                Some(InventoryZpool { id: zpool_id, total_size })
            });

        let inv_zpool_futures = future::join_all(inv_zpool_futures);
        let datasets_futures = self.dataset_task_handle.inventory(zpool_names);

        let (inv_zpools, datasets_result) =
            tokio::join!(inv_zpool_futures, datasets_futures);

        let inv_zpools = inv_zpools.into_iter().flatten().collect();
        let datasets = match datasets_result {
            Ok(datasets) => datasets,
            Err(err) => {
                warn!(
                    self.log, "Failed to list dataset properties";
                    InlineErrorChain::new(&err),
                );
                Vec::new()
            }
        };

        (inv_zpools, datasets)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CurrentConfig {
    // We're still waiting on the M.2 drives to be found: We don't yet know
    // whether we have a ledgered config, nor would we be able to write one.
    WaitingForInternalDisks,
    // We have at least one M.2 drive, but there is no ledgered config: we're
    // waiting for rack setup to run.
    WaitingForRackSetup,
    // We have a ledgered config.
    Ledgered(OmicronSledConfig),
}

#[derive(Debug, Clone)]
pub(crate) enum ReconcilerTaskStatus {
    WaitingForInternalDisks,
    WaitingForRackSetup,
    PerformingReconciliation { config: OmicronSledConfig, started: Instant },
    Idle { completed: Instant, elapsed: Duration },
}

#[derive(Debug, Clone)]
pub(crate) struct ReconcilerTaskState {
    last_reconciled_config: Option<OmicronSledConfig>,
    external_disks: ExternalDiskMap,
    datasets: DatasetMap,
    zones: ZoneMap,
    timesync_status: TimeSyncStatus,
    status: ReconcilerTaskStatus,
}

impl ReconcilerTaskState {
    fn new(mount_config: Arc<MountConfig>) -> Self {
        Self {
            last_reconciled_config: None,
            external_disks: ExternalDiskMap::new(mount_config),
            datasets: DatasetMap::default(),
            zones: ZoneMap::default(),
            timesync_status: TimeSyncStatus::NotYetChecked,
            status: ReconcilerTaskStatus::WaitingForInternalDisks,
        }
    }

    fn has_retryable_error(&self) -> bool {
        self.external_disks.has_disk_with_retryable_error()
            || self.datasets.has_dataset_with_retryable_error()
            || self.zones.has_zone_with_retryable_error()
    }

    // TODO-john comments! only use this for "is the zpool still here"; maybe we
    // need a different function?
    pub fn all_managed_external_disk_pools(
        &self,
    ) -> impl Iterator<Item = &ZpoolName> + '_ {
        self.external_disks.all_managed_external_disk_pools()
    }

    pub(crate) fn timesync_status(&self) -> &TimeSyncStatus {
        &self.timesync_status
    }

    pub(crate) fn all_mounted_zone_root_datasets(
        &self,
    ) -> impl Iterator<Item = PathInPool> + '_ {
        self.datasets
            .all_mounted_zone_root_datasets(self.external_disks.mount_config())
    }

    pub(crate) fn all_mounted_debug_datasets(
        &self,
    ) -> impl Iterator<Item = PathInPool> + '_ {
        self.datasets
            .all_mounted_debug_datasets(self.external_disks.mount_config())
    }

    pub(crate) fn to_inventory(&self) -> ConfigReconcilerInventory {
        use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus as InvStatus;
        ConfigReconcilerInventory {
            last_reconciled_config: self.last_reconciled_config.clone(),
            external_disks: self.external_disks.to_inventory(),
            datasets: self.datasets.to_inventory(),
            zones: self.zones.to_inventory(),
            status: match &self.status {
                ReconcilerTaskStatus::WaitingForInternalDisks
                | ReconcilerTaskStatus::WaitingForRackSetup => {
                    InvStatus::NotYetRun
                }
                ReconcilerTaskStatus::PerformingReconciliation {
                    config,
                    started,
                    ..
                } => InvStatus::Running {
                    config: config.clone(),
                    running_for: started.elapsed(),
                },
                ReconcilerTaskStatus::Idle { elapsed, .. } => {
                    InvStatus::Idle { ran_for: *elapsed }
                }
            },
        }
    }
}

struct ReconcilerTask {
    state: watch::Sender<Arc<ReconcilerTaskState>>,
    current_config_rx: watch::Receiver<CurrentConfig>,
    raw_disks_rx: watch::Receiver<IdMap<RawDisk>>,
    time_sync_config: TimeSyncConfig,
    service_manager: ServiceManager,
    key_requester: StorageKeyRequester,
    dataset_task_handle: DatasetTaskHandle,
    metrics_queue: MetricsRequestQueue,
    underlay_vnic: EtherstubVnic,
    log: Logger,
}

impl ReconcilerTask {
    async fn run(mut self) {
        // If reconciliation fails, we may want to retry it. The "happy path"
        // that requires this is waiting for time sync: during RSS, cold boot,
        // or replacement of the NTP zone, we may fail to start any zones that
        // depend on time sync. We want to retry this pretty frequently: it's
        // cheap if we haven't time sync'd yet, and we'd like to move on to
        // starting zones as soon as we can.
        //
        // We could use a more complicated retry policy than "sleep for a few
        // seconds" (e.g., backoff, or even "pick the retry policy based on the
        // particular kind of failure we're retrying"). For now we'll just take
        // this pretty aggressive policy.
        const SLEEP_BETWEEN_RETRIES: Duration = Duration::from_secs(5);

        loop {
            let maybe_retry = match self.do_reconcilation().await {
                ReconciliationResult::NoRetryNeeded => {
                    Either::Left(future::pending())
                }
                ReconciliationResult::ShouldRetry => {
                    Either::Right(tokio::time::sleep(SLEEP_BETWEEN_RETRIES))
                }
            };

            // Wait for one of:
            //
            // 1. The current ledgered `OmicronSledConfig` has changed
            // 2. The set of `RawDisk`s has changed
            // 3. Our retry timer expires
            tokio::select! {
                // Cancel-safe per docs on `changed()`
                result = self.current_config_rx.changed() => {
                    match result {
                        Ok(()) => continue,
                        Err(_closed) => {
                            // This should never happen in production, but may
                            // in tests.
                            warn!(
                                self.log,
                                "current_config watch channel closed; exiting"
                            );
                            return;
                        }
                    }
                }

                // Cancel-safe per docs on `changed()`
                result = self.raw_disks_rx.changed() => {
                    match result {
                        Ok(()) => continue,
                        Err(_closed) => {
                            // This should never happen in production, but may
                            // in tests.
                            warn!(
                                self.log,
                                "raw_disks watch channel closed; exiting"
                            );
                            return;
                        }
                    }
                }

                // Cancel-safe: this is either `future::pending()` (never
                // completes) or `sleep()` (we don't care if it's cancelled)
                _ = maybe_retry => {
                    continue;
                }
            }
        }
    }

    async fn do_reconcilation(&mut self) -> ReconciliationResult {
        let current_config = self.current_config_rx.borrow_and_update().clone();
        let current_raw_disks = self.raw_disks_rx.borrow_and_update().clone();

        // If we're still waiting for the internal disks (i.e., we don't yet
        // know whether we have a ledgered config to read) or RSS, just update
        // the status and return. We don't need to retry in these cases: we'll
        // key off of changes to `current_config` when there's progress from
        // either of these states.
        let sled_config = match current_config {
            CurrentConfig::WaitingForInternalDisks => {
                self.state.send_if_modified(|state| {
                    if matches!(
                        state.status,
                        ReconcilerTaskStatus::WaitingForInternalDisks
                    ) {
                        false
                    } else {
                        // TODO-performance This clones the entire `state` if
                        // another caller is holding a clone of the Arc. Most of
                        // our modifications are only to the `status` field, so
                        // we could break the Arc up into finer-grained pieces
                        // if the cost of this potential clone is high. (Ir
                        // probably isn't: it's a handful of maps containing
                        // metadata about the disks + datasets + zones we're
                        // managing.)
                        //
                        // The same note applies to a couple other places in
                        // this function where we call `make_mut` exclusively to
                        // change `state.status`.
                        let state = Arc::make_mut(state);
                        state.status =
                            ReconcilerTaskStatus::WaitingForInternalDisks;
                        true
                    }
                });
                return ReconciliationResult::NoRetryNeeded;
            }
            CurrentConfig::WaitingForRackSetup => {
                self.state.send_if_modified(|state| {
                    if matches!(
                        state.status,
                        ReconcilerTaskStatus::WaitingForRackSetup
                    ) {
                        false
                    } else {
                        let state = Arc::make_mut(state);
                        state.status =
                            ReconcilerTaskStatus::WaitingForRackSetup;
                        true
                    }
                });
                return ReconciliationResult::NoRetryNeeded;
            }
            CurrentConfig::Ledgered(omicron_sled_config) => omicron_sled_config,
        };

        // Update the current state to note that we're performing reconcilation,
        // and also stash a clone of it in `current_state`.
        let started = Instant::now();
        let mut current_state = None;
        self.state.send_modify(|state| {
            let state = Arc::make_mut(state);
            state.status = ReconcilerTaskStatus::PerformingReconciliation {
                config: sled_config.clone(),
                started,
            };
            current_state = Some(state.clone());
        });
        let mut current_state =
            current_state.expect("always populated by send_modify");

        // Perform the actual reconcilation.
        self.reconcile_against_config(
            &mut current_state,
            &current_raw_disks,
            &sled_config,
        )
        .await;

        let result = if current_state.has_retryable_error() {
            ReconciliationResult::ShouldRetry
        } else {
            ReconciliationResult::NoRetryNeeded
        };

        // Notify any receivers of our post-reconciliation state. We always
        // update the `status`, and may or may not have updated other fields.
        current_state.last_reconciled_config = Some(sled_config);
        current_state.status = ReconcilerTaskStatus::Idle {
            completed: Instant::now(),
            elapsed: started.elapsed(),
        };
        self.state.send_modify(|state| {
            *state = Arc::new(current_state);
        });

        result
    }

    async fn reconcile_against_config(
        &self,
        state: &mut ReconcilerTaskState,
        raw_disks: &IdMap<RawDisk>,
        sled_config: &OmicronSledConfig,
    ) {
        // ---
        // We go through the removal process first: shut down zones, then remove
        // datasets, then remove disks.
        // ---

        // First, shut down any zones that need to be shut down.
        state
            .zones
            .shut_down_zones_if_needed(
                &sled_config.zones,
                &self.metrics_queue,
                self.service_manager.zone_bundler(),
                self.service_manager.ddm_reconciler(),
                &self.underlay_vnic,
                &self.log,
            )
            .await;

        // Next, delete any datasets that need to be deleted.
        //
        // TODO We don't do this yet:
        // https://github.com/oxidecomputer/omicron/issues/6177

        // Now remove any disks we're no longer supposed to use.
        state.external_disks.stop_managing_if_needed(
            raw_disks,
            &sled_config.disks,
            &self.log,
        );

        // Make sure the zone bundler has stopped writing to any disk(s) we just
        // stopped managing.
        self.service_manager
            .zone_bundler()
            .await_completion_of_prior_bundles()
            .await;

        // ---
        // Now go through the add process: start managing disks, create
        // datasets, start zones.
        // ---

        state
            .external_disks
            .start_managing_if_needed(
                raw_disks,
                &sled_config.disks,
                &self.key_requester,
                &self.log,
            )
            .await;

        // Both dataset and zone creation want to check zpool details against
        // what zpools we are managing; grab that snapshot now that we've
        // (potentially) started managing more disks.
        let managed_external_zpools = state
            .external_disks
            .all_managed_external_disk_pools()
            .cloned()
            .collect::<Vec<_>>();

        match self
            .dataset_task_handle
            .datasets_ensure(
                sled_config.datasets.clone(),
                Arc::clone(state.external_disks.mount_config()),
                managed_external_zpools
                    .iter()
                    .map(|zpool| zpool.id())
                    .collect(),
            )
            .await
        {
            Ok(datasets) => {
                state.datasets = datasets;
            }
            Err(err) => {
                warn!(
                    self.log,
                    "Failed to ensure datasets";
                    InlineErrorChain::new(&err),
                );
            }
        }

        // Finally, start up any new zones. This may include relaunching zones
        // we shut down above (e.g., if they've just been upgraded).
        //
        // To start zones, we need to know if we're currently timesync'd.
        state.timesync_status =
            state.zones.timesync_status(&self.time_sync_config).await;

        if state.timesync_status.is_synchronized() {
            // TODO-cleanup Should we do this work instead?
            self.service_manager.on_time_sync().await;
        }

        state
            .zones
            .start_zones_if_needed(
                &sled_config.zones,
                &self.service_manager,
                state.external_disks.mount_config(),
                state.timesync_status.is_synchronized(),
                &managed_external_zpools,
                &self.log,
            )
            .await;
    }
}

enum ReconciliationResult {
    NoRetryNeeded,
    ShouldRetry,
}
