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

use id_map::Entry;
use id_map::IdMap;
use id_map::IdMappable as _;
use illumos_utils::dladm::EtherstubVnic;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::disk::DiskIdentity;
use sled_storage::config::MountConfig;
use sled_storage::disk::RawDisk;
use slog::Logger;
use tokio::sync::watch;

mod datasets;
mod external_disks;
mod internal_disks;
mod ledger;
mod raw_disks;
mod zones;

use crate::ddm_reconciler::DdmReconciler;
use crate::metrics::MetricsRequestQueue;
use crate::services::ServiceManager;
use crate::services::TimeSyncConfig;
use crate::zone_bundle::ZoneBundler;

use self::external_disks::ExternalDisks;
use self::internal_disks::InternalDisksTask;
use self::ledger::LedgerTask;
use self::ledger::LedgerTaskHandle;
use self::zones::TimeSyncStatus;
use self::zones::ZoneMap;

pub use self::ledger::LedgerTaskError;

pub struct ConfigReconcilerHandle {
    reconciler_state_rx: watch::Receiver<Arc<ReconcilerTaskState>>,
    raw_disks: watch::Sender<IdMap<RawDisk>>,
    ledger_task: LedgerTaskHandle,
    hold_while_waiting_for_sled_agent:
        Mutex<Option<ReconcilerTaskDependenciesHeldUntilSledAgentStarted>>,
    log: Logger,
}

struct ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
    reconciler_state_tx: watch::Sender<Arc<ReconcilerTaskState>>,
    current_config_rx: watch::Receiver<CurrentConfig>,
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    log: Logger,
}

impl ConfigReconcilerHandle {
    pub(crate) fn new(
        key_requester: StorageKeyRequester,
        mount_config: Arc<MountConfig>,
        time_sync_config: TimeSyncConfig,
        base_log: &Logger,
    ) -> Self {
        let (raw_disks, raw_disks_rx) = watch::channel(IdMap::new());
        let internal_disks_rx = InternalDisksTask::spawn(
            Arc::clone(&mount_config),
            raw_disks_rx,
            base_log.new(slog::o!("component" => "InternalDisksTask")),
        );

        let (ledger_task, current_config_rx) = LedgerTask::spawn(
            internal_disks_rx,
            base_log.new(slog::o!("component" => "LedgerTask")),
        );

        let (reconciler_state_tx, reconciler_state_rx) =
            watch::channel(Arc::new(ReconcilerTaskState::new(mount_config)));

        let hold_while_waiting_for_sled_agent = Mutex::new(Some(
            ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
                reconciler_state_tx,
                current_config_rx,
                key_requester,
                time_sync_config,
                log: base_log.new(slog::o!("component" => "ReconcilerTask")),
            },
        ));

        let log =
            base_log.new(slog::o!("component" => "ConfigReconcilerHandle"));

        Self {
            reconciler_state_rx,
            raw_disks,
            ledger_task,
            hold_while_waiting_for_sled_agent,
            log,
        }
    }

    pub(crate) fn spawn_reconciliation_task(
        &self,
        service_manager: ServiceManager,
        metrics_queue: MetricsRequestQueue,
        zone_bundler: ZoneBundler,
        ddm_reconciler: DdmReconciler,
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
            current_config_rx,
            key_requester,
            time_sync_config,
            log,
        } = deps;

        let reconciler_task = ReconcilerTask {
            state: reconciler_state_tx,
            current_config_rx,
            time_sync_config,
            service_manager,
            key_requester,
            metrics_queue,
            zone_bundler,
            ddm_reconciler,
            underlay_vnic,
            log,
        };

        tokio::task::spawn(reconciler_task.run());
    }

    pub fn set_raw_disks<I>(&self, raw_disks: I)
    where
        I: Iterator<Item = RawDisk>,
    {
        let new_raw_disks = raw_disks.collect::<IdMap<_>>();
        self.raw_disks.send_if_modified(|prev_raw_disks| {
            if *prev_raw_disks == new_raw_disks {
                false
            } else {
                *prev_raw_disks = new_raw_disks;
                true
            }
        });
    }

    pub fn add_or_insert_raw_disk(&self, raw_disk: RawDisk) {
        self.raw_disks.send_if_modified(|raw_disks| {
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
        self.raw_disks
            .send_if_modified(|raw_disks| raw_disks.remove(identity).is_some());
    }

    pub async fn set_new_config(
        &self,
        new_config: OmicronSledConfig,
    ) -> Result<(), LedgerTaskError> {
        self.ledger_task.set_new_config(new_config).await
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
    PerformingReconciliation {
        config: OmicronSledConfig,
        started: Instant,
    },
    Idle {
        last_reconciled_config: OmicronSledConfig,
        completed: Instant,
        elapsed: Duration,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ReconcilerTaskState {
    external_disks: ExternalDisks,
    zones: ZoneMap,
    timesync_status: TimeSyncStatus,
    status: ReconcilerTaskStatus,
}

impl ReconcilerTaskState {
    fn new(mount_config: Arc<MountConfig>) -> Self {
        Self {
            external_disks: ExternalDisks::new(mount_config),
            zones: ZoneMap::default(),
            timesync_status: TimeSyncStatus::NotYetChecked,
            status: ReconcilerTaskStatus::WaitingForInternalDisks,
        }
    }
}

struct ReconcilerTask {
    state: watch::Sender<Arc<ReconcilerTaskState>>,
    current_config_rx: watch::Receiver<CurrentConfig>,
    time_sync_config: TimeSyncConfig,
    service_manager: ServiceManager,
    key_requester: StorageKeyRequester,
    metrics_queue: MetricsRequestQueue,
    zone_bundler: ZoneBundler,
    ddm_reconciler: DdmReconciler,
    underlay_vnic: EtherstubVnic,
    log: Logger,
}

impl ReconcilerTask {
    async fn run(mut self) {
        loop {
            self.do_reconcilation().await;

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
            }
        }
    }

    async fn do_reconcilation(&mut self) {
        let current_config = self.current_config_rx.borrow_and_update().clone();

        let sled_config = match current_config {
            CurrentConfig::WaitingForInternalDisks => {
                self.state.send_if_modified(|state| {
                    if matches!(
                        state.status,
                        ReconcilerTaskStatus::WaitingForInternalDisks
                    ) {
                        false
                    } else {
                        let state = Arc::make_mut(state);
                        state.status =
                            ReconcilerTaskStatus::WaitingForInternalDisks;
                        true
                    }
                });
                return;
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
                return;
            }
            CurrentConfig::Ledgered(omicron_sled_config) => omicron_sled_config,
        };

        // Update the current state to note that we're performing reconcilation,
        // and also stash a clone of it in `current_state`.
        let started = Instant::now();
        let mut current_state = None;
        self.state.send_modify(|state| {
            let mut state = Arc::clone(state);
            Arc::make_mut(&mut state).status =
                ReconcilerTaskStatus::PerformingReconciliation {
                    config: sled_config.clone(),
                    started,
                };
            current_state = Some(state);
        });
        let mut current_state =
            current_state.expect("always populated by send_modify");

        // Perform the actual reconcilation.
        let mutable_state = Arc::make_mut(&mut current_state);
        self.reconcile_against_config(mutable_state, &sled_config).await;

        // Notify any receivers of our post-reconciliation state. We always
        // update the `status`, and may or may not have updated other fields.
        mutable_state.status = ReconcilerTaskStatus::Idle {
            last_reconciled_config: sled_config,
            completed: Instant::now(),
            elapsed: started.elapsed(),
        };
        self.state.send_modify(|state| {
            *state = current_state;
        });
    }

    async fn reconcile_against_config(
        &self,
        state: &mut ReconcilerTaskState,
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
                &self.zone_bundler,
                &self.ddm_reconciler,
                &self.underlay_vnic,
                &self.log,
            )
            .await;

        // Next, delete any datasets that need to be deleted.
        //
        // TODO We don't do this yet:
        // https://github.com/oxidecomputer/omicron/issues/6177

        // Now remove any disks we're no longer supposed to use.
        //state.external_disks.retain_present_and_managed(raw_disks, config);

        // ---
        // Now go through the add process: start managing disks, create
        // datasets, start zones.
        // ---

        //state.external_disks.ensure_managing(raw_disks, config, key_requester, log)

        // TODO datasets

        // Finally, start up any new zones. This may include relaunching zones
        // we shut down above (e.g., if they've just been upgraded).
        //
        // To start zones, we need to know if we're currently timesync'd.
        state.timesync_status =
            state.zones.timesync_status(&self.time_sync_config).await;

        state
            .zones
            .start_zones_if_needed(
                &sled_config.zones,
                &self.service_manager,
                state.external_disks.mount_config(),
                state.timesync_status.is_synchronized(),
                &state.external_disks.all_u2_pools(),
                &self.log,
            )
            .await;
    }
}
