// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler task to ensure the sled is configured to match the
//! most-recently-received `OmicronSledConfig` from Nexus.

// TODO-john remove
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;

use id_map::Entry;
use id_map::IdMap;
use id_map::IdMappable as _;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::disk::DiskIdentity;
use sled_storage::config::MountConfig;
use sled_storage::disk::RawDisk;
use slog::Logger;
use tokio::sync::oneshot;
use tokio::sync::watch;

mod datasets;
mod external_disks;
mod internal_disks;
mod key_requester;
mod ledger;
mod raw_disks;
mod zones;

use self::external_disks::ExternalDisks;
use self::internal_disks::InternalDisksTask;
use self::key_requester::KeyManagerWaiter;
use self::ledger::LedgerTask;
use self::ledger::LedgerTaskHandle;
use self::zones::ZoneMap;

pub use self::ledger::LedgerTaskError;

pub struct ConfigReconcilerHandle {
    reconciler_state_rx: watch::Receiver<Arc<ReconcilerTaskState>>,
    key_manager_waiter: Arc<KeyManagerWaiter>,
    raw_disks: watch::Sender<IdMap<RawDisk>>,
    ledger_task: LedgerTaskHandle,
    hold_while_waiting_for_sled_agent:
        Mutex<Option<ReconcilerTaskDependenciesHeldUntilSledAgentStarted>>,
    log: Logger,
}

struct ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
    reconciler_state_tx: watch::Sender<Arc<ReconcilerTaskState>>,
    current_config_rx: watch::Receiver<CurrentConfig>,
    key_requester_rx: oneshot::Receiver<StorageKeyRequester>,
}

impl ConfigReconcilerHandle {
    pub fn new(
        key_requester: StorageKeyRequester,
        mount_config: Arc<MountConfig>,
        base_log: &Logger,
    ) -> Self {
        let (raw_disks, raw_disks_rx) = watch::channel(IdMap::new());
        let internal_disks_rx = InternalDisksTask::spawn(
            Arc::clone(&mount_config),
            raw_disks_rx,
            base_log.new(slog::o!("component" => "InternalDisksTask")),
        );

        let (key_manager_waiter, key_requester_rx) =
            KeyManagerWaiter::hold_requester_until_key_manager_ready(
                key_requester,
            );
        let key_manager_waiter = Arc::new(key_manager_waiter);

        let (ledger_task, current_config_rx) = LedgerTask::spawn(
            internal_disks_rx,
            Arc::clone(&key_manager_waiter),
            base_log.new(slog::o!("component" => "LedgerTask")),
        );

        let (reconciler_state_tx, reconciler_state_rx) =
            watch::channel(Arc::new(ReconcilerTaskState::new(mount_config)));

        let hold_while_waiting_for_sled_agent = Mutex::new(Some(
            ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
                reconciler_state_tx,
                current_config_rx,
                key_requester_rx,
            },
        ));

        let log =
            base_log.new(slog::o!("component" => "ConfigReconcilerHandle"));

        Self {
            reconciler_state_rx,
            key_manager_waiter,
            raw_disks,
            ledger_task,
            hold_while_waiting_for_sled_agent,
            log,
        }
    }

    pub fn notify_key_manager_ready(&self) {
        self.key_manager_waiter.notify_key_manager_ready();
    }

    pub fn notify_sled_agent_started(&self) {
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
                        "notify_sled_agent_started() called multiple times \
                         (ignored after first call)"
                    );
                    return;
                }
            };
        let ReconcilerTaskDependenciesHeldUntilSledAgentStarted {
            reconciler_state_tx,
            current_config_rx,
            key_requester_rx,
        } = deps;

        let reconciler_task = ReconcilerTask {
            state: reconciler_state_tx,
            current_config_rx,
        };

        tokio::task::spawn(reconciler_task.run(key_requester_rx));
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

#[derive(Debug)]
pub(crate) enum ReconcilerTaskStatus {
    WaitingForInternalDisks,
}

#[derive(Debug)]
pub(crate) struct ReconcilerTaskState {
    external_disks: ExternalDisks,
    zones: ZoneMap,
    status: ReconcilerTaskStatus,
}

impl ReconcilerTaskState {
    fn new(mount_config: Arc<MountConfig>) -> Self {
        Self {
            external_disks: ExternalDisks::new(mount_config),
            zones: ZoneMap::default(),
            status: ReconcilerTaskStatus::WaitingForInternalDisks,
        }
    }
}

struct ReconcilerTask {
    state: watch::Sender<Arc<ReconcilerTaskState>>,
    current_config_rx: watch::Receiver<CurrentConfig>,
}

impl ReconcilerTask {
    async fn run(
        self,
        key_requester_rx: oneshot::Receiver<StorageKeyRequester>,
    ) {
    }
}
