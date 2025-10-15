// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The primary task for sled config reconciliation.

use chrono::DateTime;
use chrono::Utc;
use either::Either;
use futures::future;
use iddqd::IdOrdMap;
use illumos_utils::zpool::PathInPool;
use illumos_utils::zpool::ZpoolOrRamdisk;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::BootPartitionContents as BootPartitionContentsInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use nexus_sled_agent_shared::inventory::RemoveMupdateOverrideInventory;
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use sled_storage::config::MountConfig;
use sled_storage::dataset::U2_DEBUG_DATASET;
use sled_storage::dataset::ZONE_DATASET;
use sled_storage::disk::Disk;
use slog::Logger;
use slog::info;
use slog::warn;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::InternalDisksReceiver;
use crate::SledAgentArtifactStore;
use crate::TimeSyncConfig;
use crate::dataset_serialization_task::DatasetTaskHandle;
use crate::host_phase_2::BootPartitionReconciler;
use crate::ledger::CurrentSledConfig;
use crate::raw_disks::RawDisksReceiver;
use crate::sled_agent_facilities::SledAgentFacilities;

mod datasets;
mod external_disks;
mod zones;

use self::datasets::OmicronDatasets;
use self::external_disks::ExternalDisks;
use self::zones::OmicronZones;

pub use self::external_disks::CurrentlyManagedZpools;
pub use self::external_disks::CurrentlyManagedZpoolsReceiver;
pub use self::zones::TimeSyncError;
pub use self::zones::TimeSyncStatus;
use crate::dump_setup::FormerZoneRootRequest;

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn<T: SledAgentFacilities, U: SledAgentArtifactStore>(
    mount_config: Arc<MountConfig>,
    dataset_task: DatasetTaskHandle,
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    current_config_rx: watch::Receiver<CurrentSledConfig>,
    reconciler_result_tx: watch::Sender<ReconcilerResult>,
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
    internal_disks_rx: InternalDisksReceiver,
    external_disks_tx: watch::Sender<HashSet<Disk>>,
    former_zone_roots_tx: mpsc::Sender<FormerZoneRootRequest>,
    raw_disks_rx: RawDisksReceiver,
    sled_agent_facilities: T,
    sled_agent_artifact_store: U,
    log: Logger,
) {
    let external_disks = ExternalDisks::new(
        Arc::clone(&mount_config),
        currently_managed_zpools_tx,
        external_disks_tx,
        former_zone_roots_tx.clone(),
    );
    let datasets = OmicronDatasets::new(dataset_task);
    let zones = OmicronZones::new(mount_config, time_sync_config);
    let boot_partitions = BootPartitionReconciler::default();

    tokio::spawn(
        ReconcilerTask {
            key_requester,
            current_config_rx,
            reconciler_result_tx,
            raw_disks_rx,
            internal_disks_rx,
            external_disks,
            former_zone_roots_tx,
            datasets,
            zones,
            boot_partitions,
            log,
        }
        .run(sled_agent_facilities, sled_agent_artifact_store),
    );
}

#[derive(Debug)]
pub(crate) struct ReconcilerResult {
    mount_config: Arc<MountConfig>,
    status: ReconcilerTaskStatus,
    latest_result: Option<LatestReconciliationResult>,
}

impl ReconcilerResult {
    pub(crate) fn new(mount_config: Arc<MountConfig>) -> Self {
        Self {
            mount_config,
            status: ReconcilerTaskStatus::NotYetRunning,
            latest_result: None,
        }
    }

    pub(crate) fn timesync_status(&self) -> TimeSyncStatus {
        self.latest_result
            .as_ref()
            .map(|inner| inner.timesync_status.clone())
            .unwrap_or(TimeSyncStatus::NotYetChecked)
    }

    pub(crate) fn all_mounted_debug_datasets(
        &self,
    ) -> impl Iterator<Item = PathInPool> + '_ {
        let Some(latest_result) = &self.latest_result else {
            return Either::Left(std::iter::empty());
        };
        Either::Right(
            latest_result
                .all_mounted_datasets(&self.mount_config, DatasetKind::Debug),
        )
    }

    pub(crate) fn all_mounted_zone_root_datasets(
        &self,
    ) -> impl Iterator<Item = PathInPool> + '_ {
        let Some(latest_result) = &self.latest_result else {
            return Either::Left(std::iter::empty());
        };
        Either::Right(latest_result.all_mounted_datasets(
            &self.mount_config,
            DatasetKind::TransientZoneRoot,
        ))
    }

    pub(crate) fn to_inventory(
        &self,
    ) -> (ConfigReconcilerInventoryStatus, Option<ConfigReconcilerInventory>)
    {
        let status = self.status.to_inventory();
        let latest_result =
            self.latest_result.as_ref().map(|r| r.to_inventory());
        (status, latest_result)
    }
}

#[derive(Debug, Clone)]
pub enum ReconcilerTaskStatus {
    NotYetRunning,
    WaitingForInternalDisks,
    WaitingForInitialConfig,
    PerformingReconciliation {
        config: OmicronSledConfig,
        started_at_time: DateTime<Utc>,
        started_at_instant: Instant,
    },
    Idle {
        completed_at_time: DateTime<Utc>,
        ran_for: Duration,
    },
}

impl ReconcilerTaskStatus {
    fn to_inventory(&self) -> ConfigReconcilerInventoryStatus {
        match self {
            Self::NotYetRunning
            | Self::WaitingForInternalDisks
            | Self::WaitingForInitialConfig => {
                ConfigReconcilerInventoryStatus::NotYetRun
            }
            Self::PerformingReconciliation {
                config,
                started_at_time,
                started_at_instant,
            } => ConfigReconcilerInventoryStatus::Running {
                config: config.clone(),
                started_at: *started_at_time,
                running_for: started_at_instant.elapsed(),
            },
            Self::Idle { completed_at_time, ran_for } => {
                ConfigReconcilerInventoryStatus::Idle {
                    completed_at: *completed_at_time,
                    ran_for: *ran_for,
                }
            }
        }
    }
}

#[derive(Debug)]
struct LatestReconciliationResult {
    sled_config: OmicronSledConfig,
    external_disks_inventory:
        BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult>,
    datasets: BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult>,
    orphaned_datasets: IdOrdMap<OrphanedDataset>,
    zones_inventory: BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult>,
    timesync_status: TimeSyncStatus,
    boot_partitions: BootPartitionContentsInventory,
    remove_mupdate_override: Option<RemoveMupdateOverrideInventory>,
}

impl LatestReconciliationResult {
    fn to_inventory(&self) -> ConfigReconcilerInventory {
        ConfigReconcilerInventory {
            last_reconciled_config: self.sled_config.clone(),
            external_disks: self.external_disks_inventory.clone(),
            datasets: self.datasets.clone(),
            orphaned_datasets: self.orphaned_datasets.clone(),
            zones: self.zones_inventory.clone(),
            boot_partitions: self.boot_partitions.clone(),
            remove_mupdate_override: self.remove_mupdate_override.clone(),
        }
    }

    fn all_mounted_datasets<'a>(
        &'a self,
        mount_config: &'a MountConfig,
        kind: DatasetKind,
    ) -> impl Iterator<Item = PathInPool> + 'a {
        // This is a private method only called by this file; we only have to
        // handle the specific `DatasetKind`s used by our callers.
        let mountpoint = match &kind {
            DatasetKind::Debug => U2_DEBUG_DATASET,
            DatasetKind::TransientZoneRoot => ZONE_DATASET,
            _ => unreachable!(
                "private function called with unexpected kind {kind:?}"
            ),
        };
        self.datasets
            .iter()
            // Filter down to successfully-ensured datasets
            .filter_map(|(dataset_id, result)| match result {
                ConfigReconcilerInventoryResult::Ok => {
                    self.sled_config.datasets.get(dataset_id)
                }
                ConfigReconcilerInventoryResult::Err { .. } => None,
            })
            // Filter down to matching dataset kinds
            .filter(move |config| *config.name.kind() == kind)
            .map(|config| {
                let pool = *config.name.pool();
                PathInPool {
                    pool: ZpoolOrRamdisk::Zpool(pool),
                    path: pool
                        .dataset_mountpoint(&mount_config.root, mountpoint),
                }
            })
    }
}

struct ReconcilerTask {
    key_requester: StorageKeyRequester,
    current_config_rx: watch::Receiver<CurrentSledConfig>,
    reconciler_result_tx: watch::Sender<ReconcilerResult>,
    raw_disks_rx: RawDisksReceiver,
    internal_disks_rx: InternalDisksReceiver,
    external_disks: ExternalDisks,
    former_zone_roots_tx: mpsc::Sender<FormerZoneRootRequest>,
    datasets: OmicronDatasets,
    zones: OmicronZones,
    boot_partitions: BootPartitionReconciler,
    log: Logger,
}

impl ReconcilerTask {
    async fn run<T: SledAgentFacilities, U: SledAgentArtifactStore>(
        mut self,
        sled_agent_facilities: T,
        sled_agent_artifact_store: U,
    ) {
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
            let result = self
                .do_reconcilation(
                    &sled_agent_facilities,
                    &sled_agent_artifact_store,
                )
                .await;

            let maybe_retry = match result {
                ReconciliationResult::NoRetryNeeded => {
                    Either::Left(future::pending())
                }
                ReconciliationResult::ShouldRetry => {
                    info!(
                        self.log,
                        "reconcilation result has retryable error; will retry";
                        "retry_after" => ?SLEEP_BETWEEN_RETRIES,
                    );
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
                        Ok(()) => {
                            info!(
                                self.log,
                                "starting reconciliation due to config change"
                            );
                            continue;
                        }
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
                        Ok(()) => {
                            info!(
                                self.log,
                                "starting reconciliation due to raw disk change"
                            );
                            continue;
                        }
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
                    info!(
                        self.log,
                        "starting reconciliation due to retryable error"
                    );
                    continue;
                }
            }
        }
    }

    async fn do_reconcilation<
        T: SledAgentFacilities,
        U: SledAgentArtifactStore,
    >(
        &mut self,
        sled_agent_facilities: &T,
        sled_agent_artifact_store: &U,
    ) -> ReconciliationResult {
        // Take a snapshot of the current state of the input channels on which
        // we act. Clone both to avoid keeping the channels locked while we
        // reconcile.
        let current_config = self.current_config_rx.borrow_and_update().clone();
        let current_raw_disks = self.raw_disks_rx.borrow_and_update().clone();

        // See whether we actually have a config to reconcile against.
        let started_at_instant = Instant::now();
        let sled_config = match current_config {
            // In both `WaitingFor...` cases, we don't need to retry on our own:
            // we'll retry as soon as there's a change in our inputs that might
            // nudge us out of these states.
            CurrentSledConfig::WaitingForInternalDisks => {
                self.reconciler_result_tx.send_modify(|r| {
                    r.status = ReconcilerTaskStatus::WaitingForInternalDisks;
                });
                return ReconciliationResult::NoRetryNeeded;
            }
            CurrentSledConfig::WaitingForInitialConfig => {
                self.reconciler_result_tx.send_modify(|r| {
                    r.status = ReconcilerTaskStatus::WaitingForInitialConfig;
                });
                return ReconciliationResult::NoRetryNeeded;
            }
            CurrentSledConfig::Ledgered(sled_config) => {
                self.reconciler_result_tx.send_modify(|r| {
                    r.status = ReconcilerTaskStatus::PerformingReconciliation {
                        config: sled_config.clone(),
                        started_at_time: Utc::now(),
                        started_at_instant,
                    };
                });
                sled_config
            }
        };

        let internal_disks = self.internal_disks_rx.current();

        // Reconcile the mupdate override field. This can be done independently
        // of the other parts of reconciliation (and this doesn't have to block
        // other parts of reconciliation), but the argument for this is somewhat
        // non-trivial. See
        // https://rfd.shared.oxide.computer/rfd/556#sa_reconciler_error_handling.
        let remove_mupdate_override =
            if let Some(override_id) = sled_config.remove_mupdate_override {
                Some(
                    sled_agent_facilities
                        .remove_mupdate_override(override_id, &internal_disks),
                )
            } else {
                None
            };

        // Obtain the resolver status. This will be used to account for mupdate
        // overrides, as well as errors while reading this information.
        //
        // This status is obtained after remove_mupdate_override is processed.
        let resolver_status =
            sled_agent_facilities.zone_image_resolver_status();

        // Reconcile any changes to our boot partitions. This is typically a
        // no-op; if we've successfully read both boot partitions in a previous
        // reconciliation and don't have new contents to write, it will just
        // return cached status.
        let boot_partitions = self
            .boot_partitions
            .reconcile(
                &resolver_status,
                &internal_disks,
                &sled_config.host_phase_2,
                sled_agent_artifact_store,
                &self.log,
            )
            .await;

        // ---
        // We go through the removal process first: shut down zones, then stop
        // managing disks, then remove any orphaned datasets.
        // ---

        // First, shut down zones if needed.
        let zone_shutdown_result = self
            .zones
            .shut_down_zones_if_needed(
                &sled_config.zones,
                &resolver_status,
                &internal_disks,
                sled_agent_facilities,
                &self.log,
            )
            .await;

        // Next, remove any external disks we're no longer supposed to use
        // (either due to config changes or the raw disk being gone).
        self.external_disks.stop_managing_if_needed(
            &current_raw_disks,
            &sled_config.disks,
            &self.log,
        );

        // Finally, remove any "orphaned" datasets (i.e., datasets of a kind
        // that we ought to be managing that exist on disks we're managing but
        // don't have entries in our current config).
        self.datasets
            .remove_datasets_if_needed(
                &sled_config.datasets,
                self.external_disks.currently_managed_zpools(),
                &self.log,
            )
            .await;

        // ---
        // Now go through the add process: start managing disks, create
        // datasets, start zones.
        // ---

        // Start managing disks.
        self.external_disks
            .start_managing_if_needed(
                &current_raw_disks,
                &sled_config.disks,
                &self.key_requester,
                &self.log,
            )
            .await;

        // Ensure all the datasets we want exist.
        self.datasets
            .ensure_datasets_if_needed(
                sled_config.datasets.clone(),
                self.external_disks.currently_managed_zpools(),
                &self.log,
            )
            .await;

        // Collect the current timesync status (needed to start any new zones,
        // and also we want to report it as part of each reconciler result).
        let timesync_status = self.zones.check_timesync(&self.log).await;

        // Call back into sled-agent and let it do any work that needs to happen
        // once time is sync'd (e.g., rewrite `uptime`).
        if timesync_status.is_synchronized() {
            sled_agent_facilities.on_time_sync();
        }

        // We conservatively refuse to start any new zones if any zones have
        // failed to shut down cleanly. This could be more precise, but we want
        // to avoid wandering into some really weird cases, such as:
        //
        // * Multiple NTP zones active concurrently
        // * Multiple Crucible zones trying to manage the same zpool
        //
        // which could happen if we're upgrading a zone but failed to shut down
        // the old instance.
        match zone_shutdown_result {
            Ok(()) => {
                self.zones
                    .start_zones_if_needed(
                        &sled_config.zones,
                        &resolver_status,
                        &internal_disks,
                        sled_agent_facilities,
                        timesync_status.is_synchronized(),
                        &self.datasets,
                        &self.log,
                    )
                    .await;
            }
            Err(nfailed) => {
                warn!(
                    self.log,
                    "skipping attempt to start new zones; \
                     {nfailed} zones failed to shut down cleanly"
                );
            }
        }

        // We'll retry even if there have been no config changes if (a) time
        // isn't sync'd yet or (b) any of our disk/dataset/zone attempts failed
        // with a retryable error.
        let result = if !timesync_status.is_synchronized()
            || self.external_disks.has_retryable_error()
            || self.zones.has_retryable_error()
            || self.datasets.has_retryable_error()
        {
            ReconciliationResult::ShouldRetry
        } else {
            ReconciliationResult::NoRetryNeeded
        };

        let inner = LatestReconciliationResult {
            sled_config,
            external_disks_inventory: self.external_disks.to_inventory(),
            datasets: self.datasets.to_inventory(),
            orphaned_datasets: self.datasets.orphaned_datasets().clone(),
            zones_inventory: self.zones.to_inventory(),
            timesync_status,
            boot_partitions: boot_partitions.into_inventory(),
            remove_mupdate_override: remove_mupdate_override
                .map(|v| v.to_inventory()),
        };
        self.reconciler_result_tx.send_modify(|r| {
            r.status = ReconcilerTaskStatus::Idle {
                completed_at_time: Utc::now(),
                ran_for: started_at_instant.elapsed(),
            };
            r.latest_result = Some(inner);
        });

        result
    }
}

enum ReconciliationResult {
    NoRetryNeeded,
    ShouldRetry,
}
