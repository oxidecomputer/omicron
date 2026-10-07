// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for managing external disks (on gimlet/cosmo, U.2s).
//!
//! There is no separate tokio task here; our parent reconciler task owns this
//! set of disks and is able to mutate it in place during reconciliation.

use futures::future;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use illumos_utils::zfs::DestroyDatasetError;
use illumos_utils::zfs::DestroyDatasetErrorVariant;
use illumos_utils::zfs::EnsureDatasetError;
use illumos_utils::zfs::ListDatasetsError;
use illumos_utils::zfs::SetValueError;
use illumos_utils::zfs::Zfs;
use illumos_utils::zpool::Zpool;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use rand::distr::{Alphanumeric, SampleString};
use sled_agent_types::disk::DiskVariant;
use sled_agent_types::disk::OmicronPhysicalDiskConfig;
use sled_agent_types::inventory::ConfigReconcilerInventoryResult;
use sled_agent_types::inventory::ZpoolHealth;
use sled_storage::config::MountConfig;
use sled_storage::dataset::CRYPT_DATASET;
use sled_storage::dataset::DatasetError;
use sled_storage::dataset::ZONE_DATASET;
use sled_storage::disk::Disk;
use sled_storage::disk::DiskError;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;
use trust_quorum_types::types::Epoch;

use super::datasets::DiskRekeyInfo;
use super::datasets::OmicronDatasets;
use super::datasets::RequiredDatasetError;
use crate::dataset_serialization_task::DatasetTaskError;
use crate::dataset_serialization_task::RekeyResult;
use crate::debug_collector::FormerZoneRootArchiver;
use crate::disks_common::MaybeUpdatedDisk;
use crate::disks_common::update_properties_from_raw_disk;
use camino::Utf8PathBuf;
use illumos_utils::zfs::Mountpoint;

#[derive(Debug, thiserror::Error)]
enum DiskManagementError {
    #[error("Disk requested by control plane, but not found on device")]
    NotFound,

    #[error("Disk requested by control plane is an internal disk: {0}")]
    InternalDiskControlPlaneRequest(PhysicalDiskUuid),

    #[error("Expected zpool UUID of {expected}, but saw {observed}")]
    ZpoolUuidMismatch { expected: ZpoolUuid, observed: ZpoolUuid },

    #[error("Failed to adopt disk containing zpool {zpool_id}")]
    AdoptDisk {
        zpool_id: ZpoolUuid,
        #[source]
        error: DiskError,
    },

    // The errors below are all from `illumos-utils`, and already include
    // context like the name of the dataset on which we're operating.
    #[error(transparent)]
    ListDatasets(#[from] ListDatasetsError),

    #[error(transparent)]
    EnsureDataset(#[from] EnsureDatasetError),

    #[error(transparent)]
    DestroyDataset(#[from] DestroyDatasetError),

    #[error(transparent)]
    SetValues(#[from] SetValueError),

    #[error(transparent)]
    RequiredDataset(RequiredDatasetError),

    #[error("Could not check disk's required datasets")]
    DatasetTaskUnavailable(#[source] DatasetTaskError),
}

impl DiskManagementError {
    fn retryable(&self) -> bool {
        match self {
            // definitely retryable
            Self::AdoptDisk {
                zpool_id: _,
                error: DiskError::Dataset(DatasetError::KeyManager(_)),
            } => true,

            // definitely not retryable
            Self::NotFound
            | Self::InternalDiskControlPlaneRequest(_)
            | Self::ZpoolUuidMismatch { .. }
            | Self::DestroyDataset(DestroyDatasetError {
                name: _,
                err: DestroyDatasetErrorVariant::NotFound,
            }) => false,

            // might be retryable? in many of these cases we'd need more
            // information from the inner error than they expose, so we'll
            // err on the side of retrying
            Self::AdoptDisk { .. }
            | Self::ListDatasets(_)
            | Self::EnsureDataset(_)
            | Self::DestroyDataset(DestroyDatasetError {
                name: _,
                err: DestroyDatasetErrorVariant::Other(_),
            })
            | Self::SetValues(_)
            | Self::DatasetTaskUnavailable(_) => true,

            Self::RequiredDataset(err) => err.is_retryable(),
        }
    }
}

/// Set of currently managed zpools.
///
/// This handle should only be used to decide to _stop_ using a zpool (e.g., if
/// a previously-launched zone is on a zpool that is no longer managed). It does
/// not expose a means to list or choose from the currently-managed pools;
/// instead, consumers should choose mounted datasets.
///
/// This level of abstraction even for "when to stop using a zpool" is probably
/// wrong: if we choose a dataset on which to place a zone's root, we should
/// shut that zone down if the _dataset_ goes away, not the zpool. For now we
/// live with "assume the dataset bases we choose stick around as long as their
/// parent zpool does".
#[derive(Default, Debug, Clone)]
pub struct CurrentlyManagedZpools(BTreeSet<ZpoolName>);

impl CurrentlyManagedZpools {
    /// Returns true if `zpool` is currently managed.
    pub fn contains(&self, zpool: &ZpoolName) -> bool {
        self.0.contains(zpool)
    }

    /// Within this crate, directly expose the set of zpools.
    ///
    /// We never use this to "pick a zpool to use" (any choosing should be
    /// picking _datasets_, not zpools). We use it when we need to know all the
    /// zpools we have to scan for something (e.g., orphaned datasets to
    /// delete).
    pub(crate) fn iter(&self) -> impl Iterator<Item = ZpoolName> + '_ {
        self.0.iter().copied()
    }
}

/// Wrapper around a tokio watch channel containing the set of currently managed
/// zpools.
#[derive(Debug, Clone)]
pub struct CurrentlyManagedZpoolsReceiver {
    inner: CurrentlyManagedZpoolsReceiverInner,
}

#[derive(Debug, Clone)]
enum CurrentlyManagedZpoolsReceiverInner {
    Real(watch::Receiver<Arc<CurrentlyManagedZpools>>),
    #[cfg(any(test, feature = "testing"))]
    FakeDynamic(watch::Receiver<BTreeSet<ZpoolName>>),
    #[cfg(any(test, feature = "testing"))]
    FakeStatic(BTreeSet<ZpoolName>),
}

impl CurrentlyManagedZpoolsReceiver {
    #[cfg(any(test, feature = "testing"))]
    pub fn fake_dynamic(rx: watch::Receiver<BTreeSet<ZpoolName>>) -> Self {
        Self { inner: CurrentlyManagedZpoolsReceiverInner::FakeDynamic(rx) }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn fake_static(zpools: impl Iterator<Item = ZpoolName>) -> Self {
        Self {
            inner: CurrentlyManagedZpoolsReceiverInner::FakeStatic(
                zpools.collect(),
            ),
        }
    }

    pub(crate) fn new(
        rx: watch::Receiver<Arc<CurrentlyManagedZpools>>,
    ) -> Self {
        Self { inner: CurrentlyManagedZpoolsReceiverInner::Real(rx) }
    }

    /// Get the current set of managed zpools without marking the value as seen.
    ///
    /// Analogous to [`watch::Receiver::borrow()`].
    pub fn current(&self) -> Arc<CurrentlyManagedZpools> {
        match &self.inner {
            CurrentlyManagedZpoolsReceiverInner::Real(rx) => {
                Arc::clone(&*rx.borrow())
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeDynamic(rx) => {
                Arc::new(CurrentlyManagedZpools(rx.borrow().clone()))
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeStatic(zpools) => {
                Arc::new(CurrentlyManagedZpools(zpools.clone()))
            }
        }
    }

    /// Get the current set of managed zpools and mark the value as seen.
    ///
    /// Analogous to [`watch::Receiver::borrow_and_update()`].
    pub fn current_and_update(&mut self) -> Arc<CurrentlyManagedZpools> {
        match &mut self.inner {
            CurrentlyManagedZpoolsReceiverInner::Real(rx) => {
                Arc::clone(&*rx.borrow_and_update())
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeDynamic(rx) => {
                Arc::new(CurrentlyManagedZpools(rx.borrow_and_update().clone()))
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeStatic(zpools) => {
                Arc::new(CurrentlyManagedZpools(zpools.clone()))
            }
        }
    }

    /// Wait for changes in the underlying watch channel.
    ///
    /// Cancel-safe.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        match &mut self.inner {
            CurrentlyManagedZpoolsReceiverInner::Real(rx) => rx.changed().await,
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeDynamic(rx) => {
                rx.changed().await
            }
            #[cfg(any(test, feature = "testing"))]
            CurrentlyManagedZpoolsReceiverInner::FakeStatic(_) => {
                // Static set of zpools never changes
                std::future::pending().await
            }
        }
    }

    // This returns a tuple that can be converted into an `InventoryZpool`. It
    // doesn't return an `InventoryZpool` directly because the latter only
    // contains the zpool's ID, not the full name, and our caller wants the
    // names too.
    pub(crate) async fn to_inventory(
        &self,
        log: &Logger,
    ) -> Vec<(ZpoolName, ByteCount, ZpoolHealth)> {
        let current_zpools = self.current();

        let zpool_futs =
            current_zpools.0.iter().map(|&zpool_name| async move {
                let info_result =
                    Zpool::get_info(&zpool_name.to_string()).await;

                (zpool_name, info_result)
            });

        future::join_all(zpool_futs)
            .await
            .into_iter()
            .filter_map(|(zpool_name, info_result)| {
                let info = match info_result {
                    Ok(info) => info,
                    Err(err) => {
                        warn!(
                            log, "Failed to access zpool info";
                            "zpool" => %zpool_name,
                            InlineErrorChain::new(&err),
                        );
                        return None;
                    }
                };
                let total_size = match ByteCount::try_from(info.size()) {
                    Ok(n) => n,
                    Err(err) => {
                        warn!(
                            log, "Failed to parse zpool size";
                            "zpool" => %zpool_name,
                            "raw_size" => info.size(),
                            InlineErrorChain::new(&err),
                        );
                        return None;
                    }
                };
                Some((zpool_name, total_size, info.health()))
            })
            .collect()
    }
}

/// How far a newly-adopted disk has progressed toward being put into service
/// (see [`DiskState::Adopting`]).
///
/// A disk we've just adopted is not published as managed (to the rest of
/// sled-agent) until its required datasets have been ensured and its former
/// zone roots have been cleaned up. See
/// [`ExternalDisks::finish_adopting_disks()`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdoptionPhase {
    /// We've just adopted this disk, and are waiting for its required
    /// datasets to be ensured.
    AwaitingRequiredDatasets,
    /// This disk's required datasets have been ensured. Its debug dataset is
    /// visible to `DebugCollectorTask` (so former zone roots can be archived
    /// into it), but we're waiting to clean up its former zone roots before
    /// putting it into service.
    AwaitingZoneRootCleanup,
}

impl AdoptionPhase {
    fn description(&self) -> &'static str {
        match self {
            Self::AwaitingRequiredDatasets => "awaiting required datasets",
            Self::AwaitingZoneRootCleanup => {
                "awaiting cleanup of former zone roots"
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct ExternalDisks {
    disks: IdOrdMap<ExternalDiskState>,
    mount_config: Arc<MountConfig>,

    // Output channel for the set of zpools we're managing. Used by sled-agent
    // generally to decide when to _stop_ something (e.g., stopping instances
    // that were running on a zpool that's no longer available).
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,

    // Output channel for the managed disks whose debug dataset is available
    // (see `debug_dataset_disks()`). This is only consumed within this crate
    // by `DebugCollectorTask` (for managing dump devices and archiving logs).
    debug_dataset_disks_tx: watch::Sender<HashSet<Disk>>,

    // For requesting archival of former zone root directories.
    archiver: FormerZoneRootArchiver,
}

impl ExternalDisks {
    pub(super) fn new(
        mount_config: Arc<MountConfig>,
        currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
        debug_dataset_disks_tx: watch::Sender<HashSet<Disk>>,
        archiver: FormerZoneRootArchiver,
    ) -> Self {
        Self {
            disks: IdOrdMap::default(),
            mount_config,
            currently_managed_zpools_tx,
            debug_dataset_disks_tx,
            archiver,
        }
    }

    pub(crate) fn has_retryable_error(&self) -> bool {
        self.disks.iter().any(|disk| match &disk.state {
            DiskState::Managed(_) | DiskState::Adopting(..) => false,
            DiskState::FailedToManage(err) => err.retryable(),
        })
    }

    /// Returns the zpools of disks we've adopted but not yet put into service.
    pub(super) fn zpools_being_adopted(&self) -> BTreeSet<ZpoolName> {
        self.managed_disks()
            .filter(|(phase, _)| phase.is_some())
            .map(|(_, disk)| *disk.zpool_name())
            .collect()
    }

    /// Returns the zpools of all managed disks, including those we've adopted
    /// but not yet put into service.
    ///
    /// This is only for ensuring datasets; everything else should use
    /// [`Self::currently_managed_zpools()`].
    pub(super) fn all_managed_zpools(&self) -> Arc<CurrentlyManagedZpools> {
        Arc::new(CurrentlyManagedZpools(
            self.managed_disks().map(|(_, disk)| *disk.zpool_name()).collect(),
        ))
    }

    /// Returns the IDs and zpools of disks in the given adoption `phase`.
    fn disks_in_phase(
        &self,
        phase: AdoptionPhase,
    ) -> Vec<(PhysicalDiskUuid, ZpoolName)> {
        self.disks
            .iter()
            .filter_map(|disk| match &disk.state {
                DiskState::Adopting(d, p) if *p == phase => {
                    Some((disk.config.id, *d.zpool_name()))
                }
                _ => None,
            })
            .collect()
    }

    /// Un-adopt `disk_id` (which we'll retry on a later reconciliation
    /// attempt) because we couldn't put it into service.
    fn fail_adoption(
        &mut self,
        disk_id: PhysicalDiskUuid,
        error: DiskManagementError,
    ) {
        // unwrap(): Callers only pass IDs of disks we're adopting.
        let mut disk = self.disks.get_mut(&disk_id).unwrap();
        *disk = ExternalDiskState::failed(disk.config.clone(), error);
    }

    /// Un-adopt all disks we're still adopting, because we couldn't learn
    /// whether their required datasets exist.
    ///
    /// They'll be adopted again on a later reconciliation attempt.
    pub(super) fn fail_adopting_disks(
        &mut self,
        error: DatasetTaskError,
        log: &Logger,
    ) {
        for (disk_id, zpool) in
            self.disks_in_phase(AdoptionPhase::AwaitingRequiredDatasets)
        {
            warn!(
                log,
                "not putting disk into service: \
                 could not check required datasets";
                "pool" => %zpool,
                InlineErrorChain::new(&error),
            );
            self.fail_adoption(
                disk_id,
                DiskManagementError::DatasetTaskUnavailable(error.clone()),
            );
        }
        self.update_output_watch_channels();
    }

    /// Put newly-adopted disks into service, once their required datasets
    /// have been ensured (see [`OmicronDatasets::check_required_datasets()`])
    /// and their former zone roots have been archived and destroyed.
    ///
    /// Disks that fail either step are un-adopted without ever having been put
    /// into service.
    pub(super) async fn finish_adopting_disks(
        &mut self,
        datasets: &OmicronDatasets,
        log: &Logger,
    ) {
        self.finish_adopting_disks_with_cleaner(
            datasets,
            log,
            &RealZoneRootCleaner,
        )
        .await
    }

    async fn finish_adopting_disks_with_cleaner<T: ZoneRootCleaner>(
        &mut self,
        datasets: &OmicronDatasets,
        log: &Logger,
        cleaner: &T,
    ) {
        self.verify_adopted_disks(datasets, log);
        self.clean_up_adopted_disks(log, cleaner).await;
    }

    fn verify_adopted_disks(
        &mut self,
        datasets: &OmicronDatasets,
        log: &Logger,
    ) {
        for (disk_id, zpool) in
            self.disks_in_phase(AdoptionPhase::AwaitingRequiredDatasets)
        {
            match datasets.check_required_datasets(&zpool) {
                Ok(()) => {
                    // unwrap(): `disks_in_phase()` only returns IDs of disks
                    // we have.
                    let mut disk = self.disks.get_mut(&disk_id).unwrap();
                    if let DiskState::Adopting(_, phase) = &mut disk.state {
                        *phase = AdoptionPhase::AwaitingZoneRootCleanup;
                    }
                }
                Err(err) => {
                    warn!(
                        log,
                        "not putting disk into service: \
                         required dataset unavailable";
                        "pool" => %zpool,
                        InlineErrorChain::new(&err),
                    );
                    self.fail_adoption(
                        disk_id,
                        DiskManagementError::RequiredDataset(err),
                    );
                }
            }
        }
        // `DebugCollectorTask` must see verified disks' debug datasets before
        // we archive former zone roots into them.
        self.update_output_watch_channels();
    }

    async fn clean_up_adopted_disks<T: ZoneRootCleaner>(
        &mut self,
        log: &Logger,
        cleaner: &T,
    ) {
        for (disk_id, zpool_name) in
            self.disks_in_phase(AdoptionPhase::AwaitingZoneRootCleanup)
        {
            match cleaner
                .archive_and_destroy_former_zone_roots(
                    &zpool_name,
                    &self.mount_config,
                    &self.archiver,
                    log,
                )
                .await
            {
                Ok(()) => {
                    // This disk is now in service.
                    //
                    // unwrap(): `disks_in_phase()` only returns IDs of disks
                    // we have.
                    let mut disk = self.disks.get_mut(&disk_id).unwrap();
                    if let DiskState::Adopting(d, _) = &disk.state {
                        let d = d.clone();
                        disk.state = DiskState::Managed(d);
                    }
                }
                Err(error) => {
                    // Un-adopt this disk. It was never put into service, and
                    // we'll try to adopt it again on a later reconciliation.
                    error!(
                        log,
                        "failed to destroy former zone roots on pool";
                        "pool" => %zpool_name,
                        InlineErrorChain::new(&error),
                    );
                    self.fail_adoption(disk_id, error);
                }
            }
        }
        self.update_output_watch_channels();
    }

    pub(crate) fn to_inventory(
        &self,
    ) -> BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult> {
        self.disks
            .iter()
            .map(|disk| match &disk.state {
                DiskState::Managed(_) => {
                    (disk.config.id, ConfigReconcilerInventoryResult::Ok)
                }
                // Shouldn't happen (see `DiskState::Adopting`).
                DiskState::Adopting(_, phase) => (
                    disk.config.id,
                    ConfigReconcilerInventoryResult::Err {
                        message: format!(
                            "not yet in service: {}",
                            phase.description()
                        ),
                    },
                ),
                DiskState::FailedToManage(err) => (
                    disk.config.id,
                    ConfigReconcilerInventoryResult::Err {
                        message: InlineErrorChain::new(err).to_string(),
                    },
                ),
            })
            .collect()
    }

    pub(super) fn currently_managed_zpools(
        &self,
    ) -> Arc<CurrentlyManagedZpools> {
        Arc::clone(&*self.currently_managed_zpools_tx.borrow())
    }

    /// Returns rekey info for all managed disks.
    pub(super) fn disk_rekey_info(
        &self,
    ) -> impl Iterator<Item = DiskRekeyInfo<'_>> {
        self.disks.iter().filter_map(|disk_state| {
            disk_state.state.adopted_disk().map(|disk| DiskRekeyInfo {
                disk,
                disk_id: disk_state.config.id,
                cached_epoch: disk_state.epoch,
            })
        })
    }

    /// Apply the results of a rekey operation, updating cached epochs for succeeded disks.
    pub(super) fn apply_rekey_result(
        &mut self,
        result: &RekeyResult,
        target_epoch: Epoch,
    ) {
        for &disk_id in &result.succeeded {
            if let Some(mut disk_state) = self.disks.get_mut(&disk_id) {
                disk_state.epoch = Some(target_epoch);
            }
        }
    }

    /// Returns each managed disk, along with its adoption phase if we're
    /// still adopting it.
    fn managed_disks(
        &self,
    ) -> impl Iterator<Item = (Option<&AdoptionPhase>, &Disk)> {
        self.disks.iter().filter_map(|disk| match &disk.state {
            DiskState::Managed(d) => Some((None, d)),
            DiskState::Adopting(d, phase) => Some((Some(phase), d)),
            DiskState::FailedToManage(_) => None,
        })
    }

    /// Returns the managed disks to make visible to `DebugCollectorTask`:
    /// those whose required datasets have been verified.
    ///
    /// Once verified, a debug dataset stays mounted, so we don't re-check it
    /// (e.g., after a later failure to update its properties).
    fn debug_dataset_disks(&self) -> HashSet<Disk> {
        self.managed_disks()
            .filter(|(phase, _)| {
                *phase != Some(&AdoptionPhase::AwaitingRequiredDatasets)
            })
            .map(|(_, disk)| disk.clone())
            .collect()
    }

    fn update_output_watch_channels(&self) {
        // Disks we're still adopting are not yet in service.
        let current_zpools: BTreeSet<_> = self
            .managed_disks()
            .filter(|(phase, _)| phase.is_none())
            .map(|(_, disk)| *disk.zpool_name())
            .collect();
        let debug_dataset_disks = self.debug_dataset_disks();
        self.debug_dataset_disks_tx.send_if_modified(|disks| {
            if *disks == debug_dataset_disks {
                false
            } else {
                *disks = debug_dataset_disks;
                true
            }
        });

        self.currently_managed_zpools_tx.send_if_modified(|zpools| {
            if zpools.0 == current_zpools {
                false
            } else {
                *zpools = Arc::new(CurrentlyManagedZpools(current_zpools));
                true
            }
        });
    }

    /// Retain all disks that we are supposed to manage (based on `config`) that
    /// are also physically present (based on `raw_disks`), removing any disks
    /// we'd previously started to manage that are no longer present in either
    /// set.
    pub(super) fn stop_managing_if_needed(
        &mut self,
        raw_disks: &IdOrdMap<RawDisk>,
        config: &IdOrdMap<OmicronPhysicalDiskConfig>,
        log: &Logger,
    ) {
        debug_assert!(
            self.disks
                .iter()
                .all(|d| !matches!(d.state, DiskState::Adopting(..))),
            "disks should not be left in `DiskState::Adopting`",
        );

        let mut disk_ids_to_remove = Vec::new();
        let mut marked_disk_not_found = false;

        for mut disk in &mut self.disks {
            let disk_id = disk.config.id;
            if !config.contains_key(&disk_id) {
                info!(
                    log,
                    "removing managed disk: no longer present in config";
                    "disk_id" => %disk_id,
                    "disk" => ?disk.config.identity,
                );
                disk_ids_to_remove.push(disk_id);
            } else if !raw_disks.contains_key(&disk.config.identity) {
                // Disk is still present in config, but no longer available:
                // make sure we've set the state appropriately.
                if !matches!(
                    disk.state,
                    DiskState::FailedToManage(DiskManagementError::NotFound)
                ) {
                    warn!(
                        log,
                        "removing managed disk: still present in config, \
                         but no longer available from OS";
                        "disk_id" => %disk_id,
                        "disk" => ?disk.config.identity,
                    );
                    disk.state = DiskState::FailedToManage(
                        DiskManagementError::NotFound,
                    );
                    marked_disk_not_found = true;
                }
            }
        }

        // Remove the disks not present in `config`.
        for disk_id in &disk_ids_to_remove {
            self.disks.remove(disk_id);
        }

        // If we made any changes, update the set of disks visbile to external
        // consumers. (It would be correct to call this unconditionally, but we
        // can save a bit of work by skipping it in the common case of "no disks
        // were removed".)
        if !disk_ids_to_remove.is_empty() || marked_disk_not_found {
            self.update_output_watch_channels();
        }
    }

    /// Attempt to start managing any disks specified by `config` that we aren't
    /// already managing.
    ///
    /// Newly-adopted disks are not put into service until
    /// [`Self::finish_adopting_disks()`].
    pub(super) async fn start_managing_if_needed(
        &mut self,
        raw_disks: &IdOrdMap<RawDisk>,
        config: &IdOrdMap<OmicronPhysicalDiskConfig>,
        key_requester: &StorageKeyRequester,
        log: &Logger,
    ) {
        self.start_managing_if_needed_with_disk_adopter(
            raw_disks,
            config,
            log,
            &RealDiskAdopter { key_requester },
        )
        .await
    }

    async fn start_managing_if_needed_with_disk_adopter<T: DiskAdopter>(
        &mut self,
        raw_disks: &IdOrdMap<RawDisk>,
        config: &IdOrdMap<OmicronPhysicalDiskConfig>,
        log: &Logger,
        disk_adopter: &T,
    ) {
        // Loop over all the disks in `config`, and collect for each either a
        // future to ensure we're managing the disk (the common case) or an
        // error (if we know we can't manage it based on just our inputs alone).
        let mut try_ensure_managed_futures = Vec::new();
        let mut failed_disk_states = Vec::new();

        for config in config.iter().cloned() {
            // We can only manage disks if the raw disk is present.
            let Some(raw_disk) = raw_disks.get(&config.identity) else {
                warn!(
                    log,
                    "Control plane disk requested, but not detected within sled";
                    "disk_identity" => ?&config.identity
                );
                let err = DiskManagementError::NotFound;
                failed_disk_states.push(ExternalDiskState::failed(config, err));
                continue;
            };

            // Refuse to manage internal disks.
            match raw_disk.variant() {
                DiskVariant::U2 => (),
                DiskVariant::M2 => {
                    warn!(
                        log,
                        "Control plane requested management of internal disk";
                        "config" => ?config,
                    );
                    let err =
                        DiskManagementError::InternalDiskControlPlaneRequest(
                            config.id,
                        );
                    failed_disk_states
                        .push(ExternalDiskState::failed(config, err));
                    continue;
                }
            }

            try_ensure_managed_futures.push(self.try_ensure_disk_managed(
                self.disks.get(&config.id),
                config,
                raw_disk,
                disk_adopter,
                log,
            ));
        }

        // Run all the disk management futures concurrently...
        let disk_states = future::join_all(try_ensure_managed_futures).await;

        // Then record the new states for each disk in `config`.
        for disk_state in failed_disk_states.into_iter().chain(disk_states) {
            self.disks.insert_overwrite(disk_state);
        }

        self.update_output_watch_channels();
    }

    async fn try_ensure_disk_managed<T: DiskAdopter>(
        &self,
        current: Option<&ExternalDiskState>,
        config: OmicronPhysicalDiskConfig,
        raw_disk: &RawDisk,
        disk_adopter: &T,
        log: &Logger,
    ) -> ExternalDiskState {
        match current {
            // If we're already managing this disk, check whether there are any
            // new properties to update.
            Some(ExternalDiskState {
                state: DiskState::Managed(disk),
                epoch,
                ..
            }) => match self
                .update_disk_properties(disk, &config, raw_disk, log)
            {
                Ok(disk) => ExternalDiskState::managed(config, disk, *epoch),
                Err(err) => ExternalDiskState::failed(config, err),
            },
            // Shouldn't happen (see `DiskState::Adopting`). If it does, update
            // its properties, but leave its adoption phase alone.
            Some(ExternalDiskState {
                state: DiskState::Adopting(disk, phase),
                epoch,
                ..
            }) => match self
                .update_disk_properties(disk, &config, raw_disk, log)
            {
                Ok(disk) => {
                    ExternalDiskState::adopting(config, disk, *phase, *epoch)
                }
                Err(err) => ExternalDiskState::failed(config, err),
            },
            // If we previously failed to manage this disk, try again.
            Some(ExternalDiskState {
                state: DiskState::FailedToManage(prev_err),
                ..
            }) => {
                info!(
                    log, "Retrying management of disk";
                    "disk_identity" => ?config.identity,
                    "prev_err" => InlineErrorChain::new(&prev_err),
                );
                self.start_managing_disk(
                    config,
                    raw_disk.clone(),
                    disk_adopter,
                    log,
                )
                .await
            }
            // If we're not managing this disk, try to.
            None => {
                info!(
                    log, "Starting management of disk";
                    "disk_identity" => ?config.identity,
                );
                self.start_managing_disk(
                    config,
                    raw_disk.clone(),
                    disk_adopter,
                    log,
                )
                .await
            }
        }
    }

    fn update_disk_properties(
        &self,
        disk: &Disk,
        config: &OmicronPhysicalDiskConfig,
        raw_disk: &RawDisk,
        log: &Logger,
    ) -> Result<Disk, DiskManagementError> {
        // Make sure the incoming config's zpool ID matches our
        // previously-managed disk's.
        if disk.zpool_name().id() != config.pool_id {
            let expected = config.pool_id;
            let observed = disk.zpool_name().id();
            let err =
                DiskManagementError::ZpoolUuidMismatch { expected, observed };
            warn!(
                log,
                "Observed an unexpected zpool uuid";
                "disk_identity" => ?config.identity,
                InlineErrorChain::new(&err),
            );
            return Err(err);
        }

        // Update any properties that have changed from `disk` based on the
        // current `raw_disk`. We don't do anything different whether or not any
        // changes were actually made.
        let disk = match update_properties_from_raw_disk(disk, raw_disk, log) {
            MaybeUpdatedDisk::Updated(disk) => disk,
            MaybeUpdatedDisk::Unchanged => disk.clone(),
        };

        Ok(disk)
    }

    async fn start_managing_disk<T: DiskAdopter>(
        &self,
        config: OmicronPhysicalDiskConfig,
        raw_disk: RawDisk,
        disk_adopter: &T,
        log: &Logger,
    ) -> ExternalDiskState {
        match disk_adopter
            .adopt_disk(raw_disk, &self.mount_config, config.pool_id, log)
            .await
        {
            Ok(AdoptedDisk { disk, epoch }) => {
                info!(
                    log, "Successfully started management of disk";
                    "disk_identity" => ?config.identity,
                    "epoch" => ?epoch,
                );
                ExternalDiskState::adopting(
                    config,
                    disk,
                    AdoptionPhase::AwaitingRequiredDatasets,
                    epoch,
                )
            }
            Err(err) => {
                warn!(
                    log, "Disk adoption failed";
                    "disk_identity" => ?config.identity,
                    InlineErrorChain::new(&err),
                );
                ExternalDiskState::failed(config, err)
            }
        }
    }
}

#[derive(Debug)]
struct ExternalDiskState {
    config: OmicronPhysicalDiskConfig,
    state: DiskState,
    /// The current encryption epoch for this disk's crypt dataset.
    /// None if the disk is not yet managed or doesn't have encryption.
    epoch: Option<Epoch>,
}

impl ExternalDiskState {
    fn managed(
        config: OmicronPhysicalDiskConfig,
        disk: Disk,
        epoch: Option<Epoch>,
    ) -> Self {
        Self { config, state: DiskState::Managed(disk), epoch }
    }

    fn adopting(
        config: OmicronPhysicalDiskConfig,
        disk: Disk,
        phase: AdoptionPhase,
        epoch: Option<Epoch>,
    ) -> Self {
        Self { config, state: DiskState::Adopting(disk, phase), epoch }
    }

    fn failed(
        config: OmicronPhysicalDiskConfig,
        err: DiskManagementError,
    ) -> Self {
        Self { config, state: DiskState::FailedToManage(err), epoch: None }
    }
}

impl IdOrdItem for ExternalDiskState {
    type Key<'a> = PhysicalDiskUuid;

    fn key(&self) -> Self::Key<'_> {
        self.config.id
    }

    id_upcast!();
}

#[derive(Debug)]
enum DiskState {
    /// We're managing this disk, and it's in service.
    Managed(Disk),
    /// We've adopted this disk, but haven't put it into service yet.
    ///
    /// Disks are only in this state partway through a reconciliation pass: by
    /// the end of it, we've either put them into service or given up on them
    /// (in which case we'll try to adopt them again on a later pass).
    Adopting(Disk, AdoptionPhase),
    FailedToManage(DiskManagementError),
}

impl DiskState {
    /// Returns the disk if we've adopted it, whether or not it's in service.
    fn adopted_disk(&self) -> Option<&Disk> {
        match self {
            Self::Managed(disk) | Self::Adopting(disk, _) => Some(disk),
            Self::FailedToManage(_) => None,
        }
    }
}

/// Result of successfully adopting a disk.
struct AdoptedDisk {
    /// The adopted disk.
    disk: Disk,
    /// The current encryption epoch for this disk's crypt dataset.
    /// None if the disk doesn't have encryption or the epoch could not be read.
    epoch: Option<Epoch>,
}

/// Helper to allow unit tests to run without interacting with the real [`Disk`]
/// implementation. In production, the only implementor of this trait is
/// [`RealDiskAdopter`].
trait DiskAdopter {
    /// Adopt a disk, returning the disk and its current encryption epoch.
    ///
    /// The epoch is read from the oxide:epoch property on the crypt dataset
    /// after successful adoption. Returns `None` for the epoch if the disk
    /// is not encrypted or the epoch could not be read.
    fn adopt_disk(
        &self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        pool_id: ZpoolUuid,
        log: &Logger,
    ) -> impl Future<Output = Result<AdoptedDisk, DiskManagementError>> + Send;
}

/// Helper to allow unit tests to run without destroying real datasets. In
/// production, the only implementor of this trait is [`RealZoneRootCleaner`].
trait ZoneRootCleaner {
    fn archive_and_destroy_former_zone_roots(
        &self,
        zpool_name: &ZpoolName,
        mount_config: &MountConfig,
        archiver: &FormerZoneRootArchiver,
        log: &Logger,
    ) -> impl Future<Output = Result<(), DiskManagementError>> + Send;
}

struct RealDiskAdopter<'a> {
    key_requester: &'a StorageKeyRequester,
}

impl DiskAdopter for RealDiskAdopter<'_> {
    async fn adopt_disk(
        &self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        pool_id: ZpoolUuid,
        log: &Logger,
    ) -> Result<AdoptedDisk, DiskManagementError> {
        let disk = Disk::new(
            log,
            mount_config,
            raw_disk,
            Some(pool_id),
            Some(self.key_requester),
        )
        .await
        .map_err(|error| DiskManagementError::AdoptDisk {
            zpool_id: pool_id,
            error,
        })?;

        // Read the epoch from the crypt dataset after successful adoption.
        // This tells us what encryption key the disk is currently using.
        let crypt_dataset = format!("{}/{}", disk.zpool_name(), CRYPT_DATASET);
        let epoch = match Zfs::get_oxide_value(&crypt_dataset, "epoch").await {
            Ok(epoch_str) => match epoch_str.parse::<u64>() {
                Ok(epoch_val) => {
                    debug!(
                        log,
                        "Read epoch from adopted disk";
                        "zpool" => %disk.zpool_name(),
                        "epoch" => epoch_val,
                    );
                    // ZFS stores epoch as u64; we wrap it in the Epoch
                    // newtype for type safety in the reconciler.
                    Some(Epoch(epoch_val))
                }
                Err(e) => {
                    warn!(
                        log,
                        "Failed to parse epoch from adopted disk";
                        "zpool" => %disk.zpool_name(),
                        "epoch_str" => &epoch_str,
                        "error" => %e,
                    );
                    None
                }
            },
            Err(e) => {
                // This could happen if the disk doesn't have an encrypted
                // crypt dataset (shouldn't happen in production) or if there
                // was an error reading the property.
                warn!(
                    log,
                    "Failed to read epoch from adopted disk";
                    "zpool" => %disk.zpool_name(),
                    InlineErrorChain::new(&e),
                );
                None
            }
        };

        Ok(AdoptedDisk { disk, epoch })
    }
}

struct RealZoneRootCleaner;

impl ZoneRootCleaner for RealZoneRootCleaner {
    async fn archive_and_destroy_former_zone_roots(
        &self,
        zpool_name: &ZpoolName,
        mount_config: &MountConfig,
        archiver: &FormerZoneRootArchiver,
        log: &Logger,
    ) -> Result<(), DiskManagementError> {
        // Attempt to archive and then wipe the contents of the zones dataset.
        //
        // There's a chain of design goals and compromises here:
        //
        // In general, across the control plane, we want to carefully manage
        // persistent storage in a way that will ensure the system's fault
        // tolerance.  Important data generally needs to be stored in
        // CockroachDB or some other replicated storage, not the local
        // filesystem.  We want some guard rails to prevent developers from
        // accidentally using the local filesystem to store important data that
        // really ought to be replicated.
        //
        // In an ideal world, we might make the root filesystem read-only
        // altogether or at least isolate the parts that really need to be
        // writeable (e.g., for logging) from the rest of it.  But that's a fair
        // bit of work we haven't done yet.
        //
        // Instead, we make zone root filesystems transient, which is to say
        // that their contents are not preserved after every kind of restart.
        // But we still need to put the data somewhere, and it should be on disk
        // rather than in memory, so we still use these ZFS pools for them.
        // That means we have to wipe that data at some point.  And before
        // wiping it, we want to archive any log files for debugging.
        //
        // So, when should we archive and wipe zone root filesystems?  In an
        // ideal world, we'd do it each time the zone starts (to make sure we
        // wipe them even if the sled reboots unexpectedly) as well as when the
        // zone halts (to make sure we archive files from zones that will never
        // start again).  See oxidecomputer/omicron#8316.  But this too is
        // tricky and we haven't done this work yet.
        //
        // So instead, we take a pretty blunt hammer: the first time we adopt
        // any disk in the lifetime of this sled agent process, we archive and
        // destroy all the zone root filesystems on it.
        //
        // To determine whether we've already done this, we construct a unique
        // value once in the lifetime of each sled agent process.  After we
        // destroy and re-create the dataset, we'll set this property.
        //
        // ---
        //
        // It is also worth noting that it's conceivable that we find a zoneroot
        // here for a zone that is still running.  This could happen if we're
        // doing the first adoption of disks after sled agent restarts.  In that
        // case, we will wind up archiving (and deleting) its log files out from
        // under it.  We deem this okay because in this case, we're about to
        // restart that zone anyway.
        static AGENT_LOCAL_VALUE: OnceLock<String> = OnceLock::new();
        let agent_local_value = AGENT_LOCAL_VALUE
            .get_or_init(|| Alphanumeric.sample_string(&mut rand::rng(), 20));

        let zone_dataset_name = format!("{}/{}", zpool_name, ZONE_DATASET);
        match Zfs::get_oxide_value(&zone_dataset_name, "agent").await {
            Ok(v) if &v == agent_local_value => {
                info!(
                    log,
                    "Skipping automatic archive/wipe of dataset: {}",
                    zone_dataset_name
                );
            }
            Ok(_) | Err(_) => {
                info!(
                    log,
                    "Automatically archiving/wipe of dataset: {}",
                    zone_dataset_name
                );
                cleanup_former_zone_roots(
                    log,
                    mount_config,
                    archiver,
                    &zpool_name,
                )
                .await?;
                Zfs::set_oxide_value(
                    &zone_dataset_name,
                    "agent",
                    agent_local_value,
                )
                .await?;
            }
        };

        Ok(())
    }
}

/// Given a pool name, find any zone root filesystems, attempt to archive their
/// log files, and destroy them.
async fn cleanup_former_zone_roots(
    log: &Logger,
    mount_config: &MountConfig,
    archiver: &FormerZoneRootArchiver,
    zpool_name: &ZpoolName,
) -> Result<(), DiskManagementError> {
    // Within each pool, ZONE_DATASET is the name of the dataset that's the
    // parent of all the zone root filesystems' datasets.
    let parent_dataset_name = format!("{}/{}", zpool_name, ZONE_DATASET);
    let child_datasets = Zfs::list_datasets(&parent_dataset_name).await?;

    for child_name in child_datasets {
        // Determine the mountpoint of the child dataset.
        // `dataset_mountpoint()` expects a path relative to the root of the
        // pool.  We could chop off the zpool_name from `parent_dataset_name`,
        // or (what we do here) construct the name we need directly.
        //
        // This works only because ZONE_DATASET itself is relative to the root
        // of the pool.
        let child_dataset_relative_to_pool =
            format!("{}/{}", ZONE_DATASET, child_name);
        let mountpoint = zpool_name.dataset_mountpoint(
            &mount_config.root,
            &child_dataset_relative_to_pool,
        );

        // We need this dataset to be mounted in order to archive its logs.
        // On initial sled boot, it won't be mounted yet.  In other cases (e.g.,
        // sled-agent restart), it may already be.
        let child_dataset_name =
            format!("{}/{}", parent_dataset_name, child_name);
        debug!(
            log,
            "ensuring dataset mounted to archive former zone root";
            "path" => %mountpoint,
            "dataset" => &child_dataset_name,
        );
        Zfs::ensure_dataset_mounted_and_exists(
            &child_dataset_name,
            &Mountpoint(Utf8PathBuf::from(&mountpoint)),
        )
        .await?;

        // Attempt to archive this dataset as though it's a former zone root.
        // This is best-effort.
        info!(
            log,
            "archiving logs from former zone root";
            "path" => %mountpoint
        );
        archiver.archive_former_zone_root(mountpoint).await;

        // Finally, destroy it.  This preserves historical behavior of wiping
        // these datasets when we adopt disks.
        info!(
            log,
            "destroying former zone root";
            "dataset_name" => &child_dataset_name,
        );
        Zfs::destroy_dataset(&child_dataset_name).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use illumos_utils::zpool::ZpoolName;
    use omicron_common::disk::DatasetName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types::disk::DatasetConfig;
    use sled_agent_types::disk::DiskIdentity;
    use sled_agent_types::disk::PerDiskDatasetKind;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_hardware::UnparsedDisk;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use strum::IntoEnumIterator;
    use test_strategy::proptest;

    #[derive(Debug, Default)]
    struct TestDiskAdopter {
        requests: Mutex<Vec<RawDisk>>,
    }

    impl DiskAdopter for TestDiskAdopter {
        async fn adopt_disk(
            &self,
            raw_disk: RawDisk,
            _mount_config: &MountConfig,
            pool_id: ZpoolUuid,
            _log: &Logger,
        ) -> Result<AdoptedDisk, DiskManagementError> {
            // ExternalDisks should only adopt U2 disks
            assert_eq!(raw_disk.variant(), DiskVariant::U2);
            let disk = Disk::Real(PooledDisk {
                paths: DiskPaths {
                    devfs_path: "/fake-disk".into(),
                    dev_path: None,
                },
                slot: raw_disk.slot(),
                identity: raw_disk.identity().clone(),
                is_boot_disk: raw_disk.is_boot_disk(),
                partitions: vec![],
                zpool_name: ZpoolName::new_external(pool_id),
                firmware: raw_disk.firmware().clone(),
            });
            self.requests.lock().unwrap().push(raw_disk);
            // In tests, use epoch 0 as the initial epoch
            Ok(AdoptedDisk { disk, epoch: Some(Epoch(0)) })
        }
    }

    // All our tests operate on fake in-memory disks, so the mount config
    // shouldn't matter. Populate something that won't exist on real systems so
    // if we miss something and try to operate on a real disk it will fail.
    fn nonexistent_mount_config() -> Arc<MountConfig> {
        Arc::new(MountConfig {
            root: "/tmp/test-external-disks/bogus/root".into(),
            synthetic_disk_root: "/tmp/test-external-disks/bogus/disk".into(),
        })
    }

    fn make_raw_test_disk(variant: DiskVariant, serial: &str) -> RawDisk {
        RawDisk::Real(UnparsedDisk::new(
            "/test-devfs".into(),
            None,
            0,
            variant,
            DiskIdentity {
                vendor: "test".into(),
                model: "test".into(),
                serial: serial.into(),
            },
            false,
            DiskFirmware::new(0, None, false, 1, vec![]),
        ))
    }

    fn with_test_runtime<Fut, T>(fut: Fut) -> T
    where
        Fut: Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("tokio Runtime built successfully");
        runtime.block_on(fut)
    }

    /// Start managing disks, and put any we adopt into service (as though
    /// their required datasets exist, and cleaning up their former zone roots
    /// succeeds).
    async fn start_managing_and_put_into_service(
        external_disks: &mut ExternalDisks,
        raw_disks: &IdOrdMap<RawDisk>,
        config_disks: &IdOrdMap<OmicronPhysicalDiskConfig>,
        disk_adopter: &TestDiskAdopter,
        log: &Logger,
    ) {
        external_disks
            .start_managing_if_needed_with_disk_adopter(
                raw_disks,
                config_disks,
                log,
                disk_adopter,
            )
            .await;
        let datasets =
            required_datasets_on(&external_disks.zpools_being_adopted());
        external_disks
            .finish_adopting_disks_with_cleaner(
                &datasets,
                log,
                &TestZoneRootCleaner::default(),
            )
            .await;
    }

    // Check that the contents of `currently_managed_zpools_tx` are consistent
    // with the contents of `disks`.
    #[track_caller]
    fn assert_currently_managed_zpools_is_consistent(
        external_disks: &ExternalDisks,
    ) {
        let expected_current_pools = external_disks
            .disks
            .iter()
            .filter_map(|d| match &d.state {
                DiskState::Managed(disk) => Some(*disk.zpool_name()),
                DiskState::Adopting(..) | DiskState::FailedToManage(_) => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            expected_current_pools,
            external_disks.currently_managed_zpools_tx.borrow().0
        );
    }

    // If the control plane asks for managed internal disks, we refuse.
    #[proptest]
    fn internal_disks_are_rejected(disks: BTreeMap<String, bool>) {
        let disks = disks
            .into_iter()
            .map(|(serial, is_internal)| {
                let variant =
                    if is_internal { DiskVariant::M2 } else { DiskVariant::U2 };
                make_raw_test_disk(variant, &serial)
            })
            .collect();
        with_test_runtime(async move {
            internal_disks_are_rejected_impl(disks).await
        })
    }

    async fn internal_disks_are_rejected_impl(raw_disks: IdOrdMap<RawDisk>) {
        let logctx = dev::test_setup_log("internal_disks_are_rejected");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let (debug_dataset_disks_tx, _rx) = watch::channel(HashSet::default());
        let archiver = FormerZoneRootArchiver::noop(&logctx.log);
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
            debug_dataset_disks_tx,
            archiver,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Claim the control plane wants to manage all disks. (This is bogus:
        // we should never try to manage internal disks.)
        let config_disks = raw_disks
            .iter()
            .map(|disk| OmicronPhysicalDiskConfig {
                identity: disk.identity().clone(),
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            })
            .collect::<IdOrdMap<_>>();

        // This should partially succeed: we should adopt the U.2s and report
        // errors on the M.2s.
        let disk_adopter = TestDiskAdopter::default();
        start_managing_and_put_into_service(
            &mut external_disks,
            &raw_disks,
            &config_disks,
            &disk_adopter,
            &logctx.log,
        )
        .await;

        // We should only have attempted disk adoptions for external disks.
        let num_external =
            raw_disks.iter().filter(|d| d.variant() == DiskVariant::U2).count();
        {
            let requests = disk_adopter.requests.lock().unwrap();
            assert_eq!(requests.len(), num_external);
            assert!(
                requests.iter().all(|req| req.variant() == DiskVariant::U2),
                "found non-U2 disk adoption request: {:?}",
                disk_adopter.requests
            );
        }

        // Ensure each disk is in the state we expect: either adopted or
        // reported as an error.
        for disk in &config_disks {
            let disk_state = &external_disks
                .disks
                .get(&disk.id)
                .expect("all config disks have entries")
                .state;
            match raw_disks.get(&disk.identity).unwrap().variant() {
                DiskVariant::U2 => match disk_state {
                    DiskState::Managed(_) => (),
                    _ => panic!("unexpected state: {disk_state:?}"),
                },
                DiskVariant::M2 => match disk_state {
                    DiskState::FailedToManage(
                        DiskManagementError::InternalDiskControlPlaneRequest(
                            id,
                        ),
                    ) if *id == disk.id => (),
                    _ => panic!("unexpected state: {disk_state:?}"),
                },
            }
        }

        // All the zpools for the external disks should be reported as managed.
        assert_currently_managed_zpools_is_consistent(&external_disks);

        logctx.cleanup_successful();
    }

    // Report errors for any requested disks that don't exist.
    #[proptest]
    fn fail_if_disk_not_present(disks: BTreeMap<String, bool>) {
        let mut raw_disks = IdOrdMap::default();
        let mut config_disks = IdOrdMap::default();
        let mut not_present = BTreeSet::new();

        for (serial, is_present) in disks {
            let raw_disk = make_raw_test_disk(DiskVariant::U2, &serial);
            let config_disk = OmicronPhysicalDiskConfig {
                identity: raw_disk.identity().clone(),
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            };
            if is_present {
                raw_disks.insert_overwrite(raw_disk);
            } else {
                not_present.insert(config_disk.id);
            }
            config_disks.insert_overwrite(config_disk);
        }

        with_test_runtime(async move {
            fail_if_disk_not_present_impl(raw_disks, config_disks, not_present)
                .await
        })
    }

    async fn fail_if_disk_not_present_impl(
        raw_disks: IdOrdMap<RawDisk>,
        config_disks: IdOrdMap<OmicronPhysicalDiskConfig>,
        not_present: BTreeSet<PhysicalDiskUuid>,
    ) {
        let logctx = dev::test_setup_log("fail_if_disk_not_present");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let (debug_dataset_disks_tx, _rx) = watch::channel(HashSet::default());
        let archiver = FormerZoneRootArchiver::noop(&logctx.log);
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
            debug_dataset_disks_tx,
            archiver,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Attempt to adopt all the config disks.
        let disk_adopter = TestDiskAdopter::default();
        start_managing_and_put_into_service(
            &mut external_disks,
            &raw_disks,
            &config_disks,
            &disk_adopter,
            &logctx.log,
        )
        .await;

        // Ensure each disk is in the state we expect: either adopted (if the
        // corresponding disk was present) or reported as an error (if not).
        for disk in &config_disks {
            let disk = external_disks
                .disks
                .get(&disk.id)
                .expect("all config disks have entries");
            if not_present.contains(&disk.config.id) {
                assert_matches!(
                    disk.state,
                    DiskState::FailedToManage(DiskManagementError::NotFound)
                );
            } else {
                assert_matches!(
                    &disk.state,
                    DiskState::Managed(d)
                        if *d.identity() == disk.config.identity
                );
            }
        }

        // All the zpools for the external disks should be reported as managed.
        assert_currently_managed_zpools_is_consistent(&external_disks);

        logctx.cleanup_successful();
    }

    // Stop managing disks if so requested.
    #[proptest]
    fn firmware_updates_are_propagated(disks: BTreeMap<String, bool>) {
        let mut raw_disks = IdOrdMap::default();
        let mut config_disks = IdOrdMap::default();
        let mut should_mutate_firmware = BTreeSet::new();

        for (serial, should_mutate) in disks {
            let raw_disk = make_raw_test_disk(DiskVariant::U2, &serial);
            let config_disk = OmicronPhysicalDiskConfig {
                identity: raw_disk.identity().clone(),
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            };
            if should_mutate {
                should_mutate_firmware.insert(raw_disk.identity().clone());
            }
            raw_disks.insert_overwrite(raw_disk);
            config_disks.insert_overwrite(config_disk);
        }

        with_test_runtime(async move {
            firmware_updates_are_propagated_impl(
                raw_disks,
                config_disks,
                should_mutate_firmware,
            )
            .await
        })
    }

    async fn firmware_updates_are_propagated_impl(
        mut raw_disks: IdOrdMap<RawDisk>,
        config_disks: IdOrdMap<OmicronPhysicalDiskConfig>,
        should_mutate_firmware: BTreeSet<DiskIdentity>,
    ) {
        let logctx = dev::test_setup_log("firmware_updates_are_propagated");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let (debug_dataset_disks_tx, _rx) = watch::channel(HashSet::default());
        let archiver = FormerZoneRootArchiver::noop(&logctx.log);
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
            debug_dataset_disks_tx,
            archiver,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Attempt to adopt all the config disks.
        let disk_adopter = TestDiskAdopter::default();
        start_managing_and_put_into_service(
            &mut external_disks,
            &raw_disks,
            &config_disks,
            &disk_adopter,
            &logctx.log,
        )
        .await;

        // All of them should have succeeded.
        for disk in &config_disks {
            let disk = external_disks
                .disks
                .get(&disk.id)
                .expect("all config disks have entries");
            assert_matches!(
                &disk.state,
                DiskState::Managed(d)
                    if *d.identity() == disk.config.identity
            );
        }
        assert_currently_managed_zpools_is_consistent(&external_disks);

        // Change the firmware on some subset of disks.
        for id in should_mutate_firmware {
            let mut entry = raw_disks.get_mut(&id).unwrap();
            let mut raw_disk = entry.clone();
            let new_firmware = DiskFirmware::new(
                raw_disk.firmware().active_slot().wrapping_add(1),
                None,
                false,
                1,
                Vec::new(),
            );
            *raw_disk.firmware_mut() = new_firmware;
            *entry = raw_disk;
        }

        // Attempt to adopt all the config disks again; we should pick up the
        // new firmware.
        start_managing_and_put_into_service(
            &mut external_disks,
            &raw_disks,
            &config_disks,
            &disk_adopter,
            &logctx.log,
        )
        .await;

        // All of them should have succeeded and have matching firmware to their
        // corresponding raw disk.
        assert_eq!(external_disks.disks.len(), config_disks.len());
        for disk in &config_disks {
            let disk = external_disks
                .disks
                .get(&disk.id)
                .expect("all config disks have entries");
            let raw_disk = raw_disks.get(&disk.config.identity).unwrap();
            match &disk.state {
                DiskState::Managed(disk) => {
                    assert_eq!(disk.firmware(), raw_disk.firmware());
                }
                other => panic!("unexpecte disk state {other:?}"),
            }
        }
        assert_currently_managed_zpools_is_consistent(&external_disks);

        logctx.cleanup_successful();
    }

    // Check that firmware changes from `RawDisk`s propagate out to our
    // `ExternalDiskState`.
    #[proptest]
    fn remove_disks_not_in_config(disks: BTreeMap<String, bool>) {
        let mut raw_disks = IdOrdMap::default();
        let mut config_disks = IdOrdMap::default();
        let mut should_remove_after_adding = BTreeSet::new();

        for (serial, should_remove) in disks {
            let raw_disk = make_raw_test_disk(DiskVariant::U2, &serial);
            let config_disk = OmicronPhysicalDiskConfig {
                identity: raw_disk.identity().clone(),
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            };
            if should_remove {
                should_remove_after_adding.insert(config_disk.id);
            }
            raw_disks.insert_overwrite(raw_disk);
            config_disks.insert_overwrite(config_disk);
        }

        with_test_runtime(async move {
            remove_disks_not_in_config_impl(
                raw_disks,
                config_disks,
                should_remove_after_adding,
            )
            .await
        })
    }

    async fn remove_disks_not_in_config_impl(
        raw_disks: IdOrdMap<RawDisk>,
        mut config_disks: IdOrdMap<OmicronPhysicalDiskConfig>,
        should_remove_after_adding: BTreeSet<PhysicalDiskUuid>,
    ) {
        let logctx = dev::test_setup_log("remove_disks_not_in_config");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let (debug_dataset_disks_tx, _rx) = watch::channel(HashSet::default());
        let archiver = FormerZoneRootArchiver::noop(&logctx.log);
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
            debug_dataset_disks_tx,
            archiver,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Attempt to adopt all the config disks.
        let disk_adopter = TestDiskAdopter::default();
        start_managing_and_put_into_service(
            &mut external_disks,
            &raw_disks,
            &config_disks,
            &disk_adopter,
            &logctx.log,
        )
        .await;

        // All of them should have succeeded.
        for disk in &config_disks {
            let disk = external_disks
                .disks
                .get(&disk.id)
                .expect("all config disks have entries");
            assert_matches!(
                &disk.state,
                DiskState::Managed(d)
                    if *d.identity() == disk.config.identity
            );
        }
        assert_currently_managed_zpools_is_consistent(&external_disks);

        // Drop some subset of them.
        config_disks.retain(|d| !should_remove_after_adding.contains(&d.id));

        // Stop managing them.
        external_disks.stop_managing_if_needed(
            &raw_disks,
            &config_disks,
            &logctx.log,
        );

        // We should only have the remaining disks left.
        assert_eq!(external_disks.disks.len(), config_disks.len());
        for disk in &config_disks {
            let disk = external_disks
                .disks
                .get(&disk.id)
                .expect("all config disks have entries");
            assert_matches!(
                &disk.state,
                DiskState::Managed(d)
                    if *d.identity() == disk.config.identity
            );
        }
        assert_currently_managed_zpools_is_consistent(&external_disks);

        logctx.cleanup_successful();
    }

    /// Zone root cleaner that fails on a fixed set of zpools, and records the
    /// zpools it cleaned.
    #[derive(Debug, Default)]
    struct TestZoneRootCleaner {
        fail_on: Mutex<BTreeSet<ZpoolName>>,
        cleaned: Mutex<Vec<ZpoolName>>,
        // If set, assert that each zpool is visible to `DebugCollectorTask`
        // but not yet in service when it's cleaned.
        debug_dataset_disks_rx: Option<watch::Receiver<HashSet<Disk>>>,
        currently_managed_zpools_rx:
            Option<watch::Receiver<Arc<CurrentlyManagedZpools>>>,
    }

    impl ZoneRootCleaner for TestZoneRootCleaner {
        async fn archive_and_destroy_former_zone_roots(
            &self,
            zpool_name: &ZpoolName,
            _mount_config: &MountConfig,
            _archiver: &FormerZoneRootArchiver,
            _log: &Logger,
        ) -> Result<(), DiskManagementError> {
            if let Some(rx) = &self.debug_dataset_disks_rx {
                assert!(
                    rx.borrow().iter().any(|d| d.zpool_name() == zpool_name),
                    "{zpool_name} not visible to DebugCollectorTask",
                );
            }
            if let Some(rx) = &self.currently_managed_zpools_rx {
                assert!(
                    !rx.borrow().contains(zpool_name),
                    "{zpool_name} in service before cleanup",
                );
            }
            if self.fail_on.lock().unwrap().contains(zpool_name) {
                return Err(DiskManagementError::DestroyDataset(
                    DestroyDatasetError {
                        name: format!("{zpool_name}/{ZONE_DATASET}/oxz_test"),
                        err: DestroyDatasetErrorVariant::Other(
                            illumos_utils::ExecutionError::ExecutionStart {
                                command: "zfs destroy".to_string(),
                                err: std::io::Error::other("test error"),
                            },
                        ),
                    },
                ));
            }
            self.cleaned.lock().unwrap().push(*zpool_name);
            Ok(())
        }
    }

    /// Returns datasets where all the per-disk datasets on each of `zpools`
    /// have been ensured.
    fn required_datasets_on<'a>(
        zpools: impl IntoIterator<Item = &'a ZpoolName>,
    ) -> OmicronDatasets {
        let datasets = zpools.into_iter().flat_map(|zpool| {
            PerDiskDatasetKind::iter().map(|kind| {
                let config = DatasetConfig {
                    id: DatasetUuid::new_v4(),
                    name: DatasetName::new(*zpool, kind.into()),
                    inner: kind.config(),
                };
                (config, Ok(()))
            })
        });
        OmicronDatasets::with_datasets(datasets)
    }

    struct AdoptionTest {
        logctx: omicron_test_utils::dev::LogContext,
        external_disks: ExternalDisks,
        debug_dataset_disks_rx: watch::Receiver<HashSet<Disk>>,
        currently_managed_zpools_rx:
            watch::Receiver<Arc<CurrentlyManagedZpools>>,
        raw_disks: IdOrdMap<RawDisk>,
        config_disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    }

    impl AdoptionTest {
        fn new(test_name: &str, serials: &[&str]) -> Self {
            let logctx = dev::test_setup_log(test_name);
            let (currently_managed_zpools_tx, currently_managed_zpools_rx) =
                watch::channel(Arc::default());
            let (debug_dataset_disks_tx, debug_dataset_disks_rx) =
                watch::channel(HashSet::default());
            let external_disks = ExternalDisks::new(
                nonexistent_mount_config(),
                currently_managed_zpools_tx,
                debug_dataset_disks_tx,
                FormerZoneRootArchiver::noop(&logctx.log),
            );
            let mut raw_disks = IdOrdMap::default();
            let mut config_disks = IdOrdMap::default();
            for serial in serials {
                let raw_disk = make_raw_test_disk(DiskVariant::U2, serial);
                config_disks.insert_overwrite(OmicronPhysicalDiskConfig {
                    identity: raw_disk.identity().clone(),
                    id: PhysicalDiskUuid::new_v4(),
                    pool_id: ZpoolUuid::new_v4(),
                });
                raw_disks.insert_overwrite(raw_disk);
            }
            Self {
                logctx,
                external_disks,
                debug_dataset_disks_rx,
                currently_managed_zpools_rx,
                raw_disks,
                config_disks,
            }
        }

        fn cleaner(&self) -> TestZoneRootCleaner {
            TestZoneRootCleaner {
                debug_dataset_disks_rx: Some(
                    self.debug_dataset_disks_rx.clone(),
                ),
                currently_managed_zpools_rx: Some(
                    self.currently_managed_zpools_rx.clone(),
                ),
                ..Default::default()
            }
        }

        /// Finish adopting disks, as though only the required datasets on
        /// `zpools` have been ensured.
        async fn finish_adopting<'a>(
            &mut self,
            zpools: impl IntoIterator<Item = &'a ZpoolName>,
            cleaner: &TestZoneRootCleaner,
        ) {
            self.external_disks
                .finish_adopting_disks_with_cleaner(
                    &required_datasets_on(zpools),
                    &self.logctx.log,
                    cleaner,
                )
                .await;
        }

        fn zpool(&self, serial: &str) -> ZpoolName {
            let config = self
                .config_disks
                .iter()
                .find(|d| d.identity.serial == serial)
                .unwrap();
            ZpoolName::new_external(config.pool_id)
        }

        fn disk_id(&self, serial: &str) -> PhysicalDiskUuid {
            self.config_disks
                .iter()
                .find(|d| d.identity.serial == serial)
                .unwrap()
                .id
        }

        async fn adopt(&mut self) {
            self.external_disks
                .start_managing_if_needed_with_disk_adopter(
                    &self.raw_disks,
                    &self.config_disks,
                    &self.logctx.log,
                    &TestDiskAdopter::default(),
                )
                .await;
        }

        fn published_zpools(&self) -> BTreeSet<ZpoolName> {
            self.external_disks.currently_managed_zpools().iter().collect()
        }

        fn debug_collector_zpools(&self) -> BTreeSet<ZpoolName> {
            self.debug_dataset_disks_rx
                .borrow()
                .iter()
                .map(|d| *d.zpool_name())
                .collect()
        }
    }

    #[tokio::test]
    async fn adopted_disks_are_put_into_service_in_phases() {
        let mut t = AdoptionTest::new(
            "adopted_disks_are_put_into_service_in_phases",
            &["a", "b"],
        );
        let both = BTreeSet::from([t.zpool("a"), t.zpool("b")]);

        // Newly-adopted disks are not visible to anything else yet.
        t.adopt().await;
        assert_eq!(t.external_disks.zpools_being_adopted(), both);
        assert_eq!(
            t.external_disks
                .all_managed_zpools()
                .iter()
                .collect::<BTreeSet<_>>(),
            both
        );
        assert!(t.published_zpools().is_empty());
        assert!(t.debug_collector_zpools().is_empty());

        // `cleaner` checks that each disk's debug dataset is visible to the
        // debug collector (for archival), but the disk isn't in service, when
        // it's cleaned up. Afterwards, they're in service.
        let cleaner = t.cleaner();
        t.finish_adopting(&both, &cleaner).await;
        assert_eq!(
            cleaner
                .cleaned
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            both
        );
        assert_eq!(t.published_zpools(), both);
        assert_eq!(t.debug_collector_zpools(), both);
        assert!(t.external_disks.zpools_being_adopted().is_empty());
        assert_currently_managed_zpools_is_consistent(&t.external_disks);

        // They aren't adopted (or cleaned up) again.
        t.adopt().await;
        assert!(t.external_disks.zpools_being_adopted().is_empty());

        t.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn disks_that_fail_verification_are_never_published() {
        let mut t = AdoptionTest::new(
            "disks_that_fail_verification_are_never_published",
            &["ok", "bad"],
        );
        let (ok, bad) = (t.zpool("ok"), t.zpool("bad"));

        t.adopt().await;
        // `bad` has none of its required datasets in the config.
        let cleaner = t.cleaner();
        t.finish_adopting(&[ok], &cleaner).await;

        // The disk that failed verification is no longer managed, and was
        // never cleaned up or published.
        assert_matches!(
            &t.external_disks.disks.get(&t.disk_id("bad")).unwrap().state,
            DiskState::FailedToManage(DiskManagementError::RequiredDataset(
                RequiredDatasetError::NotInConfig { .. }
            ))
        );
        assert_eq!(*cleaner.cleaned.lock().unwrap(), [ok]);
        assert_eq!(t.debug_collector_zpools(), BTreeSet::from([ok]));
        assert_eq!(t.published_zpools(), BTreeSet::from([ok]));

        // We'll try to adopt it again later.
        t.adopt().await;
        assert_eq!(
            t.external_disks.zpools_being_adopted(),
            BTreeSet::from([bad])
        );

        t.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn disks_are_unadopted_if_dataset_task_is_unavailable() {
        let mut t = AdoptionTest::new(
            "disks_are_unadopted_if_dataset_task_is_unavailable",
            &["a", "b"],
        );
        let both = BTreeSet::from([t.zpool("a"), t.zpool("b")]);

        t.adopt().await;
        t.external_disks
            .fail_adopting_disks(DatasetTaskError::Busy, &t.logctx.log);

        // The disks are no longer managed, were never published, and will be
        // retried.
        for serial in ["a", "b"] {
            assert_matches!(
                &t.external_disks.disks.get(&t.disk_id(serial)).unwrap().state,
                DiskState::FailedToManage(
                    DiskManagementError::DatasetTaskUnavailable(
                        DatasetTaskError::Busy
                    )
                )
            );
        }
        assert!(t.published_zpools().is_empty());
        assert!(t.debug_collector_zpools().is_empty());
        assert!(t.external_disks.has_retryable_error());

        // We'll adopt them again on the next attempt.
        t.adopt().await;
        assert_eq!(t.external_disks.zpools_being_adopted(), both);

        t.logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn disks_that_fail_cleanup_are_unadopted() {
        let mut t = AdoptionTest::new(
            "disks_that_fail_cleanup_are_unadopted",
            &["ok", "bad"],
        );
        let (ok, bad) = (t.zpool("ok"), t.zpool("bad"));

        t.adopt().await;
        let cleaner = t.cleaner();
        cleaner.fail_on.lock().unwrap().insert(bad);
        t.finish_adopting(&[ok, bad], &cleaner).await;

        // The disk that failed cleanup is no longer managed (and will be
        // retried).
        assert_eq!(t.published_zpools(), BTreeSet::from([ok]));
        assert_eq!(t.debug_collector_zpools(), BTreeSet::from([ok]));
        assert!(t.external_disks.has_retryable_error());
        assert_currently_managed_zpools_is_consistent(&t.external_disks);

        t.logctx.cleanup_successful();
    }
}

#[cfg(all(test, target_os = "illumos"))]
mod illumos_tests {
    use super::*;
    use crate::dataset_serialization_task::DatasetTaskHandle;
    use crate::dataset_serialization_task::illumos_tests::RealZfsTestHarness;
    use assert_matches::assert_matches;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_common::zpool_name::ZpoolKind;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::DatasetUuid;
    use sled_agent_types::disk::DatasetConfig;
    use sled_agent_types::disk::SharedDatasetConfig;

    fn zone_dataset(zpool: ZpoolName) -> String {
        format!("{zpool}/{ZONE_DATASET}")
    }

    fn former_zone_root(zpool: ZpoolName) -> String {
        format!("{zpool}/{ZONE_DATASET}/oxz_former")
    }

    // Create the transient zone root dataset on `zpool`, optionally
    // containing a former zone root.
    async fn create_zone_dataset(
        harness: &RealZfsTestHarness,
        zpool: ZpoolName,
        with_former_zone_root: bool,
        log: &Logger,
    ) {
        let config = |kind| DatasetConfig {
            id: DatasetUuid::new_v4(),
            name: DatasetName::new(zpool, kind),
            inner: SharedDatasetConfig::default(),
        };
        let task = DatasetTaskHandle::spawn_dataset_task(
            Arc::new(harness.mount_config.clone()),
            log,
        );
        let mut configs = vec![config(DatasetKind::TransientZoneRoot)];
        if with_former_zone_root {
            configs.push(config(DatasetKind::TransientZone {
                name: "oxz_former".to_string(),
            }));
        }
        let results = task
            .datasets_ensure(
                configs.into_iter().collect(),
                harness.current_zpools(),
            )
            .await
            .expect("dataset task responded");
        for result in &results {
            assert_matches!(result.result, Ok(()));
        }
    }

    async fn clean_up(
        harness: &RealZfsTestHarness,
        zpool: ZpoolName,
        log: &Logger,
    ) -> Result<(), DiskManagementError> {
        RealZoneRootCleaner
            .archive_and_destroy_former_zone_roots(
                &zpool,
                &harness.mount_config,
                &FormerZoneRootArchiver::noop(log),
                log,
            )
            .await
    }

    async fn exists(name: &str) -> bool {
        Zfs::dataset_exists(name).await.expect("checked dataset existence")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cleanup_succeeds_without_former_zone_roots() {
        let logctx =
            dev::test_setup_log("cleanup_succeeds_without_former_zone_roots");
        let mut harness = RealZfsTestHarness::new(logctx.log.clone());
        let zpool = harness.add_zpool(ZpoolKind::External).await;

        // An empty zone dataset (e.g., because we just created it on a new
        // disk) has nothing to clean up.
        create_zone_dataset(&harness, zpool, false, &logctx.log).await;
        clean_up(&harness, zpool, &logctx.log)
            .await
            .expect("cleanup succeeded");
        assert!(exists(&zone_dataset(zpool)).await);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cleanup_destroys_former_zone_roots() {
        let logctx = dev::test_setup_log("cleanup_destroys_former_zone_roots");
        let mut harness = RealZfsTestHarness::new(logctx.log.clone());
        let zpool = harness.add_zpool(ZpoolKind::External).await;
        create_zone_dataset(&harness, zpool, true, &logctx.log).await;
        assert!(exists(&former_zone_root(zpool)).await);

        clean_up(&harness, zpool, &logctx.log)
            .await
            .expect("cleanup succeeded");
        assert!(!exists(&former_zone_root(zpool)).await);
        assert!(exists(&zone_dataset(zpool)).await);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }
}
