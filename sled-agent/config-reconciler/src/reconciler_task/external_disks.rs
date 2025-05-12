// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for managing external disks (on gimlet/cosmo, U.2s).
//!
//! There is no separate tokio task here; our parent reconciler task owns this
//! set of disks and is able to mutate it in place during reconciliation.

use futures::future;
use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskManagementError;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_storage::config::MountConfig;
use sled_storage::dataset::DatasetError;
use sled_storage::disk::Disk;
use sled_storage::disk::DiskError;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

use crate::disks_common::MaybeUpdatedDisk;
use crate::disks_common::update_properties_from_raw_disk;
use crate::raw_disks::RawDiskWithId;

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
}

#[derive(Debug)]
pub(super) struct ExternalDisks {
    disks: IdMap<ExternalDiskState>,
    mount_config: Arc<MountConfig>,
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
}

impl ExternalDisks {
    pub(super) fn new(
        mount_config: Arc<MountConfig>,
        currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
    ) -> Self {
        Self {
            disks: IdMap::default(),
            mount_config,
            currently_managed_zpools_tx,
        }
    }

    fn update_currently_managed_zpools(&self) {
        let new_zpools = self
            .disks
            .iter()
            .filter_map(|disk| match &disk.state {
                DiskState::Managed(disk) => Some(*disk.zpool_name()),
                DiskState::FailedToManage(_) => None,
            })
            .collect::<BTreeSet<_>>();

        self.currently_managed_zpools_tx.send_if_modified(|zpools| {
            if zpools.0 == new_zpools {
                false
            } else {
                *zpools = Arc::new(CurrentlyManagedZpools(new_zpools));
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
        raw_disks: &IdMap<RawDiskWithId>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
        log: &Logger,
    ) {
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
            self.update_currently_managed_zpools();
        }
    }

    /// Attempt to start managing any disks specified by `config` that we aren't
    /// already managing.
    pub(super) async fn start_managing_if_needed(
        &mut self,
        raw_disks: &IdMap<RawDiskWithId>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
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
        raw_disks: &IdMap<RawDiskWithId>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
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
        for disk_state in disk_states {
            self.disks.insert(disk_state);
        }
        for disk_state in failed_disk_states {
            self.disks.insert(disk_state);
        }

        self.update_currently_managed_zpools();
    }

    async fn try_ensure_disk_managed<T: DiskAdopter>(
        &self,
        current: Option<&ExternalDiskState>,
        config: OmicronPhysicalDiskConfig,
        raw_disk: &RawDisk,
        disk_adopter: &T,
        log: &Logger,
    ) -> ExternalDiskState {
        match current.map(|d| &d.state) {
            // If we're already managing this disk, check whether there are any
            // new properties to update.
            Some(DiskState::Managed(disk)) => {
                self.update_disk_properties(disk, config, raw_disk, log)
            }
            // If we previously failed to manage this disk, try again.
            Some(DiskState::FailedToManage(prev_err)) => {
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
        config: OmicronPhysicalDiskConfig,
        raw_disk: &RawDisk,
        log: &Logger,
    ) -> ExternalDiskState {
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
            return ExternalDiskState::failed(config, err);
        }

        // Update any properties that have changed from `disk` based on the
        // current `raw_disk`. We don't do anything different whether or not any
        // changes were actually made.
        let disk = match update_properties_from_raw_disk(disk, raw_disk, log) {
            MaybeUpdatedDisk::Updated(disk) => disk,
            MaybeUpdatedDisk::Unchanged => disk.clone(),
        };

        ExternalDiskState::managed(config, disk)
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
            Ok(disk) => {
                info!(
                    log, "Successfully started management of disk";
                    "disk_identity" => ?config.identity,
                );
                ExternalDiskState::managed(config, disk)
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
}

impl ExternalDiskState {
    fn managed(config: OmicronPhysicalDiskConfig, disk: Disk) -> Self {
        Self { config, state: DiskState::Managed(disk) }
    }

    fn failed(
        config: OmicronPhysicalDiskConfig,
        err: DiskManagementError,
    ) -> Self {
        Self { config, state: DiskState::FailedToManage(err) }
    }
}

impl IdMappable for ExternalDiskState {
    type Id = PhysicalDiskUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}

#[derive(Debug)]
enum DiskState {
    Managed(Disk),
    FailedToManage(DiskManagementError),
}

/// Helper to allow unit tests to run without interacting with the real [`Disk`]
/// implementation. In production, the only implementor of this trait is
/// [`RealDiskAdopter`].
trait DiskAdopter {
    fn adopt_disk(
        &self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        pool_id: ZpoolUuid,
        log: &Logger,
    ) -> impl Future<Output = Result<Disk, DiskManagementError>> + Send;
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
    ) -> Result<Disk, DiskManagementError> {
        Disk::new(
            log,
            mount_config,
            raw_disk,
            Some(pool_id),
            Some(self.key_requester),
        )
        .await
        .map_err(|err| {
            let err_string = InlineErrorChain::new(&err).to_string();
            match err {
                // We pick this error out and identify it separately because
                // it may be transient, and should sometimes be handled with
                // a retry.
                DiskError::Dataset(DatasetError::KeyManager(_)) => {
                    DiskManagementError::KeyManager(err_string)
                }
                _ => DiskManagementError::Other(err_string),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use illumos_utils::zpool::ZpoolName;
    use omicron_common::disk::DiskIdentity;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_hardware::UnparsedDisk;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
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
        ) -> Result<Disk, DiskManagementError> {
            // ExternalDisks should only adopt U2 disks
            assert_eq!(raw_disk.variant(), DiskVariant::U2);
            let disk = Disk::Real(PooledDisk {
                paths: DiskPaths {
                    devfs_path: "/fake-disk".into(),
                    dev_path: None,
                },
                slot: raw_disk.slot(),
                variant: raw_disk.variant(),
                identity: raw_disk.identity().clone(),
                is_boot_disk: raw_disk.is_boot_disk(),
                partitions: vec![],
                zpool_name: ZpoolName::new_external(pool_id),
                firmware: raw_disk.firmware().clone(),
            });
            self.requests.lock().unwrap().push(raw_disk);
            Ok(disk)
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

    fn make_raw_test_disk(variant: DiskVariant, serial: &str) -> RawDiskWithId {
        RawDisk::Real(UnparsedDisk::new(
            "/test-devfs".into(),
            None,
            0,
            variant,
            omicron_common::disk::DiskIdentity {
                vendor: "test".into(),
                model: "test".into(),
                serial: serial.into(),
            },
            false,
            DiskFirmware::new(0, None, false, 1, vec![]),
        ))
        .into()
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
                DiskState::FailedToManage(_) => None,
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

    async fn internal_disks_are_rejected_impl(raw_disks: IdMap<RawDiskWithId>) {
        let logctx = dev::test_setup_log("internal_disks_are_rejected");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
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
            .collect::<IdMap<_>>();

        // This should partially succeed: we should adopt the U.2s and report
        // errors on the M.2s.
        let disk_adopter = TestDiskAdopter::default();
        external_disks
            .start_managing_if_needed_with_disk_adopter(
                &raw_disks,
                &config_disks,
                &logctx.log,
                &disk_adopter,
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
        let mut raw_disks = IdMap::default();
        let mut config_disks = IdMap::default();
        let mut not_present = BTreeSet::new();

        for (serial, is_present) in disks {
            let raw_disk = make_raw_test_disk(DiskVariant::U2, &serial);
            let config_disk = OmicronPhysicalDiskConfig {
                identity: raw_disk.identity().clone(),
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            };
            if is_present {
                raw_disks.insert(raw_disk);
            } else {
                not_present.insert(config_disk.id);
            }
            config_disks.insert(config_disk);
        }

        with_test_runtime(async move {
            fail_if_disk_not_present_impl(raw_disks, config_disks, not_present)
                .await
        })
    }

    async fn fail_if_disk_not_present_impl(
        raw_disks: IdMap<RawDiskWithId>,
        config_disks: IdMap<OmicronPhysicalDiskConfig>,
        not_present: BTreeSet<PhysicalDiskUuid>,
    ) {
        let logctx = dev::test_setup_log("fail_if_disk_not_present");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Attempt to adopt all the config disks.
        let disk_adopter = TestDiskAdopter::default();
        external_disks
            .start_managing_if_needed_with_disk_adopter(
                &raw_disks,
                &config_disks,
                &logctx.log,
                &disk_adopter,
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
        let mut raw_disks = IdMap::default();
        let mut config_disks = IdMap::default();
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
            raw_disks.insert(raw_disk);
            config_disks.insert(config_disk);
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
        mut raw_disks: IdMap<RawDiskWithId>,
        config_disks: IdMap<OmicronPhysicalDiskConfig>,
        should_mutate_firmware: BTreeSet<DiskIdentity>,
    ) {
        let logctx = dev::test_setup_log("firmware_updates_are_propagated");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Attempt to adopt all the config disks.
        let disk_adopter = TestDiskAdopter::default();
        external_disks
            .start_managing_if_needed_with_disk_adopter(
                &raw_disks,
                &config_disks,
                &logctx.log,
                &disk_adopter,
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
            let mut raw_disk = RawDisk::from(entry.clone());
            let new_firmware = DiskFirmware::new(
                raw_disk.firmware().active_slot().wrapping_add(1),
                None,
                false,
                1,
                Vec::new(),
            );
            *raw_disk.firmware_mut() = new_firmware;
            *entry = raw_disk.into();
        }

        // Attempt to adopt all the config disks again; we should pick up the
        // new firmware.
        external_disks
            .start_managing_if_needed_with_disk_adopter(
                &raw_disks,
                &config_disks,
                &logctx.log,
                &disk_adopter,
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
        let mut raw_disks = IdMap::default();
        let mut config_disks = IdMap::default();
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
            raw_disks.insert(raw_disk);
            config_disks.insert(config_disk);
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
        raw_disks: IdMap<RawDiskWithId>,
        mut config_disks: IdMap<OmicronPhysicalDiskConfig>,
        should_remove_after_adding: BTreeSet<PhysicalDiskUuid>,
    ) {
        let logctx = dev::test_setup_log("remove_disks_not_in_config");

        let (currently_managed_zpools_tx, _rx) = watch::channel(Arc::default());
        let mut external_disks = ExternalDisks::new(
            nonexistent_mount_config(),
            currently_managed_zpools_tx,
        );

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

        // Attempt to adopt all the config disks.
        let disk_adopter = TestDiskAdopter::default();
        external_disks
            .start_managing_if_needed_with_disk_adopter(
                &raw_disks,
                &config_disks,
                &logctx.log,
                &disk_adopter,
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
    // TODO-john more tests
    // * change disk properties
}
