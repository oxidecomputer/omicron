// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tokio task responsible for starting management of our "internal" disks (on
//! gimlet/cosmo, M.2s). These disks are usable earlier than external disks, in
//! particular:
//!
//! * Before rack setup
//! * During sled-agent startup, before trust quorum has unlocked

use camino::Utf8PathBuf;
use core::cmp;
use futures::future;
use futures::future::Either;
use id_map::IdMap;
use id_map::IdMappable;
use omicron_common::backoff::Backoff as _;
use omicron_common::backoff::ExponentialBackoffBuilder;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::M2Slot;
use omicron_common::zpool_name::ZpoolName;
use sled_hardware::PooledDiskError;
use sled_storage::config::MountConfig;
use sled_storage::dataset::CLUSTER_DATASET;
use sled_storage::dataset::CONFIG_DATASET;
use sled_storage::dataset::INSTALL_DATASET;
use sled_storage::dataset::M2_ARTIFACT_DATASET;
use sled_storage::dataset::M2_DEBUG_DATASET;
use sled_storage::disk::Disk;
use sled_storage::disk::DiskError;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

use crate::disks_common::MaybeUpdatedDisk;
use crate::disks_common::update_properties_from_raw_disk;
use crate::raw_disks::RawDiskWithId;
use crate::raw_disks::RawDisksReceiver;

/// A thin wrapper around a [`watch::Receiver`] that presents a similar API.
#[derive(Debug, Clone)]
pub struct InternalDisksReceiver {
    mount_config: Arc<MountConfig>,
    errors_rx: watch::Receiver<Arc<BTreeMap<DiskIdentity, DiskError>>>,
    inner: InternalDisksReceiverInner,
}

#[derive(Debug, Clone)]
enum InternalDisksReceiverInner {
    Real(watch::Receiver<IdMap<InternalDisk>>),
    #[cfg(any(test, feature = "testing"))]
    FakeStatic(Arc<IdMap<InternalDiskDetails>>),
    #[cfg(any(test, feature = "testing"))]
    FakeDynamic(watch::Receiver<Arc<IdMap<InternalDiskDetails>>>),
}

impl InternalDisksReceiver {
    /// Create an `InternalDisksReceiver` that always reports a fixed set of
    /// disks.
    ///
    /// The first disk is set as the boot disk.
    #[cfg(any(test, feature = "testing"))]
    pub fn fake_static(
        mount_config: Arc<MountConfig>,
        disks: impl Iterator<Item = (DiskIdentity, ZpoolName)>,
    ) -> Self {
        let inner = InternalDisksReceiverInner::FakeStatic(Arc::new(
            disks
                .enumerate()
                .map(|(i, (identity, zpool_name))| {
                    InternalDiskDetails::fake_details(
                        identity,
                        zpool_name,
                        i == 0,
                    )
                })
                .collect(),
        ));

        // We never report errors from our static set. If there's a Tokio
        // runtime, move the sender to a task that idles so we don't get recv
        // errors; otherwise, don't bother because no one can await changes on
        // `errors_rx` anyway (e.g., if we're being used from a non-tokio test).
        let (errors_tx, errors_rx) = watch::channel(Arc::default());
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                errors_tx.closed().await;
            });
        }

        Self { mount_config, inner, errors_rx }
    }

    /// Create an `InternalDisksReceiver` that forwards any changes made in the
    /// given watch channel out to its consumers.
    ///
    /// Note that this forwarding is not immediate: this method spawns a tokio
    /// task responsible for reading changes on `disks_rx` and then publishing
    /// them out to the consumers of the returned `InternalDisksReceiver`.
    #[cfg(any(test, feature = "testing"))]
    pub fn fake_dynamic(
        mount_config: Arc<MountConfig>,
        mut disks_rx: watch::Receiver<Vec<(DiskIdentity, ZpoolName)>>,
    ) -> Self {
        let current = disks_rx
            .borrow_and_update()
            .clone()
            .into_iter()
            .map(|(identity, zpool_name)| {
                InternalDiskDetails::fake_details(identity, zpool_name, false)
            })
            .collect();
        let (mapped_tx, mapped_rx) = watch::channel(Arc::new(current));

        // Spawn the task that forwards changes from channel to channel.
        tokio::spawn(async move {
            while let Ok(()) = disks_rx.changed().await {
                let remapped = disks_rx
                    .borrow_and_update()
                    .clone()
                    .into_iter()
                    .map(|(identity, zpool_name)| {
                        InternalDiskDetails::fake_details(
                            identity, zpool_name, false,
                        )
                    })
                    .collect();
                if mapped_tx.send(Arc::new(remapped)).is_err() {
                    break;
                }
            }
        });

        // We never report errors from our static set; move the sender to a task
        // that idles so we don't get recv errors.
        let (errors_tx, errors_rx) = watch::channel(Arc::default());
        tokio::spawn(async move {
            errors_tx.closed().await;
        });

        let inner = InternalDisksReceiverInner::FakeDynamic(mapped_rx);
        Self { mount_config, inner, errors_rx }
    }

    pub(crate) fn spawn_internal_disks_task(
        mount_config: Arc<MountConfig>,
        raw_disks_rx: RawDisksReceiver,
        base_log: &Logger,
    ) -> Self {
        Self::spawn_with_disk_adopter(
            mount_config,
            raw_disks_rx,
            base_log,
            RealDiskAdopter,
        )
    }

    pub(crate) fn mount_config(&self) -> &Arc<MountConfig> {
        &self.mount_config
    }

    fn spawn_with_disk_adopter<T: DiskAdopter>(
        mount_config: Arc<MountConfig>,
        raw_disks_rx: RawDisksReceiver,
        base_log: &Logger,
        disk_adopter: T,
    ) -> Self {
        let (disks_tx, disks_rx) = watch::channel(IdMap::default());
        let (errors_tx, errors_rx) = watch::channel(Arc::default());

        tokio::spawn(
            InternalDisksTask {
                disks_tx,
                errors_tx,
                raw_disks_rx,
                mount_config: Arc::clone(&mount_config),
                log: base_log.new(slog::o!("component" => "InternalDisksTask")),
            }
            .run(disk_adopter),
        );

        Self {
            mount_config,
            inner: InternalDisksReceiverInner::Real(disks_rx),
            errors_rx,
        }
    }

    /// Get the current set of managed internal disks without marking the
    /// returned value as seen.
    ///
    /// This is analogous to [`watch::Receiver::borrow()`], except it returns an
    /// owned value that does not keep the internal watch lock held.
    pub fn current(&self) -> InternalDisks {
        let disks = match &self.inner {
            InternalDisksReceiverInner::Real(rx) => {
                Arc::new(rx.borrow().iter().map(From::from).collect())
            }
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeStatic(disks) => Arc::clone(disks),
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeDynamic(rx) => {
                Arc::clone(&*rx.borrow())
            }
        };
        InternalDisks { disks, mount_config: Arc::clone(&self.mount_config) }
    }

    /// Get an [`InternalDisksWithBootDisk`], panicking if we have no boot
    /// disk.
    ///
    /// This method is only available to tests; production code should instead
    /// use `current()` and/or `wait_for_boot_disk()`.
    #[cfg(any(test, feature = "testing"))]
    pub fn current_with_boot_disk(&self) -> InternalDisksWithBootDisk {
        let disks = self.current();
        InternalDisksWithBootDisk::new(disks).expect(
            "current_with_boot_disk() should be called by \
             tests that set up a fake boot disk",
        )
    }

    /// Get the current set of managed internal disks and mark the returned
    /// value as seen.
    ///
    /// This is analogous to [`watch::Receiver::borrow_and_update()`], except it
    /// returns an owned value that does not keep the internal watch lock held.
    pub fn current_and_update(&mut self) -> InternalDisks {
        let disks = match &mut self.inner {
            InternalDisksReceiverInner::Real(rx) => Arc::new(
                rx.borrow_and_update().iter().map(From::from).collect(),
            ),
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeStatic(disks) => Arc::clone(disks),
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeDynamic(rx) => {
                Arc::clone(&*rx.borrow_and_update())
            }
        };
        InternalDisks { disks, mount_config: Arc::clone(&self.mount_config) }
    }

    /// Get the raw set of `Disk`s.
    ///
    /// This operation will panic if this receiver is backed by a fake set of
    /// disk properties (e.g., as created by `Self::fake_static()`). It is also
    /// only exposed to this crate; external callers should only operate on the
    /// view provided by `InternalDisks`. Internal-to-this-crate callers should
    /// take care not to hold the returned reference too long (as this keeps the
    /// watch channel locked).
    pub(crate) fn borrow_and_update_raw_disks(
        &mut self,
    ) -> watch::Ref<'_, IdMap<InternalDisk>> {
        match &mut self.inner {
            InternalDisksReceiverInner::Real(rx) => rx.borrow_and_update(),
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeStatic(_)
            | InternalDisksReceiverInner::FakeDynamic(_) => panic!(
                "borrow_and_update_raw_disks not supported by fake impls"
            ),
        }
    }

    /// Wait for changes to the set of managed internal disks.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        match &mut self.inner {
            InternalDisksReceiverInner::Real(rx) => rx.changed().await,
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeStatic(_) => {
                // Static set of disks never changes
                std::future::pending().await
            }
            #[cfg(any(test, feature = "testing"))]
            InternalDisksReceiverInner::FakeDynamic(rx) => rx.changed().await,
        }
    }

    /// Wait until the boot disk is managed, returning its identity.
    ///
    /// Internally updates the most-recently-seen value.
    pub(crate) async fn wait_for_boot_disk(
        &mut self,
    ) -> InternalDisksWithBootDisk {
        loop {
            let current = self.current_and_update();
            if let Some(with_boot_disk) =
                InternalDisksWithBootDisk::new(current)
            {
                return with_boot_disk;
            };
            self.changed().await.expect("InternalDisks task never dies");
        }
    }

    /// Return the most recent set of internal disk reconciliation errors.
    ///
    /// Note that this error set is not atomically collected with the
    /// `current()` set of disks. It is only useful for inventory reporting
    /// purposes.
    pub(crate) fn errors(&self) -> Arc<BTreeMap<DiskIdentity, DiskError>> {
        Arc::clone(&*self.errors_rx.borrow())
    }
}

pub struct InternalDisks {
    disks: Arc<IdMap<InternalDiskDetails>>,
    mount_config: Arc<MountConfig>,
}

impl InternalDisks {
    pub fn mount_config(&self) -> &MountConfig {
        &self.mount_config
    }

    pub fn boot_disk_zpool(&self) -> Option<ZpoolName> {
        self.disks.iter().find_map(|d| {
            if d.is_boot_disk() { Some(d.zpool_name) } else { None }
        })
    }

    pub fn boot_disk_install_dataset(&self) -> Option<Utf8PathBuf> {
        self.boot_disk_zpool().map(|zpool| {
            zpool.dataset_mountpoint(&self.mount_config.root, INSTALL_DATASET)
        })
    }

    pub fn image_raw_devfs_path(
        &self,
        slot: M2Slot,
    ) -> Option<Result<Utf8PathBuf, Arc<PooledDiskError>>> {
        self.disks.iter().find_map(|disk| {
            if disk.slot == Some(slot) {
                disk.raw_devfs_path.clone()
            } else {
                None
            }
        })
    }

    /// Returns all `CONFIG_DATASET` paths within available M.2 disks.
    pub fn all_config_datasets(
        &self,
    ) -> impl ExactSizeIterator<Item = Utf8PathBuf> + '_ {
        self.all_datasets(CONFIG_DATASET)
    }

    /// Returns all `M2_DEBUG_DATASET` paths within available M.2 disks.
    pub fn all_debug_datasets(
        &self,
    ) -> impl ExactSizeIterator<Item = Utf8PathBuf> + '_ {
        self.all_datasets(M2_DEBUG_DATASET)
    }

    /// Returns all `CLUSTER_DATASET` paths within available M.2 disks.
    pub fn all_cluster_datasets(
        &self,
    ) -> impl ExactSizeIterator<Item = Utf8PathBuf> + '_ {
        self.all_datasets(CLUSTER_DATASET)
    }

    /// Returns all `ARTIFACT_DATASET` paths within available M.2 disks.
    pub fn all_artifact_datasets(
        &self,
    ) -> impl ExactSizeIterator<Item = Utf8PathBuf> + '_ {
        self.all_datasets(M2_ARTIFACT_DATASET)
    }

    /// Return the directories for storing zone service bundles.
    pub fn all_zone_bundle_directories(
        &self,
    ) -> impl ExactSizeIterator<Item = Utf8PathBuf> + '_ {
        // The directory within the debug dataset in which bundles are created.
        const BUNDLE_DIRECTORY: &str = "bundle";

        // The directory for zone bundles.
        const ZONE_BUNDLE_DIRECTORY: &str = "zone";

        self.all_debug_datasets()
            .map(|p| p.join(BUNDLE_DIRECTORY).join(ZONE_BUNDLE_DIRECTORY))
    }

    fn all_datasets(
        &self,
        dataset_name: &'static str,
    ) -> impl ExactSizeIterator<Item = Utf8PathBuf> + '_ {
        self.disks.iter().map(|disk| {
            disk.zpool_name
                .dataset_mountpoint(&self.mount_config.root, dataset_name)
        })
    }
}

/// An [`InternalDisks`] with a guaranteed-present boot disk.
pub struct InternalDisksWithBootDisk {
    inner: InternalDisks,
    boot_disk: InternalDiskDetailsId,
}

impl InternalDisksWithBootDisk {
    fn new(inner: InternalDisks) -> Option<Self> {
        let boot_disk = inner
            .disks
            .iter()
            .find_map(|d| if d.is_boot_disk() { Some(d.id()) } else { None })?;
        Some(Self { inner, boot_disk })
    }

    fn boot_disk(&self) -> &InternalDiskDetails {
        match self.inner.disks.get(&self.boot_disk) {
            Some(details) => details,
            None => unreachable!("boot disk present by construction"),
        }
    }

    pub fn boot_disk_id(&self) -> &Arc<DiskIdentity> {
        &self.boot_disk().id.identity
    }

    pub fn boot_disk_zpool(&self) -> ZpoolName {
        self.boot_disk().zpool_name
    }

    pub fn boot_disk_install_dataset(&self) -> Utf8PathBuf {
        self.boot_disk_zpool().dataset_mountpoint(
            &self.inner.mount_config().root,
            INSTALL_DATASET,
        )
    }

    pub fn non_boot_disk_install_datasets(
        &self,
    ) -> impl Iterator<Item = (ZpoolName, Utf8PathBuf)> + '_ {
        self.inner.disks.iter().filter(|disk| disk.id != self.boot_disk).map(
            |disk| {
                let dataset = disk.zpool_name.dataset_mountpoint(
                    &self.inner.mount_config.root,
                    INSTALL_DATASET,
                );
                (disk.zpool_name, dataset)
            },
        )
    }
}

// A subset of `Disk` properties. We store this in `InternalDisks` instead of
// `Disk`s both to avoid exposing raw `Disk`s outside this crate and to support
// easier faking for tests.
#[derive(Debug, Clone)]
struct InternalDiskDetails {
    id: InternalDiskDetailsId,
    zpool_name: ZpoolName,

    // These two fields are optional because they don't exist for synthetic
    // disks.
    slot: Option<M2Slot>,
    raw_devfs_path: Option<Result<Utf8PathBuf, Arc<PooledDiskError>>>,
}

impl IdMappable for InternalDiskDetails {
    type Id = InternalDiskDetailsId;

    fn id(&self) -> Self::Id {
        self.id.clone()
    }
}

impl From<&'_ Disk> for InternalDiskDetails {
    fn from(disk: &'_ Disk) -> Self {
        // Synthetic disks panic if asked for their `slot()`, so filter
        // them out first.
        let slot = if disk.is_synthetic() {
            None
        } else {
            M2Slot::try_from(disk.slot()).ok()
        };

        // Same story for devfs path.
        let raw_devfs_path = if disk.is_synthetic() {
            None
        } else {
            Some(disk.boot_image_devfs_path(true).map_err(Arc::new))
        };

        Self {
            id: InternalDiskDetailsId {
                identity: Arc::new(disk.identity().clone()),
                is_boot_disk: disk.is_boot_disk(),
            },
            zpool_name: *disk.zpool_name(),
            slot,
            raw_devfs_path,
        }
    }
}

impl From<&'_ InternalDisk> for InternalDiskDetails {
    fn from(disk: &'_ InternalDisk) -> Self {
        Self::from(&disk.disk)
    }
}

impl InternalDiskDetails {
    #[cfg(any(test, feature = "testing"))]
    fn fake_details(
        identity: DiskIdentity,
        zpool_name: ZpoolName,
        is_boot_disk: bool,
    ) -> Self {
        Self {
            id: InternalDiskDetailsId {
                identity: Arc::new(identity),
                is_boot_disk,
            },
            zpool_name,
            // We can expand the interface for fake disks if we need to be able
            // to specify more of these properties in future tests.
            slot: None,
            raw_devfs_path: None,
        }
    }

    fn is_boot_disk(&self) -> bool {
        self.id.is_boot_disk
    }
}

// Special ID type for `InternalDiskDetails` that lets us guarantee we sort boot
// disks ahead of non-boot disks. There's a whole set of thorny problems here
// about what to do if we think both internal disks should have the same
// contents but they disagree; we'll at least try to have callers prefer the
// boot disk if they're performing a "check the first disk that succeeds" kind
// of operation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InternalDiskDetailsId {
    is_boot_disk: bool,
    identity: Arc<DiskIdentity>,
}

impl cmp::Ord for InternalDiskDetailsId {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.is_boot_disk.cmp(&other.is_boot_disk) {
            cmp::Ordering::Equal => {}
            // `false` normally sorts before `true`, so intentionally reverse
            // this so that we sort boot disks ahead of non-boot disks.
            ord => return ord.reverse(),
        }
        self.identity.cmp(&other.identity)
    }
}

impl cmp::PartialOrd for InternalDiskDetailsId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Wrapper around a `Disk` with a cheaply-cloneable identity.
#[derive(Debug, Clone)]
pub(crate) struct InternalDisk {
    identity: Arc<DiskIdentity>,
    disk: Disk,
}

impl From<Disk> for InternalDisk {
    fn from(disk: Disk) -> Self {
        // Invariant: We should only be constructed for internal disks; our
        // caller should have already filtered out all external disks.
        match disk.variant() {
            DiskVariant::M2 => (),
            DiskVariant::U2 => panic!("InternalDisk called with external Disk"),
        }
        Self { identity: Arc::new(disk.identity().clone()), disk }
    }
}

impl Deref for InternalDisk {
    type Target = Disk;

    fn deref(&self) -> &Self::Target {
        &self.disk
    }
}

impl IdMappable for InternalDisk {
    type Id = Arc<DiskIdentity>;

    fn id(&self) -> Self::Id {
        Arc::clone(&self.identity)
    }
}

struct InternalDisksTask {
    // Input channel for changes to the raw disks sled-agent sees.
    raw_disks_rx: RawDisksReceiver,

    // The set of disks we've successfully started managing.
    disks_tx: watch::Sender<IdMap<InternalDisk>>,

    // Output channel summarizing any adoption errors.
    //
    // Because this is a different channel from `disks_tx`, it isn't possible
    // for a caller to get an atomic view of "the current disks and errors", so
    // it's possible a disk could appear in both or neither if snapshots are
    // taken at just the wrong time. This isn't great, but is maybe better than
    // squishing both into a single channel. If we combined them, then we'd wake
    // up any receivers that really only care about disk changes any time we had
    // changes to errors. (And if we get an error, we'll retry; if it continues
    // to fail, we'd continue to wake up receivers after each retry, because we
    // don't have an easy way of checking whether errors are equal.)
    //
    // The errors are only reported in inventory, which already has similar
    // non-atomic properties across other fields all reported together.
    errors_tx: watch::Sender<Arc<BTreeMap<DiskIdentity, DiskError>>>,

    mount_config: Arc<MountConfig>,
    log: Logger,
}

impl InternalDisksTask {
    async fn run<T: DiskAdopter>(mut self, disk_adopter: T) {
        // If disk adoption fails, the most likely cause is that the disk is not
        // formatted correctly, and we have no automated means to recover that.
        // However, it's always possible we could fail to adopt due to some
        // transient error. Construct an exponential backoff that scales up to
        // waiting a minute between attempts; that should let us handle any
        // short transient errors without constantly retrying a doomed
        // operation.
        //
        // We could be smarter here and check for retryable vs non-retryable
        // adoption errors.
        let mut next_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None)
            .build();

        loop {
            // Collect the internal disks to avoid holding the watch channel
            // lock while we attempt to adopt any new disks.
            let internal_raw_disks = self
                .raw_disks_rx
                .borrow_and_update()
                .iter()
                .filter(|disk| match disk.variant() {
                    DiskVariant::U2 => false,
                    DiskVariant::M2 => true,
                })
                .cloned()
                .collect::<IdMap<_>>();

            // Perform actual reconciliation, updating our output watch
            // channels.
            self.reconcile_internal_disks(internal_raw_disks, &disk_adopter)
                .await;

            // If any adoption attempt failed, we'll retry; otherwise we'll wait
            // for a change in `raw_disks_rx`.
            let retry_timeout = if self.errors_tx.borrow().is_empty() {
                next_backoff.reset();
                Either::Left(future::pending())
            } else {
                let timeout = next_backoff
                    .next_backoff()
                    .expect("backoff configured with no max elapsed time");
                info!(
                    self.log,
                    "Will retry failed disk adoption after {:?}", timeout
                );
                Either::Right(tokio::time::sleep(timeout))
            };

            // Wait until either we need to retry (if the above adoption failed)
            // or there's a change in the raw disks.
            tokio::select! {
                // Cancel-safe: This is either `pending()` (never ready) or
                // `sleep()` (cancel-safe).
                _ = retry_timeout => {
                    continue;
                }
                // Cancel-safe per docs on `changed()`.
                result = self.raw_disks_rx.changed() => {
                    match result {
                        Ok(()) => (),
                        Err(_) => {
                            // The `RawDisk` watch channel should never be
                            // closed in production, but could be in tests. All
                            // we can do here is exit; no further updates are
                            // coming.
                            error!(
                                self.log,
                                "InternalDisksTask exiting unexpectedly: \
                                 RawDisk watch channel closed by sender"
                            );
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn reconcile_internal_disks<T: DiskAdopter>(
        &mut self,
        internal_raw_disks: IdMap<RawDiskWithId>,
        disk_adopter: &T,
    ) {
        info!(
            self.log,
            "Attempting to ensure adoption of {} internal disks",
            internal_raw_disks.len(),
        );

        // We don't want to hold the `disks_tx` lock while we adopt disks, so
        // first capture a snapshot of which disks we have.
        let current_disks =
            self.disks_tx.borrow().keys().cloned().collect::<BTreeSet<_>>();

        // Built the list of disks that are gone.
        let mut disks_to_remove = Vec::new();
        for disk_id in &current_disks {
            if !internal_raw_disks.contains_key(disk_id) {
                disks_to_remove.push(disk_id);
            }
        }

        // Build the list of disks that exist, divided into three categories:
        //
        // 1. We're already managing the disk (we only need to check whether
        //    there have been updates to its properties)
        // 2. We're not yet managing the disk, and succeeded in adopting it
        // 3. We're not yet managing the disk, but failed to adopt it
        let mut disks_to_maybe_update = Vec::new();
        let mut disks_to_insert = Vec::new();
        let mut errors = BTreeMap::default();

        for raw_disk in internal_raw_disks {
            // If we already have this disk, we'll just check whether any
            // properties (e.g., firmware revisions) have been changed.
            if current_disks.contains(raw_disk.identity()) {
                disks_to_maybe_update.push(raw_disk);
                continue;
            }

            // This is a new disk: attempt to adopt it.
            let identity = raw_disk.identity().clone();
            let adopt_result = disk_adopter
                .adopt_disk(raw_disk.into(), &self.mount_config, &self.log)
                .await;

            match adopt_result {
                Ok(disk) => {
                    info!(
                        self.log, "Adopted new internal disk";
                        "identity" => ?identity,
                    );
                    disks_to_insert.push(disk);
                }
                Err(err) => {
                    warn!(
                        self.log, "Internal disk adoption failed";
                        "identity" => ?identity,
                        InlineErrorChain::new(&err),
                    );
                    errors.insert(identity, err);
                }
            }
        }

        // Possibly update the set of disks based on the results of the above.
        self.disks_tx.send_if_modified(|disks| {
            let mut changed = false;

            // Do any raw removals and additions.
            for disk_id in disks_to_remove {
                disks.remove(disk_id);
                changed = true;
            }
            for new_disk in disks_to_insert {
                disks.insert(new_disk.into());
                changed = true;
            }

            // Check for property updates to existing disks.
            for raw_disk in disks_to_maybe_update {
                // We only push into `disks_to_maybe_update` if this disk
                // existed when we looked at `disks_tx` above, and this is the
                // only spot where we change it. It must still exist now.
                let existing_disk = disks
                    .get(raw_disk.identity())
                    .expect("disk should still be present");
                match update_properties_from_raw_disk(
                    &existing_disk.disk,
                    &raw_disk,
                    &self.log,
                ) {
                    MaybeUpdatedDisk::Updated(new_disk) => {
                        disks.insert(InternalDisk::from(new_disk));
                        changed = true;
                    }
                    MaybeUpdatedDisk::Unchanged => (),
                }
            }

            changed
        });

        // Update our output error watch channel if we have any errors or we're
        // going from "some errors" to "no errors". It'd be nice to only send
        // out a modification here if the error _content_ changed, but we don't
        // derive `PartialEq` on error types.
        self.errors_tx.send_if_modified(|errors_tx| {
            if !errors.is_empty() || !errors_tx.is_empty() {
                *errors_tx = Arc::new(errors);
                true
            } else {
                false
            }
        });
    }
}

/// Helper to allow unit tests to run without interacting with the real [`Disk`]
/// implementation. In production, the only implementor of this trait is
/// [`RealDiskAdopter`].
trait DiskAdopter: Send + Sync + 'static {
    fn adopt_disk(
        &self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        log: &Logger,
    ) -> impl Future<Output = Result<Disk, DiskError>> + Send;
}

struct RealDiskAdopter;

impl DiskAdopter for RealDiskAdopter {
    async fn adopt_disk(
        &self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        log: &Logger,
    ) -> Result<Disk, DiskError> {
        let pool_id = None; // control plane doesn't manage M.2s
        let key_requester = None; // M.2s are unencrypted
        Disk::new(log, mount_config, raw_disk, pool_id, key_requester).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::poll::CondCheckError;
    use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
    use omicron_uuid_kinds::ZpoolUuid;
    use proptest::sample::size_range;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_hardware::UnparsedDisk;
    use std::sync::Mutex;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;

    #[derive(Debug, Arbitrary)]
    struct ArbitraryInternalDiskDetailsId {
        is_boot_disk: bool,
        vendor: String,
        model: String,
        serial: String,
    }

    impl From<ArbitraryInternalDiskDetailsId> for InternalDiskDetailsId {
        fn from(id: ArbitraryInternalDiskDetailsId) -> Self {
            InternalDiskDetailsId {
                identity: Arc::new(DiskIdentity {
                    vendor: id.vendor,
                    model: id.model,
                    serial: id.serial,
                }),
                is_boot_disk: id.is_boot_disk,
            }
        }
    }

    #[proptest]
    fn boot_disks_sort_ahead_of_non_boot_disks_in_id_map(
        #[any(size_range(2..4).lift())] values: Vec<
            ArbitraryInternalDiskDetailsId,
        >,
    ) {
        let disk_map: IdMap<_> = values
            .into_iter()
            .map(|id| InternalDiskDetails {
                id: id.into(),
                zpool_name: ZpoolName::new_internal(ZpoolUuid::new_v4()),
                slot: None,
                raw_devfs_path: None,
            })
            .collect();

        // When iterating over the contents, we should never see a boot disk
        // after a non-boot disk.
        let mut saw_non_boot_disk = false;
        for disk in disk_map.iter() {
            if disk.is_boot_disk() {
                assert!(!saw_non_boot_disk);
            } else {
                saw_non_boot_disk = true;
            }
        }
    }

    #[derive(Default)]
    struct TestDiskAdopter {
        inner: Mutex<TestDiskAdopterInner>,
    }

    #[derive(Default)]
    struct TestDiskAdopterInner {
        requests: Vec<RawDisk>,
        should_fail_requests:
            BTreeMap<DiskIdentity, Box<dyn Fn() -> DiskError + Send + 'static>>,
    }

    impl DiskAdopter for Arc<TestDiskAdopter> {
        async fn adopt_disk(
            &self,
            raw_disk: RawDisk,
            _mount_config: &MountConfig,
            _log: &Logger,
        ) -> Result<Disk, DiskError> {
            // InternalDisks should only adopt M2 disks
            assert_eq!(raw_disk.variant(), DiskVariant::M2);

            let mut inner = self.inner.lock().unwrap();
            inner.requests.push(raw_disk.clone());

            if let Some(make_err) =
                inner.should_fail_requests.get(raw_disk.identity())
            {
                return Err(make_err());
            }

            Ok(Disk::Real(PooledDisk {
                paths: DiskPaths {
                    devfs_path: "/fake-disk".into(),
                    dev_path: None,
                },
                slot: raw_disk.slot(),
                variant: raw_disk.variant(),
                identity: raw_disk.identity().clone(),
                is_boot_disk: raw_disk.is_boot_disk(),
                partitions: vec![],
                zpool_name: ZpoolName::new_internal(ZpoolUuid::new_v4()),
                firmware: raw_disk.firmware().clone(),
            }))
        }
    }

    fn any_mount_config() -> MountConfig {
        MountConfig {
            root: "/tmp/sled-agent-config-reconciler/internal-disks-tests"
                .into(),
            synthetic_disk_root:
                "/tmp/sled-agent-config-reconciler/internal-disks-tests".into(),
        }
    }

    fn new_raw_test_disk(variant: DiskVariant, serial: &str) -> RawDisk {
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

    #[tokio::test]
    async fn only_m2_disks_are_adopted() {
        let logctx = dev::test_setup_log("only_m2_disks_are_adopted");

        let (raw_disks_tx, raw_disks_rx) = watch::channel(Arc::default());
        let disk_adopter = Arc::new(TestDiskAdopter::default());
        let mut disks_rx = InternalDisksReceiver::spawn_with_disk_adopter(
            Arc::new(any_mount_config()),
            RawDisksReceiver(raw_disks_rx),
            &logctx.log,
            Arc::clone(&disk_adopter),
        );

        // There should be no disks to start.
        assert!(disks_rx.current_and_update().disks.is_empty());

        // Add four disks: two M.2 and two U.2.
        raw_disks_tx.send_modify(|disks| {
            let disks = Arc::make_mut(disks);
            for disk in [
                new_raw_test_disk(DiskVariant::M2, "m2-0"),
                new_raw_test_disk(DiskVariant::U2, "u2-0"),
                new_raw_test_disk(DiskVariant::M2, "m2-1"),
                new_raw_test_disk(DiskVariant::U2, "u2-1"),
            ] {
                disks.insert(disk.into());
            }
        });

        // Wait for the adopted disks to change; this should happen nearly
        // immediately, but we'll put a timeout on to avoid hanging if something
        // is broken.
        tokio::time::timeout(Duration::from_secs(60), disks_rx.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");

        // We should see the two M.2s only.
        let adopted_disks = Arc::clone(&disks_rx.current_and_update().disks);
        let serials = adopted_disks
            .iter()
            .map(|d| d.id.identity.serial.as_str())
            .collect::<BTreeSet<_>>();
        let expected_serials =
            ["m2-0", "m2-1"].into_iter().collect::<BTreeSet<_>>();
        assert_eq!(serials, expected_serials);

        // Our test disk adopter should also have only seen two requests.
        let adoption_inner = disk_adopter.inner.lock().unwrap();
        let adopted_serials = adoption_inner
            .requests
            .iter()
            .map(|d| d.identity().serial.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(adopted_serials, expected_serials);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn firmware_updates_are_propagated() {
        let logctx = dev::test_setup_log("firmware_updates_are_propagated");

        // Setup: one disk.
        let mut raw_disk = new_raw_test_disk(DiskVariant::M2, "test-m2");
        let (raw_disks_tx, raw_disks_rx) = watch::channel(Arc::new(
            [raw_disk.clone().into()].into_iter().collect(),
        ));
        let disk_adopter = Arc::new(TestDiskAdopter::default());
        let mut disks_rx = InternalDisksReceiver::spawn_with_disk_adopter(
            Arc::new(any_mount_config()),
            RawDisksReceiver(raw_disks_rx),
            &logctx.log,
            Arc::clone(&disk_adopter),
        );

        // Wait for the test disk to be adopted.
        tokio::time::timeout(Duration::from_secs(60), disks_rx.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");
        assert_eq!(disks_rx.current_and_update().disks.len(), 1);

        // Modify the firmware of the raw disk and publish that change.
        let new_firmware = DiskFirmware::new(
            raw_disk.firmware().active_slot().wrapping_add(1),
            None,
            false,
            1,
            Vec::new(),
        );
        *raw_disk.firmware_mut() = new_firmware;
        raw_disks_tx.send_modify(|disks| {
            Arc::make_mut(disks).insert(raw_disk.clone().into());
        });

        // Wait for the change to be noticed.
        tokio::time::timeout(Duration::from_secs(60), disks_rx.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");

        // We should still only have one disk, and it should have the new
        // firmware.
        let current = disks_rx.borrow_and_update_raw_disks();
        assert_eq!(current.len(), 1);
        let adopted_disk = current.iter().next().unwrap();
        assert_eq!(adopted_disk.firmware(), raw_disk.firmware());

        // Our test disk adopter should only have seen a single request:
        // changing the firmware doesn't require readoption.
        assert_eq!(disk_adopter.inner.lock().unwrap().requests.len(), 1);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn removed_disks_are_propagated() {
        let logctx = dev::test_setup_log("removed_disks_are_propagated");

        // Setup: two disks.
        let raw_disk1 = new_raw_test_disk(DiskVariant::M2, "m2-1");
        let raw_disk2 = new_raw_test_disk(DiskVariant::M2, "m2-2");
        let (raw_disks_tx, raw_disks_rx) = watch::channel(Arc::new(
            [&raw_disk1, &raw_disk2]
                .into_iter()
                .cloned()
                .map(From::from)
                .collect(),
        ));
        let disk_adopter = Arc::new(TestDiskAdopter::default());
        let mut disks_rx = InternalDisksReceiver::spawn_with_disk_adopter(
            Arc::new(any_mount_config()),
            RawDisksReceiver(raw_disks_rx),
            &logctx.log,
            Arc::clone(&disk_adopter),
        );

        // Wait for the test disks to be adopted.
        tokio::time::timeout(Duration::from_secs(60), disks_rx.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");
        assert_eq!(disks_rx.current_and_update().disks.len(), 2);

        // Remove test disk 1.
        raw_disks_tx.send_modify(|raw_disks| {
            Arc::make_mut(raw_disks).remove(raw_disk1.identity());
        });

        // Wait for the removal to be propagated.
        tokio::time::timeout(Duration::from_secs(60), disks_rx.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");
        let adopted_disks = Arc::clone(&disks_rx.current_and_update().disks);
        let serials = adopted_disks
            .iter()
            .map(|d| d.id.identity.serial.as_str())
            .collect::<BTreeSet<_>>();
        let expected_serials = ["m2-2"].into_iter().collect::<BTreeSet<_>>();
        assert_eq!(serials, expected_serials);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn failures_are_retried() {
        let logctx = dev::test_setup_log("failures_are_retried");

        // Setup: one disk, and configure the disk adopter to fail.
        let raw_disk = new_raw_test_disk(DiskVariant::M2, "test-m2");
        let (_raw_disks_tx, raw_disks_rx) = watch::channel(Arc::new(
            [&raw_disk].into_iter().cloned().map(From::from).collect(),
        ));
        let disk_adopter = Arc::new(TestDiskAdopter::default());
        disk_adopter.inner.lock().unwrap().should_fail_requests.insert(
            raw_disk.identity().clone(),
            Box::new(|| {
                DiskError::PooledDisk(PooledDiskError::UnexpectedVariant)
            }),
        );

        let mut disks_rx = InternalDisksReceiver::spawn_with_disk_adopter(
            Arc::new(any_mount_config()),
            RawDisksReceiver(raw_disks_rx),
            &logctx.log,
            Arc::clone(&disk_adopter),
        );

        // Wait for the error to be reported.
        tokio::time::timeout(
            Duration::from_secs(60),
            disks_rx.errors_rx.changed(),
        )
        .await
        .expect("errors changed before timeout")
        .expect("changed() succeeded");

        let errors = Arc::clone(&*disks_rx.errors_rx.borrow_and_update());
        assert_eq!(errors.len(), 1);
        assert_matches!(
            errors.get(raw_disk.identity()),
            Some(DiskError::PooledDisk(PooledDiskError::UnexpectedVariant))
        );

        // Change our disk adopter to allow these requests to succeed.
        disk_adopter.inner.lock().unwrap().should_fail_requests.clear();

        // Wait for the disk to be adopted.
        tokio::time::timeout(Duration::from_secs(60), disks_rx.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");
        assert_eq!(disks_rx.current_and_update().disks.len(), 1);

        // Wait for the errors to be cleared. This is a separate channel, so
        // it's possible we're racing, but this should happen quickly after the
        // disk is adopted.
        wait_for_watch_channel_condition(
            &mut disks_rx.errors_rx,
            async |errors| {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            Duration::from_secs(30),
        )
        .await
        .expect("error should be gone");

        logctx.cleanup_successful();
    }
}
