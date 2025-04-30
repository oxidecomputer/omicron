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
use id_map::IdMap;
use id_map::IdMappable;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::M2Slot;
use omicron_common::zpool_name::ZpoolName;
use sled_hardware::PooledDiskError;
use sled_storage::config::MountConfig;
use sled_storage::dataset::CLUSTER_DATASET;
use sled_storage::dataset::CONFIG_DATASET;
use sled_storage::dataset::M2_ARTIFACT_DATASET;
use sled_storage::dataset::M2_DEBUG_DATASET;
use sled_storage::disk::Disk;
use slog::Logger;
use std::mem;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

use crate::dump_setup::DumpSetup;
use crate::raw_disks::RawDiskWithId;

/// A thin wrapper around a [`watch::Receiver`] that presents a similar API.
#[derive(Debug, Clone)]
pub struct InternalDisksReceiver {
    mount_config: Arc<MountConfig>,
    inner: InternalDisksReceiverInner,
}

#[derive(Debug, Clone)]
enum InternalDisksReceiverInner {
    Real(watch::Receiver<Arc<IdMap<InternalDiskDetails>>>),
    #[cfg(feature = "testing")]
    FakeStatic(Arc<IdMap<InternalDiskDetails>>),
}

impl InternalDisksReceiver {
    #[cfg(feature = "testing")]
    pub fn fake_static(
        mount_config: Arc<MountConfig>,
        disks: impl Iterator<Item = (DiskIdentity, ZpoolName)>,
    ) -> Self {
        let inner = InternalDisksReceiverInner::FakeStatic(Arc::new(
            disks
                .map(|(identity, zpool_name)| InternalDiskDetails {
                    identity: Arc::new(identity),
                    zpool_name,
                    // We can expand the interface for fake disks if we need to
                    // be able to specify these fields in future tests.
                    is_boot_disk: false,
                    slot: None,
                    raw_devfs_path: None,
                })
                .collect(),
        ));
        Self { mount_config, inner }
    }

    pub(crate) fn spawn_internal_disks_task(
        mount_config: Arc<MountConfig>,
        raw_disks_rx: watch::Receiver<IdMap<RawDiskWithId>>,
        base_log: &Logger,
    ) -> Self {
        let (disks_tx, disks_rx) = watch::channel(Arc::default());

        tokio::spawn(
            InternalDisksTask {
                disks_tx,
                raw_disks_rx,
                mount_config: Arc::clone(&mount_config),
                dump_setup: DumpSetup::new(base_log, Arc::clone(&mount_config)),
                log: base_log.new(slog::o!("component" => "InternalDisksTask")),
            }
            .run(),
        );

        Self { mount_config, inner: InternalDisksReceiverInner::Real(disks_rx) }
    }

    /// Get the current set of managed internal disks without marking the
    /// returned value as seen.
    ///
    /// This is analogous to [`watch::Receiver::borrow()`], except it returns an
    /// owned value that does not keep the internal watch lock held.
    pub fn current(&self) -> InternalDisks {
        let disks = match &self.inner {
            InternalDisksReceiverInner::Real(rx) => Arc::clone(&*rx.borrow()),
            #[cfg(feature = "testing")]
            InternalDisksReceiverInner::FakeStatic(disks) => Arc::clone(disks),
        };
        InternalDisks { disks, mount_config: Arc::clone(&self.mount_config) }
    }

    /// Get the current set of managed internal disks and mark the returned
    /// value as seen.
    ///
    /// This is analogous to [`watch::Receiver::borrow_and_update()`], except it
    /// returns an owned value that does not keep the internal watch lock held.
    pub fn current_and_update(&mut self) -> InternalDisks {
        let disks = match &mut self.inner {
            InternalDisksReceiverInner::Real(rx) => {
                Arc::clone(&*rx.borrow_and_update())
            }
            #[cfg(feature = "testing")]
            InternalDisksReceiverInner::FakeStatic(disks) => Arc::clone(disks),
        };
        InternalDisks { disks, mount_config: Arc::clone(&self.mount_config) }
    }

    /// Wait for changes to the set of managed internal disks.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        match &mut self.inner {
            InternalDisksReceiverInner::Real(rx) => rx.changed().await,
            #[cfg(feature = "testing")]
            InternalDisksReceiverInner::FakeStatic(_) => {
                // Static set of disks never changes
                std::future::pending().await
            }
        }
    }

    /// Wait until the boot disk is managed, returning its identity.
    ///
    /// Internally updates the most-recently-seen value.
    pub(crate) async fn wait_for_boot_disk(&mut self) -> DiskIdentity {
        let disks_rx = match &mut self.inner {
            InternalDisksReceiverInner::Real(rx) => rx,
            #[cfg(feature = "testing")]
            InternalDisksReceiverInner::FakeStatic(disks) => {
                if let Some(disk) = disks.iter().find(|d| d.is_boot_disk) {
                    return (*disk.identity).clone();
                }
                panic!("fake InternalDisksReceiver has no boot disk")
            }
        };
        loop {
            let disks = disks_rx.borrow_and_update();
            if let Some(disk) = disks.iter().find(|d| d.is_boot_disk) {
                return (*disk.identity).clone();
            }
            mem::drop(disks);

            disks_rx.changed().await.expect("InternalDisks task never dies");
        }
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

    pub fn boot_disk_zpool(&self) -> Option<&ZpoolName> {
        self.disks.iter().find_map(|d| {
            if d.is_boot_disk { Some(&d.zpool_name) } else { None }
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

// A summary of a [`Disk`] without providing any of the associated functionality.
#[derive(Debug)]
struct InternalDiskDetails {
    identity: Arc<DiskIdentity>,
    zpool_name: ZpoolName,
    is_boot_disk: bool,

    // These two fields are optional because they don't exist for synthetic
    // disks.
    slot: Option<M2Slot>,
    raw_devfs_path: Option<Result<Utf8PathBuf, Arc<PooledDiskError>>>,
}

impl IdMappable for InternalDiskDetails {
    type Id = InternalDiskDetailsId;

    fn id(&self) -> Self::Id {
        InternalDiskDetailsId {
            identity: Arc::clone(&self.identity),
            is_boot_disk: self.is_boot_disk,
        }
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
            identity: Arc::new(disk.identity().clone()),
            zpool_name: disk.zpool_name().clone(),
            is_boot_disk: disk.is_boot_disk(),
            slot,
            raw_devfs_path,
        }
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

struct InternalDisksTask {
    disks_tx: watch::Sender<Arc<IdMap<InternalDiskDetails>>>,
    raw_disks_rx: watch::Receiver<IdMap<RawDiskWithId>>,
    mount_config: Arc<MountConfig>,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: DumpSetup,

    log: Logger,
}

impl InternalDisksTask {
    async fn run(self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_uuid_kinds::ZpoolUuid;
    use proptest::sample::size_range;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;

    #[derive(Debug, Arbitrary)]
    struct ArbitraryInternalDiskDetailsId {
        is_boot_disk: bool,
        vendor: String,
        model: String,
        serial: String,
    }

    #[proptest]
    fn boot_disks_sort_ahead_of_non_boot_disks_in_id_map(
        #[any(size_range(2..4).lift())] values: Vec<
            ArbitraryInternalDiskDetailsId,
        >,
    ) {
        let disk_map: IdMap<_> = values
            .into_iter()
            .map(|value| InternalDiskDetails {
                identity: Arc::new(DiskIdentity {
                    vendor: value.vendor,
                    model: value.model,
                    serial: value.serial,
                }),
                zpool_name: ZpoolName::new_internal(ZpoolUuid::new_v4()),
                is_boot_disk: value.is_boot_disk,
                slot: None,
                raw_devfs_path: None,
            })
            .collect();

        // When iterating over the contents, we should never see a boot disk
        // after a non-boot disk.
        let mut saw_non_boot_disk = false;
        for disk in disk_map.iter() {
            if disk.is_boot_disk {
                assert!(!saw_non_boot_disk);
            } else {
                saw_non_boot_disk = true;
            }
        }
    }
}
