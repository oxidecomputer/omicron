// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Tools for collecting and inspecting service bundles for zones.

use anyhow::Context;
use anyhow::anyhow;
use camino::FromPathBufError;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use flate2::bufread::GzDecoder;
use illumos_utils::running_zone::RunningZone;
use illumos_utils::running_zone::ServiceProcess;
use illumos_utils::running_zone::is_oxide_smf_log_file;
use illumos_utils::zfs::CreateSnapshotError;
use illumos_utils::zfs::DestroyDatasetError;
use illumos_utils::zfs::DestroySnapshotError;
use illumos_utils::zfs::EnsureDatasetError;
use illumos_utils::zfs::GetValueError;
use illumos_utils::zfs::ListDatasetsError;
use illumos_utils::zfs::ListSnapshotsError;
use illumos_utils::zfs::SetValueError;
use illumos_utils::zfs::Snapshot;
use illumos_utils::zfs::ZFS;
use illumos_utils::zfs::Zfs;
use illumos_utils::zone::AdmError;
use sled_agent_types::zone_bundle::*;
use sled_storage::dataset::U2_DEBUG_DATASET;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tar::Archive;
use tar::Builder;
use tar::Header;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio::time::sleep;
use uuid::Uuid;

// The name of the snapshot created from the zone root filesystem.
const ZONE_ROOT_SNAPSHOT_NAME: &'static str = "zone-root";

// The prefix for all the snapshots for each filesystem containing archived
// logs. Each debug data, such as `oxp_<UUID>/crypt/debug`, generates a snapshot
// named `zone-archives-<UUID>`.
const ARCHIVE_SNAPSHOT_PREFIX: &'static str = "zone-archives-";

// An extra ZFS user property attached to all zone bundle snapshots.
//
// This is used to ensure that we are not accidentally deleting ZFS objects that
// a user has created, but which happen to be named the same thing.
const ZONE_BUNDLE_ZFS_PROPERTY_NAME: &'static str = "oxide:for-zone-bundle";
const ZONE_BUNDLE_ZFS_PROPERTY_VALUE: &'static str = "true";

// Initialize the ZFS resources we need for zone bundling.
//
// This deletes any snapshots matching the names we expect to create ourselves
// during bundling.
#[cfg(not(test))]
async fn initialize_zfs_resources(log: &Logger) -> Result<(), BundleError> {
    let mut zb_snapshots = Vec::new();
    for snap in Zfs::list_snapshots().await.unwrap().into_iter() {
        // Check for snapshots named how we expect to create them.
        if snap.snap_name != ZONE_ROOT_SNAPSHOT_NAME
            || !snap.snap_name.starts_with(ARCHIVE_SNAPSHOT_PREFIX)
        {
            continue;
        }

        // Additionally check for the zone-bundle-specific property.
        //
        // If we find a dataset that matches our names, but which _does not_
        // have such a property (or has in invalid property), we'll log it
        // but avoid deleting the snapshot.
        let name = snap.to_string();
        let Ok([value]) = Zfs::get_values(
            &name,
            &[ZONE_BUNDLE_ZFS_PROPERTY_NAME],
            Some(illumos_utils::zfs::PropertySource::Local),
        )
        .await
        else {
            warn!(
                log,
                "Found a ZFS snapshot with a name reserved for zone \
                bundling, but which does not have the zone-bundle-specific \
                property. Bailing out, rather than risking deletion of \
                user data.";
                "snap_name" => &name,
                "property" => ZONE_BUNDLE_ZFS_PROPERTY_NAME
            );
            continue;
        };
        if value != ZONE_BUNDLE_ZFS_PROPERTY_VALUE {
            warn!(
                log,
                "Found a ZFS snapshot with a name reserved for zone \
                bundling, with an unexpected property value. \
                Bailing out, rather than risking deletion of user data.";
                "snap_name" => &name,
                "property" => ZONE_BUNDLE_ZFS_PROPERTY_NAME,
                "property_value" => value,
            );
            continue;
        }
        zb_snapshots.push(snap);
    }
    for snapshot in zb_snapshots {
        Zfs::destroy_snapshot(&snapshot.filesystem, &snapshot.snap_name)
            .await?;
        debug!(
            log,
            "destroyed pre-existing zone bundle snapshot";
            "snapshot" => %snapshot,
        );
    }
    Ok(())
}

/// A type managing zone bundle creation and automatic cleanup.
#[derive(Clone)]
pub struct ZoneBundler {
    log: Logger,
    inner: Arc<Mutex<Inner>>,
    // Channel for notifying the cleanup task that it should reevaluate.
    notify_cleanup: Arc<Notify>,
}

// State shared between tasks, e.g., used when creating a bundle in different
// tasks or between a creation and cleanup.
struct Inner {
    storage_handle: StorageHandle,
    cleanup_context: CleanupContext,
    last_cleanup_at: Instant,
}

impl Inner {
    // Return the time at which the next cleanup should occur, and the duration
    // until that time.
    //
    // The instant may be in the past, in which case duration would be 0.
    fn next_cleanup(&self) -> (Instant, Duration) {
        let next =
            self.last_cleanup_at + self.cleanup_context.period.as_duration();
        let delta = next.saturating_duration_since(Instant::now());
        (next, delta)
    }

    // Ensure that the zone bundle directories that _can_ exist in fact do.
    //
    // The zone bundles are stored in a ZFS dataset on each M.2. These datasets
    // are created by the storage manager upon request. Until those parent
    // datasets exist, the bundle directories themselves cannot be accessed
    // either.
    //
    // This method takes the _expected_ zone bundle directories; creates any
    // that can exist but do not, i.e., those whose parent datasets already
    // exist; and returns those.
    async fn bundle_directories(&self) -> Vec<Utf8PathBuf> {
        let resources = self.storage_handle.get_latest_disks().await;
        // NOTE: These bundle directories are always stored on M.2s, so we don't
        // need to worry about synchronizing with U.2 disk expungement at the
        // callsite.
        let expected = resources.all_zone_bundle_directories();
        let mut out = Vec::with_capacity(expected.len());
        for each in expected.into_iter() {
            if tokio::fs::create_dir_all(&each).await.is_ok() {
                out.push(each);
            }
        }
        out.sort();
        out
    }
}

impl ZoneBundler {
    // A task run in the background that periodically cleans up bundles.
    //
    // This waits for:
    //
    // - A timeout at the current cleanup period
    // - A notification that the cleanup context has changed.
    //
    // When needed, it actually runs the period cleanup itself, using the
    // current context.
    async fn periodic_cleanup(
        log: Logger,
        inner: Arc<Mutex<Inner>>,
        notify_cleanup: Arc<Notify>,
    ) {
        let (mut next_cleanup, mut time_to_next_cleanup) =
            inner.lock().await.next_cleanup();
        loop {
            info!(
                log,
                "top of bundle cleanup loop";
                "next_cleanup" => ?&next_cleanup,
                "time_to_next_cleanup" => ?time_to_next_cleanup,
            );

            // Wait for the cleanup period to expire, or a notification that the
            // context has been changed.
            tokio::select! {
                _ = sleep(time_to_next_cleanup) => {
                    info!(log, "running automatic periodic zone bundle cleanup");
                    let mut inner_ = inner.lock().await;
                    let dirs = inner_.bundle_directories().await;
                    let res = run_cleanup(&log, &dirs, &inner_.cleanup_context).await;
                    inner_.last_cleanup_at = Instant::now();
                    (next_cleanup, time_to_next_cleanup) = inner_.next_cleanup();
                    debug!(log, "cleanup completed"; "result" => ?res);
                }
                _ = notify_cleanup.notified() => {
                    debug!(log, "notified about cleanup context change");
                    let inner_ = inner.lock().await;
                    (next_cleanup, time_to_next_cleanup) = inner_.next_cleanup();
                }
            }
        }
    }

    /// Create a new zone bundler.
    ///
    /// This creates an object that manages zone bundles on the system. It can
    /// be used to create bundles from running zones, and runs a periodic task
    /// to clean them up to free up space.
    pub async fn new(
        log: Logger,
        storage_handle: StorageHandle,
        cleanup_context: CleanupContext,
    ) -> Self {
        // This is compiled out in tests because there's no way to set our
        // expectations on the mockall object it uses internally. Not great.
        //
        // NOTE: ^ This comment was written when mockall was still used in
        // Omicron. It has since been removed; it may be possible to use
        // dependency injection here ("fake" vs "real" implementation of ZFS)
        // instead of conditional compilation.
        // See also: The "sled-storage" `StorageManagerTestHarness`, which
        // might be useful for making ZFS datasets on top of a test-only
        // temporary directory.
        #[cfg(not(test))]
        initialize_zfs_resources(&log)
            .await
            .expect("Failed to initialize existing ZFS resources");
        let notify_cleanup = Arc::new(Notify::new());
        let inner = Arc::new(Mutex::new(Inner {
            storage_handle,
            cleanup_context,
            last_cleanup_at: Instant::now(),
        }));
        let cleanup_log = log.new(slog::o!("component" => "auto-cleanup-task"));
        let notify_clone = notify_cleanup.clone();
        let inner_clone = inner.clone();
        tokio::task::spawn(Self::periodic_cleanup(
            cleanup_log,
            inner_clone,
            notify_clone,
        ));
        Self { log, inner, notify_cleanup }
    }

    /// Trigger an immediate cleanup of low-priority zone bundles.
    pub async fn cleanup(
        &self,
    ) -> Result<BTreeMap<Utf8PathBuf, CleanupCount>, BundleError> {
        let mut inner = self.inner.lock().await;
        let dirs = inner.bundle_directories().await;
        let res = run_cleanup(&self.log, &dirs, &inner.cleanup_context).await;
        inner.last_cleanup_at = Instant::now();
        self.notify_cleanup.notify_one();
        res
    }

    /// Return the utilization of the system for zone bundles.
    pub async fn utilization(
        &self,
    ) -> Result<BTreeMap<Utf8PathBuf, BundleUtilization>, BundleError> {
        let inner = self.inner.lock().await;
        let dirs = inner.bundle_directories().await;
        compute_bundle_utilization(&self.log, &dirs, &inner.cleanup_context)
            .await
    }

    /// Return the context used to periodically clean up zone bundles.
    pub async fn cleanup_context(&self) -> CleanupContext {
        self.inner.lock().await.cleanup_context
    }

    /// Update the context used to periodically clean up zone bundles.
    pub async fn update_cleanup_context(
        &self,
        new_period: Option<CleanupPeriod>,
        new_storage_limit: Option<StorageLimit>,
        new_priority: Option<PriorityOrder>,
    ) -> Result<(), BundleError> {
        let mut inner = self.inner.lock().await;
        info!(
            self.log,
            "received request to update cleanup context";
            "period" => ?new_period,
            "priority" => ?new_priority,
            "storage_limit" => ?new_storage_limit,
        );
        let mut notify_cleanup_task = false;
        if let Some(new_period) = new_period {
            if new_period < inner.cleanup_context.period {
                warn!(
                    self.log,
                    "auto cleanup period has been reduced, \
                    the cleanup task will be notified"
                );
                notify_cleanup_task = true;
            }
            inner.cleanup_context.period = new_period;
        }
        if let Some(new_priority) = new_priority {
            inner.cleanup_context.priority = new_priority;
        }
        if let Some(new_storage_limit) = new_storage_limit {
            if new_storage_limit < inner.cleanup_context.storage_limit {
                notify_cleanup_task = true;
                warn!(
                    self.log,
                    "storage limit has been lowered, a \
                    cleanup will be run immediately"
                );
            }
            inner.cleanup_context.storage_limit = new_storage_limit;
        }
        if notify_cleanup_task {
            self.notify_cleanup.notify_one();
        }
        Ok(())
    }

    /// Create a bundle from the provided zone.
    pub async fn create(
        &self,
        zone: &RunningZone,
        cause: ZoneBundleCause,
    ) -> Result<ZoneBundleMetadata, BundleError> {
        // NOTE: [Self::await_completion_of_prior_bundles] relies on this lock
        // being held across this whole function. If we want more concurrency,
        // we'll need to add a barrier-like mechanism to let callers know when
        // prior bundles have completed.
        let inner = self.inner.lock().await;
        let storage_dirs = inner.bundle_directories().await;
        let resources = inner.storage_handle.get_latest_disks().await;
        let extra_log_dirs = resources
            .all_u2_mountpoints(U2_DEBUG_DATASET)
            .into_iter()
            .map(|pool_path| pool_path.path)
            .collect();
        let context = ZoneBundleContext { cause, storage_dirs, extra_log_dirs };
        info!(
            self.log,
            "creating zone bundle";
            "zone_name" => zone.name(),
            "context" => ?context,
        );
        create(&self.log, zone, &context).await
    }

    /// Awaits the completion of all prior calls to [ZoneBundler::create].
    ///
    /// This is critical for disk expungement, which wants to ensure that the
    /// Sled Agent is no longer using devices after they have been expunged.
    pub async fn await_completion_of_prior_bundles(&self) {
        let _ = self.inner.lock().await;
    }

    /// Return the paths for all bundles of the provided zone and ID.
    pub async fn bundle_paths(
        &self,
        name: &str,
        id: &Uuid,
    ) -> Result<Vec<Utf8PathBuf>, BundleError> {
        let inner = self.inner.lock().await;
        let dirs = inner.bundle_directories().await;
        get_zone_bundle_paths(&self.log, &dirs, name, id).await
    }

    /// List bundles for a zone with the provided name.
    pub async fn list_for_zone(
        &self,
        name: &str,
    ) -> Result<Vec<ZoneBundleMetadata>, BundleError> {
        // The zone bundles are replicated in several places, so we'll use a set
        // to collect them all, to avoid duplicating.
        let mut bundles = BTreeSet::new();
        let inner = self.inner.lock().await;
        let dirs = inner.bundle_directories().await;
        for dir in dirs.iter() {
            bundles.extend(
                list_bundles_for_zone(&self.log, &dir, name)
                    .await?
                    .into_iter()
                    .map(|(_path, bdl)| bdl),
            );
        }
        Ok(bundles.into_iter().collect())
    }

    /// List all zone bundles that match the provided filter, if any.
    ///
    /// The filter is a simple substring match -- any zone bundle with a zone
    /// name that contains the filter anywhere will match. If no filter is
    /// provided, all extant bundles will be listed.
    pub async fn list(
        &self,
        filter: Option<&str>,
    ) -> Result<Vec<ZoneBundleMetadata>, BundleError> {
        // The zone bundles are replicated in several places, so we'll use a set
        // to collect them all, to avoid duplicating.
        let mut bundles = BTreeSet::new();
        let inner = self.inner.lock().await;
        let dirs = inner.bundle_directories().await;
        for dir in dirs.iter() {
            let mut rd = tokio::fs::read_dir(dir).await.map_err(|err| {
                BundleError::ReadDirectory { directory: dir.to_owned(), err }
            })?;
            while let Some(entry) = rd.next_entry().await.map_err(|err| {
                BundleError::ReadDirectory { directory: dir.to_owned(), err }
            })? {
                let search_dir = Utf8PathBuf::try_from(entry.path())?;
                bundles.extend(
                    filter_zone_bundles(&self.log, &search_dir, |md| {
                        filter
                            .map(|filt| md.id.zone_name.contains(filt))
                            .unwrap_or(true)
                    })
                    .await?
                    .into_values(),
                );
            }
        }
        Ok(bundles.into_iter().collect())
    }
}

// Context for creating a bundle of a specified zone.
#[derive(Debug, Default)]
struct ZoneBundleContext {
    // The directories into which the zone bundles are written.
    storage_dirs: Vec<Utf8PathBuf>,
    // The reason or cause for creating a zone bundle.
    cause: ZoneBundleCause,
    // Extra directories searched for logfiles for the named zone.
    //
    // Logs are periodically archived out of their original location, and onto
    // one or more U.2 drives. This field is used to specify that archive
    // location, so that rotated logs for the zone's services may be found.
    extra_log_dirs: Vec<Utf8PathBuf>,
}

// The set of zone-wide commands, which don't require any details about the
// processes we've launched in the zone.
const ZONE_WIDE_COMMANDS: [&[&str]; 6] = [
    &["ptree"],
    &["uptime"],
    &["last"],
    &["who"],
    &["svcs", "-p"],
    &["netstat", "-an"],
];

// The name for zone bundle metadata files.
const ZONE_BUNDLE_METADATA_FILENAME: &str = "metadata.toml";

/// Errors related to managing service zone bundles.
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("I/O error running command '{cmd}'")]
    Command {
        cmd: String,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error creating directory '{directory}'")]
    CreateDirectory {
        directory: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error opening bundle tarball '{path}'")]
    OpenBundleFile {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error adding bundle tarball data to '{tarball_path}'")]
    AddBundleData {
        tarball_path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error reading bundle tarball data from '{path}'")]
    ReadBundleData {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error copying bundle tarball from '{from}' to '{to}'")]
    CopyArchive {
        from: Utf8PathBuf,
        to: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error reading directory '{directory}'")]
    ReadDirectory {
        directory: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("I/O error fetching metadata for '{path}'")]
    Metadata {
        path: Utf8PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("TOML serialization failure")]
    Serialization(#[from] toml::ser::Error),

    #[error("TOML deserialization failure")]
    Deserialization(#[from] toml::de::Error),

    #[error("No zone named '{name}' is available for bundling")]
    NoSuchZone { name: String },

    #[error("No storage available for bundles")]
    NoStorage,

    #[error("Failed to join zone bundling task")]
    Task(#[from] tokio::task::JoinError),

    #[error("Failed to send request to instance/instance manager")]
    FailedSend(anyhow::Error),

    #[error("Instance/Instance Manager dropped our request")]
    DroppedRequest(anyhow::Error),

    #[error("Failed to create bundle: {0}")]
    BundleFailed(#[from] anyhow::Error),

    #[error("Zone error")]
    Zone(#[from] AdmError),

    #[error(transparent)]
    PathBuf(#[from] FromPathBufError),

    #[error("Zone '{name}' cannot currently be bundled")]
    Unavailable { name: String },

    #[error(transparent)]
    StorageLimitCreate(#[from] StorageLimitCreateError),

    #[error(transparent)]
    CleanupPeriodCreate(#[from] CleanupPeriodCreateError),

    #[error(transparent)]
    PriorityOrderCreate(#[from] PriorityOrderCreateError),

    #[error("Cleanup failed")]
    Cleanup(#[source] anyhow::Error),

    #[error("Failed to create ZFS snapshot")]
    CreateSnapshot(#[from] CreateSnapshotError),

    #[error("Failed to destroy ZFS snapshot")]
    DestroySnapshot(#[from] DestroySnapshotError),

    #[error("Failed to list ZFS snapshots")]
    ListSnapshot(#[from] ListSnapshotsError),

    #[error("Failed to ensure ZFS dataset")]
    EnsureDataset(#[from] EnsureDatasetError),

    #[error("Failed to destroy ZFS dataset")]
    DestroyDataset(#[from] DestroyDatasetError),

    #[error("Failed to list ZFS datasets")]
    ListDatasets(#[from] ListDatasetsError),

    #[error("Failed to set Oxide-specific ZFS property")]
    SetProperty(#[from] SetValueError),

    #[error("Failed to get ZFS property value")]
    GetProperty(#[from] GetValueError),

    #[error("Instance is terminating")]
    InstanceTerminating,

    /// The `walkdir` crate's errors already include the path which could not be
    /// read (if one exists), so we can just wrap them directly.
    #[error(transparent)]
    WalkDir(#[from] walkdir::Error),
}

// Helper function to write an array of bytes into the tar archive, with
// the provided name.
fn insert_data<W: std::io::Write>(
    builder: &mut Builder<W>,
    name: &str,
    contents: &[u8],
) -> Result<(), BundleError> {
    let mtime = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("failed to compute mtime")?
        .as_secs();

    let mut hdr = Header::new_ustar();
    hdr.set_size(contents.len().try_into().unwrap());
    hdr.set_mode(0o444);
    hdr.set_mtime(mtime);
    hdr.set_entry_type(tar::EntryType::Regular);
    // NOTE: This internally sets the path and checksum.
    builder.append_data(&mut hdr, name, Cursor::new(contents)).map_err(|err| {
        BundleError::AddBundleData { tarball_path: name.into(), err }
    })
}

// Create a read-only snapshot from an existing filesystem.
async fn create_snapshot(
    log: &Logger,
    filesystem: &str,
    snap_name: &str,
) -> Result<Snapshot, BundleError> {
    Zfs::create_snapshot(
        filesystem,
        snap_name,
        &[(ZONE_BUNDLE_ZFS_PROPERTY_NAME, ZONE_BUNDLE_ZFS_PROPERTY_VALUE)],
    )
    .await?;
    debug!(
        log,
        "created snapshot";
        "filesystem" => filesystem,
        "snap_name" => snap_name,
    );
    Ok(Snapshot {
        filesystem: filesystem.to_string(),
        snap_name: snap_name.to_string(),
    })
}

// Create snapshots for the filesystems we need to copy out all log files.
//
// A key feature of the zone-bundle process is that we pull all the log files
// for a zone. This is tricky. The logs are both being written to by the
// programs we're interested in, and also potentially being rotated by `logadm`,
// and / or archived out to the U.2s through the code in `crate::dump_setup`.
//
// We need to capture all these logs, while avoiding inconsistent state (e.g., a
// missing log message that existed when the bundle was created) and also
// interrupting the rotation and archival processes. We do this by taking ZFS
// snapshots of the relevant datasets when we want to create the bundle.
//
// When we receive a bundling request, we take a snapshot of a few datasets:
//
// - The zone filesystem itself, `oxz_<UUID>/crypt/zone/<ZONE_NAME>`.
//
// - All of the U.2 debug datasets, like `oxp_<UUID>/crypt/debug`, which we know
// contain logs for the given zone. This is done by looking at all the service
// processes in the zone, and mapping the locations of archived logs, such as
// `/pool/ext/<UUID>/crypt/debug/<ZONE_NAME>` to the zpool name.
//
// This provides us with a consistent view of the log files at the time the
// bundle was requested. Note that this ordering, taking the root FS snapshot
// first followed by the archive datasets, ensures that we don't _miss_ log
// messages that existed when the bundle was requested. It's possible that we
// double-count them however: the archiver could run concurrently, and result in
// a log file existing on the root snapshot when we create it, and also on the
// achive snapshot by the time we get around to creating that.
//
// At this point, we operate entirely on those snapshots. We search for
// "current" log files in the root snapshot, and archived log files in the
// archive snapshots.
async fn create_zfs_snapshots(
    log: &Logger,
    zone: &RunningZone,
    extra_log_dirs: &[Utf8PathBuf],
) -> Result<Vec<Snapshot>, BundleError> {
    // Snapshot the root filesystem.
    let dataset = Zfs::get_dataset_name(zone.root().as_str()).await?;
    let root_snapshot =
        create_snapshot(log, &dataset, ZONE_ROOT_SNAPSHOT_NAME).await?;
    let mut snapshots = vec![root_snapshot];

    // Look at all the provided extra log directories, and take a snapshot for
    // any that have a directory with the zone name. These may not have any log
    // file in them yet, but we'll snapshot now and then filter more judiciously
    // when we actually find the files we want to bundle.
    let mut maybe_err = None;
    for dir in extra_log_dirs.iter() {
        let zone_dir = dir.join(zone.name());
        match std::fs::metadata(&zone_dir) {
            Ok(d) => {
                if d.is_dir() {
                    let dataset =
                        match Zfs::get_dataset_name(zone_dir.as_str()).await {
                            Ok(ds) => Utf8PathBuf::from(ds),
                            Err(e) => {
                                error!(
                                    log,
                                    "failed to list datasets, will \
                                    unwind any previously created snapshots";
                                    "error" => ?e,
                                );
                                assert!(
                                    maybe_err
                                        .replace(BundleError::from(e))
                                        .is_none()
                                );
                                break;
                            }
                        };

                    // These datasets are named like `<pool_name>/...`. Since
                    // we're snapshotting zero or more of them, we disambiguate
                    // with the pool name.
                    let pool_name = dataset
                        .components()
                        .next()
                        .expect("Zone archive datasets must be non-empty");
                    let snap_name =
                        format!("{}{}", ARCHIVE_SNAPSHOT_PREFIX, pool_name);
                    match create_snapshot(log, dataset.as_str(), &snap_name)
                        .await
                    {
                        Ok(snapshot) => snapshots.push(snapshot),
                        Err(e) => {
                            error!(
                                log,
                                "failed to create snapshot, will \
                                unwind any previously created";
                                "error" => ?e,
                            );
                            assert!(maybe_err.replace(e).is_none());
                            break;
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                trace!(
                    log,
                    "skipping non-existent zone-bundle directory";
                    "dir" => %zone_dir,
                );
            }
            Err(e) => {
                error!(
                    log,
                    "failed to get metadata for potential zone directory";
                    "zone_dir" => %zone_dir,
                    "error" => ?e,
                );
            }
        }
    }
    if let Some(err) = maybe_err {
        cleanup_zfs_snapshots(log, &snapshots).await;
        return Err(err);
    };
    Ok(snapshots)
}

// Destroy any created ZFS snapshots.
async fn cleanup_zfs_snapshots(log: &Logger, snapshots: &[Snapshot]) {
    for snapshot in snapshots.iter() {
        match Zfs::destroy_snapshot(&snapshot.filesystem, &snapshot.snap_name)
            .await
        {
            Ok(_) => debug!(
                log,
                "destroyed zone bundle ZFS snapshot";
                "snapshot" => %snapshot,
            ),
            Err(e) => error!(
                log,
                "failed to destroy zone bundle ZFS snapshot";
                "snapshot" => %snapshot,
                "error" => ?e,
            ),
        }
    }
}

// List all log files (current, rotated, and archived) that should be part of
// the zone bundle for a single service.
async fn find_service_log_files(
    log: &Logger,
    zone_name: &str,
    svc: &ServiceProcess,
    extra_log_dirs: &[Utf8PathBuf],
    snapshots: &[Snapshot],
) -> Result<Vec<Utf8PathBuf>, BundleError> {
    // The current and any rotated, but not archived, log files live in the zone
    // root filesystem. Extract any which match.
    //
    // There are a few path tricks to keep in mind here. We've created a
    // snapshot from the zone's filesystem, which is usually something like
    // `<pool_name>/crypt/zone/<zone_name>`. That snapshot is placed in a hidden
    // directory within the base dataset, something like
    // `oxp_<pool_id>/crypt/zone/.zfs/snapshot/<snap_name>`.
    //
    // The log files themselves are things like `/var/svc/log/...`, but in the
    // actual ZFS dataset comprising the root FS for the zone, there are
    // additional directories, most notably `<zonepath>/root`. So the _cloned_
    // log file will live at
    // `/<pool_name>/crypt/zone/.zfs/snapshot/<snap_name>/root/var/svc/log/...`.
    let mut current_log_file = snapshots[0].full_path().await?;
    current_log_file.push(RunningZone::ROOT_FS_PATH);
    current_log_file.push(svc.log_file.as_str().trim_start_matches('/'));
    let log_dir =
        current_log_file.parent().expect("Current log file must have a parent");
    let mut log_files = vec![current_log_file.clone()];
    for entry in log_dir.read_dir_utf8().map_err(|err| {
        BundleError::ReadDirectory { directory: log_dir.into(), err }
    })? {
        let entry = entry.map_err(|err| BundleError::ReadDirectory {
            directory: log_dir.into(),
            err,
        })?;
        let path = entry.path();

        // Camino's Utf8Path only considers whole path components to match,
        // so convert both paths into a &str and use that object's
        // starts_with. See the `camino_starts_with_behaviour` test.
        let path_ref: &str = path.as_ref();
        let current_log_file_ref: &str = current_log_file.as_ref();
        if path != current_log_file
            && path_ref.starts_with(current_log_file_ref)
        {
            log_files.push(path.into());
        }
    }

    // The _archived_ log files are slightly trickier. They can technically live
    // in many different datasets, because the archive process may need to start
    // archiving to one location, but move to another if a quota is hit. We'll
    // iterate over all the extra log directories and try to find any log files
    // in those filesystem snapshots.
    let mut snapped_extra_log_dirs = BTreeSet::new();

    for snapshot in snapshots.iter().skip(1) {
        for d in extra_log_dirs {
            // Join the snapshot path with both the log directory and the
            // zone name, to arrive at something like:
            // /path/to/dataset/.zfs/snapshot/<snap_name>/path/to/extra/<zone_name>
            let p = snapshot.full_path().await?;
            snapped_extra_log_dirs.insert(p.join(d).join(zone_name));
        }
    }
    debug!(
        log,
        "looking for extra log files in filesystem snapshots";
        "extra_dirs" => ?&snapped_extra_log_dirs,
    );
    log_files.extend(
        find_archived_log_files(
            log,
            zone_name,
            &svc.service_name,
            svc.log_file.file_name().unwrap(),
            snapped_extra_log_dirs.iter(),
        )
        .await,
    );
    debug!(
        log,
        "found log files";
        "log_files" => ?&log_files,
    );
    Ok(log_files)
}

// Create a service bundle for the provided zone.
//
// This runs a series of debugging commands in the zone, to collect data about
// the state of the zone and any Oxide service processes running inside. The
// data is packaged into a tarball, and placed in the provided output
// directories.
async fn create(
    log: &Logger,
    zone: &RunningZone,
    context: &ZoneBundleContext,
) -> Result<ZoneBundleMetadata, BundleError> {
    // Fetch the directory into which we'll store data, and ensure it exists.
    if context.storage_dirs.is_empty() {
        warn!(log, "no directories available for zone bundles");
        return Err(BundleError::NoStorage);
    }
    let mut zone_bundle_dirs = Vec::with_capacity(context.storage_dirs.len());
    for dir in context.storage_dirs.iter() {
        let bundle_dir = dir.join(zone.name());
        debug!(log, "creating bundle directory"; "dir" => %bundle_dir);
        tokio::fs::create_dir_all(&bundle_dir).await.map_err(|err| {
            BundleError::CreateDirectory {
                directory: bundle_dir.to_owned(),
                err,
            }
        })?;
        zone_bundle_dirs.push(bundle_dir);
    }

    // Create metadata and the tarball writer.
    //
    // We'll write the contents of the bundle into a gzipped tar archive,
    // including metadata and a file for the output of each command we run in
    // the zone.
    let zone_metadata = ZoneBundleMetadata::new(zone.name(), context.cause);
    let filename = format!("{}.tar.gz", zone_metadata.id.bundle_id);
    let full_path = zone_bundle_dirs[0].join(&filename);
    let file = match tokio::fs::File::create(&full_path).await {
        Ok(f) => f.into_std().await,
        Err(e) => {
            error!(
                log,
                "failed to create bundle file";
                "zone" => zone.name(),
                "file" => %full_path,
                "error" => ?e,
            );
            return Err(BundleError::OpenBundleFile {
                path: full_path.to_owned(),
                err: e,
            });
        }
    };
    debug!(
        log,
        "created bundle tarball file";
        "zone" => zone.name(),
        "path" => %full_path
    );
    let gz = flate2::GzBuilder::new()
        .filename(filename.as_str())
        .write(file, flate2::Compression::best());
    let mut builder = Builder::new(gz);

    // Write the metadata file itself, in TOML format.
    let contents = toml::to_string(&zone_metadata)?;
    insert_data(
        &mut builder,
        ZONE_BUNDLE_METADATA_FILENAME,
        contents.as_bytes(),
    )?;
    debug!(
        log,
        "wrote zone bundle metadata";
        "zone" => zone.name(),
    );
    for cmd in ZONE_WIDE_COMMANDS {
        debug!(
            log,
            "running zone bundle command";
            "zone" => zone.name(),
            "command" => ?cmd,
        );
        let output = match zone.run_cmd(cmd) {
            Ok(s) => s,
            Err(e) => format!("{}", e),
        };
        let contents = format!("Command: {:?}\n{}", cmd, output).into_bytes();
        if let Err(e) = insert_data(&mut builder, cmd[0], &contents) {
            error!(
                log,
                "failed to save zone bundle command output";
                "zone" => zone.name(),
                "command" => ?cmd,
                "error" => ?e,
            );
        }
    }

    // Enumerate the list of Oxide-specific services inside the zone that we
    // want to include in the bundling process.
    let procs = match zone
        .service_processes()
        .context("failed to enumerate zone service processes")
    {
        Ok(p) => {
            debug!(
                log,
                "enumerated service processes";
                "zone" => zone.name(),
                "procs" => ?p,
            );
            p
        }
        Err(e) => {
            error!(
                log,
                "failed to enumerate zone service processes";
                "zone" => zone.name(),
                "error" => ?e,
            );
            return Err(BundleError::from(e));
        }
    };

    // Create ZFS snapshots of filesystems containing log files.
    //
    // We need to capture log files from two kinds of locations:
    //
    // - The zone root filesystem, where the current and rotated (but not
    // archived) log files live.
    // - Zero or more filesystems on the U.2s used for archiving older log
    // files.
    //
    // Both of these are dynamic. The current log file is likely being written
    // by the service itself, and `logadm` may also be rotating files. At the
    // same time, the log-archival process in `dump_setup.rs` may be copying
    // these out to the U.2s, after which it deletes those on the zone
    // filesystem itself.
    //
    // To avoid various kinds of corruption, such as a bad tarball or missing
    // log messages, we'll create ZFS snapshots of each of these relevant
    // filesystems, and insert those (now-static) files into the zone-bundle
    // tarballs.
    let snapshots =
        match create_zfs_snapshots(log, zone, &context.extra_log_dirs).await {
            Ok(snapshots) => snapshots,
            Err(e) => {
                error!(
                    log,
                    "failed to create ZFS snapshots";
                    "zone_name" => zone.name(),
                    "error" => ?e,
                );
                return Err(e);
            }
        };

    // Debugging commands run on the specific processes this zone defines.
    const ZONE_PROCESS_COMMANDS: [&str; 3] = [
        "pfiles", "pstack",
        "pargs",
        // TODO-completeness: We may want `gcore`, since that encompasses
        // the above commands and much more. It seems like overkill now,
        // however.
    ];
    for svc in procs.into_iter() {
        let pid_s = svc.pid.to_string();
        for cmd in ZONE_PROCESS_COMMANDS {
            let args = &[cmd, &pid_s];
            debug!(
                log,
                "running zone bundle command";
                "zone" => zone.name(),
                "command" => ?args,
            );
            let output = match zone.run_cmd(args) {
                Ok(s) => s,
                Err(e) => format!("{}", e),
            };
            let contents =
                format!("Command: {:?}\n{}", args, output).into_bytes();

            // There may be multiple Oxide service processes for which we
            // want to capture the command output. Name each output after
            // the command and PID to disambiguate.
            let filename = format!("{}.{}", cmd, svc.pid);
            if let Err(e) = insert_data(&mut builder, &filename, &contents) {
                error!(
                    log,
                    "failed to save zone bundle command output";
                    "zone" => zone.name(),
                    "command" => ?args,
                    "error" => ?e,
                );
            }
        }

        // Collect and insert all log files.
        //
        // This takes files from the snapshot of either the zone root
        // filesystem, or the filesystem containing the archived log files.
        let all_log_files = match find_service_log_files(
            log,
            zone.name(),
            &svc,
            &context.extra_log_dirs,
            &snapshots,
        )
        .await
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    log,
                    "failed to find service log files";
                    "zone" => zone.name(),
                    "error" => ?e,
                );
                cleanup_zfs_snapshots(&log, &snapshots).await;
                return Err(e);
            }
        };
        for log_file in all_log_files.into_iter() {
            match builder
                .append_path_with_name(&log_file, log_file.file_name().unwrap())
            {
                Ok(_) => {
                    debug!(
                        log,
                        "appended log file to zone bundle";
                        "zone" => zone.name(),
                        "log_file" => %log_file,
                    );
                }
                Err(e) => {
                    error!(
                        log,
                        "failed to append log file to zone bundle";
                        "zone" => zone.name(),
                        "log_file" => %svc.log_file,
                        "error" => ?e,
                    );
                }
            }
        }
    }

    // Finish writing out the tarball itself.
    if let Err(e) = builder.into_inner().context("Failed to build bundle") {
        cleanup_zfs_snapshots(&log, &snapshots).await;
        return Err(BundleError::from(e));
    }

    // Copy the bundle to the other locations. We really want the bundles to
    // be duplicates, not an additional, new bundle.
    //
    // TODO-robustness: We should probably create the bundle in a temp dir, and
    // copy it to all the final locations. This would make it easier to cleanup
    // the final locations should that last copy fail for any of them.
    //
    // See: https://github.com/oxidecomputer/omicron/issues/3876.
    let mut copy_err = None;
    for other_dir in zone_bundle_dirs.iter().skip(1) {
        let to = other_dir.join(&filename);
        debug!(log, "copying bundle"; "from" => %full_path, "to" => %to);
        if let Err(e) = tokio::fs::copy(&full_path, &to).await.map_err(|err| {
            BundleError::CopyArchive { from: full_path.to_owned(), to, err }
        }) {
            copy_err = Some(e);
            break;
        }
    }
    cleanup_zfs_snapshots(&log, &snapshots).await;
    if let Some(err) = copy_err {
        return Err(err);
    }
    info!(log, "finished zone bundle"; "metadata" => ?zone_metadata);
    Ok(zone_metadata)
}

// Find log files for the specified zone / SMF service, which may have been
// archived out to a U.2 dataset.
//
// Note that errors are logged, rather than failing the whole function, so that
// one failed listing does not prevent collecting any other log files.
async fn find_archived_log_files<'a, T: Iterator<Item = &'a Utf8PathBuf>>(
    log: &Logger,
    zone_name: &str,
    svc_name: &str,
    log_file_prefix: &str,
    dirs: T,
) -> Vec<Utf8PathBuf> {
    // The `dirs` should be things like
    // `/pool/ext/<ZPOOL_UUID>/crypt/debug/<ZONE_NAME>`, but it's really up to
    // the caller to verify these exist and possibly contain what they expect.
    //
    // Within that, we'll just look for things that appear to be Oxide-managed
    // SMF service log files.
    let mut files = Vec::new();
    for dir in dirs {
        if dir.exists() {
            let mut rd = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(e) => {
                    error!(
                        log,
                        "failed to read zone debug directory";
                        "directory" => ?dir,
                        "reason" => ?e,
                    );
                    continue;
                }
            };
            loop {
                match rd.next_entry().await {
                    Ok(None) => break,
                    Ok(Some(entry)) => {
                        let Ok(path) = Utf8PathBuf::try_from(entry.path())
                        else {
                            error!(
                                log,
                                "skipping possible archived log file with \
                                non-UTF-8 path";
                                "path" => ?entry.path(),
                            );
                            continue;
                        };
                        let fname = path.file_name().unwrap();
                        let is_oxide = is_oxide_smf_log_file(fname);
                        let matches_log_file =
                            fname.starts_with(log_file_prefix);
                        if is_oxide && matches_log_file {
                            debug!(
                                log,
                                "found archived log file";
                                "zone_name" => zone_name,
                                "service_name" => svc_name,
                                "path" => ?path,
                            );
                            files.push(path);
                        } else {
                            trace!(
                                log,
                                "skipping non-matching log file";
                                "zone_name" => zone_name,
                                "service_name" => svc_name,
                                "filename" => fname,
                                "is_oxide_smf_log_file" => is_oxide,
                                "matches_log_file" => matches_log_file,
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            log,
                            "failed to fetch zone debug directory entry";
                            "directory" => ?dir,
                            "reason" => ?e,
                        );
                    }
                }
            }
        } else {
            // The logic in `dump_setup` picks some U.2 in which to start
            // archiving logs, and thereafter tries to keep placing new ones
            // there, subject to space constraints. It's not really an error for
            // there to be no entries for the named zone in any particular U.2
            // debug dataset.
            slog::trace!(
                log,
                "attempting to find archived log files in \
                non-existent directory";
                "directory" => ?dir,
            );
        }
    }
    files
}

// Extract the zone bundle metadata from a file, if it exists.
fn extract_zone_bundle_metadata_impl(
    path: &Utf8PathBuf,
) -> Result<ZoneBundleMetadata, BundleError> {
    // Build a reader for the whole archive.
    let reader = std::fs::File::open(path).map_err(|err| {
        BundleError::OpenBundleFile { path: path.clone(), err }
    })?;
    let buf_reader = std::io::BufReader::new(reader);
    let gz = GzDecoder::new(buf_reader);
    let mut archive = Archive::new(gz);

    // Find the metadata entry, if it exists.
    let entries = archive.entries().map_err(|err| {
        BundleError::ReadBundleData { path: path.clone(), err }
    })?;
    let Some(md_entry) = entries
        // The `Archive::entries` iterator
        // returns a result, so filter to those
        // that are OK first.
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .path()
                .map(|p| p.to_str() == Some(ZONE_BUNDLE_METADATA_FILENAME))
                .unwrap_or(false)
        })
    else {
        return Err(BundleError::from(anyhow!(
            "Zone bundle is missing metadata file"
        )));
    };

    // Extract its contents and parse as metadata.
    let contents = std::io::read_to_string(md_entry).map_err(|err| {
        BundleError::ReadBundleData { path: path.clone(), err }
    })?;
    toml::from_str(&contents).map_err(BundleError::from)
}

// List the extant zone bundles for the provided zone, in the provided
// directory.
async fn list_bundles_for_zone(
    log: &Logger,
    path: &Utf8Path,
    zone_name: &str,
) -> Result<Vec<(Utf8PathBuf, ZoneBundleMetadata)>, BundleError> {
    let zone_bundle_dir = path.join(zone_name);
    Ok(filter_zone_bundles(log, &zone_bundle_dir, |md| {
        md.id.zone_name == zone_name
    })
    .await?
    .into_iter()
    .collect::<Vec<_>>())
}

// Extract zone bundle metadata from the provided file, if possible.
async fn extract_zone_bundle_metadata(
    path: Utf8PathBuf,
) -> Result<ZoneBundleMetadata, BundleError> {
    let task = tokio::task::spawn_blocking(move || {
        extract_zone_bundle_metadata_impl(&path)
    });
    task.await?
}

// Find zone bundles in the provided directory, which match the filter function.
async fn filter_zone_bundles(
    log: &Logger,
    directory: &Utf8PathBuf,
    filter: impl Fn(&ZoneBundleMetadata) -> bool,
) -> Result<BTreeMap<Utf8PathBuf, ZoneBundleMetadata>, BundleError> {
    let mut out = BTreeMap::new();
    debug!(log, "searching directory for zone bundles"; "directory" => %directory);
    let mut rd = tokio::fs::read_dir(directory).await.map_err(|err| {
        BundleError::ReadDirectory { directory: directory.to_owned(), err }
    })?;
    while let Some(entry) = rd.next_entry().await.map_err(|err| {
        BundleError::ReadDirectory { directory: directory.to_owned(), err }
    })? {
        let path = Utf8PathBuf::try_from(entry.path())?;
        debug!(log, "checking path as zone bundle"; "path" => %path);
        match extract_zone_bundle_metadata(path.clone()).await {
            Ok(md) => {
                trace!(log, "extracted zone bundle metadata"; "metadata" => ?md);
                if filter(&md) {
                    trace!(log, "filter matches bundle metadata"; "metadata" => ?md);
                    out.insert(path, md);
                }
            }
            Err(e) => {
                warn!(
                    log,
                    "failed to extract zone bundle metadata, skipping";
                    "path" => %path,
                    "reason" => ?e,
                );
            }
        }
    }
    Ok(out)
}

// Get the paths to a zone bundle, if it exists.
//
// Zone bundles are replicated in multiple storage directories. This returns
// every path at which the bundle with the provided ID exists, in the same
// order as `directories`.
async fn get_zone_bundle_paths(
    log: &Logger,
    directories: &[Utf8PathBuf],
    zone_name: &str,
    id: &Uuid,
) -> Result<Vec<Utf8PathBuf>, BundleError> {
    let mut out = Vec::with_capacity(directories.len());
    for dir in directories {
        let mut rd = tokio::fs::read_dir(dir).await.map_err(|err| {
            BundleError::ReadDirectory { directory: dir.to_owned(), err }
        })?;
        while let Some(entry) = rd.next_entry().await.map_err(|err| {
            BundleError::ReadDirectory { directory: dir.to_owned(), err }
        })? {
            let search_dir = Utf8PathBuf::try_from(entry.path())?;
            out.extend(
                filter_zone_bundles(log, &search_dir, |md| {
                    md.id.zone_name == zone_name && md.id.bundle_id == *id
                })
                .await?
                .into_keys(),
            );
        }
    }
    Ok(out)
}

// Enumerate all zone bundles under the provided directory.
async fn enumerate_zone_bundles(
    log: &Logger,
    dirs: &[Utf8PathBuf],
) -> Result<BTreeMap<Utf8PathBuf, Vec<ZoneBundleInfo>>, BundleError> {
    let mut out = BTreeMap::new();

    // Each of these is a storage directory.
    //
    // We should have under here zone-names, followed by bundles within each of
    // those.
    for dir in dirs.iter() {
        let mut rd = tokio::fs::read_dir(dir).await.map_err(|err| {
            BundleError::ReadDirectory { directory: dir.to_owned(), err }
        })?;
        let mut info_by_dir = Vec::new();
        while let Some(zone_dir) = rd.next_entry().await.map_err(|err| {
            BundleError::ReadDirectory { directory: dir.to_owned(), err }
        })? {
            let mut zone_rd = tokio::fs::read_dir(zone_dir.path())
                .await
                .map_err(|err| BundleError::ReadDirectory {
                    directory: zone_dir.path().try_into().unwrap(),
                    err,
                })?;
            while let Some(maybe_bundle) =
                zone_rd.next_entry().await.map_err(|err| {
                    BundleError::ReadDirectory {
                        directory: zone_dir.path().try_into().unwrap(),
                        err,
                    }
                })?
            {
                // TODO-robustness: What do we do with files that do _not_
                // appear to be valid zone bundles.
                //
                // On the one hand, someone may have put something there
                // intentionally. On the other hand, that would be weird, and we
                // _also_ know that it's possible that IO errors happen while
                // creating the bundle that render it impossible to recover the
                // metadata. So it's plausible that we end up with a lot of
                // detritus here in that case.
                let path = Utf8PathBuf::try_from(maybe_bundle.path())?;
                if let Ok(metadata) =
                    extract_zone_bundle_metadata(path.clone()).await
                {
                    let info = ZoneBundleInfo {
                        metadata,
                        path: path.clone(),
                        bytes: maybe_bundle
                            .metadata()
                            .await
                            .map_err(|err| BundleError::Metadata { path, err })?
                            .len(),
                    };
                    info_by_dir.push(info);
                } else {
                    warn!(
                        log,
                        "found non-zone-bundle file in zone bundle directory";
                        "path" => %path,
                    );
                }
            }
        }
        out.insert(dir.clone(), info_by_dir);
    }
    Ok(out)
}

// Run a cleanup, removing old bundles according to the strategy.
//
// Return the number of bundles removed and the new usage.
async fn run_cleanup(
    log: &Logger,
    storage_dirs: &[Utf8PathBuf],
    context: &CleanupContext,
) -> Result<BTreeMap<Utf8PathBuf, CleanupCount>, BundleError> {
    // First, determine how much space we are allowed to use and have used.
    //
    // Let's avoid doing anything at all if we're still within the limits.
    let usages = compute_bundle_utilization(log, storage_dirs, context).await?;
    if usages.values().all(|usage| usage.bytes_used <= usage.bytes_available) {
        debug!(log, "all usages below storage limit, returning");
        return Ok(BTreeMap::new());
    }

    // There's some work to do, let's enumerate all the bundles.
    let bundles = enumerate_zone_bundles(log, &storage_dirs).await?;
    debug!(
        log,
        "enumerated {} zone bundles across {} directories",
        bundles.values().map(Vec::len).sum::<usize>(),
        bundles.len(),
    );

    // Remove bundles from each storage directory, until we fall below the
    // number of bytes we would like to use to satisfy the storage limit.
    let mut cleanup_counts = BTreeMap::new();
    for (dir, mut info) in bundles.into_iter() {
        debug!(
            log,
            "cleaning up bundles from directory";
            "directory" => dir.as_str()
        );
        let mut count = CleanupCount::default();

        // Sort all the bundles in the current directory, using the priority
        // described in `context.priority`.
        info.sort_by(|lhs, rhs| context.priority.compare_bundles(lhs, rhs));
        let current_usage = usages.get(&dir).unwrap();

        // Remove bundles until we fall below the threshold.
        let mut n_bytes = current_usage.bytes_used;
        for each in info.into_iter() {
            if n_bytes <= current_usage.bytes_available {
                break;
            }
            tokio::fs::remove_file(&each.path).await.map_err(|_| {
                BundleError::Cleanup(anyhow!("failed to remove bundle"))
            })?;
            trace!(log, "removed old zone bundle"; "info" => ?&each);
            n_bytes = n_bytes.saturating_sub(each.bytes);
            count.bundles += 1;
            count.bytes += each.bytes;
        }

        cleanup_counts.insert(dir, count);
    }
    info!(log, "finished bundle cleanup"; "cleanup_counts" => ?&cleanup_counts);
    Ok(cleanup_counts)
}

// Return the total utilization for all zone bundles.
async fn compute_bundle_utilization(
    log: &Logger,
    storage_dirs: &[Utf8PathBuf],
    context: &CleanupContext,
) -> Result<BTreeMap<Utf8PathBuf, BundleUtilization>, BundleError> {
    let mut out = BTreeMap::new();
    for dir in storage_dirs.iter() {
        debug!(log, "computing bundle usage"; "directory" => %dir);
        // Fetch the ZFS dataset quota.
        let dataset_quota = zfs_quota(dir).await?;
        debug!(log, "computed dataset quota"; "quota" => dataset_quota);

        // Compute the bytes available, using the provided storage limit.
        let bytes_available =
            context.storage_limit.bytes_available(dataset_quota);
        debug!(
            log,
            "computed bytes available";
            "storage_limit" => %context.storage_limit,
            "bytes_available" => bytes_available
        );

        // Compute the size of the actual storage directory.
        //
        // TODO-correctness: This takes into account the directories themselves,
        // and may be not quite what we want. But it is very easy and pretty
        // close.
        let bytes_used = dir_size(dir).await?;
        debug!(log, "computed bytes used"; "bytes_used" => bytes_used);
        out.insert(
            dir.clone(),
            BundleUtilization { dataset_quota, bytes_available, bytes_used },
        );
    }
    Ok(out)
}

// Return the number of bytes occupied by the provided directory.
//
// This returns an error if reading any file or directory within the provided
// directory fails.
async fn dir_size(path: &Utf8PathBuf) -> Result<u64, BundleError> {
    let path = path.clone();
    // We could, alternatively, just implement this using `tokio::fs` to read
    // the directory and so on. However, the `tokio::fs` APIs are basically just
    // a wrapper around `spawn_blocking`-ing code that does the `std::fs`
    // functions. So, putting the whole thing in one big `spawn_blocking`
    // closure that just does all the blocking fs ops lets us spend less time
    // creating and destroying tiny blocking tasks.
    tokio::task::spawn_blocking(move || {
        let mut sum = 0;
        for entry in walkdir::WalkDir::new(&path).into_iter() {
            let entry = entry?;
            sum += entry.metadata()?.len()
        }

        Ok(sum)
    })
    .await
    .expect("spawned blocking task should not be cancelled or panic")
}

// Return the quota for a ZFS dataset, or the available size.
//
// This fails if:
//
// - The "zfs" command fails
// - Parsing stdout fails
// - Parsing the actual quota as a u64 fails
async fn zfs_quota(path: &Utf8PathBuf) -> Result<u64, BundleError> {
    let args = &["list", "-Hpo", "quota,avail", path.as_str()];
    let output =
        Command::new(ZFS).args(args).output().await.map_err(|err| {
            BundleError::Command {
                cmd: format!("{ZFS} {}", args.join(" ")),
                err,
            }
        })?;
    let err = |msg: &str| {
        BundleError::Cleanup(anyhow!(
            "failed to fetch ZFS quota for {}: {}",
            path,
            msg,
        ))
    };
    if !output.status.success() {
        return Err(err("zfs list command failed"));
    }
    let Ok(s) = std::str::from_utf8(&output.stdout) else {
        return Err(err("non-UTF8 stdout"));
    };
    let Some(line) = s.lines().next() else {
        return Err(err("no lines in zfs list output"));
    };
    let mut parts = line.split_ascii_whitespace();
    let quota = parts.next().ok_or_else(|| err("no quota part of line"))?;
    let avail = parts.next().ok_or_else(|| err("no avail part of line"))?;

    // Parse the available space, which is always defined.
    let avail = avail
        .trim()
        .parse()
        .map_err(|_| err("failed to parse available space"))?;

    // Quotas can be reported a few different ways.
    //
    // If the dataset is a volume (which should not happen, but we don't enforce
    // here), then this is a literal dash `-`. Something without a quota is
    // reported as `0`. Anything else is an integer.
    //
    // No quota is reported as `u64::MAX`.
    match quota.trim() {
        "-" | "0" => Ok(avail),
        x => x.parse().or(Ok(avail)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dir_size() {
        let path =
            Utf8PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        let usage = dir_size(&path).await.unwrap();
        assert!(
            usage >= 1024 * 400,
            "sled-agent manifest directory disk usage not correct?"
        );
        let path = Utf8PathBuf::from("/some/nonexistent/path");
        assert!(dir_size(&path).await.is_err());
    }

    #[tokio::test]
    // Different operating systems ship slightly different versions of `du(1)`,
    // with differing behaviors. We really only care that the `dir_size`
    // function behaves the same as the illumos `du(1)`, so skip this test on
    // other systems.
    #[cfg_attr(not(target_os = "illumos"), ignore)]
    async fn test_dir_size_matches_du() {
        const DU: &str = "du";
        async fn dir_size_du(path: &Utf8PathBuf) -> Result<u64, BundleError> {
            let args = &["-A", "-s", path.as_str()];
            let output =
                Command::new(DU).args(args).output().await.map_err(|err| {
                    BundleError::Command {
                        cmd: format!("{DU} {}", args.join(" ")),
                        err,
                    }
                })?;
            let err = |msg: &str| {
                BundleError::Cleanup(anyhow!(
                    "failed to fetch disk usage for {}: {}",
                    path,
                    msg,
                ))
            };
            if !output.status.success() {
                return Err(err("du command failed"));
            }
            let Ok(s) = std::str::from_utf8(&output.stdout) else {
                return Err(err("non-UTF8 stdout"));
            };
            let Some(line) = s.lines().next() else {
                return Err(err("no lines in du output"));
            };
            let Some(part) = line.trim().split_ascii_whitespace().next() else {
                return Err(err("no disk usage size computed in output"));
            };
            part.parse().map_err(|_| err("failed to parse du output"))
        }

        let du_output =
            Command::new(DU).arg("--version").output().await.unwrap();
        eprintln!(
            "du --version:\n{}\n",
            String::from_utf8_lossy(&du_output.stdout)
        );

        let path =
            Utf8PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        let t0 = std::time::Instant::now();
        let usage =
            dbg!(dir_size(&path).await).expect("disk usage for src dir failed");
        eprintln!("dir_size({path}) took {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        // Run `du -As /path/to/omicron/sled-agent/src`, which currently shows this
        // directory is ~450 KiB.
        let du_usage =
            dbg!(dir_size_du(&path).await).expect("running du failed!");
        eprintln!("du -s {path} took {:?}", t0.elapsed());

        assert_eq!(
            usage, du_usage,
            "expected `dir_size({path})` to == `du -s {path}`\n\
             {usage}B (`dir_size`) != {du_usage}B (`du`)",
        );
    }
}

#[cfg(all(target_os = "illumos", test))]
mod illumos_tests {
    use super::CleanupContext;
    use super::CleanupPeriod;
    use super::PriorityOrder;
    use super::StorageLimit;
    use super::Utf8Path;
    use super::Utf8PathBuf;
    use super::ZoneBundleCause;
    use super::ZoneBundleId;
    use super::ZoneBundleInfo;
    use super::ZoneBundleMetadata;
    use super::ZoneBundler;
    use super::find_archived_log_files;
    use super::zfs_quota;
    use anyhow::Context;
    use chrono::DateTime;
    use chrono::TimeZone;
    use chrono::Timelike;
    use chrono::Utc;
    use rand::RngCore;
    use sled_storage::manager_test_harness::StorageManagerTestHarness;
    use slog::Drain;
    use slog::Logger;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// An iterator that returns the date of consecutive days beginning with 1st
    /// January 2020. The time portion of each returned date will be fixed at
    /// midnight in UTC.
    struct DaysOfOurBundles {
        next: DateTime<Utc>,
    }

    impl DaysOfOurBundles {
        fn new() -> DaysOfOurBundles {
            DaysOfOurBundles {
                next: Utc
                    // Set the start date to 1st January 2020:
                    .with_ymd_and_hms(2020, 1, 1, 0, 0, 0)
                    .single()
                    .unwrap(),
            }
        }
    }

    impl Iterator for DaysOfOurBundles {
        type Item = DateTime<Utc>;

        fn next(&mut self) -> Option<Self::Item> {
            let out = self.next;

            // We promise that all returned dates are aligned with midnight UTC:
            assert_eq!(out.hour(), 0);
            assert_eq!(out.minute(), 0);
            assert_eq!(out.second(), 0);

            self.next =
                self.next.checked_add_days(chrono::Days::new(1)).unwrap();

            Some(out)
        }
    }

    #[tokio::test]
    async fn test_zfs_quota() {
        let path =
            Utf8PathBuf::try_from(std::env::current_dir().unwrap()).unwrap();
        let quota = zfs_quota(&path).await.unwrap();
        assert!(
            quota < (100 * 1024 * 1024 * 1024 * 1024),
            "100TiB should be enough for anyone",
        );
        let path = Utf8PathBuf::from("/some/nonexistent/path");
        assert!(zfs_quota(&path).await.is_err());
    }

    struct CleanupTestContextInner {
        resource_wrapper: ResourceWrapper,
        context: CleanupContext,
        bundler: ZoneBundler,
    }

    // Practically, we only expect one thread to "own" this context at a time.
    // However, with the "run_test_with_zfs_dataset", it's hard to pass an
    // async function as a parameter ("test") that acts on a mutable reference
    // without some fancy HRTB shenanigans.
    //
    // Reader: If you think you can pass a "&mut CleanupTestContextInner"
    // there instead of an "Arc<Mutex<...>>", I welcome you to try!
    #[derive(Clone)]
    struct CleanupTestContext {
        ctx: Arc<Mutex<CleanupTestContextInner>>,
    }

    // A wrapper around `StorageResources`, that automatically creates dummy
    // directories in the provided test locations and removes them on drop.
    //
    // They don't exist when you just do `StorageResources::new_for_test()`.
    // This type creates the datasets at the expected mountpoints, backed by the
    // ramdisk, and removes them on drop. This is basically a tempdir-like
    // system, that creates the directories implied by the `StorageResources`
    // expected disk structure.
    struct ResourceWrapper {
        storage_test_harness: StorageManagerTestHarness,
        dirs: Vec<Utf8PathBuf>,
    }

    async fn setup_storage(log: &Logger) -> StorageManagerTestHarness {
        let mut harness = StorageManagerTestHarness::new(&log).await;

        harness.handle().key_manager_ready().await;
        let _raw_disks =
            harness.add_vdevs(&["m2_left.vdev", "m2_right.vdev"]).await;
        harness
    }

    impl ResourceWrapper {
        // Create new storage resources, and mount datasets at the required
        // locations.
        async fn new(log: &Logger) -> Self {
            // Spawn the storage related tasks required for testing and insert
            // synthetic disks.
            let storage_test_harness = setup_storage(log).await;
            let resources =
                storage_test_harness.handle().get_latest_disks().await;
            let mut dirs = resources.all_zone_bundle_directories();
            dirs.sort();
            Self { storage_test_harness, dirs }
        }
    }

    fn test_logger() -> Logger {
        let dec =
            slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(dec).build().fuse();
        let log =
            Logger::root(drain, slog::o!("component" => "fake-cleanup-task"));
        log
    }

    async fn setup_fake_cleanup_task() -> anyhow::Result<CleanupTestContext> {
        let log = test_logger();
        let context = CleanupContext::default();
        let resource_wrapper = ResourceWrapper::new(&log).await;
        let bundler = ZoneBundler::new(
            log,
            resource_wrapper.storage_test_harness.handle().clone(),
            context,
        )
        .await;
        Ok(CleanupTestContext {
            ctx: Arc::new(Mutex::new(CleanupTestContextInner {
                resource_wrapper,
                context,
                bundler,
            })),
        })
    }

    #[tokio::test]
    async fn test_context() {
        let context = setup_fake_cleanup_task().await.unwrap();
        let mut ctx = context.ctx.lock().await;
        let context = ctx.bundler.cleanup_context().await;
        assert_eq!(context, ctx.context, "received incorrect context");
        ctx.resource_wrapper.storage_test_harness.cleanup().await;
    }

    #[tokio::test]
    async fn test_update_context() {
        let context = setup_fake_cleanup_task().await.unwrap();
        let mut ctx = context.ctx.lock().await;
        let new_context = CleanupContext {
            period: CleanupPeriod::new(ctx.context.period.as_duration() / 2)
                .unwrap(),
            storage_limit: StorageLimit::new(
                ctx.context.storage_limit.as_u8() / 2,
            )
            .unwrap(),
            priority: PriorityOrder::new(
                &ctx.context.priority.iter().copied().rev().collect::<Vec<_>>(),
            )
            .unwrap(),
        };
        ctx.bundler
            .update_cleanup_context(
                Some(new_context.period),
                Some(new_context.storage_limit),
                Some(new_context.priority),
            )
            .await
            .expect("failed to set context");
        let context = ctx.bundler.cleanup_context().await;
        assert_eq!(context, new_context, "failed to update context");
        ctx.resource_wrapper.storage_test_harness.cleanup().await;
    }

    // Quota applied to test datasets.
    //
    // ZFS will not allow a quota to be set that is smaller than the bytes
    // presently stored for that dataset, either at dataset creation time or
    // later by setting the "quota" property.  The exact minimum number of bytes
    // depends on many factors, including the block size of the underlying pool;
    // i.e., the "ashift" value.  An empty dataset is unlikely to contain more
    // than one megabyte of overhead, so use that as a conservative test size to
    // avoid issues.
    use sled_storage::dataset::DEBUG_DATASET_QUOTA as TEST_QUOTA;

    async fn run_test_with_zfs_dataset<T, Fut>(test: T)
    where
        T: FnOnce(CleanupTestContext) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let context = setup_fake_cleanup_task()
            .await
            .expect("failed to create cleanup task");
        let result = test(context.clone()).await;

        let mut ctx = context.ctx.lock().await;
        info!(
            &ctx.bundler.log,
            "Test completed, performing cleanup before emitting result"
        );
        ctx.resource_wrapper.storage_test_harness.cleanup().await;
        result.expect("test failed!");
    }

    #[tokio::test]
    async fn test_utilization() {
        run_test_with_zfs_dataset(test_utilization_body).await;
    }

    async fn test_utilization_body(
        ctx: CleanupTestContext,
    ) -> anyhow::Result<()> {
        let ctx = ctx.ctx.lock().await;
        let utilization = ctx.bundler.utilization().await?;
        let paths = utilization.keys().cloned().collect::<Vec<_>>();

        // Check that we've looked at all the paths in the context.
        anyhow::ensure!(
            paths == ctx.resource_wrapper.dirs,
            "Usage RPC returned incorrect paths"
        );

        // Check that we fetched the correct quota from the datasets.
        let bundle_utilization = utilization
            .values()
            .next()
            .context("no utilization information?")?;

        // If this needs to change, go modify the "add_vdevs" call in
        // "setup_storage".
        assert!(
            TEST_QUOTA
                < StorageManagerTestHarness::DEFAULT_VDEV_SIZE
                    .try_into()
                    .unwrap(),
            "Quota larger than underlying device (quota: {:?}, device size: {})",
            TEST_QUOTA,
            StorageManagerTestHarness::DEFAULT_VDEV_SIZE,
        );

        anyhow::ensure!(
            bundle_utilization.dataset_quota == TEST_QUOTA.to_bytes(),
            "computed incorrect dataset quota"
        );

        // Check that the number of bytes available is accurate.
        let pct = u64::from(ctx.context.storage_limit.as_u8());
        let expected_bytes_available =
            (bundle_utilization.dataset_quota * pct) / 100;
        anyhow::ensure!(
            bundle_utilization.bytes_available == expected_bytes_available,
            "incorrect bytes available computed for storage: actual {}, expected {}",
            bundle_utilization.bytes_available,
            expected_bytes_available,
        );
        anyhow::ensure!(
            bundle_utilization.bytes_used < 64,
            "there should be basically zero bytes used"
        );

        // Now let's add a fake bundle, and make sure that we get the right size
        // back.
        let info = insert_fake_bundle(
            &paths[0],
            DaysOfOurBundles::new().next().unwrap(),
            ZoneBundleCause::UnexpectedZone,
        )
        .await
        .context("Failed to insert_fake_bundle")?;

        let new_utilization =
            ctx.bundler.utilization().await.context(
                "Failed to get utilization after inserting fake bundle",
            )?;
        anyhow::ensure!(
            paths == new_utilization.keys().cloned().collect::<Vec<_>>(),
            "paths should not change"
        );
        let new_bundle_utilization = new_utilization
            .values()
            .next()
            .context("no utilization information?")?;
        anyhow::ensure!(
            bundle_utilization.dataset_quota
                == new_bundle_utilization.dataset_quota,
            "dataset quota should not change"
        );
        anyhow::ensure!(
            bundle_utilization.bytes_available
                == new_bundle_utilization.bytes_available,
            "bytes available for bundling should not change",
        );

        // We should have consumed _some_ bytes, at least the size of the
        // tarball itself.
        let change =
            new_bundle_utilization.bytes_used - bundle_utilization.bytes_used;
        anyhow::ensure!(
            change > info.bytes,
            "bytes used should drop by at least the size of the tarball",
        );

        // This is a pretty weak test, but let's make sure that the actual
        // number of bytes we use is within 5% of the computed size of the
        // tarball in bytes. This should account for the directories containing
        // it.
        const THRESHOLD: f64 = 0.05;
        let used = new_bundle_utilization.bytes_used as f64;
        let size = info.bytes as f64;
        let change = (used - size) / used;
        anyhow::ensure!(
            change > 0.0 && change <= THRESHOLD,
            "bytes used should be positive and within {:02} of the \
            size of the new tarball, used = {}, tarball size = {}",
            THRESHOLD,
            used,
            size,
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup() {
        run_test_with_zfs_dataset(test_cleanup_body).await;
    }

    async fn test_cleanup_body(ctx: CleanupTestContext) -> anyhow::Result<()> {
        let ctx = ctx.ctx.lock().await;
        // Let's add a bunch of fake bundles, until we should be over the
        // storage limit. These will all be explicit requests, so the priority
        // should be decided based on time, i.e., the ones first added should be
        // removed.
        //
        // First, reduce the storage limit, so that we only need to add a few
        // bundles.
        ctx.bundler
            .update_cleanup_context(
                None,
                Some(StorageLimit::new(2).unwrap()),
                None,
            )
            .await
            .context("failed to update cleanup context")?;

        let mut days = DaysOfOurBundles::new();
        let mut info = Vec::new();
        let mut utilization = ctx.bundler.utilization().await?;
        let bundle_dir = &ctx.resource_wrapper.dirs[0];
        loop {
            let us = utilization
                .get(bundle_dir)
                .context("no utilization information")?;

            if us.bytes_used > us.bytes_available {
                break;
            }

            let it = insert_fake_bundle(
                bundle_dir,
                days.next().unwrap(),
                ZoneBundleCause::UnexpectedZone,
            )
            .await?;
            info.push(it);
            utilization = ctx.bundler.utilization().await?;
        }

        // Trigger a cleanup.
        let counts =
            ctx.bundler.cleanup().await.context("failed to run cleanup")?;

        // We should have cleaned up the first-inserted bundle.
        let count = counts.get(bundle_dir).context("no cleanup counts")?;
        anyhow::ensure!(count.bundles == 1, "expected to cleanup one bundle");
        anyhow::ensure!(
            count.bytes == info[0].bytes,
            "expected to cleanup the number of bytes occupied by the first bundle",
        );
        let exists = tokio::fs::try_exists(&info[0].path)
            .await
            .context("failed to check if file exists")?;
        anyhow::ensure!(
            !exists,
            "the cleaned up bundle still appears to exist on-disk",
        );
        for each in info.iter().skip(1) {
            let exists = tokio::fs::try_exists(&each.path)
                .await
                .context("failed to check if file exists")?;
            anyhow::ensure!(exists, "cleaned up an unexpected bundle");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_list_with_filter() {
        run_test_with_zfs_dataset(test_list_with_filter_body).await;
    }

    async fn test_list_with_filter_body(
        ctx: CleanupTestContext,
    ) -> anyhow::Result<()> {
        let ctx = ctx.ctx.lock().await;
        let mut days = DaysOfOurBundles::new();
        let mut info = Vec::new();
        const N_BUNDLES: usize = 3;
        for i in 0..N_BUNDLES {
            let it = insert_fake_bundle_with_zone_name(
                &ctx.resource_wrapper.dirs[0],
                days.next().unwrap(),
                ZoneBundleCause::UnexpectedZone,
                format!("oxz_whatever_{i}").as_str(),
            )
            .await?;
            info.push(it);
        }

        // Listing with no filter should return all of them.
        let all_md = ctx.bundler.list(None).await?;
        anyhow::ensure!(
            all_md
                == info
                    .iter()
                    .map(|each| each.metadata.clone())
                    .collect::<Vec<_>>(),
            "Expected listing with no filter to return all bundles"
        );

        // Each bundle is from a zone named like `oxz_whatver_<INDEX>`.
        //
        // So filters like `oxz_` should return all of them, while ones on the
        // index should return exactly that one matching.
        let filt = Some("oxz_");
        let all_md = ctx.bundler.list(filt).await?;
        anyhow::ensure!(
            all_md
                == info
                    .iter()
                    .map(|each| each.metadata.clone())
                    .collect::<Vec<_>>(),
            "Expected listing with simple to return all bundles"
        );
        for i in 0..N_BUNDLES {
            let filt = Some(i.to_string());
            let matching_md = ctx.bundler.list(filt.as_deref()).await?;
            let expected_md = &info[i].metadata;
            anyhow::ensure!(
                matching_md.len() == 1,
                "expected exactly one bundle"
            );
            anyhow::ensure!(
                &matching_md[0] == expected_md,
                "Matched incorrect zone bundle with a filter",
            );
        }
        Ok(())
    }

    async fn insert_fake_bundle(
        dir: &Utf8Path,
        time_created: DateTime<Utc>,
        cause: ZoneBundleCause,
    ) -> anyhow::Result<ZoneBundleInfo> {
        insert_fake_bundle_with_zone_name(
            dir,
            time_created,
            cause,
            "oxz_whatever",
        )
        .await
    }

    async fn insert_fake_bundle_with_zone_name(
        dir: &Utf8Path,
        time_created: DateTime<Utc>,
        cause: ZoneBundleCause,
        zone_name: &str,
    ) -> anyhow::Result<ZoneBundleInfo> {
        assert_eq!(time_created.hour(), 0);
        assert_eq!(time_created.minute(), 0);
        assert_eq!(time_created.second(), 0);

        let metadata = ZoneBundleMetadata {
            id: ZoneBundleId {
                zone_name: String::from(zone_name),
                bundle_id: uuid::Uuid::new_v4(),
            },
            time_created,
            cause,
            version: 0,
        };

        let zone_dir = dir.join(&metadata.id.zone_name);
        tokio::fs::create_dir_all(&zone_dir)
            .await
            .context("failed to create zone directory")?;
        let path = zone_dir.join(format!("{}.tar.gz", metadata.id.bundle_id));

        // Create a tarball at the path with this fake metadata.
        let file = tokio::fs::File::create(&path)
            .await
            .context("failed to open zone bundle path")?
            .into_std()
            .await;
        let gz = flate2::GzBuilder::new()
            .filename(path.as_str())
            .write(file, flate2::Compression::best());
        let mut builder = tar::Builder::new(gz);
        let contents = toml::to_string(&metadata)?;
        super::insert_data(
            &mut builder,
            super::ZONE_BUNDLE_METADATA_FILENAME,
            contents.as_bytes(),
        )?;

        // Inject some ~incompressible ballast to ensure the bundles are, though
        // fake, not also microscopic:
        let mut ballast = vec![0; 64 * 1024];
        rand::thread_rng().fill_bytes(&mut ballast);
        super::insert_data(&mut builder, "ballast.bin", &ballast)?;

        let _ = builder.into_inner().context("failed to finish tarball")?;
        let bytes = tokio::fs::metadata(&path).await?.len();
        Ok(ZoneBundleInfo { metadata, path, bytes })
    }

    #[tokio::test]
    async fn test_find_archived_log_files() {
        let log = test_logger();
        let tmpdir = tempfile::tempdir().expect("Failed to make tempdir");

        for prefix in ["oxide", "system-illumos"] {
            let mut should_match = [
                format!("{prefix}-foo:default.log"),
                format!("{prefix}-foo:default.log.1000"),
            ];
            let should_not_match = [
                format!("{prefix}-foo:default"),
                format!("not-{prefix}-foo:default.log.1000"),
            ];
            for name in should_match.iter().chain(should_not_match.iter()) {
                let path = tmpdir.path().join(name);
                tokio::fs::File::create(path)
                    .await
                    .expect("failed to create dummy file");
            }

            let path = Utf8PathBuf::try_from(
                tmpdir.path().as_os_str().to_str().unwrap(),
            )
            .unwrap();
            let mut files = find_archived_log_files(
                &log,
                "zone-name", // unused here, for logging only
                "svc:/oxide/foo:default",
                &format!("{prefix}-foo:default.log"),
                std::iter::once(&path),
            )
            .await;

            // Sort everything to compare correctly.
            should_match.sort();
            files.sort();
            assert_eq!(files.len(), should_match.len());
            assert!(
                files
                    .iter()
                    .zip(should_match.iter())
                    .all(|(file, name)| { file.file_name().unwrap() == *name })
            );
        }
    }

    #[test]
    fn camino_starts_with_behaviour() {
        let logfile =
            Utf8PathBuf::from("/zonepath/var/svc/log/oxide-nexus:default.log");
        let rotated_logfile = Utf8PathBuf::from(
            "/zonepath/var/svc/log/oxide-nexus:default.log.0",
        );

        let logfile_as_string: &str = logfile.as_ref();
        let rotated_logfile_as_string: &str = rotated_logfile.as_ref();

        assert!(logfile != rotated_logfile);
        assert!(logfile_as_string != rotated_logfile_as_string);

        assert!(!rotated_logfile.starts_with(&logfile));
        assert!(rotated_logfile_as_string.starts_with(&logfile_as_string));
    }
}
