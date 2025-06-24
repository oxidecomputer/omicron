// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Operations for dealing with persistent backing mounts for OS data

// On Oxide hardware, the root filesystem is backed by a ramdisk and
// non-persistent. However, there are several things within the root filesystem
// which are useful to preserve across reboots, and these are backed persistent
// datasets on the boot disk.
//
// Each boot disk contains a dataset sled_hardware::disk::M2_BACKING_DATASET
// and for each backing mount, a child dataset is created under there that
// is configured with the desired mountpoint in the root filesystem. Since
// there are multiple disks which can be used to boot, these datasets are also
// marked with the "canmount=noauto" attribute so that they do not all try to
// mount automatically and race -- only one could ever succeed. This allows us
// to come along later and specifically mount the one that we want (the one from
// the current boot disk) and also perform an overlay mount so that it succeeds
// even if there is content from the ramdisk image or early boot services
// present underneath. The overlay mount action is optionally bracketed with a
// service stop/start.

use camino::Utf8PathBuf;
use illumos_utils::zfs::{
    CanMount, DatasetEnsureArgs, EnsureDatasetError, GetValueError, Mountpoint,
    SizeDetails, Zfs,
};
use omicron_common::api::external::ByteCount;
use omicron_common::disk::CompressionAlgorithm;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum BackingFsError {
    #[error("Error administering service: {0}")]
    Adm(#[from] smf::AdmError),

    #[error("Error retrieving dataset property: {0}")]
    DatasetProperty(#[from] GetValueError),

    #[error("Error initializing dataset: {0}")]
    Mount(#[from] EnsureDatasetError),

    #[error("Failed to ensure subdirectory {0}")]
    EnsureSubdir(#[from] io::Error),
}

struct BackingFs<'a> {
    // Dataset name
    name: &'static str,
    // Mountpoint
    mountpoint: &'static str,
    // Optional quota, in _bytes_
    quota: Option<ByteCount>,
    // Optional compression mode
    compression: CompressionAlgorithm,
    // Linked service
    service: Option<&'static str>,
    // Subdirectories to ensure
    subdirs: Option<&'a [&'static str]>,
}

impl<'a> BackingFs<'a> {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            mountpoint: "legacy",
            quota: None,
            compression: CompressionAlgorithm::Off,
            service: None,
            subdirs: None,
        }
    }

    const fn mountpoint(mut self, mountpoint: &'static str) -> Self {
        self.mountpoint = mountpoint;
        self
    }

    const fn quota(mut self, quota: ByteCount) -> Self {
        self.quota = Some(quota);
        self
    }

    const fn compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }

    const fn service(mut self, service: &'static str) -> Self {
        self.service = Some(service);
        self
    }

    const fn subdirs(mut self, subdirs: &'a [&'static str]) -> Self {
        self.subdirs = Some(subdirs);
        self
    }
}

const BACKING_FMD_DATASET: &'static str = "fmd";
const BACKING_FMD_MOUNTPOINT: &'static str = "/var/fm/fmd";
const BACKING_FMD_SUBDIRS: [&'static str; 3] = ["rsrc", "ckpt", "xprt"];
const BACKING_FMD_SERVICE: &'static str = "svc:/system/fmd:default";
const BACKING_FMD_QUOTA: ByteCount = ByteCount::from_mebibytes_u32(500);

const BACKING_COMPRESSION: CompressionAlgorithm = CompressionAlgorithm::On;

const BACKINGFS_COUNT: usize = 1;
const BACKINGFS: [BackingFs; BACKINGFS_COUNT] =
    [BackingFs::new(BACKING_FMD_DATASET)
        .mountpoint(BACKING_FMD_MOUNTPOINT)
        .subdirs(&BACKING_FMD_SUBDIRS)
        .quota(BACKING_FMD_QUOTA)
        .compression(BACKING_COMPRESSION)
        .service(BACKING_FMD_SERVICE)];

/// Ensure that the backing filesystems are mounted.
/// If the underlying dataset for a backing fs does not exist on the specified
/// boot disk then it will be created.
pub(crate) async fn ensure_backing_fs(
    log: &slog::Logger,
    boot_zpool_name: &illumos_utils::zpool::ZpoolName,
) -> Result<(), BackingFsError> {
    let log = log.new(o!(
        "component" => "BackingFs",
    ));
    for bfs in BACKINGFS.iter() {
        info!(log, "Processing {}", bfs.name);

        let dataset = format!(
            "{}/{}/{}",
            boot_zpool_name,
            sled_storage::dataset::M2_BACKING_DATASET,
            bfs.name
        );
        let mountpoint = Mountpoint(Utf8PathBuf::from(bfs.mountpoint));

        info!(log, "Ensuring dataset {}", dataset);

        let size_details = Some(SizeDetails {
            quota: bfs.quota,
            reservation: None,
            compression: bfs.compression,
        });

        Zfs::ensure_dataset(DatasetEnsureArgs {
            name: &dataset,
            mountpoint: mountpoint.clone(),
            can_mount: CanMount::NoAuto,
            zoned: false,
            encryption_details: None,
            size_details,
            id: None,
            additional_options: None,
        })
        .await?;

        // Check if a ZFS filesystem is already mounted on bfs.mountpoint by
        // retrieving the ZFS `mountpoint` property and comparing it. This
        // might seem counter-intuitive but if there is a filesystem mounted
        // there, its mountpoint will match, and if not then we will retrieve
        // the mountpoint of a higher level filesystem, such as '/'. If we
        // can't retrieve the property at all, then there is definitely no ZFS
        // filesystem mounted there - most likely we are running with a non-ZFS
        // root, such as when net booted during CI.
        if Zfs::get_value(&bfs.mountpoint, "mountpoint", false)
            .await
            .unwrap_or("not-zfs".to_string())
            == bfs.mountpoint
        {
            info!(log, "{} is already mounted", bfs.mountpoint);
            return Ok(());
        }

        if let Some(service) = bfs.service {
            info!(log, "Stopping service {}", service);
            smf::Adm::new()
                .disable()
                .temporary()
                .synchronous()
                .run(smf::AdmSelection::ByPattern(&[service]))?;
        }

        info!(log, "Mounting {} on {}", dataset, mountpoint);

        Zfs::mount_overlay_dataset(&dataset).await?;

        if let Some(subdirs) = bfs.subdirs {
            for dir in subdirs {
                let subdir = format!("{}/{}", mountpoint, dir);

                info!(log, "Ensuring directory {}", subdir);
                std::fs::create_dir_all(subdir)?;
            }
        }

        if let Some(service) = bfs.service {
            info!(log, "Starting service {}", service);
            smf::Adm::new()
                .enable()
                .synchronous()
                .run(smf::AdmSelection::ByPattern(&[service]))?;
        }
    }

    Ok(())
}
