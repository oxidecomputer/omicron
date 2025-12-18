// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! illumos-specific mechanisms for parsing disk info.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::DiskPaths;
use crate::Partition;
use crate::PooledDiskError;
use crate::illumos::gpt;
use crate::is_oxide_sled;
use camino::Utf8Path;
use illumos_utils::zpool::Zpool;
use illumos_utils::zpool::ZpoolName;
use omicron_common::disk::{DiskIdentity, DiskVariant};
use omicron_uuid_kinds::ZpoolUuid;
use slog::Logger;
use slog::info;

/// NVMe devices use a meta size of 0 as we don't support writing addditional
/// metadata
static NVME_LBA_META_SIZE: u32 = 0;
/// NVMe devices default to using 4k logical block addressing unless overriden.
static DEFAULT_NVME_LBA_DATA_SIZE: u64 = 4096;

/// NVMe device settings for a particular NVMe model.
struct NvmeDeviceSettings {
    /// The desired disk size for dealing with overprovisioning.
    resize: NvmeVendorResize,
    /// An override for the default 4k LBA formatting.
    lba_data_size_override: Option<u64>,
}

enum NvmeVendorResize {
    Wdc(u32),
    Unsupported,
}

/// A mapping from model to desired settings.
/// A device not found in this lookup table will not be modified by sled-agent.
static PREFERRED_NVME_DEVICE_SETTINGS: OnceLock<
    HashMap<&'static str, NvmeDeviceSettings>,
> = OnceLock::new();

fn preferred_nvme_device_settings()
-> &'static HashMap<&'static str, NvmeDeviceSettings> {
    PREFERRED_NVME_DEVICE_SETTINGS.get_or_init(|| {
        HashMap::from([
            (
                "WUS4C6432DSP3X3",
                NvmeDeviceSettings {
                    resize: NvmeVendorResize::Wdc(3200),
                    lba_data_size_override: None,
                },
            ),
            (
                "WUS5EA138ESP7E1",
                NvmeDeviceSettings {
                    resize: NvmeVendorResize::Wdc(3200),
                    lba_data_size_override: None,
                },
            ),
            (
                "WUS5EA138ESP7E3",
                NvmeDeviceSettings {
                    resize: NvmeVendorResize::Wdc(3200),
                    lba_data_size_override: None,
                },
            ),
            (
                "WUS5EA176ESP7E1",
                NvmeDeviceSettings {
                    resize: NvmeVendorResize::Wdc(6400),
                    lba_data_size_override: None,
                },
            ),
            (
                "WUS5EA176ESP7E3",
                NvmeDeviceSettings {
                    resize: NvmeVendorResize::Wdc(6400),
                    lba_data_size_override: None,
                },
            ),
            (
                "SDS6BA138PSP9X3",
                NvmeDeviceSettings {
                    resize: NvmeVendorResize::Unsupported,
                    lba_data_size_override: None,
                },
            ),
        ])
    })
}

#[derive(Debug, thiserror::Error)]
pub enum NvmeFormattingError {
    #[error(transparent)]
    NvmeInit(#[from] libnvme::NvmeInitError),
    #[error(transparent)]
    Nvme(#[from] libnvme::NvmeError),
    #[error(transparent)]
    NvmeController(#[from] libnvme::controller::NvmeControllerError),
    #[error("Device is missing expected LBA format")]
    LbaFormatMissing,
    #[error("Device has {0} active namespaces but we expected 1")]
    UnexpectedNamespaces(usize),
    #[error(transparent)]
    InfoError(#[from] libnvme::controller_info::NvmeInfoError),
    #[error("Could not find NVMe controller for disk with serial {0}")]
    NoController(String),
    #[error("Could not determine if host is an Oxide sled: {0}")]
    SystemDetection(#[source] anyhow::Error),
}

// The expected layout of an M.2 device within the Oxide rack.
//
// Partitions beyond this "expected partition" array are ignored.
const M2_EXPECTED_PARTITION_COUNT: usize = 6;
static M2_EXPECTED_PARTITIONS: [Partition; M2_EXPECTED_PARTITION_COUNT] = [
    Partition::BootImage,
    Partition::Reserved,
    Partition::Reserved,
    Partition::Reserved,
    Partition::DumpDevice,
    Partition::ZfsPool,
];

// The expected layout of a U.2 device within the Oxide rack.
//
// Partitions beyond this "expected partition" array are ignored.
const U2_EXPECTED_PARTITION_COUNT: usize = 1;
static U2_EXPECTED_PARTITIONS: [Partition; U2_EXPECTED_PARTITION_COUNT] =
    [Partition::ZfsPool];

fn parse_partition_types<const N: usize>(
    path: &Utf8Path,
    partitions: &Vec<impl gpt::LibEfiPartition>,
    expected_partitions: &[Partition; N],
) -> Result<Vec<Partition>, PooledDiskError> {
    if partitions.len() != N {
        return Err(PooledDiskError::BadPartitionLayout {
            path: path.to_path_buf(),
            why: format!(
                "Expected {N} partitions, only saw {}",
                partitions.len(),
            ),
        });
    }
    for i in 0..N {
        if partitions[i].index() != i {
            return Err(PooledDiskError::BadPartitionLayout {
                path: path.to_path_buf(),
                why: format!(
                    "The {i}-th partition has index {}",
                    partitions[i].index()
                ),
            });
        }

        // NOTE: If we wanted to, we could validate additional information about
        // the size, GUID, or name of the partition. At the moment, however,
        // we're relying on the index within the partition table to indicate the
        // "intent" of the partition.
    }

    Ok(expected_partitions.to_vec())
}

/// Parses, validates, and ensures the partition layout within a disk.
///
/// Returns a Vec of partitions on success. The index of the Vec is guaranteed
/// to also be the index of the partition.
pub async fn ensure_partition_layout(
    log: &Logger,
    paths: &DiskPaths,
    variant: DiskVariant,
    identity: &DiskIdentity,
    zpool_id: Option<ZpoolUuid>,
) -> Result<Vec<Partition>, PooledDiskError> {
    internal_ensure_partition_layout::<libefi_illumos::Gpt>(
        log,
        &Zpool::real_api(),
        paths,
        variant,
        identity,
        zpool_id,
    )
    .await
}

// Same as the [ensure_partition_layout], but with generic parameters
// for access to external resources.
async fn internal_ensure_partition_layout<GPT: gpt::LibEfiGpt>(
    log: &Logger,
    zpool_api: &dyn illumos_utils::zpool::Api,
    paths: &DiskPaths,
    variant: DiskVariant,
    identity: &DiskIdentity,
    zpool_id: Option<ZpoolUuid>,
) -> Result<Vec<Partition>, PooledDiskError> {
    // Open the "Whole Disk" as a raw device to be parsed by the
    // libefi-illumos library. This lets us peek at the GPT before
    // making too many assumptions about it.
    let raw = true;
    let path = paths.whole_disk(raw);
    let devfs_path_str = paths.devfs_path.as_str().to_string();
    let log = log.new(slog::o!("path" => devfs_path_str));

    // The GPT returned from "GPT::read" is "!Send", so it must not live across
    // any await points.
    let err = match GPT::read(&path) {
        Ok(gpt) => {
            // This should be the common steady-state case
            info!(log, "Disk already has a GPT");
            return read_partitions(gpt, &path, variant);
        }
        Err(err) => err,
    };

    match err {
        libefi_illumos::Error::LabelNotFound => {
            // Fresh U.2 disks are an example of devices where "we don't expect
            // a GPT to exist".
            info!(log, "Disk does not have a GPT");

            // For ZFS-implementation-specific reasons, Zpool create can only
            // act on devices under the "/dev" hierarchy, rather than the device
            // path which exists in the "/devices" directory.
            let dev_path = if let Some(dev_path) = &paths.dev_path {
                dev_path
            } else {
                return Err(PooledDiskError::CannotFormatMissingDevPath {
                    path,
                });
            };
            match variant {
                DiskVariant::U2 => {
                    // First we need to check that this disk is of the proper
                    // size and correct logical block address formatting.
                    ensure_size_and_formatting(&log, identity)?;

                    info!(
                        log,
                        "Formatting zpool on disk";
                        "uuid" => ?zpool_id,
                    );
                    let Some(zpool_id) = zpool_id else {
                        return Err(PooledDiskError::MissingZpoolUuid);
                    };

                    // If a zpool does not already exist, create one.
                    let zpool_name = ZpoolName::new_external(zpool_id);
                    zpool_api.create(&zpool_name, dev_path).await?;
                    return Ok(vec![Partition::ZfsPool]);
                }
                DiskVariant::M2 => {
                    // TODO: If we see a completely empty M.2, should we create
                    // the expected partitions? Or would it be wiser to infer
                    // that this indicates an unexpected error conditions that
                    // needs mitigation?
                    return Err(PooledDiskError::CannotFormatM2NotImplemented);
                }
            }
        }
        err => {
            return Err(PooledDiskError::Gpt {
                path,
                error: anyhow::Error::new(err),
            });
        }
    };
}

fn read_partitions<GPT: gpt::LibEfiGpt>(
    gpt: GPT,
    path: &Utf8Path,
    variant: DiskVariant,
) -> Result<Vec<Partition>, PooledDiskError> {
    let mut partitions: Vec<_> = gpt.partitions();
    match variant {
        DiskVariant::U2 => {
            partitions.truncate(U2_EXPECTED_PARTITION_COUNT);
            parse_partition_types(&path, &partitions, &U2_EXPECTED_PARTITIONS)
        }
        DiskVariant::M2 => {
            partitions.truncate(M2_EXPECTED_PARTITION_COUNT);
            parse_partition_types(&path, &partitions, &M2_EXPECTED_PARTITIONS)
        }
    }
}

fn ensure_size_and_formatting(
    log: &Logger,
    identity: &DiskIdentity,
) -> Result<(), NvmeFormattingError> {
    use libnvme::Nvme;
    use libnvme::namespace::NamespaceDiscoveryLevel;

    // Check that we are on real Oxide hardware so that we avoid:
    // - Messing with NVMe devices in other environments
    // - Failing tests which use zvols rather than real NVMe devices
    // - Breaking virtual environments like a4x2 which likely don't expose or
    //   implement changing the LBA on emulated devices.
    if !is_oxide_sled().map_err(NvmeFormattingError::SystemDetection)? {
        return Ok(());
    }

    let mut controller_found = false;
    let nvme = Nvme::new()?;

    for controller in nvme.controller_discovery()? {
        let controller = controller?.write_lock().map_err(|(_, e)| e)?;
        let controller_info = controller.get_info()?;

        // Make sure we are operating on the correct NVMe device.
        if controller_info.serial() != identity.serial {
            continue;
        };
        controller_found = true;
        let nsdisc =
            controller.namespace_discovery(NamespaceDiscoveryLevel::Active)?;
        let namespaces = nsdisc.into_iter().collect::<Result<Vec<_>, _>>()?;

        // We only want to continue if there is a single namespace associated
        // with the device, so we accomplish this by pattern matching for it.
        let [namespace] = namespaces.as_slice() else {
            return Err(NvmeFormattingError::UnexpectedNamespaces(
                namespaces.len(),
            ));
        };

        // First we need to detach blkdev from the namespace.
        namespace.blkdev_detach()?;

        // Check for a known nvme drive and apply our desired configuration.
        let mut wanted_data_size = DEFAULT_NVME_LBA_DATA_SIZE;
        if let Some(nvme_settings) =
            preferred_nvme_device_settings().get(identity.model.as_str())
        {
            match nvme_settings.resize {
                NvmeVendorResize::Wdc(provisioning_size) => {
                    let size = controller.wdc_resize_get()?;

                    // Resize the device if needed to ensure we get the expected
                    // durability level in terms of drive writes per day.
                    if size != provisioning_size {
                        controller.wdc_resize_set(provisioning_size)?;
                        info!(
                            log,
                            "Resized {} from {size} to {provisioning_size}",
                            identity.serial,
                        )
                    }
                }
                // This device doesn't have a vendor specific resize command to
                // deal with overprovisioning so there's nothing to do.
                NvmeVendorResize::Unsupported => (),
            }

            if let Some(lba_data_size_override) =
                nvme_settings.lba_data_size_override
            {
                wanted_data_size = lba_data_size_override;
            }
        } else {
            info!(
                log,
                "There are no preferred NVMe settings for disk model {};  will \
                attempt to format to the default LBA data size for disk with \
                serial {}",
                identity.model,
                identity.serial
            );
        }

        // Find the LBA format we want to use for the device.
        let desired_lba = controller_info
            .lba_formats()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|lba| {
                lba.meta_size() == NVME_LBA_META_SIZE
                    && lba.data_size() == wanted_data_size
            })
            .ok_or_else(|| NvmeFormattingError::LbaFormatMissing)?;

        // If the controller isn't formatted to our desired LBA we need to
        // issue a format request.
        let ns_info = namespace.get_info()?;
        let current_lba = ns_info.current_format()?;
        if current_lba.id() != desired_lba.id() {
            controller
                .format_request()?
                .set_lbaf(desired_lba.id())?
                // TODO map this to libnvme::BROADCAST_NAMESPACE once added
                .set_nsid(u32::MAX)?
                // No secure erase
                .set_ses(0)?
                .execute()?;

            info!(
                log,
                "Formatted disk with serial {} to an LBA with data size \
                    {wanted_data_size}",
                identity.serial,
            );
        }

        // Attach blkdev to the namespace again
        namespace.blkdev_attach()?;

        // We found the disk and applied the settings so there's no use scanning
        // the rest of the devices.
        break;
    }

    if !controller_found {
        return Err(NvmeFormattingError::NoController(identity.serial.clone()));
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::DiskPaths;
    use camino::Utf8PathBuf;
    use omicron_test_utils::dev::{mock_disk_identity, test_setup_log};
    use std::path::Path;

    struct FakePartition {
        index: usize,
    }

    impl gpt::LibEfiPartition for FakePartition {
        fn index(&self) -> usize {
            self.index
        }
    }

    struct LabelNotFoundGPT {}
    impl gpt::LibEfiGpt for LabelNotFoundGPT {
        type Partition<'a> = FakePartition;
        fn read<P: AsRef<Path>>(_: P) -> Result<Self, libefi_illumos::Error> {
            Err(libefi_illumos::Error::LabelNotFound)
        }
        fn partitions(&self) -> Vec<Self::Partition<'_>> {
            vec![]
        }
    }

    #[tokio::test]
    async fn ensure_partition_layout_u2_no_format_without_dev_path() {
        let logctx = test_setup_log(
            "ensure_partition_layout_u2_no_format_without_dev_path",
        );
        let log = &logctx.log;

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        let result = internal_ensure_partition_layout::<LabelNotFoundGPT>(
            &log,
            illumos_utils::fakes::zpool::Zpool::new().as_ref(),
            &DiskPaths { devfs_path, dev_path: None },
            DiskVariant::U2,
            &mock_disk_identity(),
            None,
        )
        .await;
        match result {
            Err(PooledDiskError::CannotFormatMissingDevPath { .. }) => {}
            _ => panic!("Should have failed with a missing dev path error"),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_partition_layout_u2_format_with_dev_path() {
        let logctx =
            test_setup_log("ensure_partition_layout_u2_format_with_dev_path");
        let log = &logctx.log;

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        let partitions = internal_ensure_partition_layout::<LabelNotFoundGPT>(
            &log,
            illumos_utils::fakes::zpool::Zpool::new().as_ref(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
            },
            DiskVariant::U2,
            &mock_disk_identity(),
            Some(ZpoolUuid::new_v4()),
        )
        .await
        .expect("Should have succeeded partitioning disk");

        assert_eq!(partitions.len(), U2_EXPECTED_PARTITION_COUNT);
        assert_eq!(partitions[0], Partition::ZfsPool);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_partition_layout_m2_cannot_format() {
        let logctx = test_setup_log("ensure_partition_layout_m2_cannot_format");
        let log = &logctx.log.clone();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        assert!(
            internal_ensure_partition_layout::<LabelNotFoundGPT>(
                &log,
                illumos_utils::fakes::zpool::Zpool::new().as_ref(),
                &DiskPaths {
                    devfs_path,
                    dev_path: Some(Utf8PathBuf::from(DEV_PATH))
                },
                DiskVariant::M2,
                &mock_disk_identity(),
                None,
            )
            .await
            .is_err()
        );

        logctx.cleanup_successful();
    }

    struct FakeU2GPT {}
    impl gpt::LibEfiGpt for FakeU2GPT {
        type Partition<'a> = FakePartition;
        fn read<P: AsRef<Path>>(_: P) -> Result<Self, libefi_illumos::Error> {
            Ok(Self {})
        }
        fn partitions(&self) -> Vec<Self::Partition<'_>> {
            let mut r = vec![];
            for i in 0..U2_EXPECTED_PARTITION_COUNT {
                r.push(FakePartition { index: i });
            }
            r
        }
    }

    #[tokio::test]
    async fn ensure_partition_layout_u2_with_expected_format() {
        let logctx =
            test_setup_log("ensure_partition_layout_u2_with_expected_format");
        let log = &logctx.log;

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        let partitions = internal_ensure_partition_layout::<FakeU2GPT>(
            &log,
            illumos_utils::fakes::zpool::Zpool::new().as_ref(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
            },
            DiskVariant::U2,
            &mock_disk_identity(),
            None,
        )
        .await
        .expect("Should be able to parse disk");

        assert_eq!(partitions.len(), U2_EXPECTED_PARTITION_COUNT);
        for i in 0..U2_EXPECTED_PARTITION_COUNT {
            assert_eq!(partitions[i], U2_EXPECTED_PARTITIONS[i]);
        }

        logctx.cleanup_successful();
    }

    struct FakeM2GPT {}
    impl gpt::LibEfiGpt for FakeM2GPT {
        type Partition<'a> = FakePartition;
        fn read<P: AsRef<Path>>(_: P) -> Result<Self, libefi_illumos::Error> {
            Ok(Self {})
        }
        fn partitions(&self) -> Vec<Self::Partition<'_>> {
            let mut r = vec![];
            for i in 0..M2_EXPECTED_PARTITION_COUNT {
                r.push(FakePartition { index: i });
            }
            r
        }
    }

    #[tokio::test]
    async fn ensure_partition_layout_m2_with_expected_format() {
        let logctx =
            test_setup_log("ensure_partition_layout_m2_with_expected_format");
        let log = &logctx.log;

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        let partitions = internal_ensure_partition_layout::<FakeM2GPT>(
            &log,
            illumos_utils::fakes::zpool::Zpool::new().as_ref(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
            },
            DiskVariant::M2,
            &mock_disk_identity(),
            None,
        )
        .await
        .expect("Should be able to parse disk");

        assert_eq!(partitions.len(), M2_EXPECTED_PARTITION_COUNT);
        for i in 0..M2_EXPECTED_PARTITION_COUNT {
            assert_eq!(partitions[i], M2_EXPECTED_PARTITIONS[i]);
        }

        logctx.cleanup_successful();
    }

    struct EmptyGPT {}
    impl gpt::LibEfiGpt for EmptyGPT {
        type Partition<'a> = FakePartition;
        fn read<P: AsRef<Path>>(_: P) -> Result<Self, libefi_illumos::Error> {
            Ok(Self {})
        }
        fn partitions(&self) -> Vec<Self::Partition<'_>> {
            vec![]
        }
    }

    #[tokio::test]
    async fn ensure_partition_layout_m2_fails_with_empty_gpt() {
        let logctx =
            test_setup_log("ensure_partition_layout_m2_fails_with_empty_gpt");
        let log = &logctx.log;

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        assert!(matches!(
            internal_ensure_partition_layout::<EmptyGPT>(
                &log,
                illumos_utils::fakes::zpool::Zpool::new().as_ref(),
                &DiskPaths {
                    devfs_path,
                    dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
                },
                DiskVariant::M2,
                &mock_disk_identity(),
                None,
            )
            .await
            .expect_err("Should have failed parsing empty GPT"),
            PooledDiskError::BadPartitionLayout { .. }
        ));

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_partition_layout_u2_fails_with_empty_gpt() {
        let logctx =
            test_setup_log("ensure_partition_layout_u2_fails_with_empty_gpt");
        let log = &logctx.log;

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        assert!(matches!(
            internal_ensure_partition_layout::<EmptyGPT>(
                &log,
                illumos_utils::fakes::zpool::Zpool::new().as_ref(),
                &DiskPaths {
                    devfs_path,
                    dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
                },
                DiskVariant::U2,
                &mock_disk_identity(),
                None,
            )
            .await
            .expect_err("Should have failed parsing empty GPT"),
            PooledDiskError::BadPartitionLayout { .. }
        ));

        logctx.cleanup_successful();
    }
}
