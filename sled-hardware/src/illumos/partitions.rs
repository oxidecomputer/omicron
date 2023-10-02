// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! illumos-specific mechanisms for parsing disk info.

use crate::illumos::gpt;
use crate::{DiskError, DiskPaths, DiskVariant, Partition};
use camino::Utf8Path;
use helios_fusion::BoxedExecutor;
use illumos_utils::zpool::{Zpool, ZpoolName};
use slog::info;
use slog::Logger;
use uuid::Uuid;

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
) -> Result<Vec<Partition>, DiskError> {
    if partitions.len() != N {
        return Err(DiskError::BadPartitionLayout {
            path: path.to_path_buf(),
            why: format!(
                "Expected {} partitions, only saw {}",
                partitions.len(),
                N
            ),
        });
    }
    for i in 0..N {
        if partitions[i].index() != i {
            return Err(DiskError::BadPartitionLayout {
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
pub fn ensure_partition_layout(
    log: &Logger,
    executor: &BoxedExecutor,
    paths: &DiskPaths,
    variant: DiskVariant,
) -> Result<Vec<Partition>, DiskError> {
    internal_ensure_partition_layout::<libefi_illumos::Gpt>(
        log, executor, paths, variant,
    )
}

// Same as the [ensure_partition_layout], but with generic parameters
// for access to external resources.
fn internal_ensure_partition_layout<GPT: gpt::LibEfiGpt>(
    log: &Logger,
    executor: &BoxedExecutor,
    paths: &DiskPaths,
    variant: DiskVariant,
) -> Result<Vec<Partition>, DiskError> {
    // Open the "Whole Disk" as a raw device to be parsed by the
    // libefi-illumos library. This lets us peek at the GPT before
    // making too many assumptions about it.
    let raw = true;
    let path = paths.whole_disk(raw);

    let gpt = match GPT::read(&path) {
        Ok(gpt) => {
            // This should be the common steady-state case
            info!(log, "Disk at {} already has a GPT", paths.devfs_path);
            gpt
        }
        Err(libefi_illumos::Error::LabelNotFound) => {
            // Fresh U.2 disks are an example of devices where "we don't expect
            // a GPT to exist".
            info!(log, "Disk at {} does not have a GPT", paths.devfs_path);

            // For ZFS-implementation-specific reasons, Zpool create can only
            // act on devices under the "/dev" hierarchy, rather than the device
            // path which exists in the "/devices" directory.
            let dev_path = if let Some(dev_path) = &paths.dev_path {
                dev_path
            } else {
                return Err(DiskError::CannotFormatMissingDevPath { path });
            };
            match variant {
                DiskVariant::U2 => {
                    info!(log, "Formatting zpool on disk {}", paths.devfs_path);
                    // If a zpool does not already exist, create one.
                    let zpool_name = ZpoolName::new_external(Uuid::new_v4());
                    Zpool::create(&executor, zpool_name, dev_path)?;
                    return Ok(vec![Partition::ZfsPool]);
                }
                DiskVariant::M2 => {
                    // TODO: If we see a completely empty M.2, should we create
                    // the expected partitions? Or would it be wiser to infer
                    // that this indicates an unexpected error conditions that
                    // needs mitigation?
                    return Err(DiskError::CannotFormatM2NotImplemented);
                }
            }
        }
        Err(err) => {
            return Err(DiskError::Gpt {
                path,
                error: anyhow::Error::new(err),
            });
        }
    };
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::DiskPaths;
    use camino::Utf8PathBuf;
    use helios_fusion::{Input, OutputExt, PFEXEC};
    use helios_tokamak::{FakeChild, FakeExecutorBuilder};
    use illumos_utils::zpool::{ZpoolKind, ZPOOL};
    use omicron_test_utils::dev::test_setup_log;
    use std::path::Path;
    use std::process::Output;
    use std::str::FromStr;

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

    #[test]
    fn ensure_partition_layout_u2_no_format_without_dev_path() {
        let logctx = test_setup_log(
            "ensure_partition_layout_u2_no_format_without_dev_path",
        );
        let log = &logctx.log;
        let executor = FakeExecutorBuilder::new(log.clone()).build();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        let result = internal_ensure_partition_layout::<LabelNotFoundGPT>(
            &log,
            &executor.as_executor(),
            &DiskPaths { devfs_path, dev_path: None },
            DiskVariant::U2,
        );
        match result {
            Err(DiskError::CannotFormatMissingDevPath { .. }) => {}
            _ => panic!("Should have failed with a missing dev path error"),
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn ensure_partition_layout_u2_format_with_dev_path() {
        let logctx =
            test_setup_log("ensure_partition_layout_u2_format_with_dev_path");
        let log = &logctx.log;
        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        let mut calls = 0;
        let mut zpool_name = None;
        let wait_handler = Box::new(move |child: &mut FakeChild| -> Output {
            let input = Input::from(child.command());
            assert_eq!(input.program, PFEXEC);

            match calls {
                0 => {
                    assert_eq!(input.args.len(), 4);
                    assert_eq!(input.args[0], ZPOOL);
                    assert_eq!(input.args[1], "create");
                    let name = ZpoolName::from_str(&input.args[2])
                        .expect("Cannot parse Zpool Name");
                    assert_eq!(name.kind(), ZpoolKind::External);
                    assert_eq!(input.args[3], DEV_PATH);
                    zpool_name = Some(name);
                }
                1 => {
                    assert_eq!(input.args.len(), 4);
                    assert_eq!(input.args[0], ZPOOL);
                    assert_eq!(input.args[1], "set");
                    assert_eq!(input.args[2], "feature@encryption=enabled");
                    assert_eq!(
                        &ZpoolName::from_str(&input.args[3])
                            .expect("Cannot parse zpool name"),
                        zpool_name.as_ref().expect(
                            "Should have grabbed zpool name from prior call"
                        )
                    );
                }
                _ => panic!("Unexpected call: {}", input),
            };
            calls += 1;
            Output::success()
        });
        let executor = FakeExecutorBuilder::new(log.clone())
            .wait_handler(wait_handler)
            .build();

        let partitions = internal_ensure_partition_layout::<LabelNotFoundGPT>(
            &log,
            &executor.as_executor(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
            },
            DiskVariant::U2,
        )
        .expect("Should have succeeded partitioning disk");

        assert_eq!(partitions.len(), U2_EXPECTED_PARTITION_COUNT);
        assert_eq!(partitions[0], Partition::ZfsPool);

        logctx.cleanup_successful();
    }

    #[test]
    fn ensure_partition_layout_m2_cannot_format() {
        let logctx = test_setup_log("ensure_partition_layout_m2_cannot_format");
        let log = &logctx.log.clone();
        let executor = FakeExecutorBuilder::new(log.clone()).build();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        assert!(internal_ensure_partition_layout::<LabelNotFoundGPT>(
            &log,
            &executor.as_executor(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH))
            },
            DiskVariant::M2,
        )
        .is_err());

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

    #[test]
    fn ensure_partition_layout_u2_with_expected_format() {
        let logctx =
            test_setup_log("ensure_partition_layout_u2_with_expected_format");
        let log = &logctx.log;
        let executor = FakeExecutorBuilder::new(log.clone()).build();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        let partitions = internal_ensure_partition_layout::<FakeU2GPT>(
            &log,
            &executor.as_executor(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
            },
            DiskVariant::U2,
        )
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

    #[test]
    fn ensure_partition_layout_m2_with_expected_format() {
        let logctx =
            test_setup_log("ensure_partition_layout_m2_with_expected_format");
        let log = &logctx.log;
        let executor = FakeExecutorBuilder::new(log.clone()).build();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        let partitions = internal_ensure_partition_layout::<FakeM2GPT>(
            &log,
            &executor.as_executor(),
            &DiskPaths {
                devfs_path,
                dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
            },
            DiskVariant::M2,
        )
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

    #[test]
    fn ensure_partition_layout_m2_fails_with_empty_gpt() {
        let logctx =
            test_setup_log("ensure_partition_layout_m2_fails_with_empty_gpt");
        let log = &logctx.log;
        let executor = FakeExecutorBuilder::new(log.clone()).build();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        assert!(matches!(
            internal_ensure_partition_layout::<EmptyGPT>(
                &log,
                &executor.as_executor(),
                &DiskPaths {
                    devfs_path,
                    dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
                },
                DiskVariant::M2,
            )
            .expect_err("Should have failed parsing empty GPT"),
            DiskError::BadPartitionLayout { .. }
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn ensure_partition_layout_u2_fails_with_empty_gpt() {
        let logctx =
            test_setup_log("ensure_partition_layout_u2_fails_with_empty_gpt");
        let log = &logctx.log;
        let executor = FakeExecutorBuilder::new(log.clone()).build();

        let devfs_path = Utf8PathBuf::from("/devfs/path");
        const DEV_PATH: &'static str = "/dev/path";

        assert!(matches!(
            internal_ensure_partition_layout::<EmptyGPT>(
                &log,
                &executor.as_executor(),
                &DiskPaths {
                    devfs_path,
                    dev_path: Some(Utf8PathBuf::from(DEV_PATH)),
                },
                DiskVariant::U2,
            )
            .expect_err("Should have failed parsing empty GPT"),
            DiskError::BadPartitionLayout { .. }
        ));

        logctx.cleanup_successful();
    }
}
