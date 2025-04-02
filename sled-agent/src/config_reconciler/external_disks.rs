// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use id_map::IdMap;
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
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::future::Future;
use std::sync::Arc;

#[derive(Debug)]
pub struct ExternalDisks {
    disks: BTreeMap<PhysicalDiskUuid, Disk>,
    mount_config: Arc<MountConfig>,
}

impl ExternalDisks {
    pub fn new(mount_config: Arc<MountConfig>) -> Self {
        Self { disks: BTreeMap::new(), mount_config }
    }

    /// Retain all disks that we are supposed to manage (based on `config`) that
    /// are also physically present (based on `raw_disks`), removing any disks
    /// we'd previously started to manage that are no longer present in either
    /// set.
    pub(super) fn retain_present_and_managed(
        &mut self,
        raw_disks: &IdMap<RawDisk>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
    ) {
        self.disks.retain(|disk_id, disk| {
            raw_disks.contains_key(disk.identity())
                && config.contains_key(disk_id)
        });
    }

    /// Attempt to start managing any disks specified by `config` that we aren't
    /// already managing.
    pub(super) async fn ensure_managing(
        &mut self,
        raw_disks: &IdMap<RawDisk>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
        key_requester: &StorageKeyRequester,
        log: &Logger,
    ) -> Result<(), BTreeMap<PhysicalDiskUuid, DiskManagementError>> {
        let mut disk_adopter = RealDiskAdopter { key_requester };
        self.ensure_managing_impl(raw_disks, config, &mut disk_adopter, log)
            .await
    }

    async fn ensure_managing_impl<T: DiskAdopter>(
        &mut self,
        raw_disks: &IdMap<RawDisk>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
        disk_adopter: &mut T,
        log: &Logger,
    ) -> Result<(), BTreeMap<PhysicalDiskUuid, DiskManagementError>> {
        let mut errors = BTreeMap::new();

        for config_disk in config {
            let identity = &config_disk.identity;

            let Some(raw_disk) = raw_disks.get(identity) else {
                warn!(
                    log,
                    "Control plane disk requested, but not detected within sled";
                    "disk_identity" => ?identity
                );
                errors.insert(config_disk.id, DiskManagementError::NotFound);
                continue;
            };

            // Refuse to manage internal disks.
            match raw_disk.variant() {
                DiskVariant::U2 => (),
                DiskVariant::M2 => {
                    warn!(
                        log,
                        "Control plane requested management of internal disk";
                        "disk_identity" => ?identity,
                        "config_request" => ?config_disk,
                    );
                    errors.insert(
                        config_disk.id,
                        DiskManagementError::InternalDiskControlPlaneRequest(
                            config_disk.id,
                        ),
                    );
                    continue;
                }
            }

            match self.disks.entry(config_disk.id) {
                Entry::Vacant(entry) => {
                    info!(
                        log, "Starting management of disk";
                        "disk_identity" => ?identity,
                    );
                    let disk = match disk_adopter
                        .adopt_disk(
                            raw_disk.clone(),
                            &self.mount_config,
                            config_disk.pool_id,
                            log,
                        )
                        .await
                    {
                        Ok(disk) => disk,
                        Err(err) => {
                            warn!(
                                log, "Disk adoption failed";
                                "disk_identity" => ?identity,
                                InlineErrorChain::new(&err),
                            );
                            errors.insert(config_disk.id, err);
                            continue;
                        }
                    };

                    info!(
                        log, "Successfully started management of disk";
                        "disk_identity" => ?identity,
                    );
                    entry.insert(disk);
                }
                // Disk is already managed. Check that the configuration
                // matches what we expect.
                Entry::Occupied(entry) => {
                    let expected = config_disk.pool_id;
                    let observed = entry.get().zpool_name().id();
                    if expected == observed {
                        info!(
                            log, "Disk already managed successfully";
                            "disk_identity" => ?identity,
                        );
                    } else {
                        warn!(
                            log,
                            "Observed an unexpected zpool uuid";
                            "expected" => ?expected,
                            "observed" => ?observed,
                            "disk_identity" => ?identity,
                        );
                        errors.insert(
                            config_disk.id,
                            DiskManagementError::ZpoolUuidMismatch {
                                expected,
                                observed,
                            },
                        );
                        continue;
                    }
                }
            }
        }

        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

/// Helper to allow unit tests to run without interacting with the real [`Disk`]
/// implementation. In production, the only implementor of this trait is
/// [`RealDiskAdopter`].
trait DiskAdopter {
    fn adopt_disk(
        &mut self,
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
        &mut self,
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
    use illumos_utils::zpool::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_hardware::UnparsedDisk;

    #[derive(Debug, Default)]
    struct TestDiskAdopter {
        requests: Vec<RawDisk>,
    }

    impl DiskAdopter for TestDiskAdopter {
        async fn adopt_disk(
            &mut self,
            raw_disk: RawDisk,
            _mount_config: &MountConfig,
            pool_id: ZpoolUuid,
            _log: &Logger,
        ) -> Result<Disk, DiskManagementError> {
            // InternalDisks should only adopt U2 disks
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
            self.requests.push(raw_disk);
            Ok(disk)
        }
    }

    fn any_mount_config() -> MountConfig {
        MountConfig {
            root: "/sled-agent-tests".into(),
            synthetic_disk_root: "/sled-agent-tests".into(),
        }
    }

    fn new_raw_test_disk(variant: DiskVariant, serial: &str) -> RawDisk {
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
    }

    #[tokio::test]
    async fn test_only_adopts_u2_disks() {
        let logctx = dev::test_setup_log("test_only_adopts_m2_disks");

        let mut external_disks =
            ExternalDisks::new(Arc::new(any_mount_config()));

        // There should be no disks to start.
        assert_eq!(external_disks.disks, BTreeMap::new());

        // Add four disks: two M.2 and two U.2.
        let raw_disks = [
            new_raw_test_disk(DiskVariant::M2, "m2-0"),
            new_raw_test_disk(DiskVariant::U2, "u2-0"),
            new_raw_test_disk(DiskVariant::M2, "m2-1"),
            new_raw_test_disk(DiskVariant::U2, "u2-1"),
        ]
        .into_iter()
        .collect::<IdMap<_>>();

        // Claim the control plane wants to manage all of them. (This is bogus:
        // we should never try to manage internal disks.)
        let config_disks = raw_disks
            .iter()
            .map(|disk| OmicronPhysicalDiskConfig {
                identity: disk.identity().clone(),
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            })
            .collect::<IdMap<_>>();

        // This should partially succeed: we should adopt the two U.2s and
        // report errors on the two M.2s.
        let mut disk_adopter = TestDiskAdopter::default();
        let errors = match external_disks
            .ensure_managing_impl(
                &raw_disks,
                &config_disks,
                &mut disk_adopter,
                &logctx.log,
            )
            .await
        {
            Ok(()) => panic!("unexpected success"),
            Err(errors) => errors,
        };

        // Only two disk adoptions should have been attempted.
        assert_eq!(disk_adopter.requests.len(), 2);
        assert!(
            disk_adopter
                .requests
                .iter()
                .all(|req| req.variant() == DiskVariant::U2),
            "found non-U2 disk adoption request: {:?}",
            disk_adopter.requests
        );

        // Ensure each disk is in the state we expect: either adopted or
        // reported as an error.
        for disk in config_disks {
            match raw_disks.get(&disk.identity).unwrap().variant() {
                DiskVariant::U2 => {
                    assert!(!errors.contains_key(&disk.id));
                    assert!(external_disks.disks.contains_key(&disk.id));
                }
                DiskVariant::M2 => {
                    assert!(!external_disks.disks.contains_key(&disk.id));
                    let err =
                        errors.get(&disk.id).expect("errors contains disk");
                    assert_eq!(
                        *err,
                        DiskManagementError::InternalDiskControlPlaneRequest(
                            disk.id
                        )
                    );
                }
            }
        }

        logctx.cleanup_successful();
    }

    // TODO-john more tests
    // * change disk properties
    // * request nonexistent disk
    // * removal via retain_present_and_managed
}
