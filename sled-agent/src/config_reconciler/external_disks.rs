// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
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
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct ExternalDisk {
    config: OmicronPhysicalDiskConfig,
    state: Arc<DiskState>,
}

impl IdMappable for ExternalDisk {
    type Id = PhysicalDiskUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}

#[derive(Debug)]
enum DiskState {
    Managed(Disk),
    FailedToStartManaging(DiskManagementError),
}

#[derive(Debug, Clone)]
pub struct ExternalDiskMap {
    disks: IdMap<ExternalDisk>,
    mount_config: Arc<MountConfig>,
}

impl ExternalDiskMap {
    pub fn new(mount_config: Arc<MountConfig>) -> Self {
        Self { disks: IdMap::default(), mount_config }
    }

    pub fn all_managed_external_disk_pools(
        &self,
    ) -> impl Iterator<Item = &ZpoolName> + '_ {
        self.disks.iter().filter_map(|disk| match &*disk.state {
            DiskState::Managed(disk) => Some(disk.zpool_name()),
            DiskState::FailedToStartManaging(_) => None,
        })
    }

    pub(super) fn to_inventory(
        &self,
    ) -> BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult> {
        self.disks
            .iter()
            .map(|disk| {
                let result = match &*disk.state {
                    DiskState::Managed(_) => Ok(()),
                    DiskState::FailedToStartManaging(err) => {
                        Err(InlineErrorChain::new(&err).to_string())
                    }
                };
                (disk.config.id, result.into())
            })
            .collect()
    }

    pub(super) fn mount_config(&self) -> &Arc<MountConfig> {
        &self.mount_config
    }

    pub(super) fn has_disk_with_retryable_error(&self) -> bool {
        self.disks.iter().any(|disk| match &*disk.state {
            DiskState::Managed(_) => false,
            DiskState::FailedToStartManaging(err) => err.retryable(),
        })
    }

    /// Retain all disks that we are supposed to manage (based on `config`) that
    /// are also physically present (based on `raw_disks`), removing any disks
    /// we'd previously started to manage that are no longer present in either
    /// set.
    pub(super) fn stop_managing_if_needed(
        &mut self,
        raw_disks: &IdMap<RawDisk>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
        log: &Logger,
    ) {
        let mut disk_ids_to_remove = Vec::new();
        let mut disk_ids_to_mark_not_found = Vec::new();

        for disk in &self.disks {
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
                    &*disk.state,
                    DiskState::FailedToStartManaging(
                        DiskManagementError::NotFound
                    )
                ) {
                    warn!(
                        log,
                        "removing managed disk: still present in config, \
                         but no longer available from OS";
                        "disk_id" => %disk_id,
                        "disk" => ?disk.config.identity,
                    );
                    disk_ids_to_mark_not_found.push(disk_id);
                }
            }
        }

        for disk_id in disk_ids_to_remove {
            self.disks.remove(&disk_id);
        }
        for disk_id in disk_ids_to_mark_not_found {
            let mut entry =
                self.disks.get_mut(&disk_id).expect("IDs came from self.disks");
            entry.state = Arc::new(DiskState::FailedToStartManaging(
                DiskManagementError::NotFound,
            ));
        }
    }

    /// Attempt to start managing any disks specified by `config` that we aren't
    /// already managing.
    pub(super) async fn start_managing_if_needed(
        &mut self,
        raw_disks: &IdMap<RawDisk>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
        key_requester: &StorageKeyRequester,
        log: &Logger,
    ) {
        let mut disk_adopter = RealDiskAdopter { key_requester };
        self.start_managing_if_needed_impl(
            raw_disks,
            config,
            &mut disk_adopter,
            log,
        )
        .await
    }

    async fn start_managing_if_needed_impl<T: DiskAdopter>(
        &mut self,
        raw_disks: &IdMap<RawDisk>,
        config: &IdMap<OmicronPhysicalDiskConfig>,
        disk_adopter: &mut T,
        log: &Logger,
    ) {
        for config_disk in config {
            if let Some(result) = self
                .start_managing_single_disk_if_needed(
                    raw_disks,
                    config_disk,
                    disk_adopter,
                    log,
                )
                .await
            {
                let state = match result {
                    Ok(disk) => DiskState::Managed(disk),
                    Err(err) => DiskState::FailedToStartManaging(err),
                };
                self.disks.insert(ExternalDisk {
                    config: config_disk.clone(),
                    state: Arc::new(state),
                });
            }
        }
    }

    async fn start_managing_single_disk_if_needed<T: DiskAdopter>(
        &self,
        raw_disks: &IdMap<RawDisk>,
        config_disk: &OmicronPhysicalDiskConfig,
        disk_adopter: &mut T,
        log: &Logger,
    ) -> Option<Result<Disk, DiskManagementError>> {
        let identity = &config_disk.identity;

        let Some(raw_disk) = raw_disks.get(identity) else {
            warn!(
                log,
                "Control plane disk requested, but not detected within sled";
                "disk_identity" => ?identity
            );
            return Some(Err(DiskManagementError::NotFound));
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
                return Some(Err(
                    DiskManagementError::InternalDiskControlPlaneRequest(
                        config_disk.id,
                    ),
                ));
            }
        }

        match self.disks.get(&config_disk.id) {
            Some(disk) => match &*disk.state {
                // Disk is already managed; nothing to do but confirm we're not
                // in some weird misconfigured state.
                DiskState::Managed(disk) => {
                    let expected = config_disk.pool_id;
                    let observed = disk.zpool_name().id();
                    let maybe_new_state = if expected == observed {
                        info!(
                            log, "Disk already managed successfully";
                            "disk_identity" => ?identity,
                        );
                        None
                    } else {
                        warn!(
                            log,
                            "Observed an unexpected zpool uuid";
                            "expected" => ?expected,
                            "observed" => ?observed,
                            "disk_identity" => ?identity,
                        );
                        Some(Err(DiskManagementError::ZpoolUuidMismatch {
                            expected,
                            observed,
                        }))
                    };
                    return maybe_new_state;
                }
                DiskState::FailedToStartManaging(prev_err) => {
                    info!(
                        log, "Retrying management of disk";
                        "disk_identity" => ?identity,
                        "prev_err" => InlineErrorChain::new(&prev_err),
                    );
                    // fall through to disk adoption below
                }
            },
            None => {
                info!(
                    log, "Starting management of disk";
                    "disk_identity" => ?identity,
                );
                // fall through to disk adoption below
            }
        }

        match disk_adopter
            .adopt_disk(
                raw_disk.clone(),
                &self.mount_config,
                config_disk.pool_id,
                log,
            )
            .await
        {
            Ok(disk) => {
                info!(
                    log, "Successfully started management of disk";
                    "disk_identity" => ?identity,
                );
                Some(Ok(disk))
            }
            Err(err) => {
                warn!(
                    log, "Disk adoption failed";
                    "disk_identity" => ?identity,
                    InlineErrorChain::new(&err),
                );
                Some(Err(err))
            }
        }
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
            ExternalDiskMap::new(Arc::new(any_mount_config()));

        // There should be no disks to start.
        assert!(external_disks.disks.is_empty());

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
        external_disks
            .start_managing_if_needed_impl(
                &raw_disks,
                &config_disks,
                &mut disk_adopter,
                &logctx.log,
            )
            .await;

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
            let disk_state = &*external_disks
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
                    DiskState::FailedToStartManaging(
                        DiskManagementError::InternalDiskControlPlaneRequest(
                            id,
                        ),
                    ) if *id == disk.id => (),
                    _ => panic!("unexpected state: {disk_state:?}"),
                },
            }
        }

        logctx.cleanup_successful();
    }

    // TODO-john more tests
    // * change disk properties
    // * request nonexistent disk
    // * removal via retain_present_and_managed
}
