// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::convert::Infallible;
use std::sync::Arc;

use super::CurrentConfig;
use super::internal_disks::InternalDisksReceiver;
use super::key_requester::KeyManagerWaiter;
use super::key_requester::KeyRequesterStatus;
use crate::services::OmicronZonesConfigLocal;
use camino::Utf8PathBuf;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::api::external::Generation;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_common::ledger;
use omicron_common::ledger::Ledger;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::watch;

const CONFIG_LEDGER_FILENAME: &str = "omicron-sled-config.json";

#[derive(Debug, thiserror::Error)]
pub enum LedgerTaskError {
    #[error("cannot write sled config ledger: no M.2 disks available")]
    NoM2Disks,
    #[error("cannot accept sled config: waiting for key manager")]
    WaitingForKeyManager,
    #[error(
        "sled config generation out of date (got {requested}, have {current})"
    )]
    GenerationOutdated { current: Generation, requested: Generation },
    #[error("sled config changed with the same generation ({generation})")]
    ConfigurationChanged { generation: Generation },
    #[error("failed to commit sled config to ledger")]
    LedgerCommitFailed(#[source] ledger::Error),
    #[error("ledger task busy; cannot service new requests")]
    Busy,
    #[error("internal error: ledger task exited!")]
    Exited,
}

pub struct LedgerTaskHandle {
    tx: mpsc::Sender<WriteNewConfig>,
}

impl LedgerTaskHandle {
    pub async fn set_new_config(
        &self,
        new_config: OmicronSledConfig,
    ) -> Result<(), LedgerTaskError> {
        let (tx, rx) = oneshot::channel();
        let request = WriteNewConfig { new_config, tx };
        self.tx.try_send(request).map_err(|err| match err {
            // We should only see this error if the ledger task has gotten badly
            // behind updating ledgers on M.2s.
            TrySendError::Full(_) => LedgerTaskError::Busy,
            // We should never see this error in production, as the ledger task
            // never exits, but may see it in tests.
            TrySendError::Closed(_) => LedgerTaskError::Exited,
        })?;
        match rx.await {
            Ok(result) => result,
            // As above, we should never see this error in production.
            Err(_) => Err(LedgerTaskError::Exited),
        }
    }
}

pub struct LedgerTask {
    rx: mpsc::Receiver<WriteNewConfig>,
    current_config: watch::Sender<CurrentConfig>,
    internal_disks: InternalDisksReceiver,
    key_manager_waiter: Arc<KeyManagerWaiter>,
    log: Logger,
}

impl LedgerTask {
    pub fn spawn(
        internal_disks: InternalDisksReceiver,
        key_manager_waiter: Arc<KeyManagerWaiter>,
        log: Logger,
    ) -> (LedgerTaskHandle, watch::Receiver<CurrentConfig>) {
        // We don't expect messages to be queued for long in this channel, so
        // create it with a small bound. `LedgerTaskHandle` uses `try_send` to
        // detect if the task has gotten wedged (e.g., trying to write ledgers
        // to one of the M.2s).
        //
        // We expect 3 Nexuses running concurrently, so we should make this at
        // least 3 to avoid spurious 503s if they all try to set our config
        // simultaneously.
        let (tx, rx) = mpsc::channel(4);
        let (current_config, current_config_rx) =
            watch::channel(CurrentConfig::WaitingForInternalDisks);

        tokio::spawn(
            Self {
                rx,
                current_config,
                internal_disks,
                key_manager_waiter,
                log,
            }
            .run(),
        );

        (LedgerTaskHandle { tx }, current_config_rx)
    }

    async fn run(self) {
        // This looks strange, but `run_impl()` cannot return `Ok(_)`; it must
        // run forever (or until failure).
        let Err((log, err)) = self.run_impl().await;
        error!(
            log,
            "LedgerTask::run() unexpectedly exited; this should only be \
             possible in tests due to runtime shutdown ordering";
            InlineErrorChain::new(&err),
        );
    }

    async fn run_impl(
        mut self,
    ) -> Result<Infallible, (Logger, LedgerTaskExit)> {
        // We created `self.current_config` in `spawn()` and own the only
        // sender, so it should start out in the `WaitingForM2Disks` state. We
        // need to wait for at least one M.2 before we can do anything, and
        // `wait_for_m2_disks` should not return until we've seen one.
        assert_eq!(
            *self.current_config.borrow(),
            CurrentConfig::WaitingForInternalDisks
        );
        if let Err(err) = self.wait_for_m2_disks().await {
            return Err((self.log, err));
        }
        assert_ne!(
            *self.current_config.borrow(),
            CurrentConfig::WaitingForInternalDisks
        );

        loop {
            let Some(WriteNewConfig { new_config, tx }) = self.rx.recv().await
            else {
                return Err((
                    self.log,
                    LedgerTaskExit::LedgerTaskHandleDropped,
                ));
            };

            let response = self.set_new_config(new_config).await;
            // We don't care if the receiver is gone.
            let _ = tx.send(response);
        }
    }

    async fn set_new_config(
        &mut self,
        new_config: OmicronSledConfig,
    ) -> Result<(), LedgerTaskError> {
        // Refuse to accept new configs before the storage key manager is ready.
        // Technically we could accept them, but we can't act on them in any
        // meaningful way (other than writing them to the ledger), and we also
        // can't compare our actual on-disk state to check for validity of the
        // incoming request.
        match self.key_manager_waiter.status() {
            KeyRequesterStatus::Ready => (),
            KeyRequesterStatus::WaitingForKeyManager => {
                return Err(LedgerTaskError::WaitingForKeyManager);
            }
        }

        let config_datasets =
            self.internal_disks.borrow_and_update().all_config_datasets();

        // This would be very unusual: We waited for at least one M.2 to be
        // present in `run_impl`, so if this is empty that means we've lost all
        // M.2s. Refuse to accept new config.
        if config_datasets.is_empty() {
            error!(self.log, "no M.2 drives available any longer");
            return Err(LedgerTaskError::NoM2Disks);
        }

        // Check that we can accept the new ledger. The first check (and only
        // one likely to actually fail) is whether the generation is too old.
        // Clone our current config to avoid holding the watch channel lock
        // while we do our check.
        let current_config = self.current_config.borrow().clone();
        match current_config {
            CurrentConfig::WaitingForInternalDisks => {
                unreachable!("already waited for M.2 disks")
            }
            // If this is the first config we've gotten, we have no previous
            // generation to check.
            CurrentConfig::WaitingForRackSetup => (),
            CurrentConfig::Ledgered(omicron_sled_config) => {
                if new_config.generation < omicron_sled_config.generation {
                    info!(
                        self.log,
                        "rejecting config request due to out-of-date generation";
                        "current-gen" => %omicron_sled_config.generation,
                        "request-gen" => %new_config.generation,
                    );
                    return Err(LedgerTaskError::GenerationOutdated {
                        current: omicron_sled_config.generation,
                        requested: new_config.generation,
                    });
                } else if new_config.generation
                    == omicron_sled_config.generation
                {
                    if new_config != omicron_sled_config {
                        warn!(
                            self.log,
                            "requested config changed (with same generation)";
                            "generation" => %new_config.generation,
                        );
                        return Err(LedgerTaskError::ConfigurationChanged {
                            generation: new_config.generation,
                        });
                    }
                    info!(
                        self.log, "configuration unchanged";
                        "generation" => %new_config.generation,
                    );

                    // We now fall through to committing the ledger to disk.
                    // This seems unnecessary: if the generation matches, we
                    // already committed the ledger previously. But we don't
                    // know that it's been written to both M.2s:
                    //
                    // * It's possible only one M.2 was available when we
                    //   previously committed
                    // * It's possible we had two M.2s, but one of the writes
                    //   failed (`Ledger::commit()` returns `Ok(())` as long as
                    //   at least one of the writes succeeds)
                    //
                    // We could probably address both of those and avoid
                    // spurious writes, but it seems a little tricky and not
                    // particularly important.
                }
            }
        }

        // TODO-correctness We should check that the incoming config is
        // self-consistent (e.g., that zones reference datasets that exist which
        // themselves reference disks that exist) and that it's not violating
        // constraints we ourselves enforce (e.g., at most one dataset of a
        // given kind per zpool:
        // https://github.com/oxidecomputer/omicron/issues/7311).

        let config_paths = config_datasets
            .iter()
            .map(|p| p.join(CONFIG_LEDGER_FILENAME))
            .collect::<Vec<_>>();
        let mut ledger = Ledger::new_with(&self.log, config_paths, new_config);
        match ledger.commit().await {
            Ok(()) => {
                info!(
                    self.log, "updated sled config ledger";
                    "generation" => %ledger.data().generation,
                );

                // Now that we've committed the ledger, update our watch channel
                let new_config = CurrentConfig::Ledgered(ledger.into_inner());
                self.current_config.send_if_modified(|c| {
                    if *c == new_config {
                        false
                    } else {
                        *c = new_config;
                        true
                    }
                });

                Ok(())
            }
            Err(err) => {
                warn!(
                    self.log, "Failed to write sled config to ledger";
                    "generation" => %ledger.data().generation,
                    InlineErrorChain::new(&err),
                );
                Err(LedgerTaskError::LedgerCommitFailed(err))
            }
        }
    }

    async fn wait_for_m2_disks(&mut self) -> Result<(), LedgerTaskExit> {
        loop {
            let config_datasets =
                self.internal_disks.borrow_and_update().all_config_datasets();

            // The condition we're waiting for: do we have at least one M.2?
            if !config_datasets.is_empty() {
                let loaded_config =
                    load_sled_config(&config_datasets, &self.log).await;
                assert_ne!(
                    loaded_config,
                    CurrentConfig::WaitingForInternalDisks
                );

                self.current_config.send_modify(|c| *c = loaded_config);
                return Ok(());
            }

            // We don't have at least one M.2; wait for a change in
            // internal_disks, and reject any incoming requests in the meantime.
            tokio::select! {
                // Cancel-safe per docs on `changed()`
                result = self.internal_disks.changed() => {
                    if result.is_err() {
                        return Err(LedgerTaskExit::InternalDisksSenderDropped);
                    }
                    continue;
                }

                // Cancel-safe per docs on `recv()`
                request = self.rx.recv() => {
                    let Some(WriteNewConfig { tx, .. }) = request else {
                        return Err(LedgerTaskExit::LedgerTaskHandleDropped);
                    };
                    // We don't care if the receiver is gone.
                    let _ = tx.send(Err(LedgerTaskError::NoM2Disks));
                }
            }
        }
    }
}

// `LedgerTask` should not exit in production, but may exit during tests
// depending on the drop order of various channels. All such exits are covered
// by this error (in the hopefully-impossible event we see it outside of tests).
#[derive(Debug, thiserror::Error)]
enum LedgerTaskExit {
    #[error("internal error: InternalDisks watch channel Sender dropped")]
    InternalDisksSenderDropped,
    #[error("internal error: LedgerTaskHandle dropped")]
    LedgerTaskHandleDropped,
}

#[derive(Debug)]
struct WriteNewConfig {
    new_config: OmicronSledConfig,
    tx: oneshot::Sender<Result<(), LedgerTaskError>>,
}

async fn load_sled_config(
    config_datasets: &[Utf8PathBuf],
    log: &Logger,
) -> CurrentConfig {
    assert!(
        !config_datasets.is_empty(),
        "load_sled_config called with no config_datasets"
    );
    info!(
        log, "Attempting to load sled config from ledger";
        "paths" => ?config_datasets,
    );

    // First try to load the ledger from our expect path(s).
    let paths = config_datasets
        .iter()
        .map(|p| p.join(CONFIG_LEDGER_FILENAME))
        .collect();
    if let Some(config) = Ledger::new(log, paths).await {
        info!(log, "Ledger of sled config exists");
        return CurrentConfig::Ledgered(config.into_inner());
    }

    // If we have no ledgered config, see if we can convert from the previous
    // triple of legacy ledgers.
    if let Some(config) = convert_legacy_ledgers(&config_datasets, log).await {
        info!(log, "Converted legacy triple of ledgers into new sled config");
        return CurrentConfig::Ledgered(config);
    }

    // We have no ledger and didn't find legacy ledgers to convert; we must be
    // waiting for RSS.
    info!(log, "No sled config ledger exists");
    CurrentConfig::WaitingForRackSetup
}

const LEGACY_DISKS_LEDGER_FILENAME: &str = "omicron-physical-disks.json";
const LEGACY_DATASETS_LEDGER_FILENAME: &str = "omicron-datasets.json";
const LEGACY_ZONES_LEDGER_FILENAME: &str = "omicron-zones.json";

async fn convert_legacy_ledgers(
    config_datasets: &[Utf8PathBuf],
    log: &Logger,
) -> Option<OmicronSledConfig> {
    let disk_paths = config_datasets
        .iter()
        .map(|p| p.join(LEGACY_DISKS_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let dataset_paths = config_datasets
        .iter()
        .map(|p| p.join(LEGACY_DATASETS_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let zone_paths = config_datasets
        .iter()
        .map(|p| p.join(LEGACY_ZONES_LEDGER_FILENAME))
        .collect::<Vec<_>>();

    let loaded_ledgers = futures::join!(
        Ledger::<OmicronPhysicalDisksConfig>::new(log, disk_paths.clone()),
        Ledger::<DatasetsConfig>::new(log, dataset_paths.clone()),
        Ledger::<OmicronZonesConfigLocal>::new(log, zone_paths.clone()),
    );

    let (disks, datasets, zones) = match loaded_ledgers {
        // If we have all three or none of the three, our decision is easy.
        (Some(disks), Some(datasets), Some(zones)) => {
            (disks.into_inner(), datasets.into_inner(), zones.into_inner())
        }
        (None, None, None) => return None,

        // Any other combo is terrible: we have one or two legacy ledgers, but
        // are missing the other two or one. Log an error and treat this as "no
        // ledgered config". This should only be possible if we were interrupted
        // mid-rack-setup, which already requires clean-slate'ing (which should
        // remove these config files!).
        (disks, datasets, zones) => {
            error!(
                log,
                "Found partial legacy ledgers; \
                 treating as no ledgered config";
                "found-legacy-disks" => disks.is_some(),
                "found-legacy-datasets" => datasets.is_some(),
                "found-legacy-zones" => zones.is_some(),
            );
            return None;
        }
    };

    // Peform the actual conversion of the legacy configs to the new combo
    // config.
    let config = {
        OmicronSledConfig {
            // Take the zone generation as the overall config generation; this
            // is consistent with Reconfigurator's transition from three configs
            // to one.
            generation: zones.omicron_generation,
            disks: disks.disks.into_iter().collect(),
            datasets: datasets.datasets.into_values().collect(),
            zones: zones.zones.into_iter().map(|z| z.zone).collect(),
        }
    };

    // Write the new config.
    let config_paths = config_datasets
        .iter()
        .map(|p| p.join(CONFIG_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let mut config_ledger = Ledger::new_with(log, config_paths.clone(), config);
    if let Err(err) = config_ledger.commit().await {
        // We weren't able to write the new ledger, but we were still able to
        // _read_ it (via the old ones). Log this failure but return the config
        // we read; we'll try converting again the next time we run.
        warn!(
            log, "Failed to write sled config ledger built from legacy ledgers";
            InlineErrorChain::new(&err),
        );
        return Some(config_ledger.into_inner());
    }

    // Be paranoid before removing the legacy ledgers: confirm we can read back
    // the new combined config.
    match Ledger::new(log, config_paths.clone()).await {
        Some(reread_config) => {
            // Check that the contents we wrote match the contents we read. No
            // one should be modifying this file concurrently, so a failure here
            // means we've ledgered incorrect data, which could be disasterous.
            // Log an error and at least try remove the ledgers we just wrote,
            // then use the config we cobbled together from the legacy ledgers.
            if config_ledger.data() != reread_config.data() {
                error!(
                    log,
                    "Reading just-ledgered config returns unexpected contents!";
                    "written" => ?config_ledger.data(),
                    "read" => ?reread_config.data(),
                );
                for p in &config_paths {
                    if let Err(err) = tokio::fs::remove_file(p).await {
                        // We're in really big trouble now: we've written a
                        // bogus ledger and can't remove it. Maybe this should
                        // panic instead? (Except then sled-agent will
                        // immediately restart and try to use the bad ledger...)
                        error!(
                            log,
                            "Failed to remove potentially-bogus ledger!";
                            "path" => %p,
                            InlineErrorChain::new(&err),
                        );
                    }
                }
                return Some(config_ledger.into_inner());
            }
        }
        None => {
            // Not much we can do here - log the failure and return before we
            // try to remove the legacy ledgers.
            warn!(log, "Failed to read ledgered config we just wrote!");
            return Some(config_ledger.into_inner());
        }
    }

    // We've successfully written and reread our new combined config; remove the
    // old legacy ledgers.
    for old_ledger_path in
        disk_paths.iter().chain(dataset_paths.iter()).chain(zone_paths.iter())
    {
        if let Err(err) = tokio::fs::remove_file(old_ledger_path).await {
            // There isn't really anything we can do other than warn here;
            // future attempts to read the ledger will find the combined config
            // we wrote above, so we'll just leak the legacy configs here and
            // rely on support procedures to confirm we don't hit this during
            // the transition period.
            warn!(
                log,
                "Failed to remove legacy ledger";
                "path" => %old_ledger_path,
                InlineErrorChain::new(&err),
            );
        }
    }

    Some(config_ledger.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino_tempfile::Utf8TempDir;
    use id_map::IdMap;
    use illumos_utils::zpool::ZpoolName;
    use omicron_common::disk::DiskIdentity;
    use omicron_common::disk::DiskVariant;
    use omicron_common::disk::OmicronPhysicalDiskConfig;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_storage::config::MountConfig;
    use sled_storage::disk::Disk;
    use std::sync::Arc;
    use std::time::Duration;

    fn fake_m2_disk() -> Disk {
        Disk::Real(PooledDisk {
            paths: DiskPaths {
                devfs_path: "/fake-disk".into(),
                dev_path: None,
            },
            slot: 0,
            variant: DiskVariant::M2,
            identity: DiskIdentity {
                vendor: "ledger-test".into(),
                model: "ledger-test".into(),
                serial: "ledger-test-disk".into(),
            },
            is_boot_disk: false,
            partitions: vec![],
            zpool_name: ZpoolName::new_internal(ZpoolUuid::new_v4()),
            firmware: DiskFirmware::new(0, None, false, 1, Vec::new()),
        })
    }

    #[tokio::test]
    async fn test_first_config_is_ledgered() {
        let logctx = dev::test_setup_log("test_first_config_is_ledgered");

        // Set up and spawn the ledgering task.
        let tempdir = Utf8TempDir::new().expect("created temp directory");
        let mount_config = MountConfig {
            root: tempdir.path().to_owned(),
            synthetic_disk_root: "/test/todo2".into(),
        };
        let (disks_tx, disks_rx) = watch::channel(Arc::default());
        let mut internal_disks = InternalDisksReceiver::new_for_tests(
            disks_rx,
            mount_config.clone(),
        );
        let key_manager_waiter =
            Arc::new(KeyManagerWaiter::fake_key_manager_waiter());
        key_manager_waiter.notify_key_manager_ready();
        let (task_handle, mut current_config) = LedgerTask::spawn(
            internal_disks.clone(),
            key_manager_waiter,
            logctx.log.clone(),
        );

        // We have no disks, so the ledger task should be waiting for them.
        assert_eq!(
            *current_config.borrow_and_update(),
            CurrentConfig::WaitingForInternalDisks
        );

        // Tell the ledger that there's one internal disk.
        disks_tx.send_modify(|disks| {
            Arc::make_mut(disks).insert(fake_m2_disk());
        });

        // Our fake disk's datasets live underneath our `tempdir`; create the
        // intermediate subdirectories.
        let config_datasets =
            internal_disks.borrow_and_update().all_config_datasets();
        assert!(!config_datasets.is_empty());
        for path in &config_datasets {
            assert!(
                path.starts_with(tempdir.path()),
                "expected dataset path ({path}) to be under tempdir {}",
                tempdir.path(),
            );
            tokio::fs::create_dir_all(&path)
                .await
                .expect("created tempdir subdirectory");
        }

        // Wait for the ledger task to realize there's an internal disk; it has
        // no config, so it should transition to the "waiting for RSS" state.
        tokio::time::timeout(Duration::from_secs(60), current_config.changed())
            .await
            .expect("config changed before timeout")
            .expect("changed() succeeded");
        assert_eq!(
            *current_config.borrow_and_update(),
            CurrentConfig::WaitingForRackSetup
        );

        // Send a new config; the contents here don't matter much, but we'll do
        // something slightly more than `::default()` and give it a single fake
        // disk. Below we'll check that this config was successfully ledgered.
        let sled_config = OmicronSledConfig {
            generation: Generation::new().next(),
            disks: [OmicronPhysicalDiskConfig {
                identity: DiskIdentity {
                    vendor: "test-vendor".into(),
                    model: "test-model".into(),
                    serial: "test-serial".into(),
                },
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            }]
            .into_iter()
            .collect(),
            datasets: IdMap::default(),
            zones: IdMap::default(),
        };
        task_handle
            .set_new_config(sled_config.clone())
            .await
            .expect("no ledger task error");

        // Confirm that the watch channel was updated.
        assert_eq!(
            *current_config.borrow_and_update(),
            CurrentConfig::Ledgered(sled_config.clone()),
        );

        // Also confirm the config was persisted as expected.
        for path in &config_datasets {
            let path = path.join(CONFIG_LEDGER_FILENAME);
            let persisted = tokio::fs::read(&path)
                .await
                .expect("read persisted sled config");
            let parsed: OmicronSledConfig = serde_json::from_slice(&persisted)
                .expect("parsed persisted sled config");
            assert_eq!(parsed, sled_config);
        }

        logctx.cleanup_successful();
    }

    // TODO-john more tests
    // * key manager not ready yet
    // * read existing config
    // * convert legacy configs
    // * reject bad configs
}
