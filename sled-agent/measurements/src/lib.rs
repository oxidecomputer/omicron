// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reference measurements for sled-agent
//!
//! Reference measurements are used with Trust Quorum. Briefly, Trust
//! Quorum will only communicate over a trusted sprockets channel.
//! We consider a channel trusted only if both parties are running the
//! expected set of Oxide software. This code is responsible for
//! the management of reference measurements (i.e. what code we expect
//! to be running on the system).

use camino::Utf8PathBuf;
use iddqd::IdOrdMap;
use sled_agent_config_reconciler::{ConfigReconcilerHandle, InventoryError};
use sled_agent_resolvable_files::ZoneImageSourceResolver;
use sled_agent_types::inventory::{
    ConfigReconcilerInventoryResult, OmicronSingleMeasurement,
    OmicronSledConfig, SingleMeasurementInventory,
};
use sled_agent_types::resolvable_files::{
    ManifestHashError, MupdateOverrideReadError,
};
use slog::Logger;
use slog::{error, info, warn};
use slog_error_chain::InlineErrorChain;
use slog_error_chain::SlogInlineError;
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Error, SlogInlineError)]
pub enum MeasurementError {
    #[error("Error reading ledger")]
    LedgerError(#[source] InventoryError),
    #[error("Error reading boot inventory {0}")]
    BootInventoryError(String),
    #[error("Mixture of valid entries {:?} and errors {:?} ", valid, errors)]
    Paths { valid: Vec<Utf8PathBuf>, errors: Vec<String> },
    #[error("Boot disk missing")]
    BootDiskMissing,
    #[error("Hash mismatch in manifest")]
    ManifestHashError(#[source] ManifestHashError),
    #[error("Error notification from ledger start")]
    LedgerStartFailure,
    #[error("mupdate override error")]
    MupdateOverrideError(#[source] MupdateOverrideReadError),
}

#[derive(Clone)]
enum MeasurementsInner {
    Real {
        log: Logger,
        config_reconciler: Arc<ConfigReconcilerHandle>,
        zone_image_resolver: ZoneImageSourceResolver,
        ledger_task_valid: bool,
    },
    // For testing purposes only
    Fake {
        paths: Vec<Utf8PathBuf>,
    },
}

#[derive(Clone)]
pub struct MeasurementsHandle {
    inner: MeasurementsInner,
}

impl MeasurementsHandle {
    // May eventually want to take paths?
    pub fn new_fake() -> Self {
        Self { inner: MeasurementsInner::Fake { paths: vec![] } }
    }

    pub async fn new(
        log: Logger,
        config_reconciler: Arc<ConfigReconcilerHandle>,
        zone_image_resolver: ZoneImageSourceResolver,
        ledger_task_run_rx: oneshot::Receiver<()>,
    ) -> Self {
        let ledger_task_valid = match ledger_task_run_rx.await {
            Ok(()) => true,
            Err(e) => {
                error!(log, "ledger task exited: {e}");
                // something has really gone awry
                false
            }
        };

        Self {
            inner: MeasurementsInner::Real {
                log,
                config_reconciler,
                zone_image_resolver,
                ledger_task_valid,
            },
        }
    }

    pub fn to_inventory(&self) -> IdOrdMap<SingleMeasurementInventory> {
        match self.current_measurements() {
            Ok(paths) => paths
                .into_iter()
                .map(|path| SingleMeasurementInventory {
                    path,
                    result: ConfigReconcilerInventoryResult::Ok,
                })
                .collect(),
            Err(e) => {
                // We don't have a path for errors
                vec![SingleMeasurementInventory {
                    path: Utf8PathBuf::new(),
                    result: ConfigReconcilerInventoryResult::Err {
                        message: format!("{e}"),
                    },
                }]
                .into_iter()
                .collect()
            }
        }
    }

    pub fn current_measurements(
        &self,
    ) -> Result<Vec<Utf8PathBuf>, MeasurementError> {
        let (log, config_reconciler, zone_image_resolver, ledger_task_valid) =
            match &self.inner {
                MeasurementsInner::Fake { paths } => return Ok(paths.to_vec()),
                MeasurementsInner::Real {
                    log,
                    config_reconciler,
                    zone_image_resolver,
                    ledger_task_valid,
                } => (
                    log,
                    config_reconciler,
                    zone_image_resolver,
                    ledger_task_valid,
                ),
            };

        // If we don't have a valid ledger task there really isn't much we
        // can be expected to do
        if !ledger_task_valid {
            return Err(MeasurementError::LedgerStartFailure);
        }

        // Get our measurements, first we check the ledger
        match config_reconciler.ledgered_sled_config() {
            Err(e) => {
                // This is closer to an unreachable. The cases that should bring us here
                // are that the ledger task either never started (should be impossible)
                // or we are still waiting on the M.2 disks (should also be impossible
                // since we only send our start notification after the ledger task runs)
                error!(log, "Error reading sled config from ledger: {e}");
                return Err(MeasurementError::LedgerError(e));
            }
            // We haven't run RSS, we'll take what we get from the measurement manifest
            Ok(None) => match zone_image_resolver
                .status()
                .to_inventory()
                .measurement_manifest
                .boot_inventory
            {
                Err(e) => {
                    // Not much we can do!
                    error!(log, "Error reading boot inventory manifest: {e}");
                    return Err(MeasurementError::BootInventoryError(e));
                }
                Ok(s) => {
                    let mut valid = Vec::new();
                    let mut errors = Vec::new();
                    for entry in s.artifacts.iter() {
                        match &entry.status {
                            Ok(_) => valid.push(entry.path.clone()),
                            Err(e) => errors.push(e.clone()),
                        }
                    }
                    if errors.is_empty() {
                        Ok(valid)
                    } else {
                        Err(MeasurementError::Paths { valid, errors })
                    }
                }
            },
            // We have a config!
            Ok(Some(s)) => measurements_from_sled_config(
                &s,
                &log,
                &config_reconciler,
                &zone_image_resolver,
            ),
        }
    }
}

fn measurements_from_sled_config(
    config: &OmicronSledConfig,
    log: &Logger,
    config_reconciler: &Arc<ConfigReconcilerHandle>,
    zone_image_resolver: &ZoneImageSourceResolver,
) -> Result<Vec<Utf8PathBuf>, MeasurementError> {
    let status = zone_image_resolver.status();
    let use_install = config.measurements.is_empty();

    match (&status.mupdate_override.boot_disk_override, use_install) {
        // We are set to use the install dataset and there is a MUPdate override
        (Ok(Some(override_info)), true) => {
            info!(log, "mupdate override active, already using install dataset";
                        "mupdate_override_id" => %override_info.mupdate_uuid);
            measurements_from_install_dataset(
                log,
                config_reconciler,
                zone_image_resolver,
            )
        }
        // Actual override case! Use the install dataset
        (Ok(Some(override_info)), false) => {
            info!(log, "mupdate override active, redirecting to install \
                        dataset from artifacts";
                        "mupdate_override_id" => %override_info.mupdate_uuid);
            measurements_from_install_dataset(
                log,
                config_reconciler,
                zone_image_resolver,
            )
        }
        // No override is active but we're still using the install dataset
        (Ok(None), true) => measurements_from_install_dataset(
            log,
            config_reconciler,
            zone_image_resolver,
        ),
        // No override and we're not using the install dataset
        (Ok(None), false) => measurements_from_artifact_dataset(
            config_reconciler,
            &config.measurements,
        ),
        // Error getting the mupdate override but we're still set to use
        // the install dataset
        (Err(error), true) => {
            warn!(log, "error obtaining mupdate override but we're still \
                        using install dataset, proceeding with caution";
                        "error" => InlineErrorChain::new(error));
            measurements_from_install_dataset(
                log,
                config_reconciler,
                zone_image_resolver,
            )
        }
        // Error getting the mupdate override but we're supposed to
        // use the artifacts
        (Err(error), false) => {
            error!(log, "error obtaining mupdate override, can't use the \
                        artifacts, returning an error";
                        "error" => InlineErrorChain::new(error));

            Err(MeasurementError::MupdateOverrideError(error.clone()))
        }
    }
}

fn measurements_from_artifact_dataset(
    config_reconciler: &Arc<ConfigReconcilerHandle>,
    measurements: &BTreeSet<OmicronSingleMeasurement>,
) -> Result<Vec<Utf8PathBuf>, MeasurementError> {
    let mut valid = Vec::new();
    let mut errors = Vec::new();

    for m in measurements {
        // We have mulitple possible artifact paths. We only need one per hash
        let mut found = false;
        for dataset in config_reconciler
            .internal_disks_rx()
            .current()
            .all_artifact_datasets()
        {
            let potential_path = dataset.join(m.hash.to_string());

            // This could technically be a TOCTOU issue but arguably all the paths we
            // return have the same potential issue and hopefully the callers handle
            // the failure gracefully
            if potential_path.is_file() {
                found = true;
                valid.push(potential_path);
                break;
            }
        }
        if !found {
            errors.push(format!(
                "artifact {} missing from all artifact datasets",
                m.hash
            ));
        }
    }

    if errors.is_empty() {
        Ok(valid)
    } else {
        Err(MeasurementError::Paths { valid, errors })
    }
}

fn measurements_from_install_dataset(
    log: &Logger,
    config_reconciler: &Arc<ConfigReconcilerHandle>,
    zone_image_resolver: &ZoneImageSourceResolver,
) -> Result<Vec<Utf8PathBuf>, MeasurementError> {
    let mut valid = Vec::new();
    let mut errors = Vec::new();

    if let Some(path) = config_reconciler
        .internal_disks_rx()
        .current()
        .boot_disk_install_dataset()
    {
        match zone_image_resolver
            .status()
            .measurement_manifest
            .all_measurements()
        {
            Ok(entries) => {
                for e in entries {
                    match e {
                        Ok(file_name) => {
                            valid.push(
                                path.join("measurements").join(file_name),
                            );
                        }
                        Err(e) => {
                            error!(
                                log,
                                "Error in entry in measurement manifest";
                                "error" => InlineErrorChain::new(&e),
                            );
                            errors.push(e.to_string())
                        }
                    }
                }
                if errors.is_empty() {
                    Ok(valid)
                } else {
                    Err(MeasurementError::Paths { valid, errors })
                }
            }
            Err(e) => {
                error!(
                    log,
                    "Error gettting measurements from the measurement manifest";
                    "error" => InlineErrorChain::new(&e),
                );
                Err(MeasurementError::ManifestHashError(e))
            }
        }
    } else {
        error!(
            log,
            "boot disk install dataset not available, \
             not returning it as a source";
        );
        Err(MeasurementError::BootDiskMissing)
    }
}
