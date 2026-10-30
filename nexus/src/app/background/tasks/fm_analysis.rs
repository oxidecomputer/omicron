// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use chrono::Utc;
use fm::analysis_input::Input;
use fm::analysis_reports::InputReport;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_fm as fm;
use nexus_fm_preparation::InputLoader;
use nexus_fm_preparation::PreparationResult;
use nexus_types::fm::CurrentSitrep;
use nexus_types::fm::FmConfig;
use nexus_types::fm::FmConfigSource;
use nexus_types::fm::FmConfigView;
use nexus_types::internal_api::background::FmAnalysisStatus;
use nexus_types::internal_api::background::fm_analysis as status;
use nexus_types::inventory;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct FmAnalysis {
    datastore: Arc<DataStore>,
    sitrep_rx: watch::Receiver<Option<CurrentSitrep>>,
    inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
    cfg_rx: watch::Receiver<Option<FmConfigView>>,
    activators: Activators,
    nexus_id: OmicronZoneUuid,
}

/// This is just because I don't like it when a constructor takes multiple
/// positional arguments of the same type...
#[derive(Clone)]
pub struct Activators {
    pub inventory_loader: Activator,
    pub sitrep_loader: Activator,
    pub sitrep_gc: Activator,
    pub sitrep_history_pruner: Activator,
}

impl Activators {
    /// Activate the sitrep history pruning and orphan sitrep GC background
    /// tasks to try and free up some capacity for new sitreps in the database.
    ///
    /// The `fm_sitrep_gc` task deletes orphaned sitreps (i.e., those which are
    /// not in the `fm_sitrep_history`) table. Meanwhile, the
    /// `fm_sitrep_history_pruner` task deletes old sitrep versions from the end
    /// of the `fm_sitrep_history` table, leaving those sitreps as orphans that
    /// can be cleaned up by the garbage collection task. When we wish to
    /// reclaim capacity for new sitreps, we activate *both* tasks: a GC pass is
    /// necessary to delete orphaned sitreps that may have been left behind when
    /// an analysis task lost a race to insert a new sitrep, while the history
    /// pruning pass is necessary to free up capacity used by sitrep versions
    /// that *are* part of the history.
    fn reclaim_capacity(&self) {
        self.sitrep_history_pruner.activate();
        self.sitrep_gc.activate();
    }
}

impl BackgroundTask for FmAnalysis {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    let err = format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&err)
                    );
                    json!({ "error": err })
                }
            }
        })
    }
}

impl FmAnalysis {
    pub fn new(
        datastore: Arc<DataStore>,
        sitrep_rx: watch::Receiver<Option<CurrentSitrep>>,
        inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
        cfg_rx: watch::Receiver<Option<FmConfigView>>,
        activators: Activators,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        Self { datastore, sitrep_rx, inv_rx, cfg_rx, activators, nexus_id }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> FmAnalysisStatus {
        // We shall collect a list of non-fatal errors to report in the
        // activation status, in addition to the outcome. These are errors which
        // did *not* prevent the analysis step from succeeding, but which should
        // be surfaced in the activation status.
        let mut warnings = Vec::new();

        // Snapshot the static known-classes set once, up front, so it's
        // reported in the activation status regardless of which outcome
        // variant fires.
        let known_classes: Vec<String> = fm::diagnosis::known_ereport_classes()
            .iter()
            .map(|s| (*s).to_string())
            .collect();

        let parent_sitrep = self.sitrep_rx.borrow_and_update().clone();
        let parent_sitrep_id = parent_sitrep.as_ref().map(|s| s.1.id());

        let cfg = {
            let cfg_view = self.cfg_rx.borrow_and_update();
            let Some(cfg_view) = cfg_view.as_ref() else {
                // We can't proceed with analysis until the config has been
                // loaded, since otherwise, we won't know if we're at the sitrep
                // limit (or if we've been totally disabled)
                slog::info!(
                    opctx.log,
                    "fault management analysis waiting for the FM config to be \
                     loaded"
                );
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: None,
                    known_classes,
                    outcome: status::Outcome::WaitingForConfig,
                    warnings,
                };
            };

            // Check if the current config has disabled analysis. We do this
            // inside the borrow of the current config view because we would
            // like to be able to log the details of the config override that
            // has turned us off...
            if !cfg_view.config.analysis_enabled.value() {
                match cfg_view.source {
                    FmConfigSource::Default => {
                        slog::error!(
                            opctx.log,
                            "this is weird! the default FM config is not \
                             supposed to disable the analysis task!",
                        );
                    }
                    FmConfigSource::Override {
                        version,
                        ref comment,
                        time_modified,
                    } => {
                        slog::info!(
                            opctx.log,
                            "fault management analysis disabled by FM config \
                             override v{version}";
                            "config_version" => version.get(),
                            "comment" => comment,
                            "time_modified" => %time_modified,
                        );
                    }
                };
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: None,
                    known_classes,
                    outcome: status::Outcome::Disabled(cfg_view.source.clone()),
                    warnings,
                };
            }
            cfg_view.config
        };

        let Some(inv) = self.inv_rx.borrow_and_update().clone() else {
            slog::debug!(
                opctx.log,
                "fault management analysis waiting for inventory to be loaded"
            );
            return FmAnalysisStatus {
                parent_sitrep_id,
                inv_collection_id: None,
                known_classes,
                outcome: status::Outcome::WaitingForInventory,
                warnings,
            };
        };
        let inv_collection_id = inv.id;
        let opctx = opctx.child(
            [
                (
                    "parent_sitrep_id".to_string(),
                    format!("{parent_sitrep_id:?}"),
                ),
                (
                    "inv_collection_id".to_string(),
                    inv_collection_id.to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        // Construct the input builder for the preparation phase. This will fail
        // if the inventory collection is insufficiently new, so we can bail out
        // here if we need to wait for a new collection without having to spawn
        // the input-loading tasks.
        let builder = match Input::builder(parent_sitrep.clone(), inv) {
            Ok(builder) => builder.config(cfg),
            Err(stale) => {
                slog::info!(
                    opctx.log,
                    "fault management analysis: waiting for a newer inventory \
                     to be loaded";
                    "parent_sitrep_inv_id" => %stale.parent_inv_id,
                    "next_inv_min_time_started" =>
                        %stale.next_inv_min_time_started,
                    "input_inv_time_started" => %stale.input_inv_time_started,
                );
                self.activators.inventory_loader.activate();
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    known_classes,
                    outcome: status::Outcome::WaitingForNewerInventory(stale),
                    warnings,
                };
            }
        };

        // Load analysis inputs from the database.
        let PreparationResult { prep_status, prep_result } =
            InputLoader::new(self.datastore.clone())
                .prepare_inputs(&opctx, builder)
                .await;
        let (inputs, input_report) = match prep_result {
            Ok(prepared) => prepared,
            Err(failure) => {
                // Each failure has already been logged where it occurred.
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    known_classes,
                    outcome: status::Outcome::PreparationError {
                        prep_status,
                        failure,
                    },
                    warnings,
                };
            }
        };

        slog::info!(
            opctx.log,
            "fault management analysis input preparation completed in {:?}",
            prep_status.total_duration;
            "new_ereports" => input_report.new_ereport_ids.len(),
            "ereporter_restarts" => input_report.num_ereporter_restarts,
        );

        // Okay, actually run analysis and generate a new sitrep.
        let outcome = self
            .analyze(&opctx, inputs, &input_report, &cfg, &mut warnings)
            .await;

        FmAnalysisStatus {
            parent_sitrep_id,
            inv_collection_id: Some(inv_collection_id),
            known_classes,
            outcome: status::Outcome::RanAnalysis {
                prep_status,
                analysis_status: outcome,
            },
            warnings,
        }
    }
}

impl FmAnalysis {
    async fn analyze(
        &mut self,
        opctx: &OpContext,
        inputs: Input,
        input_report: &InputReport,
        cfg: &FmConfig,
        warnings: &mut Vec<String>,
    ) -> status::AnalysisStatus {
        let start_time = Utc::now();
        let mut sitrep_builder = fm::SitrepBuilder::new(&opctx.log, &inputs);
        let result = fm::diagnosis::analyze(&mut sitrep_builder);
        let end_time = Utc::now();
        let (sitrep, report) = sitrep_builder.build(self.nexus_id, end_time);

        // Did it work?
        if let Err(e) = result {
            let error = InlineErrorChain::new(&*e);
            slog::error!(
                &opctx.log,
                "fault management analysis failed";
                &error,
            );
            return status::AnalysisStatus {
                start_time,
                end_time,
                capacity: None,
                outcome: status::AnalysisOutcome::Error(e.to_string()),
            };
        }

        // Is the new sitrep identical to the parent sitrep? If so, we're done.
        if let Some(parent) = inputs.parent_sitrep() {
            if parent.compare_state() == sitrep {
                slog::info!(
                    &opctx.log,
                    "fault management analysis produced no changes from the \
                     current sitrep"
                );
                return status::AnalysisStatus {
                    start_time,
                    end_time,
                    capacity: None,
                    outcome: status::AnalysisOutcome::Unchanged,
                };
            }
        }

        // Before writing the new sitrep to the database, check whether there is
        // capacity to write a new sitrep.
        //
        // Readers who have read the similar-looking code in the
        // `blueprint_planner` background task may note that we are doing the
        // check for whether the sitrep has changed and the check for whether
        // the limit is reached in the opposite order as the Reconfigurator.
        // That is by design. In the Reconfigurator case, there isn't an
        // automated process that's automatically deleting old blueprints, so
        // hitting the limit is a condition that requires manual human
        // intervention. Therefore, it's important for them to be able to warn
        // about it as soon as it's hit. For us, it's much less important to
        // surface, and if we've hit the limit, we will probably go back down
        // below it soon when history pruning frees up capacity. Therefore,
        // there's no sense doing the fairly expensive scan to check if the
        // limit has been hit when we're not actually planning on committing the
        // new sitrep anyway.
        let capacity =
            match self.check_sitrep_limit(&opctx, cfg, warnings).await {
                Ok(capacity) => Some(capacity),
                Err(outcome) => {
                    return status::AnalysisStatus {
                        start_time,
                        end_time,
                        capacity: None,
                        outcome,
                    };
                }
            };

        let sitrep_id = sitrep.id();

        // Serialize the human-readable analysis report so they can be stored
        // alongside the sitrep for later inspection via `omdb`. This is purely
        // diagnostic; if serialization somehow fails, we log it and still
        // commit the sitrep rather than blocking fault management on it.
        let analysis_report =
            match nexus_db_model::fm::SitrepAnalysisReport::new(
                input_report,
                &report,
                self.datastore.git_version(),
            ) {
                Ok(analysis_report) => Some(analysis_report),
                Err(e) => {
                    const MESSAGE: &str = "analysis report could not be \
                        serialized, the sitrep will be committed without it";
                    let error = InlineErrorChain::new(&*e);
                    slog::warn!(
                        &opctx.log,
                        "{MESSAGE}";
                        &error,
                    );
                    warnings.push(format!("{MESSAGE}: {error}"));
                    None
                }
            };

        match self
            .datastore
            .fm_sitrep_insert(opctx, sitrep, analysis_report)
            .await
        {
            Ok(()) => {
                slog::info!(&opctx.log, "updated the current sitrep!");
                // If we committed a new sitrep, we ought to go ahead and load it
                // now...
                self.activators.sitrep_loader.activate();
                status::AnalysisStatus {
                    start_time,
                    end_time,
                    capacity,
                    outcome: status::AnalysisOutcome::Committed { sitrep_id },
                }
            }
            Err(datastore::fm::InsertSitrepError::ParentNotCurrent(_)) => {
                slog::info!(
                    &opctx.log,
                    "new sitrep was not committed as the parent sitrep was \
                     out of date";
                );
                // We are behind, activate the sitrep loader to try and catch up!
                self.activators.sitrep_loader.activate();
                // Also, we should probably clean up after ourselves...
                self.activators.sitrep_gc.activate();

                status::AnalysisStatus {
                    start_time,
                    end_time,
                    capacity,
                    outcome: status::AnalysisOutcome::NotCommitted {
                        sitrep_id,
                    },
                }
            }
            Err(datastore::fm::InsertSitrepError::Other(e)) => {
                let error = InlineErrorChain::new(&e);
                slog::error!(
                    &opctx.log,
                    "failed to insert sitrep";
                    &error,
                );
                status::AnalysisStatus {
                    start_time,
                    end_time,
                    capacity,
                    outcome: status::AnalysisOutcome::CommitFailed {
                        sitrep_id,
                        error: e.to_string(),
                    },
                }
            }
        }
    }

    async fn check_sitrep_limit(
        &self,
        opctx: &OpContext,
        cfg: &FmConfig,
        warnings: &mut Vec<String>,
    ) -> Result<status::SitrepCapacity, status::AnalysisOutcome> {
        let limit = cfg.sitrep_limit.value();
        let count = match self
            .datastore
            .fm_sitrep_check_limit_reached(&opctx, u64::from(limit.get()))
            .await
        {
            Ok(db::IsLimitReached::Yes) => {
                error!(
                    &opctx.log,
                    "sitrep capacity is at or above the limit, a new sitrep \
                     will not be written";
                     "limit" => limit.get(),
                );
                self.activators.reclaim_capacity();
                return Err(status::AnalysisOutcome::LimitReached { limit });
            }
            Ok(db::IsLimitReached::No { count }) => count,
            Err(error) => {
                const MSG: &str = "failed to check sitrep limit";
                let error = InlineErrorChain::new(&error);
                error!(&opctx.log, "{MSG}"; &error);
                return Err(status::AnalysisOutcome::Error(format!(
                    "{MSG}: {error}"
                )));
            }
        };

        let capacity = status::SitrepCapacity { count, limit };
        let usage_percent = capacity.usage_percent();
        match usage_percent {
            0..80 => {
                debug!(
                    &opctx.log,
                    "sitrep count under limit, proceeding with analysis";
                    "limit" => limit.get(),
                    "count" => count,
                    "usage_percent" => usage_percent,
                );
            }
            // With the default history limit and max sitrep limit, we start
            // pruning the history table at 80% of history, so this is just
            // operational, not a warning.
            80..95 => {
                info!(
                    &opctx.log,
                    "sitrep count above 80% of limit, proceeding with analysis \
                     and activating GC (will stop analysis if limit is reached)";
                    "limit" => limit.get(),
                    "count" => count,
                    "usage_percent" => usage_percent,
                );
                self.activators.reclaim_capacity();
            }
            // At 95% of the limit, this is where I might start to worry that
            // the GC tasks isn't running or something!
            95.. => {
                warn!(
                    &opctx.log,
                    "sitrep count above 95% of limit, proceeding with analysis \
                     and activating GC (will stop analysis if limit is reached)";
                    "limit" => limit.get(),
                    "count" => count,
                    "usage_percent" => usage_percent,
                );
                warnings.push(format!(
                    "sitrep count ({count}) at {usage_percent}% of limit \
                     ({limit})",
                ));
                self.activators.reclaim_capacity();
            }
        }

        Ok(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::fm::SitrepModel;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::fm::InventoryStale;
    use nexus_types::fm::Setting;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use nexus_types::fm::SitrepVersion;
    use omicron_generation_kinds::SitrepActiveProblemGeneration;
    use omicron_generation_kinds::SitrepAlertGeneration;
    use omicron_generation_kinds::SitrepSupportBundleGeneration;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SitrepUuid;
    use std::num::NonZeroU32;

    /// Returns an FM config view with the given sitrep limit and history
    /// pruning threshold.
    ///
    /// The config is constructed directly rather than validated through
    /// `FmConfigParam`, so tests may use limits smaller than
    /// `FmConfig::MIN_SITREP_LIMIT`.
    fn test_config(
        sitrep_limit: NonZeroU32,
        history_pruning_threshold: NonZeroU32,
    ) -> FmConfigView {
        assert!(
            history_pruning_threshold < sitrep_limit,
            "test configs must uphold the threshold < limit invariant"
        );
        let config = nexus_types::fm::FmConfig {
            analysis_enabled: Setting::new(true),
            sitrep_limit: Setting::new(sitrep_limit),
            history_pruning_threshold: Setting::new(history_pruning_threshold),
            certificate_expiry_warning_days: Setting::Default,
        };
        FmConfigView { config, source: Default::default() }
    }

    /// Returns a config watch receiver carrying the given config, as though
    /// the config loader task had published it.
    fn config_rx(view: FmConfigView) -> watch::Receiver<Option<FmConfigView>> {
        // The sender is dropped here; watch receivers continue to yield the
        // last-sent value after the channel closes.
        watch::channel(Some(view)).1
    }

    fn activators() -> Activators {
        let a = Activators {
            inventory_loader: Activator::new(),
            sitrep_loader: Activator::new(),
            sitrep_gc: Activator::new(),
            sitrep_history_pruner: Activator::new(),
        };
        a.inventory_loader.mark_wired_up().unwrap();
        a.sitrep_loader.mark_wired_up().unwrap();
        a.sitrep_gc.mark_wired_up().unwrap();
        a.sitrep_history_pruner.mark_wired_up().unwrap();
        a
    }

    /// Create a `CurrentSitrep` as though it was produced from the given
    /// inventory collection.
    fn make_current_sitrep(inv: &inventory::Collection) -> CurrentSitrep {
        let id = SitrepUuid::new_v4();
        Arc::new((
            SitrepVersion { id, version: 1, time_made_current: Utc::now() },
            Sitrep {
                metadata: SitrepMetadata {
                    id,
                    parent_sitrep_id: None,
                    inv_collection_id: inv.id,
                    next_inv_min_time_started: inv.time_done,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep".to_string(),
                    time_created: Utc::now(),
                    alert_generation: SitrepAlertGeneration::new(),
                    support_bundle_generation:
                        SitrepSupportBundleGeneration::new(),
                    active_problem_generation:
                        SitrepActiveProblemGeneration::new(),
                },
                cases: Default::default(),
                ereports_by_id: Default::default(),
                active_problems: Default::default(),
            },
        ))
    }

    #[tokio::test]
    async fn test_inventory_staleness_check() {
        let logctx = dev::test_setup_log("test_inventory_staleness_check");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Build three inventory collections. We will manually set the
        // timestamps on these collections after building them, as
        // `CollectionBuilder` produces timestamps using `Utc::now()`.
        //
        // This is the oldest one:
        let older = {
            let mut older = CollectionBuilder::new("test (oldest)").build();
            older.time_done += chrono::Duration::seconds(1);
            Arc::new(older)
        };

        // This one started after `older` finished:
        let newer = {
            let mut newer = CollectionBuilder::new("test (new)").build();
            newer.time_started = older.time_done + chrono::Duration::seconds(1);
            newer.time_done = newer.time_started + chrono::Duration::seconds(1);
            Arc::new(newer)
        };

        // This one started after `older` started, but before `older` finished:
        let overlapping = {
            let mut overlapping =
                CollectionBuilder::new("test (overlapping)").build();
            overlapping.time_started =
                older.time_started + chrono::Duration::milliseconds(500);
            overlapping.time_done =
                older.time_done + chrono::Duration::seconds(1);
            Arc::new(overlapping)
        };

        // If our "currently loaded" inventory started before the parent
        // sitrep's `next_inv_min_time_started`, we should refuse to run
        // analysis and return `WaitingForNewerInventory`.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(&newer)));
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                config_rx(Default::default()),
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            assert_eq!(
                result.outcome,
                status::Outcome::WaitingForNewerInventory(InventoryStale {
                    parent_inv_id: newer.id,
                    next_inv_min_time_started: newer.time_done,
                    input_inv_time_started: older.time_started,
                }),
            );
        }

        // If the loaded inventory collection started at or after the parent
        // sitrep's `next_inv_min_time_started`, analysis should proceed.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(&older)));
            let (_inv_tx, inv_rx) = watch::channel(Some(newer.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                config_rx(Default::default()),
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(newer.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::WaitingForNewerInventory { .. }
                ),
                "expected analysis to proceed, got: {:?}",
                result.outcome,
            );
        }

        // When there is no parent sitrep, the staleness check is
        // skipped entirely and analysis proceeds.
        {
            let (_sitrep_tx, sitrep_rx) = watch::channel(None);
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                config_rx(Default::default()),
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::WaitingForNewerInventory { .. }
                ),
                "expected analysis to proceed with no parent sitrep, \
                 got: {:?}",
                result.outcome,
            );
        }

        // When the parent sitrep's inventory collection and the currently
        // loaded collection are the same, we should still proceed with FM
        // analysis (i.e. if we were triggered by ereports rather than an
        // inventory change...)
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(&older)));
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                config_rx(Default::default()),
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::WaitingForNewerInventory { .. }
                ),
                "expected analysis to proceed with same inventory \
                 collection ID, got: {:?}",
                result.outcome,
            );
        }

        // If our "currently loaded" inventory collection overlaps with the
        // parent sitrep's inventory collection --- i.e. it started while the
        // parent's collection was still in progress --- it is NOT strictly
        // newer, so we should refuse to run analysis.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(&older)));
            let (_inv_tx, inv_rx) = watch::channel(Some(overlapping.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                config_rx(Default::default()),
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(overlapping.id));
            assert_eq!(
                result.outcome,
                status::Outcome::WaitingForNewerInventory(InventoryStale {
                    parent_inv_id: older.id,
                    next_inv_min_time_started: older.time_done,
                    input_inv_time_started: overlapping.time_started,
                }),
                "overlapping collections must wait for a newer inventory",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Exercises `check_sitrep_limit` at each capacity tier:
    ///
    /// - below 80% of the limit, the capacity is reported and nothing else
    ///   happens;
    /// - at or above 80% (but below 95%), the reclamation tasks are poked to
    ///   free up space, but no warning is recorded: steady-state pruning
    ///   keeps the history at 80% of the sitrep limit, so this is normal
    ///   operation;
    /// - at or above 95%, the reclamation tasks are poked *and* a warning is
    ///   recorded, as this suggests reclamation isn't keeping up;
    /// - at the limit, the check fails with `LimitReached` and the
    ///   reclamation tasks are poked.
    #[tokio::test]
    async fn test_check_sitrep_limit() {
        let logctx = dev::test_setup_log("test_check_sitrep_limit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const LIMIT: NonZeroU32 = NonZeroU32::new(20).unwrap();
        // The history pruning threshold doesn't participate in the capacity
        // check; any valid value will do here.
        let cfg = test_config(LIMIT, NonZeroU32::new(16).unwrap());
        let (_sitrep_tx, sitrep_rx) = watch::channel(None);
        let (_inv_tx, inv_rx) = watch::channel(None);
        let acts = activators();
        let task = FmAnalysis::new(
            datastore.clone(),
            sitrep_rx,
            inv_rx,
            config_rx(cfg.clone()),
            acts.clone(),
            OmicronZoneUuid::new_v4(),
        );

        let mut model = SitrepModel::new(datastore.clone());

        // 10 sitreps: 50% of the limit. No warning, no GC activation.
        model.insert_history(opctx, 10).await;
        let mut warnings = Vec::new();
        let result =
            task.check_sitrep_limit(opctx, &cfg.config, &mut warnings).await;
        assert_eq!(
            result,
            Ok(status::SitrepCapacity {
                count: model.sitrep_count(),
                limit: LIMIT
            })
        );
        assert_eq!(warnings, Vec::<String>::new());
        acts.sitrep_gc.assert_not_activated(
            "GC should not be activated at 50% of the sitrep limit",
        );
        acts.sitrep_history_pruner.assert_not_activated(
            "history pruning should not be activated at 50% of the sitrep \
             limit",
        );

        // 15 sitreps: 75% of the limit, just below the GC-activation tier.
        // Still no warning and no GC activation.
        model.insert_history(opctx, 5).await;
        let mut warnings = Vec::new();
        let result =
            task.check_sitrep_limit(opctx, &cfg.config, &mut warnings).await;
        assert_eq!(
            result,
            Ok(status::SitrepCapacity {
                count: model.sitrep_count(),
                limit: LIMIT
            })
        );
        assert_eq!(warnings, Vec::<String>::new());
        acts.sitrep_gc.assert_not_activated(
            "GC should not be activated at 75% of the sitrep limit",
        );
        acts.sitrep_history_pruner.assert_not_activated(
            "history pruning should not be activated at 75% of the sitrep \
             limit",
        );

        // 16 sitreps: 80% of the limit. The GC task is activated to reclaim
        // capacity. This is roughly the expected steady state of the system, if
        // the analysis and GC tasks are running mostly in sync.
        model.insert_history(opctx, 1).await;
        let mut warnings = Vec::new();
        let result =
            task.check_sitrep_limit(opctx, &cfg.config, &mut warnings).await;
        assert_eq!(
            result,
            Ok(status::SitrepCapacity {
                count: model.sitrep_count(),
                limit: LIMIT
            })
        );
        acts.sitrep_gc.assert_activated(
            "GC should be activated at 80% of the sitrep limit",
        );
        acts.sitrep_history_pruner.assert_activated(
            "history pruning should be activated at 80% of the sitrep limit",
        );

        // 19 sitreps: 95% of the limit. Now the check records a warning, since
        // GC doesn't seem to be keeping up with sitrep generation.
        model.insert_history(opctx, 3).await;
        let mut warnings = Vec::new();
        let result =
            task.check_sitrep_limit(opctx, &cfg.config, &mut warnings).await;
        assert_eq!(
            result,
            Ok(status::SitrepCapacity {
                count: model.sitrep_count(),
                limit: LIMIT
            })
        );
        assert_eq!(
            warnings.len(),
            1,
            "expected a capacity warning at 95% of the limit, got: \
             {warnings:?}"
        );
        acts.sitrep_gc.assert_activated(
            "GC should be activated at 95% of the sitrep limit",
        );
        acts.sitrep_history_pruner.assert_activated(
            "history pruning should be activated at 95% of the sitrep limit",
        );

        // 20 sitreps: at the limit. The check fails, and the GC task is
        // activated. Note that the last sitrep is an orphan that was never made
        // current. Orphans also count against the total number of sitreps in
        // the database, until GC sweeps them up.
        model.insert_orphan(opctx, None).await;
        assert_eq!(model.sitrep_count(), u64::from(LIMIT.get()));
        let mut warnings = Vec::new();
        let result =
            task.check_sitrep_limit(opctx, &cfg.config, &mut warnings).await;
        assert_eq!(
            result,
            Err(status::AnalysisOutcome::LimitReached { limit: LIMIT })
        );
        assert_eq!(warnings, Vec::<String>::new());
        acts.sitrep_gc.assert_activated(
            "GC should be activated when the sitrep limit is reached",
        );
        acts.sitrep_history_pruner.assert_activated(
            "history pruning should be activated when the sitrep limit is reached",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Exercises the sitrep limit end-to-end through `actually_activate`:
    /// below the limit, analysis commits a new sitrep and reports the
    /// capacity; once the `fm_sitrep` table is at the limit, analysis still
    /// runs but refuses to write its sitrep, reporting `LimitReached` and
    /// activating the GC task instead.
    #[tokio::test]
    async fn test_analysis_respects_sitrep_limit() {
        let logctx = dev::test_setup_log("test_analysis_respects_sitrep_limit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const LIMIT: NonZeroU32 = NonZeroU32::new(4).unwrap();
        let inv = Arc::new(CollectionBuilder::new("test").build());
        let (_sitrep_tx, sitrep_rx) = watch::channel(None);
        let (_inv_tx, inv_rx) = watch::channel(Some(inv.clone()));
        let acts = activators();
        let history_pruning_threshold =
            NonZeroU32::new(LIMIT.get() - 1).unwrap();
        let mut task = FmAnalysis::new(
            datastore.clone(),
            sitrep_rx,
            inv_rx,
            config_rx(test_config(LIMIT, history_pruning_threshold)),
            acts.clone(),
            OmicronZoneUuid::new_v4(),
        );

        let mut model = SitrepModel::new(datastore.clone());

        // With an empty database, analysis should commit the first-ever
        // sitrep, reporting a capacity of 0 out of `LIMIT`.
        let result = task.actually_activate(opctx).await;
        let sitrep_id = match result.outcome {
            status::Outcome::RanAnalysis { analysis_status, .. } => {
                assert_eq!(
                    analysis_status.capacity,
                    Some(status::SitrepCapacity { count: 0, limit: LIMIT })
                );
                match analysis_status.outcome {
                    status::AnalysisOutcome::Committed { sitrep_id } => {
                        sitrep_id
                    }
                    other => {
                        panic!("expected the sitrep to be committed: {other:?}")
                    }
                }
            }
            other => panic!("expected analysis to run, got: {other:?}"),
        };
        acts.sitrep_gc.assert_not_activated(
            "GC should not be activated below the sitrep limit",
        );
        acts.sitrep_history_pruner.assert_not_activated(
            "history pruning should not be activated below the sitrep limit",
        );
        // Committing a sitrep pokes the loader; consume that activation so
        // the assertion below observes only the second activation's behavior.
        acts.sitrep_loader.assert_activated(
            "the sitrep loader should be activated after a commit",
        );
        model.record_committed(sitrep_id);

        // Fill the `fm_sitrep` table up to the limit: two more live sitreps,
        // plus an orphan, which counts against the limit even though it was
        // never made current.
        model.insert_history(opctx, 2).await;
        model.insert_orphan(opctx, None).await;
        assert_eq!(model.sitrep_count(), u64::from(LIMIT.get()));

        // Now analysis should run, but refuse to write its sitrep.
        let result = task.actually_activate(opctx).await;
        match result.outcome {
            status::Outcome::RanAnalysis { analysis_status, .. } => {
                assert_eq!(
                    analysis_status.outcome,
                    status::AnalysisOutcome::LimitReached { limit: LIMIT }
                );
                assert_eq!(analysis_status.capacity, None);
            }
            other => panic!("expected analysis to run, got: {other:?}"),
        }
        acts.sitrep_gc.assert_activated(
            "GC should be activated when the sitrep limit is reached",
        );
        acts.sitrep_history_pruner.assert_activated(
            "history pruning should be activated when the sitrep limit is reached",
        );
        acts.sitrep_loader.assert_not_activated(
            "the sitrep loader should not be activated when no sitrep was \
             committed",
        );

        // No new sitrep row should have been written, and the orphan should
        // still be present --- the GC task was only poked, not run.
        model.assert_matches(opctx).await;

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
