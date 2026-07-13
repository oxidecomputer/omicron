// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use anyhow::Context;
use chrono::Utc;
use fm::analysis_input::InvalidInputs;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_db_model::DbMetadataNexusState;
use nexus_db_model::PhysicalDiskPolicy;
use nexus_db_model::SagaState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::pagination::Paginator;
use nexus_fm as fm;
use nexus_types::in_service_disk::InServiceDisk;
use nexus_types::internal_api::background::FmAnalysisStatus;
use nexus_types::internal_api::background::fm_analysis as status;
use nexus_types::inventory;
use nexus_types::observed_saga::{
    ObservedSaga, ObservedSagaState, SagaOwnerState,
};
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct FmAnalysis {
    datastore: Arc<DataStore>,
    sitrep_rx: watch::Receiver<Option<CurrentSitrep>>,
    inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
    activators: Activators,
    nexus_id: OmicronZoneUuid,
    analysis_enabled: bool,
}

/// This is just because I don't like it when a constructor takes multiple
/// positional arguments of the same type...
#[derive(Clone)]
pub struct Activators {
    pub inventory_loader: Activator,
    pub sitrep_loader: Activator,
    pub sitrep_gc: Activator,
}

impl BackgroundTask for FmAnalysis {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = if self.analysis_enabled {
                self.actually_activate(opctx).await
            } else {
                slog::info!(
                    opctx.log,
                    "fault management analysis explicitly disabled by config",
                );
                let known_classes: Vec<String> =
                    fm::diagnosis::known_ereport_classes()
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect();
                FmAnalysisStatus {
                    parent_sitrep_id: None,
                    inv_collection_id: None,
                    known_classes,
                    outcome: status::Outcome::Disabled,
                    warnings: Vec::new(),
                }
            };
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
        activators: Activators,
        nexus_id: OmicronZoneUuid,
        analysis_enabled: bool,
    ) -> Self {
        Self {
            datastore,
            sitrep_rx,
            inv_rx,
            activators,
            nexus_id,
            analysis_enabled,
        }
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

        // Prepare analysis inputs.
        let (inputs, prep_status) = match self
            .prepare_inputs(&opctx, parent_sitrep, inv)
            .await
        {
            Ok(inputs) => inputs,
            Err(PreparationError::Other(err)) => {
                let error = InlineErrorChain::new(&*err);
                slog::error!(
                    opctx.log,
                    "fault management analysis preparation failed";
                    &error,
                );
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    known_classes,
                    outcome: status::Outcome::PreparationError(
                        error.to_string(),
                    ),
                    warnings,
                };
            }
            Err(PreparationError::InvalidInputs(
                InvalidInputs::InventoryStale {
                    parent_inv_id,
                    next_inv_min_time_started,
                    input_inv_time_started,
                },
            )) => {
                slog::info!(
                    opctx.log,
                    "fault management analysis: waiting for a newer inventory \
                     to be loaded";
                    "parent_sitrep_inv_id" => %parent_inv_id,
                    "next_inv_min_time_started" => %next_inv_min_time_started,
                    "input_inv_time_started" => %input_inv_time_started,
                );
                self.activators.inventory_loader.activate();
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    known_classes,
                    outcome: status::Outcome::WaitingForNewerInventory {
                        parent_inv_id,
                        next_inv_min_time_started,
                        input_inv_time_started,
                    },
                    warnings,
                };
            }
        };

        // Okay, actually run analysis and generate a new sitrep.
        let outcome = self
            .analyze(&opctx, inputs, &prep_status.report, &mut warnings)
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

    async fn prepare_inputs(
        &mut self,
        opctx: &OpContext,
        parent_sitrep: Option<CurrentSitrep>,
        inv: Arc<inventory::Collection>,
    ) -> Result<
        (fm::analysis_input::Input, status::PreparationStatus),
        PreparationError,
    > {
        let mut warnings = Vec::new();

        let in_service_disks =
            Arc::new(self.load_in_service_disks(opctx, &mut warnings).await?);

        let observed_sagas =
            Arc::new(self.prepare_observed_sagas(opctx).await?);

        let mut builder = fm::analysis_input::Input::builder(
            parent_sitrep.clone(),
            inv,
            in_service_disks,
            observed_sagas,
        )?;
        self.load_ereporter_restarts(opctx, &mut builder)
            .await
            .context("failed to load ereporter restarts")?;
        self.load_new_ereports(opctx, &mut builder, &mut warnings)
            .await
            .context("failed to load new ereports")?;
        self.load_existing_alert_markers(
            opctx,
            parent_sitrep.as_ref().map(|s| &s.1),
            &mut builder,
        )
        .await
        .context("failed to load existing alert markers")?;
        self.load_existing_support_bundle_markers(
            opctx,
            parent_sitrep.as_ref().map(|s| &s.1),
            &mut builder,
        )
        .await
        .context("failed to load existing support bundle markers")?;

        let (input, report) = builder.build();
        Ok((input, status::PreparationStatus { warnings, report }))
    }

    /// Load all in-service control plane disks, projected down to FM's
    /// [`InServiceDisk`] view.
    async fn load_in_service_disks(
        &self,
        opctx: &OpContext,
        warnings: &mut Vec<String>,
    ) -> anyhow::Result<IdOrdMap<InServiceDisk>> {
        // Load all external (U.2) zpools and project them down to FM's
        // `InServiceDisk` view, filtering on `disk_policy = in_service` and a
        // live (non-soft-deleted) physical_disk row. M.2 disks are not
        // represented as control plane disks today, so the U.2-only filter
        // on the underlying query matches reality.
        //
        // See `nexus_types::in_service_disk` for why FM reads the executed
        // DB view rather than the target blueprint.
        let zpools_and_disks = self
            .datastore
            .zpool_list_all_external_batched(opctx)
            .await
            .context("failed to load in-service control plane disks")?;
        let mut in_service_disks = IdOrdMap::new();
        for (zpool, disk) in zpools_and_disks {
            if disk.time_deleted().is_some()
                || disk.disk_policy != PhysicalDiskPolicy::InService
            {
                continue;
            }
            let physical_disk_id = disk.id();
            let zpool_id = zpool.id();
            if in_service_disks
                .insert_unique(InServiceDisk {
                    physical_disk_id,
                    zpool_id,
                    sled_id: disk.sled_id.into(),
                    vendor: disk.vendor,
                    serial: disk.serial,
                    model: disk.model,
                    variant: disk.variant.into(),
                })
                .is_err()
            {
                // One live zpool per disk is a code-maintained invariant,
                // not a schema constraint. Tolerate a violation rather than
                // panicking the analysis task: keep the first zpool seen
                // for the disk.
                slog::warn!(
                    &opctx.log,
                    "multiple live zpools reference the same physical disk";
                    "physical_disk_id" => %physical_disk_id,
                    "zpool_id" => %zpool_id,
                );
                warnings.push(format!(
                    "multiple live zpools reference physical disk \
                     {physical_disk_id} (kept first seen, ignored zpool \
                     {zpool_id})"
                ));
            }
        }
        Ok(in_service_disks)
    }

    /// Build the saga diagnosis engine's input: every non-terminal saga,
    /// annotated with the timestamp of its latest node event (the progress
    /// signal) and the state of its owning Nexus.
    async fn prepare_observed_sagas(
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<IdOrdMap<ObservedSaga>> {
        use std::collections::BTreeMap;

        // All unfinished (running, unwinding, or abandoned) sagas. Completed
        // sagas are excluded; a parent case whose saga is absent from this
        // set is closed by the engine.
        let sagas = self
            .datastore
            .saga_list_unfinished_batched(opctx)
            .await
            .context("failed to list unfinished sagas")?;

        // Latest node-event time per saga: the last durably-recorded step.
        let saga_ids: Vec<_> = sagas.iter().map(|s| s.id).collect();
        let last_event_times: BTreeMap<
            steno::SagaId,
            Option<chrono::DateTime<Utc>>,
        > = self
            .datastore
            .saga_latest_node_event_times(opctx, &saga_ids)
            .await
            .context("failed to load saga node-event times")?
            .into_iter()
            .map(|(id, t)| (id.0, t))
            .collect();

        // Classify each owning Nexus (current_sec) against db_metadata_nexus.
        let nexus_states: BTreeMap<OmicronZoneUuid, DbMetadataNexusState> =
            self.datastore
                .get_db_metadata_nexus_in_state(
                    opctx,
                    vec![
                        DbMetadataNexusState::Active,
                        DbMetadataNexusState::NotYet,
                        DbMetadataNexusState::Quiesced,
                    ],
                )
                .await
                .context("failed to load db_metadata_nexus records")?
                .into_iter()
                .map(|n| (n.nexus_id(), n.state()))
                .collect();

        let mut observed = IdOrdMap::new();
        for saga in sagas {
            let saga_state = match saga.saga_state {
                SagaState::Running => ObservedSagaState::Running,
                SagaState::Unwinding => ObservedSagaState::Unwinding,
                SagaState::Abandoned => ObservedSagaState::Abandoned,
                // The query filters to unfinished states; defend anyway.
                SagaState::Done => continue,
            };
            let current_sec = saga
                .current_sec
                .map(|sec| OmicronZoneUuid::from_untyped_uuid(sec.0));
            let owner_state =
                current_sec.map(|sec_id| match nexus_states.get(&sec_id) {
                    Some(DbMetadataNexusState::Active) => {
                        SagaOwnerState::Active
                    }
                    Some(DbMetadataNexusState::NotYet) => {
                        SagaOwnerState::NotYet
                    }
                    Some(DbMetadataNexusState::Quiesced) => {
                        SagaOwnerState::Quiesced
                    }
                    None => SagaOwnerState::Absent,
                });
            let last_event_time =
                last_event_times.get(&saga.id.0).copied().flatten();
            observed
                .insert_unique(ObservedSaga {
                    saga_id: saga.id.0,
                    saga_name: saga.name,
                    saga_state,
                    time_created: saga.time_created,
                    current_sec,
                    last_event_time,
                    owner_state,
                })
                .expect(
                    "saga.id is a primary key, so duplicates are impossible",
                );
        }
        Ok(observed)
    }

    async fn load_ereporter_restarts(
        &mut self,
        opctx: &OpContext,
        builder: &mut fm::analysis_input::Builder,
    ) -> anyhow::Result<()> {
        let mut nbatches = 0;
        let mut paginator = Paginator::new(
            nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            nbatches += 1;
            let batch = self
                .datastore
                .ereporter_restart_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|e| e.id().into_untyped_uuid());
            builder.add_ereporter_restarts(batch);
        }

        slog::debug!(
            opctx.log,
            "loaded {} ereporter restarts (in {nbatches} batches)",
            builder.ereporter_restarts().len(),
        );

        Ok(())
    }

    async fn load_new_ereports(
        &mut self,
        opctx: &OpContext,
        builder: &mut fm::analysis_input::Builder,
        warnings: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        // Only surface ereports a diagnosis engine will consume.
        let classes = fm::diagnosis::known_ereport_classes();
        let mut paginator = Paginator::new(
            nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let prev_total = builder.num_ereports();
            let batch = self
                .datastore
                .ereports_list_unmarked(opctx, classes, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|e| {
                (e.restart_id.into_untyped_uuid(), e.ena)
            });
            let loaded = batch.len();

            let mut invalid = 0;
            for ereport in batch {
                let ereport = match fm::Ereport::try_from(ereport) {
                    Ok(ereport) => ereport,
                    Err(e) => {
                        invalid += 1;
                        warnings.push(e.to_string());
                        continue;
                    }
                };

                // Check if this is a reporter we know about, and issue a
                // warning if it is not.
                let id = ereport.id;
                if !builder.ereporter_restarts().contains_key(&id.restart_id) {
                    let msg = format!(
                        "ereport {id} has a restart ID not contained in the \
                         `ereporter_restart` table"
                    );
                    slog::warn!(&opctx.log, "{msg}");
                    warnings.push(msg);
                }
                builder.add_unmarked_ereports(std::iter::once(ereport));
            }

            let total = builder.num_ereports();
            let new = total - prev_total;
            if invalid > 0 {
                slog::warn!(
                    &opctx.log,
                    "loaded {loaded} ereports, {new} new, {invalid} invalid"
                );
            } else {
                slog::debug!(&opctx.log, "loaded {loaded} ereports, {new} new");
            }
        }

        Ok(())
    }

    async fn load_existing_alert_markers(
        &mut self,
        opctx: &OpContext,
        parent: Option<&nexus_types::fm::Sitrep>,
        builder: &mut fm::analysis_input::Builder,
    ) -> anyhow::Result<()> {
        let Some(parent) = parent else {
            // No parent sitrep, so no closed cases, so nothing to look up.
            return Ok(());
        };
        let candidate_ids: Vec<AlertUuid> = parent
            .cases
            .iter()
            .filter(|c| !c.is_open())
            .flat_map(|c| c.alerts_requested.iter().map(|r| r.id))
            .collect();
        if candidate_ids.is_empty() {
            return Ok(());
        }
        let marked = self
            .datastore
            .fm_rendezvous_existing_alert_markers(opctx, &candidate_ids)
            .await
            .context("failed to look up alert marker existence")?;
        builder.add_marked_alert_requests(marked);
        Ok(())
    }

    async fn load_existing_support_bundle_markers(
        &mut self,
        opctx: &OpContext,
        parent: Option<&nexus_types::fm::Sitrep>,
        builder: &mut fm::analysis_input::Builder,
    ) -> anyhow::Result<()> {
        let Some(parent) = parent else {
            // No parent sitrep, so no closed cases, so nothing to look up.
            return Ok(());
        };
        let candidate_ids: Vec<SupportBundleUuid> = parent
            .cases
            .iter()
            .filter(|c| !c.is_open())
            .flat_map(|c| c.support_bundles_requested.iter().map(|r| r.id))
            .collect();
        if candidate_ids.is_empty() {
            return Ok(());
        }
        let marked = self
            .datastore
            .fm_rendezvous_existing_support_bundle_markers(
                opctx,
                &candidate_ids,
            )
            .await
            .context("failed to look up support bundle marker existence")?;
        builder.add_marked_support_bundle_requests(marked);
        Ok(())
    }

    async fn analyze(
        &mut self,
        opctx: &OpContext,
        inputs: fm::analysis_input::Input,
        input_report: &nexus_types::fm::analysis_reports::InputReport,
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
                report,
                outcome: status::AnalysisOutcome::Error(e.to_string()),
            };
        }

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
                    report,
                    outcome: status::AnalysisOutcome::Unchanged,
                };
            }
        }

        let sitrep_id = sitrep.id();

        // Serialize the human-readable analysis report so they can be stored
        // alongside the sitrep for later inspection via `omdb`. This is purely
        // diagnostic; if serialization somehow fails, we log it and still
        // commit the sitrep rather than blocking fault management on it.
        let analysis_report =
            match nexus_db_model::fm::SitrepAnalysisReport::new(
                input_report,
                &report,
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
                    report,
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
                    report,
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
                    report,
                    outcome: status::AnalysisOutcome::CommitFailed {
                        sitrep_id,
                        error: e.to_string(),
                    },
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum PreparationError {
    #[error(transparent)]
    InvalidInputs(#[from] InvalidInputs),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::alert::AlertClass;
    use nexus_types::fm::Case;
    use nexus_types::fm::DiagnosisEngineKind;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use nexus_types::fm::SitrepVersion;
    use nexus_types::fm::case;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use std::collections::BTreeSet;

    const ANALYSIS_ENABLED: bool = true;

    fn activators() -> Activators {
        let a = Activators {
            inventory_loader: Activator::new(),
            sitrep_loader: Activator::new(),
            sitrep_gc: Activator::new(),
        };
        a.inventory_loader.mark_wired_up().unwrap();
        a.sitrep_loader.mark_wired_up().unwrap();
        a.sitrep_gc.mark_wired_up().unwrap();
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases: Default::default(),
                ereports_by_id: Default::default(),
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
                activators(),
                OmicronZoneUuid::new_v4(),
                ANALYSIS_ENABLED,
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            match &result.outcome {
                status::Outcome::WaitingForNewerInventory {
                    parent_inv_id,
                    next_inv_min_time_started,
                    input_inv_time_started,
                } => {
                    assert_eq!(*parent_inv_id, newer.id);
                    assert_eq!(*next_inv_min_time_started, newer.time_done,);
                    assert_eq!(*input_inv_time_started, older.time_started,);
                }
                other => {
                    panic!("expected WaitingForNewerInventory, got {other:?}")
                }
            }
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
                activators(),
                OmicronZoneUuid::new_v4(),
                ANALYSIS_ENABLED,
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
                activators(),
                OmicronZoneUuid::new_v4(),
                ANALYSIS_ENABLED,
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
                activators(),
                OmicronZoneUuid::new_v4(),
                ANALYSIS_ENABLED,
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
                activators(),
                OmicronZoneUuid::new_v4(),
                ANALYSIS_ENABLED,
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(overlapping.id));
            match &result.outcome {
                status::Outcome::WaitingForNewerInventory {
                    parent_inv_id,
                    next_inv_min_time_started,
                    input_inv_time_started,
                } => {
                    assert_eq!(*parent_inv_id, older.id);
                    assert_eq!(*next_inv_min_time_started, older.time_done,);
                    assert_eq!(
                        *input_inv_time_started,
                        overlapping.time_started,
                    );
                }
                other => panic!(
                    "expected WaitingForNewerInventory for overlapping \
                     collections, got {other:?}"
                ),
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Exercises the wiring in `load_existing_alert_markers`: alert request
    /// ids are collected from the parent sitrep's *closed* cases only, their
    /// `rendezvous_alert_created` markers are looked up in the database, and
    /// the results are fed to the input builder, so that:
    ///
    /// - a closed case whose alert requests are all satisfied (markers
    ///   present) is dropped from the carry-forward set,
    /// - a closed case with an unsatisfied alert request is copied forward,
    /// - open cases are copied forward as open, regardless of markers.
    ///
    /// The builder-side policy itself (including the alert-generation bump
    /// when a satisfied case is dropped) is pinned by the analysis-input
    /// tests in the `nexus-fm` crate; this test pins the datastore glue in
    /// this module.
    #[tokio::test]
    async fn test_prepare_inputs_observes_alert_markers() {
        let logctx =
            dev::test_setup_log("test_prepare_inputs_observes_alert_markers");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let inv = Arc::new(CollectionBuilder::new("test").build());
        let sitrep_id = SitrepUuid::new_v4();

        let open_case_id = CaseUuid::new_v4();
        let satisfied_case_id = CaseUuid::new_v4();
        let unsatisfied_case_id = CaseUuid::new_v4();
        let satisfied_alert_id = AlertUuid::new_v4();
        let unsatisfied_alert_id = AlertUuid::new_v4();

        let alert_request = |id: AlertUuid| case::AlertRequest {
            id,
            class: AlertClass::TestFoo,
            version: 0,
            payload: json!({}),
            requested_sitrep_id: sitrep_id,
            comment: String::new(),
        };
        let make_case =
            |id: CaseUuid, closed: bool, alert: Option<AlertUuid>| {
                let mut alerts_requested = iddqd::IdOrdMap::new();
                if let Some(alert_id) = alert {
                    alerts_requested
                        .insert_unique(alert_request(alert_id))
                        .unwrap();
                }
                Case {
                    id,
                    metadata: case::Metadata {
                        created_sitrep_id: sitrep_id,
                        closed_sitrep_id: closed.then_some(sitrep_id),
                        de: DiagnosisEngineKind::PowerShelf,
                        comment: String::new(),
                    },
                    ereports: iddqd::IdOrdMap::new(),
                    alerts_requested,
                    support_bundles_requested: iddqd::IdOrdMap::new(),
                    facts: iddqd::IdOrdMap::new(),
                }
            };

        let mut cases = iddqd::IdOrdMap::new();
        cases.insert_unique(make_case(open_case_id, false, None)).unwrap();
        cases
            .insert_unique(make_case(
                satisfied_case_id,
                true,
                Some(satisfied_alert_id),
            ))
            .unwrap();
        cases
            .insert_unique(make_case(
                unsatisfied_case_id,
                true,
                Some(unsatisfied_alert_id),
            ))
            .unwrap();

        let sitrep = Sitrep {
            metadata: SitrepMetadata {
                id: sitrep_id,
                parent_sitrep_id: None,
                inv_collection_id: inv.id,
                next_inv_min_time_started: inv.time_done,
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep".to_string(),
                time_created: Utc::now(),
                alert_generation: Generation::new(),
                support_bundle_generation: Generation::new(),
            },
            cases,
            ereports_by_id: Default::default(),
        };

        // Insert the sitrep, then satisfy one of the closed cases' alert
        // requests through the real rendezvous path, which writes the
        // `rendezvous_alert_created` marker that input preparation must
        // observe.
        datastore
            .fm_sitrep_insert(opctx, sitrep.clone(), None)
            .await
            .expect("inserted parent sitrep");
        datastore
            .fm_rendezvous_alert_create(
                opctx,
                &alert_request(satisfied_alert_id),
                satisfied_case_id,
                Generation::new(),
            )
            .await
            .expect("created the satisfied case's alert");

        let parent: CurrentSitrep = Arc::new((
            SitrepVersion {
                id: sitrep_id,
                version: 1,
                time_made_current: Utc::now(),
            },
            sitrep,
        ));

        let (_sitrep_tx, sitrep_rx) = watch::channel(None);
        let (_inv_tx, inv_rx) = watch::channel(None);
        let mut task = FmAnalysis::new(
            datastore.clone(),
            sitrep_rx,
            inv_rx,
            activators(),
            OmicronZoneUuid::new_v4(),
            ANALYSIS_ENABLED,
        );

        let (input, prep) = task
            .prepare_inputs(opctx, Some(parent), inv)
            .await
            .expect("input preparation should succeed");
        assert!(
            prep.warnings.is_empty(),
            "unexpected preparation warnings: {:?}",
            prep.warnings,
        );

        // The open case is copied forward as open.
        assert!(input.open_cases().contains_key(&open_case_id));
        assert_eq!(input.open_cases().len(), 1);
        assert_eq!(
            prep.report.open_cases.keys().collect::<Vec<_>>(),
            vec![&open_case_id]
        );

        // The closed case whose only alert request has a marker is dropped
        // from the carry-forward set entirely...
        assert!(
            !prep
                .report
                .closed_cases_copied_forward
                .contains_key(&satisfied_case_id),
            "satisfied closed case should be dropped, got: {:?}",
            prep.report.closed_cases_copied_forward,
        );
        // ...while the closed case with an unsatisfied alert request is
        // copied forward, with that request reported as outstanding.
        let carried = prep
            .report
            .closed_cases_copied_forward
            .get(&unsatisfied_case_id)
            .expect("unsatisfied closed case must be copied forward");
        assert_eq!(
            carried.unmarked_alert_requests,
            BTreeSet::from([unsatisfied_alert_id])
        );
        assert!(carried.unmarked_ereports.is_empty());
        assert_eq!(prep.report.closed_cases_copied_forward.len(), 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
