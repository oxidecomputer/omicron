// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common facilities for assembling inputs to fault management analysis.

use anyhow::Context;
use chrono::Utc;
use fm::analysis_input::Input;
use fm::analysis_reports::InputReport;
use iddqd::IdOrdMap;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::datastore::Discoverability;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::model::DbMetadataNexusState;
use nexus_db_queries::db::model::EreporterRestart;
use nexus_db_queries::db::model::PhysicalDiskPolicy;
use nexus_db_queries::db::model::SagaExecState;
use nexus_db_queries::db::model::SagaReasonAbandoned;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::pagination::Paginator;
use nexus_fm as fm;
use nexus_types::in_service_disk::InServiceDisk;
use nexus_types::internal_api::background::fm_analysis as status;
use nexus_types::observed_certificate::ObservedCertificate;
use nexus_types::observed_certificate::ObservedSiloCertificates;
use nexus_types::observed_saga::{
    ObservedSaga, ObservedSagaState, SagaAbandonInfo, SagaAbandonReason,
    SagaOwnerState,
};
use omicron_certificates::TlsCertificate;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use strum::IntoEnumIterator;
use tokio_util::task::JoinMap;

/// The outcome of [`InputLoader::prepare_inputs`].
pub struct PreparationResult {
    /// The status of each preparation step, and the total time taken by the
    /// preparation phase. This is recorded whether or not preparation
    /// succeeded.
    pub prep_status: status::PreparationStatus,
    /// The prepared analysis input and input report, or an error if the
    /// preparation step failed.
    pub prep_result: Result<(Input, InputReport), status::PreparationFailure>,
}

/// Loads fault management analysis inputs from the database.
#[derive(Clone)]
pub struct InputLoader {
    datastore: Arc<DataStore>,
}

impl InputLoader {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    /// Loads the inputs to analysis from the database into `builder`, and
    /// builds it into an [`Input`].
    ///
    /// Each input is loaded by a separate step, running on its own task, so
    /// that the steps' queries execute concurrently. Every step runs to
    /// completion even if another fails, so that all failures are reported
    /// together. As each step completes, successfully or otherwise, its status
    /// is recorded in `prep_status`.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned future aborts any steps still in flight. This is
    /// harmless, as the steps only read from the database.
    pub async fn prepare_inputs(
        &self,
        opctx: &OpContext,
        builder: fm::analysis_input::Builder,
    ) -> PreparationResult {
        // This whole function is *basically* just a call to
        // `prepare_inputs_internal`, but timed. This way,
        // `prepare_inputs_internal` can bail out with `?` when it encounters an
        // error, but we can time the duration of the preparation step
        // regardless of whether it succeeds or fails.
        let started = Instant::now();
        let mut steps = IdOrdMap::new();

        let prep_result =
            self.prepare_inputs_internal(opctx, builder, &mut steps).await;

        let total_duration = started.elapsed();
        PreparationResult {
            prep_status: status::PreparationStatus { total_duration, steps },
            prep_result,
        }
    }

    /// Internal implementation of [`Self::prepare_inputs`]. This is factored
    /// out into a separate function so that `prepare_inputs` can time the total
    /// duration regardless of whether the preparation step succeeds or fails.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned future aborts any steps still in flight. This is
    /// harmless, as the steps only read from the database.
    async fn prepare_inputs_internal(
        &self,
        opctx: &OpContext,
        mut builder: fm::analysis_input::Builder,
        step_status: &mut IdOrdMap<status::PreparationStepStatus>,
    ) -> Result<(Input, InputReport), status::PreparationFailure> {
        // Dropping this `JoinMap` aborts any tasks still running in it.
        let mut steps = JoinMap::new();

        // Spawn each preparation step in parallel.
        //
        // N.B. that right now, the concurrency limit here is just the number of
        // steps that are defined. Right now, there are only 7 preparation
        // steps, so that's fine. In the future, if there are a whole lot of
        // steps, we may eventually want to think about having a concurrency
        // limit here, especially if steps can recursively spawn other
        // steps...but they can't do that today. So.
        for step in InputStep::iter() {
            let opctx = opctx.child(BTreeMap::from([(
                "input".to_string(),
                step.to_string(),
            )]));
            let parent_sitrep = builder.parent_sitrep().cloned();
            let loader = self.clone();
            steps.spawn(step, async move {
                let parent = parent_sitrep.as_ref().map(|s| &s.1);
                loader.run_step(step, &opctx, parent).await
            });
        }

        let mut step_errors = BTreeMap::new();
        while let Some((step, result)) = steps.join_next().await {
            let result = match result {
                Ok(StepOutput { status, result }) => {
                    step_status
                        .insert_unique(status)
                        .expect("exactly one step is spawned for each input");
                    result
                }
                // This shouldn't happen in a production build, since we build
                // with `panic = "abort"`, but lets us get the panic message out
                // nicely in tests.
                Err(err) if err.is_panic() => {
                    std::panic::resume_unwind(err.into_panic())
                }
                // This also shouldn't happen, since the only thing that aborts
                // the tasks we spawn is if this function is cancelled, in which
                // case, it wouldn't be around to wait for them to complete
                // anyway. May as well handle it gracefully, though.
                Err(err) => Err(err).with_context(|| {
                    format!(
                        "input-loading task for {step} was unexpectedly \
                         canceled (this is probably a bug)",
                    )
                }),
            };
            match result {
                Ok(LoadedInput::InServiceDisks(disks)) => {
                    builder = builder.in_service_disks(Arc::new(disks));
                }
                Ok(LoadedInput::ObservedSagas(sagas)) => {
                    builder = builder.observed_sagas(Arc::new(sagas));
                }
                Ok(LoadedInput::SiloCertificates(silos)) => {
                    builder =
                        builder.observed_silo_certificates(Arc::new(silos));
                }
                Ok(LoadedInput::EreporterRestarts(restarts)) => {
                    builder.add_ereporter_restarts(restarts);
                }
                Ok(LoadedInput::Ereports(ereports)) => {
                    builder.add_unmarked_ereports(ereports);
                }
                Ok(LoadedInput::AlertMarkers(marked)) => {
                    builder.add_marked_alert_requests(marked);
                }
                Ok(LoadedInput::SupportBundleMarkers(marked)) => {
                    builder.add_marked_support_bundle_requests(marked);
                }
                Err(err) => {
                    let error = InlineErrorChain::new(&*err);
                    slog::error!(
                        opctx.log,
                        "failed to load fault management analysis input";
                        "input" => %step,
                        &error,
                    );
                    step_errors.insert(step.to_string(), error.to_string());
                }
            }
        }

        if !step_errors.is_empty() {
            return Err(status::PreparationFailure::StepsFailed(step_errors));
        }

        let (input, report) = builder.build().map_err(|err| {
            // A missing input is a bug in this task, since every step which
            // succeeded must have provided its input to the builder by this
            // point.
            let error = InlineErrorChain::new(&err);
            slog::error!(
                opctx.log,
                "fault management analysis preparation failed";
                &error,
            );
            status::PreparationFailure::InvalidInputs(error.to_string())
        })?;

        // The builder checks ereports' restart IDs only once both ereports and
        // restarts are loaded. Report any unknown ones as warnings from the
        // step which loaded the ereports.
        if !report.unknown_restart_ereport_ids.is_empty() {
            let mut ereports_step =
                step_status.get_mut(InputStep::Ereports.as_str()).expect(
                    "every step succeeded, so every step has a status entry",
                );
            for id in &report.unknown_restart_ereport_ids {
                let msg = format!(
                    "ereport {id} has a restart ID not contained in the \
                     `ereporter_restart` table"
                );
                slog::warn!(&opctx.log, "{msg}");
                ereports_step.warnings.push(msg);
            }
        }

        Ok((input, report))
    }

    /// Runs the provided [`InputStep`] and returns its [`StepOutput`].
    async fn run_step(
        &self,
        step: InputStep,
        opctx: &OpContext,
        parent_sitrep: Option<&nexus_types::fm::Sitrep>,
    ) -> StepOutput {
        let started = Instant::now();
        let mut warnings = Vec::new();
        let result = match step {
            InputStep::InServiceDisks => self
                .load_in_service_disks(opctx, &mut warnings)
                .await
                .map(LoadedInput::InServiceDisks),
            InputStep::ObservedSagas => self
                .prepare_observed_sagas(opctx)
                .await
                .map(LoadedInput::ObservedSagas),
            InputStep::SiloCertificates => self
                .load_silo_certificates(opctx, &mut warnings)
                .await
                .map(LoadedInput::SiloCertificates),
            InputStep::EreporterRestarts => self
                .load_ereporter_restarts(opctx)
                .await
                .context("failed to load ereporter restarts")
                .map(LoadedInput::EreporterRestarts),
            InputStep::Ereports => self
                .load_new_ereports(opctx, &mut warnings)
                .await
                .context("failed to load new ereports")
                .map(LoadedInput::Ereports),
            InputStep::AlertMarkers => self
                .load_existing_alert_markers(opctx, parent_sitrep)
                .await
                .context("failed to load existing alert markers")
                .map(LoadedInput::AlertMarkers),
            InputStep::SupportBundleMarkers => self
                .load_existing_support_bundle_markers(opctx, parent_sitrep)
                .await
                .context("failed to load existing support bundle markers")
                .map(LoadedInput::SupportBundleMarkers),
        };
        let status = status::PreparationStepStatus {
            name: step.to_string(),
            duration: started.elapsed(),
            warnings,
        };
        StepOutput { status, result }
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

    /// Build the certificate diagnosis engine's input: every silo, with the
    /// leaf validity window of each of its external TLS certificates.
    ///
    /// This reads the same rows the `external_endpoints` background task uses
    /// to decide which certificate to serve, and applies the same acceptance
    /// check (`TlsCertificate::try_from`), so the engine reasons about exactly
    /// the certificates Nexus can present. A certificate that fails that check
    /// is never served, however far off its expiration is, so it must not be
    /// allowed to mask an expiring certificate that is served. Certificates
    /// that fail the check, or that belong to a silo that no longer exists,
    /// are skipped with a warning, mirroring how `external_endpoints` skips
    /// them.
    async fn load_silo_certificates(
        &self,
        opctx: &OpContext,
        warnings: &mut Vec<String>,
    ) -> anyhow::Result<IdOrdMap<ObservedSiloCertificates>> {
        let mut silos = IdOrdMap::new();
        for silo in self
            .datastore
            .silo_list_all_batched(opctx, Discoverability::All)
            .await
            .context("failed to list silos")?
        {
            silos
                .insert_unique(ObservedSiloCertificates {
                    silo_id: silo.id(),
                    silo_name: silo.name().clone(),
                    certificates: IdOrdMap::new(),
                })
                .expect("silo IDs are unique");
        }

        let mut paginator = Paginator::new(
            datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .datastore
                .certificate_list_for(
                    opctx,
                    Some(ServiceKind::Nexus),
                    &PaginatedBy::Id(p.current_pagparams()),
                    None,
                )
                .await
                .context("failed to list certificates")?;
            paginator = p.found_batch(&batch, &|c| c.id());
            for cert in batch {
                let cert_id = cert.id();
                let cert_name = cert.name().clone();
                let silo_id = cert.silo_id();
                let Some(mut silo) = silos.get_mut(silo_id) else {
                    warnings.push(format!(
                        "certificate {cert_id} belongs to silo {silo_id}, \
                         which was not found; ignoring it",
                    ));
                    continue;
                };
                let validity = match TlsCertificate::try_from(cert) {
                    Ok(tls_cert) => tls_cert.validity(),
                    Err(e) => {
                        warnings.push(format!(
                            "certificate {cert_id} for silo {silo_id} cannot \
                             be served; ignoring it: {e:#}",
                        ));
                        continue;
                    }
                };
                silo.certificates
                    .insert_unique(ObservedCertificate {
                        id: cert_id,
                        name: cert_name,
                        not_before: validity.not_before,
                        not_after: validity.not_after,
                    })
                    .expect("certificate IDs are unique");
            }
        }

        Ok(silos)
    }

    /// Build the saga diagnosis engine's input: every non-terminal saga,
    /// annotated with the timestamp of its latest node event (the progress
    /// signal) and the state of its owning Nexus.
    async fn prepare_observed_sagas(
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<IdOrdMap<ObservedSaga>> {
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
            .collect();

        // Classify each owning Nexus (current_sec) against db_metadata_nexus.
        let nexus_states: BTreeMap<OmicronZoneUuid, DbMetadataNexusState> =
            self.datastore
                .get_db_metadata_nexus_in_state(
                    opctx,
                    DbMetadataNexusState::ALL.to_vec(),
                )
                .await
                .context("failed to load db_metadata_nexus records")?
                .into_iter()
                .map(|n| (n.nexus_id(), n.state()))
                .collect();

        let mut observed = IdOrdMap::new();
        for saga in sagas {
            let saga_state = match saga.saga_state {
                SagaExecState::Running => ObservedSagaState::Running,
                SagaExecState::Unwinding => ObservedSagaState::Unwinding,
                SagaExecState::Abandoned(metadata) => {
                    ObservedSagaState::Abandoned(SagaAbandonInfo {
                        time: metadata.time,
                        reason: match metadata.reason {
                            SagaReasonAbandoned::Omdb => {
                                SagaAbandonReason::Omdb
                            }
                            SagaReasonAbandoned::Unrecoverable => {
                                SagaAbandonReason::Unrecoverable
                            }
                            SagaReasonAbandoned::Orphaned => {
                                SagaAbandonReason::Orphaned
                            }
                        },
                        comment: metadata.comment,
                    })
                }
                // The query filters to unfinished states; defend anyway.
                SagaExecState::Done => continue,
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
                last_event_times.get(&saga.id).copied().flatten();
            observed
                .insert_unique(ObservedSaga {
                    saga_id: saga.id,
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
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<Vec<EreporterRestart>> {
        let mut restarts = Vec::new();
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
            restarts.extend(batch);
        }

        slog::debug!(
            opctx.log,
            "loaded {} ereporter restarts (in {nbatches} batches)",
            restarts.len(),
        );

        Ok(restarts)
    }

    /// Loads all ereports which have not been marked as seen, skipping (with a
    /// warning) any which are invalid.
    ///
    /// This includes ereports which were input to the parent sitrep but have
    /// not yet been marked; [`fm::analysis_input::Builder`] distinguishes
    /// them from new ereports. The builder also checks the ereports' restart
    /// IDs against the ereporter restarts, so that the two may be loaded
    /// concurrently.
    async fn load_new_ereports(
        &self,
        opctx: &OpContext,
        warnings: &mut Vec<String>,
    ) -> anyhow::Result<Vec<fm::Ereport>> {
        let mut ereports = Vec::new();
        // Only surface ereports a diagnosis engine will consume.
        let classes = fm::diagnosis::known_ereport_classes();
        let mut paginator = Paginator::new(
            nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
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
                match fm::Ereport::try_from(ereport) {
                    Ok(ereport) => ereports.push(ereport),
                    Err(e) => {
                        invalid += 1;
                        warnings.push(e.to_string());
                    }
                };
            }

            if invalid > 0 {
                slog::warn!(
                    &opctx.log,
                    "loaded {loaded} unmarked ereports, {invalid} invalid"
                );
            } else {
                slog::debug!(&opctx.log, "loaded {loaded} unmarked ereports");
            }
        }

        Ok(ereports)
    }

    async fn load_existing_alert_markers(
        &self,
        opctx: &OpContext,
        parent: Option<&nexus_types::fm::Sitrep>,
    ) -> anyhow::Result<HashSet<AlertUuid>> {
        let Some(parent) = parent else {
            // No parent sitrep, so no closed cases, so nothing to look up.
            return Ok(HashSet::new());
        };
        let candidate_ids: Vec<AlertUuid> = parent
            .cases
            .iter()
            .filter(|c| !c.is_open())
            .flat_map(|c| c.alerts_requested.iter().map(|r| r.id))
            .collect();
        if candidate_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let marked = self
            .datastore
            .fm_rendezvous_existing_alert_markers(opctx, &candidate_ids)
            .await
            .context("failed to look up alert marker existence")?;
        Ok(marked)
    }

    async fn load_existing_support_bundle_markers(
        &self,
        opctx: &OpContext,
        parent: Option<&nexus_types::fm::Sitrep>,
    ) -> anyhow::Result<HashSet<SupportBundleUuid>> {
        let Some(parent) = parent else {
            // No parent sitrep, so no closed cases, so nothing to look up.
            return Ok(HashSet::new());
        };
        let candidate_ids: Vec<SupportBundleUuid> = parent
            .cases
            .iter()
            .filter(|c| !c.is_open())
            .flat_map(|c| c.support_bundles_requested.iter().map(|r| r.id))
            .collect();
        if candidate_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let marked = self
            .datastore
            .fm_rendezvous_existing_support_bundle_markers(
                opctx,
                &candidate_ids,
            )
            .await
            .context("failed to look up support bundle marker existence")?;
        Ok(marked)
    }
}

struct StepOutput {
    status: status::PreparationStepStatus,
    result: anyhow::Result<LoadedInput>,
}

#[derive(strum::EnumDiscriminants)]
#[strum_discriminants(name(InputStep))]
// Traits to derive for the generated `InputStep` enum.
#[strum_discriminants(derive(
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumIter,
    strum::IntoStaticStr,
    strum::VariantNames,
))]
#[strum_discriminants(strum(serialize_all = "snake_case"))]
enum LoadedInput {
    InServiceDisks(IdOrdMap<InServiceDisk>),
    ObservedSagas(IdOrdMap<ObservedSaga>),
    SiloCertificates(IdOrdMap<ObservedSiloCertificates>),
    EreporterRestarts(Vec<EreporterRestart>),
    Ereports(Vec<fm::Ereport>),
    AlertMarkers(HashSet<AlertUuid>),
    SupportBundleMarkers(HashSet<SupportBundleUuid>),
}

impl InputStep {
    pub fn as_str(&self) -> &'static str {
        <&'static str>::from(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::alert::AlertClass;
    use nexus_types::fm::Case;
    use nexus_types::fm::CurrentSitrep;
    use nexus_types::fm::DiagnosisEngineKind;
    use nexus_types::fm::FmConfig;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use nexus_types::fm::SitrepVersion;
    use nexus_types::fm::case;
    use omicron_generation_kinds::SitrepActiveProblemGeneration;
    use omicron_generation_kinds::SitrepAlertGeneration;
    use omicron_generation_kinds::SitrepSupportBundleGeneration;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use serde_json::json;
    use std::collections::BTreeSet;

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
    /// this crate.
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
                    active_problems: iddqd::IdOrdMap::new(),
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
                alert_generation: SitrepAlertGeneration::new(),
                support_bundle_generation: SitrepSupportBundleGeneration::new(),
                active_problem_generation: SitrepActiveProblemGeneration::new(),
            },
            cases,
            ereports_by_id: Default::default(),
            active_problems: Default::default(),
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
                SitrepAlertGeneration::new(),
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

        let builder = Input::builder(Some(parent), inv)
            .expect("parent sitrep was produced from this inventory")
            .config(FmConfig::default());
        let PreparationResult { prep_status, prep_result } =
            InputLoader::new(datastore.clone())
                .prepare_inputs(opctx, builder)
                .await;
        let (input, report) =
            prep_result.expect("input preparation should succeed");
        // Every input-loading step ran and recorded its status, without
        // warnings.
        assert_eq!(
            prep_status
                .steps
                .iter()
                .map(|s| s.name.as_str())
                .collect::<BTreeSet<_>>(),
            InputStep::iter().map(|s| s.as_str()).collect::<BTreeSet<_>>(),
        );
        for step in &prep_status.steps {
            assert!(
                step.warnings.is_empty(),
                "unexpected warnings from step {}: {:?}",
                step.name,
                step.warnings,
            );
        }

        // The open case is copied forward as open.
        assert!(input.open_cases().contains_key(&open_case_id));
        assert_eq!(input.open_cases().len(), 1);
        assert_eq!(
            report.open_cases.keys().collect::<Vec<_>>(),
            vec![&open_case_id]
        );

        // The closed case whose only alert request has a marker is dropped
        // from the carry-forward set entirely...
        assert!(
            !report
                .closed_cases_copied_forward
                .contains_key(&satisfied_case_id),
            "satisfied closed case should be dropped, got: {:?}",
            report.closed_cases_copied_forward,
        );
        // ...while the closed case with an unsatisfied alert request is
        // copied forward, with that request reported as outstanding.
        let carried = report
            .closed_cases_copied_forward
            .get(&unsatisfied_case_id)
            .expect("unsatisfied closed case must be copied forward");
        assert_eq!(
            carried.unmarked_alert_requests,
            BTreeSet::from([unsatisfied_alert_id])
        );
        assert!(carried.unmarked_ereports.is_empty());
        assert_eq!(report.closed_cases_copied_forward.len(), 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Builds a self-signed certificate whose key is 1024-bit RSA, returning
    /// the certificate and key as PEM.
    ///
    /// Upload validation (`CertificateValidator`) accepts such a certificate:
    /// openssl parses the key, and it matches the leaf's public key. But
    /// rustls (via aws-lc-rs) refuses RSA keys shorter than 2048 bits, so
    /// `TlsCertificate::try_from` rejects it and Nexus never serves it.
    fn unservable_certificate(
        hostname: &str,
        not_after: chrono::DateTime<Utc>,
    ) -> (String, String) {
        use openssl::asn1::Asn1Time;
        use openssl::hash::MessageDigest;
        use openssl::nid::Nid;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::X509Builder;
        use openssl::x509::X509NameBuilder;
        use openssl::x509::extension::SubjectAlternativeName;

        let key = PKey::from_rsa(Rsa::generate(1024).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_nid(Nid::COMMONNAME, hostname).unwrap();
        let name = name.build();

        let mut builder = X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap();
        builder
            .set_not_after(&Asn1Time::from_unix(not_after.timestamp()).unwrap())
            .unwrap();
        let san = SubjectAlternativeName::new()
            .dns(hostname)
            .build(&builder.x509v3_context(None, None))
            .unwrap();
        builder.append_extension(san).unwrap();
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        let cert = builder.build();

        let cert_pem = String::from_utf8(cert.to_pem().unwrap()).unwrap();
        let key_pem =
            String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        (cert_pem, key_pem)
    }

    /// The certificate loader must apply the same acceptance check as the
    /// external endpoint code, so that a stored certificate Nexus cannot
    /// serve does not mask the expiring certificate it does serve.
    #[tokio::test]
    async fn test_load_silo_certificates_skips_unservable_certificates() {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use nexus_db_model::Certificate;
        use nexus_db_schema::schema::certificate::dsl;
        use nexus_types::external_api::certificate::CertificateCreate;
        use nexus_types::external_api::certificate::ServiceUsingCertificate;
        use nexus_types::silo::DEFAULT_SILO_ID;
        use omicron_certificates::CertificateValidator;
        use omicron_common::api::external::IdentityMetadataCreateParams;
        use omicron_test_utils::certificates::CertificateChain;
        use omicron_uuid_kinds::SiloUuid;
        use uuid::Uuid;

        let logctx = dev::test_setup_log(
            "test_load_silo_certificates_skips_unservable_certificates",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const HOSTNAME: &str = "fake.test.oxide.computer";
        let make_cert =
            |silo_id: SiloUuid, name: &str, cert: String, key: String| {
                Certificate::new_unvalidated(
                    silo_id,
                    Uuid::new_v4(),
                    ServiceKind::Nexus,
                    CertificateCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name.parse().unwrap(),
                            description: String::new(),
                        },
                        cert,
                        key,
                        service: ServiceUsingCertificate::ExternalApi,
                    },
                )
            };

        // A certificate Nexus can serve, expiring soon.
        let mut params =
            rcgen::CertificateParams::new(vec![HOSTNAME.to_string()]);
        params.not_after = (std::time::SystemTime::now()
            + std::time::Duration::from_secs(5 * 24 * 60 * 60))
        .into();
        let chain = CertificateChain::with_params(params);
        let servable = make_cert(
            DEFAULT_SILO_ID,
            "servable",
            chain.cert_chain_as_pem(),
            chain.end_cert_private_key_as_pem(),
        );
        let servable_id = servable.id();

        // A certificate Nexus cannot serve, expiring much later. If the
        // loader admitted it, it would be the silo's "best" certificate and
        // the expiring one above would go unreported.
        let (cert, key) = unservable_certificate(
            HOSTNAME,
            Utc::now() + chrono::Duration::days(730),
        );
        // Confirm the premise: upload validation accepts this certificate...
        CertificateValidator::default()
            .validate(cert.as_bytes(), key.as_bytes(), &[HOSTNAME])
            .expect("upload validation accepts a 1024-bit RSA certificate");
        let unservable = make_cert(DEFAULT_SILO_ID, "unservable", cert, key);
        let unservable_id = unservable.id();
        // ...but the external endpoint code refuses to serve it.
        TlsCertificate::try_from(unservable.clone())
            .err()
            .expect("external endpoints reject a 1024-bit RSA certificate");

        // A servable certificate whose silo does not exist. The loader has
        // no silo to attach it to, so it is skipped with a warning.
        let orphan_silo_id = SiloUuid::new_v4();
        let orphan_chain = CertificateChain::new(HOSTNAME);
        let orphan = make_cert(
            orphan_silo_id,
            "orphan",
            orphan_chain.cert_chain_as_pem(),
            orphan_chain.end_cert_private_key_as_pem(),
        );
        let orphan_id = orphan.id();

        diesel::insert_into(dsl::certificate)
            .values(vec![servable, unservable, orphan])
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .expect("inserted certificates");

        let loader = InputLoader::new(datastore.clone());
        let mut warnings = Vec::new();
        let silos = loader
            .load_silo_certificates(opctx, &mut warnings)
            .await
            .expect("loaded silo certificates");

        let silo = silos.get(&DEFAULT_SILO_ID).expect("default silo observed");
        let observed_ids: Vec<Uuid> =
            silo.certificates.iter().map(|c| c.id).collect();
        assert_eq!(observed_ids, vec![servable_id]);
        assert_eq!(
            silo.best_certificate().map(|c| c.id),
            Some(servable_id),
            "the expiring servable certificate must be the best one"
        );
        let observed = silo.certificates.get(&servable_id).unwrap();
        assert!(observed.not_before <= Utc::now());
        assert!(observed.not_after > Utc::now());
        assert!(observed.not_after < Utc::now() + chrono::Duration::days(6));

        // Both skipped certificates are reported, and the warnings are
        // readable: no run of interior whitespace from a wrapped literal.
        assert_eq!(warnings.len(), 2, "unexpected warnings: {warnings:?}");
        for warning in &warnings {
            assert!(!warning.contains("  "), "unexpected warning: {warning}");
        }
        assert!(
            warnings.iter().any(|w| {
                w.contains(&unservable_id.to_string())
                    && w.contains("cannot be served")
            }),
            "unexpected warnings: {warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| {
                w.contains(&orphan_id.to_string())
                    && w.contains(&orphan_silo_id.to_string())
                    && w.contains("which was not found")
            }),
            "unexpected warnings: {warnings:?}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
