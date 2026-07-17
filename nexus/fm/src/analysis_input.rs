// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inputs to fault management analysis.

use chrono::{DateTime, Utc};
use iddqd::IdOrdMap;
use nexus_db_model::EreporterRestart;
use nexus_types::fm::analysis_reports::ClosedCaseReport;
use nexus_types::fm::{self, Sitrep, SitrepVersion};
use nexus_types::in_service_disk::InServiceDisk;
use nexus_types::inventory;
use nexus_types::observed_saga::ObservedSaga;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;

pub use nexus_types::fm::analysis_reports::InputReport as Report;

/// A complete set of inputs to a fault management analysis phase.
///
/// This struct bundles together all the inputs to analysis, including:
///
/// - The [parent sitrep](Input::parent_sitrep)
/// - The current [inventory collection](Input::inventory)
/// - Any [new ereports](Input::new_ereports) which were received since when
///   the parent sitrep was produced
/// - The set of [open cases](Input::open_cases) which must be copied
///   forwards into the next sitrep
///
/// This type represents the outputs of the analysis preparation phase. Once it
/// is constructed, the inputs are immutable and cannot be modified. To
/// construct a new `Input` as part of a preparation phase, use
/// [`Input::builder`].
#[derive(Debug)]
pub struct Input {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    /// Ereports which are new and should be input to analysis in the next
    /// sitrep.
    new_ereports: IdOrdMap<Arc<fm::Ereport>>,
    open_cases: IdOrdMap<fm::Case>,
    closed_cases_copied_forward: IdOrdMap<fm::Case>,
    ereporter_restarts: IdOrdMap<Arc<EreporterRestart>>,
    /// Indicates whether `Builder::build` dropped any closed case
    /// from the carry-forward list whose alert request set was non-empty.
    /// ORed with [`crate::builder::AllCases::alert_set_changed`] in
    /// [`crate::builder::SitrepBuilder::build`] to decide whether to bump the
    /// new sitrep's `alert_generation`.
    alerts_changed: bool,
    /// Indicates whether `Builder::build` dropped any closed case
    /// from the carry-forward list whose support bundle request set was
    /// non-empty. ORed with
    /// [`crate::builder::AllCases::support_bundle_set_changed`] in
    /// [`crate::builder::SitrepBuilder::build`] to decide whether to bump the
    /// new sitrep's `support_bundle_generation`.
    support_bundles_changed: bool,
    in_service_disks: Arc<IdOrdMap<InServiceDisk>>,
    /// All non-terminal (running, unwinding, or abandoned) sagas, annotated
    /// with their latest node-event time and owning-Nexus state.
    observed_sagas: Arc<IdOrdMap<ObservedSaga>>,
}

impl Input {
    pub fn parent_sitrep(&self) -> Option<&Sitrep> {
        self.parent_sitrep.as_ref().map(|s| &s.1)
    }

    pub fn inventory(&self) -> &inventory::Collection {
        &self.inv
    }

    pub fn new_ereports(&self) -> &IdOrdMap<Arc<fm::Ereport>> {
        &self.new_ereports
    }

    /// Open cases from the parent sitrep, copied forward into this analysis
    /// input. Closed cases live separately on the (crate-private)
    /// `closed_cases_copied_forward` accessor.
    pub fn open_cases(&self) -> &IdOrdMap<fm::Case> {
        &self.open_cases
    }

    pub fn ereporter_restarts(&self) -> &IdOrdMap<Arc<EreporterRestart>> {
        &self.ereporter_restarts
    }

    pub(crate) fn closed_cases_copied_forward(&self) -> &IdOrdMap<fm::Case> {
        &self.closed_cases_copied_forward
    }

    pub(crate) fn alerts_changed(&self) -> bool {
        self.alerts_changed
    }

    pub(crate) fn support_bundles_changed(&self) -> bool {
        self.support_bundles_changed
    }

    /// All control-plane-managed disks (`physical_disk.disk_policy =
    /// in_service` in the DB), indexed by `physical_disk_id`.
    pub fn in_service_disks(&self) -> &IdOrdMap<InServiceDisk> {
        &self.in_service_disks
    }

    /// All non-terminal sagas observed in the database, indexed by `saga_id`.
    /// See the saga diagnosis engine for how absence (a saga that has reached
    /// a terminal state) drives case closure.
    pub fn observed_sagas(&self) -> &IdOrdMap<ObservedSaga> {
        &self.observed_sagas
    }

    /// Returns a [`Builder`] for constructing a new `Input` from the provided
    /// `parent_sitrep`, inventory collection, in-service disks, and observed
    /// sagas.
    pub fn builder(
        parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
        inv: Arc<inventory::Collection>,
        in_service_disks: Arc<IdOrdMap<InServiceDisk>>,
        observed_sagas: Arc<IdOrdMap<ObservedSaga>>,
    ) -> Result<Builder, InvalidInputs> {
        // Before preparing analysis inputs, check that the proposed input
        // inventory collection is at least as new as the parent sitrep's
        // inventory collection.
        if let Some((_, ref parent)) = parent_sitrep.as_deref() {
            let parent = &parent.metadata;
            // It is always okay to produce a new sitrep based on the same
            // inventory collection as the parent sitrep...
            if parent.inv_collection_id != inv.id
            // ...but if they are not equal, the new inventory collection must
            // have started after the minimum start time to be considered
            // newer than the parent's.
                && inv.time_started < parent.next_inv_min_time_started
            {
                return Err(InvalidInputs::InventoryStale {
                    parent_inv_id: parent.inv_collection_id,
                    next_inv_min_time_started: parent.next_inv_min_time_started,
                    input_inv_time_started: inv.time_started,
                });
            }
        }
        Ok(Builder {
            parent_sitrep,
            inv,
            in_service_disks,
            observed_sagas,
            new_ereports: IdOrdMap::default(),
            ereporter_restarts: IdOrdMap::default(),
            unmarked_seen_ereports: BTreeSet::default(),
            marked_alert_requests: HashSet::new(),
            marked_support_bundle_requests: HashSet::new(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidInputs {
    #[error(
        "inventory collection from {input_inv_time_started} is not new \
         enough: must have started at {next_inv_min_time_started} or later"
    )]
    InventoryStale {
        parent_inv_id: CollectionUuid,
        next_inv_min_time_started: DateTime<Utc>,
        input_inv_time_started: DateTime<Utc>,
    },
}

#[must_use]
pub struct Builder {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    in_service_disks: Arc<IdOrdMap<InServiceDisk>>,
    observed_sagas: Arc<IdOrdMap<ObservedSaga>>,
    /// Ereports which are new and should be input to analysis in the next
    /// sitrep.
    new_ereports: IdOrdMap<Arc<fm::Ereport>>,

    /// The IDs of any ereports which have been included in the parent sitrep,
    /// but which have *not* yet been marked as seen in the database.
    ///
    /// These must be tracked in order to determine which closed cases must be
    /// copied forwards due to containing unmarked ereports.
    unmarked_seen_ereports: BTreeSet<fm::EreportId>,

    ereporter_restarts: IdOrdMap<Arc<nexus_db_model::EreporterRestart>>,
    /// The IDs of alert requests on the parent sitrep's closed cases that
    /// already have a marker row in `rendezvous_alert_created`. A closed-case
    /// request absent from this set is outstanding work, and (like an unmarked
    /// ereport) forces its case to be copied forward. Open cases are copied
    /// forward unconditionally, so we don't need to track their requests here.
    ///
    /// Unlike [`Self::unmarked_seen_ereports`], this set holds the requests
    /// whose work is done rather than the outstanding ones. This is an artifact
    /// of the query shape in `fm_rendezvous_existing_alert_markers`, which
    /// needs to be an index lookup rather than a table scan.
    marked_alert_requests: HashSet<AlertUuid>,

    /// The IDs of support bundle requests on the parent sitrep's closed cases
    /// that already have a marker row in `rendezvous_support_bundle_created`.
    /// A closed-case request absent from this set is outstanding work, and
    /// (like an unmarked ereport) forces its case to be copied forward. Open
    /// cases are copied forward unconditionally, so we don't need to track
    /// their requests here.
    ///
    /// Unlike [`Self::unmarked_seen_ereports`], this set holds the requests
    /// whose work is done rather than the outstanding ones. This is an artifact
    /// of the query shape in `fm_rendezvous_existing_support_bundle_markers`,
    /// which needs to be an index lookup rather than a table scan.
    marked_support_bundle_requests: HashSet<SupportBundleUuid>,
}

impl Builder {
    /// Adds a set of ereports which have not been marked as "seen" in the
    /// database to the inputs under construction.
    ///
    /// This will filter out any ereports which are present in the parent sitrep
    /// and have not yet been marked in the database, and then add any ereports
    /// which remain to the set of ereports which are actually new and should be
    /// included in the inputs to the next sitrep.
    pub fn add_unmarked_ereports(
        &mut self,
        ereports: impl IntoIterator<Item = fm::Ereport>,
    ) {
        let parent_sitrep = self.parent_sitrep.as_ref().map(|s| &s.1);
        self.new_ereports.extend(ereports.into_iter().filter_map(|ereport| {
            if let Some(sitrep) = parent_sitrep {
                let id = ereport.id;
                if sitrep.ereports_by_id.contains_key(&id) {
                    self.unmarked_seen_ereports.insert(id);
                    return None;
                }
            }

            Some(Arc::new(ereport))
        }))
    }

    /// Records the alert request ids (from the parent sitrep's closed cases)
    /// whose `rendezvous_alert_created` marker already exists in the database.
    /// During [`Self::build`], a closed case with a request *not* in this set
    /// counts as having outstanding work, and is carried forward.
    pub fn add_marked_alert_requests(
        &mut self,
        marker_ids: impl IntoIterator<Item = AlertUuid>,
    ) {
        self.marked_alert_requests.extend(marker_ids);
    }

    /// Records the support bundle request ids (from the parent sitrep's
    /// closed cases) whose `rendezvous_support_bundle_created` marker already
    /// exists in the database. During [`Self::build`], a closed case with a
    /// request *not* in this set counts as having outstanding work, and is
    /// carried forward.
    pub fn add_marked_support_bundle_requests(
        &mut self,
        marker_ids: impl IntoIterator<Item = SupportBundleUuid>,
    ) {
        self.marked_support_bundle_requests.extend(marker_ids);
    }

    pub fn num_ereports(&self) -> usize {
        self.new_ereports.len()
    }

    /// Adds a set of ereport restart IDs to the input.
    pub fn add_ereporter_restarts<I>(
        &mut self,
        restarts: impl IntoIterator<Item = I>,
    ) where
        Arc<EreporterRestart>: From<I>,
    {
        self.ereporter_restarts.extend(restarts.into_iter().map(Arc::from))
    }

    /// Borrows the map of known ereport reporter restart IDs.
    pub fn ereporter_restarts(&self) -> &IdOrdMap<Arc<EreporterRestart>> {
        &self.ereporter_restarts
    }

    /// Finish constructing the [`Input`] and return it, along with a [`Report`]
    /// that provides a human-readable summary of how the inputs were
    /// constructed.
    pub fn build(self) -> (Input, Report) {
        let parent_sitrep = self.parent_sitrep.as_ref().map(|s| &s.1);
        let (parent_sitrep_id, parent_inv_id) = match parent_sitrep {
            Some(sitrep) => {
                let id = sitrep.id();
                let inv_id = sitrep.metadata.inv_collection_id;
                (Some(id), Some(inv_id))
            }
            None => (None, None),
        };

        let mut report = Report {
            parent_sitrep_id,
            parent_inv_id,
            inv_id: self.inv.id,
            new_ereport_ids: self.new_ereports.iter().map(|e| e.id).collect(),
            num_ereporter_restarts: self.ereporter_restarts.len(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
            in_service_disks: self
                .in_service_disks
                .iter()
                .map(|d| d.physical_disk_id)
                .collect(),
            observed_sagas: self
                .observed_sagas
                .iter()
                .map(|s| {
                    (
                        s.saga_id,
                        fm::analysis_reports::ObservedSagaReport {
                            saga_name: s.saga_name.clone(),
                            saga_state: s.saga_state,
                            last_event_time: s.last_event_time,
                            owner_state: s.owner_state,
                        },
                    )
                })
                .collect(),
        };

        // Determine which cases must be copied forwards into the next sitrep.
        // Cases from the parent sitrep should be copied forwards if:
        // - The case is still open
        let mut open_cases = IdOrdMap::new();
        // - The case has been closed, but still has outstanding work:
        //   - an ereport which has not yet been marked as "seen" in the
        //     database,
        //   - an alert request whose `rendezvous_alert_created` marker is not
        //     yet present, or
        //   - a support bundle request whose
        //     `rendezvous_support_bundle_created` marker is not yet present.
        let mut closed_cases_copied_forward = IdOrdMap::new();
        let mut alerts_changed = false;
        let mut support_bundles_changed = false;
        for case in parent_sitrep.iter().flat_map(|s| s.cases.iter()) {
            if case.is_open() {
                report.open_cases.insert(case.id, case.metadata.clone());
                open_cases.insert_unique(case.clone()).expect(
                    "the case UUID is coming from iterating over another \
                     `IdOrdMap`, so it must be unique",
                );
            } else {
                let unmarked_ereports = case
                    .ereports
                    .iter()
                    .filter_map(|ereport| {
                        let id = ereport.ereport_id();
                        if self.unmarked_seen_ereports.contains(&id) {
                            Some(*id)
                        } else {
                            None
                        }
                    })
                    .collect::<BTreeSet<_>>();
                let unmarked_alert_requests = case
                    .alerts_requested
                    .iter()
                    .map(|r| r.id)
                    .filter(|id| !self.marked_alert_requests.contains(id))
                    .collect::<BTreeSet<_>>();
                let unmarked_support_bundle_requests = case
                    .support_bundles_requested
                    .iter()
                    .map(|r| r.id)
                    .filter(|id| {
                        !self.marked_support_bundle_requests.contains(id)
                    })
                    .collect::<BTreeSet<_>>();
                let has_outstanding_work = !unmarked_ereports.is_empty()
                    || !unmarked_alert_requests.is_empty()
                    || !unmarked_support_bundle_requests.is_empty();
                if has_outstanding_work {
                    // The case has ereport, alert, and/or bundle work
                    // remaining, so carry it forward intact. We keep even
                    // already-satisfied alert and support bundle requests
                    // around: the marker prevents their resurrection, and
                    // pruning them would force a generation bump that buys us
                    // only earlier marker GC.
                    report.closed_cases_copied_forward.insert(
                        case.id,
                        ClosedCaseReport {
                            metadata: case.metadata.clone(),
                            unmarked_ereports,
                            unmarked_alert_requests,
                            unmarked_support_bundle_requests,
                        },
                    );
                    closed_cases_copied_forward
                        .insert_unique(case.clone())
                        .expect(
                            "the case UUID is coming from iterating over \
                             another `IdOrdMap`, so it must be unique",
                        );
                } else {
                    // Case has no outstanding work. If it had any alert or
                    // support bundle requests, dropping it removes those ids
                    // from the carry-forward set, so flag the corresponding
                    // generation as changed. Empty request sets make these
                    // `|=`s no-ops.
                    alerts_changed |= !case.alerts_requested.is_empty();
                    support_bundles_changed |=
                        !case.support_bundles_requested.is_empty();
                }
            }
        }
        let input = Input {
            parent_sitrep: self.parent_sitrep.clone(),
            inv: self.inv.clone(),
            new_ereports: self.new_ereports,
            open_cases,
            closed_cases_copied_forward,
            ereporter_restarts: self.ereporter_restarts,
            alerts_changed,
            support_bundles_changed,
            in_service_disks: self.in_service_disks,
            observed_sagas: self.observed_sagas,
        };

        (input, report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SitrepBuilder;
    use crate::test_util::FmTest;
    use nexus_types::alert::AlertClass;
    use nexus_types::alert::test_alerts;
    use nexus_types::fm;
    use nexus_types::fm::case::AlertRequest;
    use nexus_types::fm::case::CaseEreport;
    use nexus_types::fm::case::SupportBundleRequest;
    use nexus_types::fm::ereport::Reporter;
    use nexus_types::fm::{DiagnosisEngineKind, SitrepVersion};
    use nexus_types::inventory::SpType;
    use nexus_types::support_bundle::BundleDataSelection;
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::{
        CaseEreportUuid, CaseUuid, OmicronZoneUuid, SitrepUuid,
    };
    use std::sync::Arc;

    /// Wraps an `fm::Ereport` in a `fm::case::CaseEreport` for insertion into
    /// a case's `ereports` map.
    fn case_ereport(
        ereport: &Arc<fm::Ereport>,
        sitrep_id: SitrepUuid,
    ) -> CaseEreport {
        CaseEreport {
            id: CaseEreportUuid::new_v4(),
            ereport: ereport.clone(),
            assigned_sitrep_id: sitrep_id,
            comment: "test assignment".to_string(),
        }
    }

    /// This test exercises the core filtering and copy-forward logic in
    /// [`super::Builder`] and [`crate::builder::SitrepBuilder`].
    ///
    /// The scenario is:
    ///
    /// - The parent sitrep has three cases:
    ///   1. An open case, with one ereport.
    ///   2. A closed case whose ereport has NOT yet been marked seen in the
    ///      DB (i.e. it will be passed to `add_unmarked_ereports`).
    ///   3. A closed case whose ereport HAS already been marked seen (i.e. it
    ///      will NOT be passed to `add_unmarked_ereports`).
    ///
    /// - Three ereports are passed to `add_unmarked_ereports`:
    ///   1. The ereport from the open case (already in the parent sitrep).
    ///   2. The ereport from the second closed case (already in the parent
    ///      sitrep).
    ///   3. A brand-new ereport that has never appeared in any sitrep.
    #[test]
    fn test_analysis_input_builder_and_sitrep_builder() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_analysis_input_builder_and_sitrep_builder",
        );
        let log = &logctx.log;

        let mut fm_test =
            FmTest::new("test_analysis_input_builder_and_sitrep_builder", log);
        let (example_system, _blueprint) = fm_test.system_builder.build();
        let inv = Arc::new(example_system.collection);

        let now = chrono::Utc::now();
        let mut reporter = fm_test
            .reporters
            .reporter(Reporter::Sp { sp_type: SpType::Sled, slot: 0 });
        let ereport_in_open_case1 =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_in_open_case2 =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_in_closed_unmarked =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_in_closed_marked =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_new =
            Arc::new(reporter.mk_ereport(now, Default::default()));

        // Make a parent sitrep, with four cases:
        //
        // 1. an open case
        let open_case1_id = CaseUuid::new_v4();
        // 2. another open case
        let open_case2_id = CaseUuid::new_v4();
        // 3. a closed case with an unmarked ereport in it,
        let closed_case_with_unmarked_id = CaseUuid::new_v4();
        // 4. a closed case with only marked ereports.
        let closed_case_without_unmarked_id = CaseUuid::new_v4();
        let parent_sitrep_id = SitrepUuid::new_v4();
        let parent_sitrep = {
            let open_case1 = {
                let created_sitrep_id = parent_sitrep_id;
                fm::Case {
                    id: open_case1_id,
                    metadata: fm::case::Metadata {
                        created_sitrep_id,
                        closed_sitrep_id: None,
                        de: DiagnosisEngineKind::PowerShelf,
                        comment: "open case one".to_string(),
                    },
                    ereports: [
                        case_ereport(&ereport_in_open_case1, parent_sitrep_id),
                        case_ereport(
                            &ereport_in_closed_unmarked,
                            created_sitrep_id,
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    alerts_requested: Default::default(),
                    support_bundles_requested: Default::default(),
                    facts: Default::default(),
                }
            };
            let open_case2 = {
                let created_sitrep_id = parent_sitrep_id;
                fm::Case {
                    id: open_case2_id,
                    metadata: fm::case::Metadata {
                        created_sitrep_id,
                        closed_sitrep_id: None,
                        de: DiagnosisEngineKind::PowerShelf,
                        comment: "open case two".to_string(),
                    },
                    ereports: [case_ereport(
                        &ereport_in_open_case2,
                        parent_sitrep_id,
                    )]
                    .into_iter()
                    .collect(),
                    alerts_requested: Default::default(),
                    support_bundles_requested: Default::default(),
                    facts: Default::default(),
                }
            };
            let closed_case_with_unmarked = {
                let created_sitrep_id = SitrepUuid::new_v4();
                fm::Case {
                    id: closed_case_with_unmarked_id,
                    metadata: fm::case::Metadata {
                        created_sitrep_id,
                        closed_sitrep_id: Some(parent_sitrep_id),
                        de: DiagnosisEngineKind::PowerShelf,
                        comment: "closed case, has an unmarked ereport"
                            .to_string(),
                    },
                    ereports: [
                        case_ereport(
                            &ereport_in_closed_unmarked,
                            created_sitrep_id,
                        ),
                        case_ereport(
                            &ereport_in_closed_marked,
                            created_sitrep_id,
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    alerts_requested: Default::default(),
                    support_bundles_requested: Default::default(),
                    facts: Default::default(),
                }
            };
            let closed_case_without_unmarked = {
                let created_sitrep_id = SitrepUuid::new_v4();
                fm::Case {
                    id: closed_case_without_unmarked_id,
                    metadata: fm::case::Metadata {
                        created_sitrep_id,
                        closed_sitrep_id: Some(parent_sitrep_id),
                        de: DiagnosisEngineKind::PowerShelf,
                        comment: "closed case, no unmarked ereports"
                            .to_string(),
                    },
                    ereports: [case_ereport(
                        &ereport_in_closed_marked,
                        created_sitrep_id,
                    )]
                    .into_iter()
                    .collect(),
                    alerts_requested: Default::default(),
                    support_bundles_requested: Default::default(),
                    facts: Default::default(),
                }
            };

            let cases = [
                open_case1,
                open_case2,
                closed_case_with_unmarked,
                closed_case_without_unmarked,
            ]
            .into_iter()
            .collect();

            // ereports_by_id must contain all ereports referenced by cases in the
            // sitrep; add_unmarked_ereports uses this map to detect which ereports
            // have already appeared in the parent sitrep.
            let ereports_by_id = [
                ereport_in_open_case1.clone(),
                ereport_in_open_case2.clone(),
                ereport_in_closed_unmarked.clone(),
                ereport_in_closed_marked.clone(),
            ]
            .into_iter()
            .collect();

            let sitrep = fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: parent_sitrep_id,
                    parent_sitrep_id: Some(SitrepUuid::new_v4()),
                    inv_collection_id: inv.id,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "parent sitrep for test".to_string(),
                    time_created: chrono::Utc::now(),
                    next_inv_min_time_started: inv.time_done,
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id,
            };
            Arc::new((
                SitrepVersion {
                    id: parent_sitrep_id,
                    version: 420,
                    time_made_current: chrono::Utc::now(),
                },
                sitrep,
            ))
        };

        // Build analysis input
        let (input, report) = {
            let mut builder = Input::builder(
                Some(parent_sitrep),
                inv,
                Arc::new(IdOrdMap::new()),
                Arc::new(IdOrdMap::new()),
            )
            .expect("collection start time check should always pass");
            // Pass in four ereports:
            //  - two that are in the open cases of the parent sitrep
            //  - one that is in the (to-be-copied-forward) closed case
            //  - one that is brand-new
            //
            // Notably, `ereport_in_closed_marked` is NOT passed here,
            // simulating that it was already marked seen in the database.
            builder.add_unmarked_ereports([
                (*ereport_in_open_case1).clone(),
                (*ereport_in_open_case2).clone(),
                (*ereport_in_closed_unmarked).clone(),
                (*ereport_new).clone(),
            ]);
            builder.build()
        };
        eprintln!("{}", report.display_multiline(0));

        // Check the "new ereports" in the constructed input.
        assert!(
            input.new_ereports().contains_key(&ereport_new.id),
            "ereport_new should be in new_ereports (it was not in the parent \
             sitrep)"
        );
        assert!(
            !input.new_ereports().contains_key(&ereport_in_open_case1.id),
            "ereport_in_open_case1 should NOT be in new_ereports (it is \
             already associated with an open case in the parent sitrep)"
        );
        assert!(
            !input.new_ereports().contains_key(&ereport_in_open_case2.id),
            "ereport_in_open_case2 should NOT be in new_ereports (it is \
             already associated with an open case in the parent sitrep)"
        );
        assert!(
            !input.new_ereports().contains_key(&ereport_in_closed_unmarked.id),
            "ereport_in_closed_unmarked should NOT be in new_ereports (it is \
             already associated with a closed case in the parent sitrep)"
        );

        assert_eq!(
            input.new_ereports().len(),
            1,
            "exactly one new ereport (ereport_new) should be in new_ereports"
        );

        // Check which closed cases should be copied forward.
        assert!(
            input
                .closed_cases_copied_forward()
                .contains_key(&closed_case_with_unmarked_id),
            "closed_case_with_unmarked should be in closed_cases_copied_forward \
             because it has an ereport that has not yet been marked seen"
        );
        assert!(
            !input
                .closed_cases_copied_forward()
                .contains_key(&closed_case_without_unmarked_id),
            "closed_case_without_unmarked should NOT be in \
             closed_cases_copied_forward because all its ereports have been \
             marked seen"
        );
        assert_eq!(
            input.closed_cases_copied_forward().len(),
            1,
            "exactly one closed case should be copied forward"
        );
        assert!(
            !input.alerts_changed(),
            "the dropped closed case had no alert requests, so dropping it \
             does not change the carry-forward alert set"
        );

        // Check the contents of open cases.
        assert!(
            input.open_cases().contains_key(&open_case1_id),
            "open case 1 from the parent sitrep should be in input.open_cases()"
        );
        assert!(
            input.open_cases().contains_key(&open_case2_id),
            "open case 2 from the parent sitrep should be in input.open_cases()"
        );
        assert_eq!(
            input.open_cases().len(),
            2,
            "exactly two cases should be open"
        );

        // Start building a sitrep...
        let mut sitrep_builder =
            SitrepBuilder::new_with_rng(log, &input, fm_test.sitrep_rng);

        // The open cases from the parent sitrep must be accessible via
        // case_mut() so that the diagnosis engines can update it.
        assert!(
            sitrep_builder.cases.case_mut(&open_case1_id).is_some(),
            "open case 1 should be accessible via case_mut()"
        );
        assert!(
            sitrep_builder.cases.case_mut(&open_case2_id).is_some(),
            "open case 2 should be accessible via case_mut()"
        );
        assert!(
            sitrep_builder
                .cases
                .case_mut(&closed_case_with_unmarked_id)
                .is_none(),
            "the closed_case_with_unmarked should NOT be accessible via \
             case_mut() (closed cases are not open for modification)"
        );
        assert!(
            sitrep_builder
                .cases
                .case_mut(&closed_case_without_unmarked_id)
                .is_none(),
            "the closed_case_without_unmarked should NOT be accessible via \
             case_mut() (closed cases are not open for modification)"
        );
        sitrep_builder.comment_mut().push_str("my cool sitrep");

        let new_case_id = {
            let mut new_case = sitrep_builder.cases.open_case(
                DiagnosisEngineKind::PowerShelf,
                "my cool test case",
            );
            new_case.add_ereport(
                &ereport_new,
                "this ereport is important to the case somehow",
            );
            new_case
                .request_alert(
                    &test_alerts::FooBar(serde_json::json!({"alert": true})),
                    "requesting an alert to tell someone about something",
                )
                .unwrap();
            new_case.request_support_bundle(
                nexus_types::support_bundle::BundleDataSelection::all(),
                "requesting a support bundle",
            );
            *new_case.id()
        };

        // Close open case 2
        sitrep_builder
            .cases
            .case_mut(&open_case2_id)
            .unwrap()
            .close("i'm closing it because i want to");

        // Build the final sitrep
        let (output_sitrep, report) =
            sitrep_builder.build(OmicronZoneUuid::new_v4(), chrono::Utc::now());
        eprintln!("{}", report.display_multiline(0));

        let open_case1 = output_sitrep
            .cases
            .get(&open_case1_id)
            .expect("open case 1 should be in the output sitrep's cases");
        assert!(
            open_case1.is_open(),
            "open case 1 should be open in the output sitrep"
        );

        let open_case2 = output_sitrep
            .cases
            .get(&open_case2_id)
            .expect("open case 2 should be in the output sitrep's cases");
        assert!(
            !open_case2.is_open(),
            "open case 2 should be closed in the output sitrep"
        );

        let new_case = output_sitrep
            .cases
            .get(&new_case_id)
            .expect("new case should be in the output sitrep's cases");
        assert!(
            new_case.is_open(),
            "new case should be open in the output sitrep"
        );
        assert_eq!(
            new_case.alerts_requested.len(),
            1,
            "new case should have the one requested alert"
        );
        assert_eq!(
            new_case.support_bundles_requested.len(),
            1,
            "new case should have the one requested support bundle"
        );
        let bundle_req = new_case
            .support_bundles_requested
            .iter()
            .next()
            .expect("new case should have a support bundle request");
        assert_eq!(bundle_req.comment, "requesting a support bundle");
        assert_eq!(
            bundle_req.requested_sitrep_id, output_sitrep.metadata.id,
            "support bundle request should be tagged with the sitrep ID in \
             which it was requested"
        );

        assert!(
            output_sitrep.cases.contains_key(&closed_case_with_unmarked_id),
            "closed cases with unmarked ereports should be copied forward \
             into the output sitrep"
        );
        assert!(
            !output_sitrep.cases.contains_key(&closed_case_without_unmarked_id),
            "closed cases WITHOUT unmarked ereports should NOT be copied \
            forward into the output sitrep"
        );
        assert_eq!(
            output_sitrep.cases.len(),
            4,
            "the output sitrep should have exactly 4 cases: the open case, \
             the newly-closed case, the closed-but-copied-forward case, and \
             the new case"
        );

        logctx.cleanup_successful();
    }

    /// Helper for the closed-case-filter tests below: build a parent sitrep
    /// containing a single closed `case`, then construct a `Builder` pointing
    /// at it. The caller can optionally add existing alert markers before
    /// calling `build()`.
    fn builder_with_closed_case(case: fm::Case) -> Builder {
        let parent_sitrep_id = case
            .metadata
            .closed_sitrep_id
            .expect("test helper requires a closed case");
        let inv =
            Arc::new(nexus_inventory::CollectionBuilder::new("test").build());
        let parent = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_sitrep_id,
                parent_sitrep_id: None,
                inv_collection_id: inv.id,
                next_inv_min_time_started: inv.time_done,
                creator_id: OmicronZoneUuid::new_v4(),
                comment: String::new(),
                time_created: chrono::Utc::now(),
                alert_generation: Generation::new(),
                support_bundle_generation: Generation::new(),
            },
            cases: [case].into_iter().collect(),
            ereports_by_id: IdOrdMap::new(),
        };
        let parent_version = SitrepVersion {
            id: parent_sitrep_id,
            version: 1,
            time_made_current: chrono::Utc::now(),
        };
        Input::builder(
            Some(Arc::new((parent_version, parent))),
            inv,
            Arc::new(IdOrdMap::new()),
            Arc::new(IdOrdMap::new()),
        )
        .expect("collection start time check should always pass")
    }

    /// All alert requests on a closed case are satisfied and the case has no
    /// unmarked ereports: the case must be dropped entirely, and
    /// `alerts_changed` must flip to `true` because the dropped requests are
    /// leaving the carry-forward set.
    #[test]
    fn build_drops_closed_case_when_all_alerts_satisfied() {
        let alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let parent_sitrep_id = SitrepUuid::new_v4();

        let closed_case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: Some(parent_sitrep_id),
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: String::new(),
            },
            ereports: IdOrdMap::new(),
            alerts_requested: [AlertRequest {
                id: alert_id,
                class: AlertClass::TestFoo,
                version: 0,
                payload: serde_json::json!({}),
                requested_sitrep_id: parent_sitrep_id,
                comment: String::new(),
            }]
            .into_iter()
            .collect(),
            support_bundles_requested: IdOrdMap::new(),
            facts: IdOrdMap::new(),
        };

        let mut builder = builder_with_closed_case(closed_case);
        builder.add_marked_alert_requests([alert_id]);
        let (input, _) = builder.build();

        assert_eq!(input.closed_cases_copied_forward().len(), 0);
        assert!(
            input.alerts_changed(),
            "dropping a closed case removes ids from the request set, so \
             alerts_changed must be true"
        );
    }

    /// One of two alert requests has a marker; the other does not. An
    /// outstanding alert request keeps the closed case alive through
    /// carry-forward until its marker is observed, so the case must be carried
    /// forward intact (both alert requests still present) and `alerts_changed`
    /// must remain `false` (satisfied requests are not pruned: the marker
    /// prevents resurrection).
    #[test]
    fn build_keeps_closed_case_intact_when_not_all_alerts_satisfied() {
        let satisfied_alert_id = AlertUuid::new_v4();
        let unsatisfied_alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let parent_sitrep_id = SitrepUuid::new_v4();

        let alert_request = |id| AlertRequest {
            id,
            class: AlertClass::TestFoo,
            version: 0,
            payload: serde_json::json!({}),
            requested_sitrep_id: parent_sitrep_id,
            comment: String::new(),
        };
        let closed_case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: Some(parent_sitrep_id),
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: String::new(),
            },
            ereports: IdOrdMap::new(),
            alerts_requested: [
                alert_request(satisfied_alert_id),
                alert_request(unsatisfied_alert_id),
            ]
            .into_iter()
            .collect(),
            support_bundles_requested: IdOrdMap::new(),
            facts: IdOrdMap::new(),
        };

        let mut builder = builder_with_closed_case(closed_case);
        builder.add_marked_alert_requests([satisfied_alert_id]);
        let (input, _) = builder.build();

        let carried = input.closed_cases_copied_forward().get(&case_id).expect(
            "one alert request unsatisfied, so case must be carried forward",
        );
        assert_eq!(
            carried.alerts_requested.len(),
            2,
            "satisfied requests are not pruned from the carried-forward case",
        );
        assert!(carried.alerts_requested.contains_key(&satisfied_alert_id));
        assert!(carried.alerts_requested.contains_key(&unsatisfied_alert_id));
        assert!(
            !input.alerts_changed(),
            "case carried forward (not dropped), so alerts_changed must \
             remain false",
        );
    }

    /// All support bundle requests on a closed case are satisfied and the
    /// case has no unmarked ereports: the case must be dropped entirely,
    /// and `support_bundles_changed` must flip to `true` because the
    /// dropped requests are leaving the carry-forward set.
    #[test]
    fn build_drops_closed_case_when_all_bundles_satisfied() {
        let bundle_id = SupportBundleUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let parent_sitrep_id = SitrepUuid::new_v4();

        let closed_case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: Some(parent_sitrep_id),
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: String::new(),
            },
            ereports: IdOrdMap::new(),
            alerts_requested: IdOrdMap::new(),
            support_bundles_requested: [SupportBundleRequest {
                id: bundle_id,
                requested_sitrep_id: parent_sitrep_id,
                data_selection: BundleDataSelection::all(),
                comment: String::new(),
            }]
            .into_iter()
            .collect(),
            facts: IdOrdMap::new(),
        };

        let mut builder = builder_with_closed_case(closed_case);
        builder.add_marked_support_bundle_requests([bundle_id]);
        let (input, _) = builder.build();

        assert_eq!(input.closed_cases_copied_forward().len(), 0);
        assert!(
            input.support_bundles_changed(),
            "dropping a closed case removes ids from the request set, so \
             support_bundles_changed must be true"
        );
    }

    /// One of two support bundle requests has a marker; the other does not.
    /// The closed case must be carried forward intact (both bundle requests
    /// still present) and `support_bundles_changed` must remain `false`.
    /// Satisfied requests are not pruned; the marker prevents resurrection.
    #[test]
    fn build_keeps_closed_case_intact_when_not_all_bundles_satisfied() {
        let satisfied_bundle_id = SupportBundleUuid::new_v4();
        let unsatisfied_bundle_id = SupportBundleUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let parent_sitrep_id = SitrepUuid::new_v4();

        let bundle_request = |id| SupportBundleRequest {
            id,
            requested_sitrep_id: parent_sitrep_id,
            data_selection: BundleDataSelection::all(),
            comment: String::new(),
        };
        let closed_case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: Some(parent_sitrep_id),
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: String::new(),
            },
            ereports: IdOrdMap::new(),
            alerts_requested: IdOrdMap::new(),
            support_bundles_requested: [
                bundle_request(satisfied_bundle_id),
                bundle_request(unsatisfied_bundle_id),
            ]
            .into_iter()
            .collect(),
            facts: IdOrdMap::new(),
        };

        let mut builder = builder_with_closed_case(closed_case);
        builder.add_marked_support_bundle_requests([satisfied_bundle_id]);
        let (input, _) = builder.build();

        let carried = input.closed_cases_copied_forward().get(&case_id).expect(
            "one bundle request unsatisfied, so case must be carried \
                 forward",
        );
        assert_eq!(
            carried.support_bundles_requested.len(),
            2,
            "satisfied requests are not pruned from the carried-forward case",
        );
        assert!(
            carried
                .support_bundles_requested
                .contains_key(&satisfied_bundle_id)
        );
        assert!(
            carried
                .support_bundles_requested
                .contains_key(&unsatisfied_bundle_id)
        );
        assert!(
            !input.support_bundles_changed(),
            "case carried forward (not dropped), so support_bundles_changed \
             must remain false",
        );
    }
}
