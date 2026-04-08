// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inputs to fault management analysis.

use iddqd::IdOrdMap;
use nexus_types::fm::analysis_reports::ClosedCaseReport;
use nexus_types::fm::{self, Sitrep, SitrepVersion};
use nexus_types::inventory;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

pub use nexus_types::fm::analysis_reports::AnalysisInputReport as Report;

/// A complete set of inputs to a fault management analysis phase.
///
/// This struct bundles together all the inputs to analysis, including:
///
/// - The [parent sitrep](Input::parent_sitrep)
/// - The current [inventory collection](Input::inventory)
/// - Any [new ereports](Input::new_ereports) which were received since when
///   the parent sitrep was produced
/// - The set of [cases](Input::cases) which must be copied forwards into
///   the next sitrep
///
/// This type represents the outputs of the analysis preparation phase. Once it
/// is constructed, the inputs are immutable and cannot be modified. To
/// construct a new `Input` as part of a preparaation phase, use
/// [`Input::builder`].
pub struct Input {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    /// Ereports which are new and should be input to analysis in the next
    /// sitrep.
    new_ereports: IdOrdMap<fm::Ereport>,
    open_cases: IdOrdMap<fm::Case>,
    closed_cases_copied_forward: IdOrdMap<fm::Case>,
}

impl Input {
    pub fn parent_sitrep(&self) -> Option<&Sitrep> {
        self.parent_sitrep.as_ref().map(|s| &s.1)
    }

    pub fn inventory(&self) -> &inventory::Collection {
        &self.inv
    }

    pub fn new_ereports(&self) -> &IdOrdMap<fm::Ereport> {
        &self.new_ereports
    }

    pub fn cases(&self) -> &IdOrdMap<fm::Case> {
        &self.open_cases
    }

    pub(crate) fn closed_cases_copied_forward(&self) -> &IdOrdMap<fm::Case> {
        &self.closed_cases_copied_forward
    }

    /// Returns a [`Builder`] for constructing a new `Input` from the provided
    /// `parent_sitrep` and inventory collection.
    pub fn builder(
        parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
        inv: Arc<inventory::Collection>,
    ) -> Builder {
        Builder {
            parent_sitrep,
            inv,
            new_ereports: IdOrdMap::default(),
            unmarked_seen_ereports: BTreeSet::default(),
        }
    }
}

#[must_use]
pub struct Builder {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    /// Ereports which are new and should be input to analysis in the next
    /// sitrep.
    new_ereports: IdOrdMap<fm::Ereport>,

    /// The IDs of any ereports which have been included in the parent sitrep,
    /// but which have *not* yet been marked as seen in the database.
    ///
    /// These must be tracked in order to determine which closed cases must be
    /// copied forwards due to containing unmarked ereports.
    unmarked_seen_ereports: BTreeSet<fm::EreportId>,
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
                let id = ereport.id();
                if sitrep.ereports_by_id.contains_key(&id) {
                    self.unmarked_seen_ereports.insert(*id);
                    return None;
                }
            }

            Some(ereport)
        }))
    }

    pub fn num_ereports(&self) -> usize {
        self.new_ereports.len()
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
            new_ereport_ids: self
                .new_ereports
                .iter()
                .map(|e| *e.id())
                .collect(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
        };

        // Determine which cases must be copied forwards into the next sitrep.
        // Cases from the parent sitrep should be copied forwards if:
        // - The case is still open
        let mut open_cases = IdOrdMap::new();
        // - The case has been closed, but it contains an ereport which has not
        //   yet been marked as "seen" in the database.
        let mut closed_cases_copied_forward = IdOrdMap::new();
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
                if !unmarked_ereports.is_empty() {
                    report.closed_cases_copied_forward.insert(
                        case.id,
                        ClosedCaseReport {
                            metadata: case.metadata.clone(),
                            unmarked_ereports,
                        },
                    );
                    closed_cases_copied_forward.insert_unique(case.clone()).expect(
                        "the case UUID is coming from iterating over another \
                         `IdOrdMap`, so it must be unique",
                    );
                }
            }
        }
        let input = Input {
            parent_sitrep: self.parent_sitrep.clone(),
            inv: self.inv.clone(),
            new_ereports: self.new_ereports,
            open_cases,
            closed_cases_copied_forward,
        };

        (input, report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SitrepBuilder;
    use crate::test_util::FmTest;
    use nexus_types::fm;
    use nexus_types::fm::case::CaseEreport;
    use nexus_types::fm::ereport::Reporter;
    use nexus_types::fm::{DiagnosisEngineKind, SitrepVersion};
    use nexus_types::inventory::SpType;
    use omicron_uuid_kinds::{
        CaseEreportUuid, CaseUuid, CollectionUuid, OmicronZoneUuid, SitrepUuid,
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
        let ereport_in_open_case =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_in_closed_unmarked =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_in_closed_marked =
            Arc::new(reporter.mk_ereport(now, Default::default()));
        let ereport_new =
            Arc::new(reporter.mk_ereport(now, Default::default()));

        // Make a parent sitrep, with three cases:
        //
        // 1. an open case
        let open_case_id = CaseUuid::new_v4();
        // 2. a closed case with an unmarked ereport in it,
        let closed_case_with_unmarked_id = CaseUuid::new_v4();
        // 3. a closed case with only marked ereports.
        let closed_case_without_unmarked_id = CaseUuid::new_v4();
        let parent_sitrep_id = SitrepUuid::new_v4();
        let parent_sitrep = {
            let open_case = {
                let created_sitrep_id = parent_sitrep_id;
                fm::Case {
                    id: open_case_id,
                    metadata: fm::case::Metadata {
                        created_sitrep_id,
                        closed_sitrep_id: None,
                        de: DiagnosisEngineKind::PowerShelf,
                        comment: "open case".to_string(),
                    },
                    ereports: [
                        case_ereport(&ereport_in_open_case, parent_sitrep_id),
                        case_ereport(
                            &ereport_in_closed_unmarked,
                            created_sitrep_id,
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    alerts_requested: Default::default(),
                    support_bundles_requested: Default::default(),
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
                }
            };

            let cases = [
                open_case,
                closed_case_with_unmarked,
                closed_case_without_unmarked,
            ]
            .into_iter()
            .collect();

            // ereports_by_id must contain all ereports referenced by cases in the
            // sitrep — add_unmarked_ereports uses this map to detect which ereports
            // have already appeared in the parent sitrep.
            let ereports_by_id = [
                ereport_in_open_case.clone(),
                ereport_in_closed_unmarked.clone(),
                ereport_in_closed_marked.clone(),
            ]
            .into_iter()
            .collect();

            let sitrep = fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: parent_sitrep_id,
                    parent_sitrep_id: Some(SitrepUuid::new_v4()),
                    inv_collection_id: CollectionUuid::new_v4(),
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "parent sitrep for test".to_string(),
                    time_created: chrono::Utc::now(),
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
            let mut builder = Input::builder(Some(parent_sitrep), inv);
            // Pass in three ereports:
            //  - one that is in the open case of the parent sitrep
            //  - one that is in the (to-be-copied-forward) closed case
            //  - one that is brand-new
            //
            // Notably, `ereport_in_closed_marked` is NOT passed here,
            // simulating that it was already marked seen in the database.
            builder.add_unmarked_ereports([
                (*ereport_in_open_case).clone(),
                (*ereport_in_closed_unmarked).clone(),
                (*ereport_new).clone(),
            ]);
            builder.build()
        };
        dbg!(report);

        // Check the "new ereports" in the constructed input.
        assert!(
            input.new_ereports().contains_key(ereport_new.id()),
            "ereport_new should be in new_ereports (it was not in the parent \
             sitrep)"
        );
        assert!(
            !input.new_ereports().contains_key(ereport_in_open_case.id()),
            "ereport_in_open_case should NOT be in new_ereports (it is \
             already associated with an open case in the parent sitrep)"
        );
        assert!(
            !input.new_ereports().contains_key(ereport_in_closed_unmarked.id()),
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

        // Check the contents of open cases.
        assert!(
            input.cases().contains_key(&open_case_id),
            "the open case from the parent sitrep should be in input.cases()"
        );
        assert_eq!(input.cases().len(), 1, "exactly one case should be open");

        // Start building a sitrep...
        let mut sitrep_builder =
            SitrepBuilder::new_with_rng(log, &input, fm_test.sitrep_rng);

        // The open case from the parent sitrep must be accessible via
        // case_mut() so that the diagnosis engine can update it.
        assert!(
            sitrep_builder.cases.case_mut(&open_case_id).is_some(),
            "the open case should be accessible via case_mut()"
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

        // Build the final sitrep
        let output_sitrep = dbg!(
            sitrep_builder.build(OmicronZoneUuid::new_v4(), chrono::Utc::now())
        );

        assert!(
            output_sitrep.cases.contains_key(&open_case_id),
            "open case should be in the output sitrep's cases"
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
            2,
            "the output sitrep should have exactly 2 cases: the open case and \
             the closed-but-copied-forward case"
        );

        logctx.cleanup_successful();
    }
}
