// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sitrep builder

use crate::analysis_input;
use iddqd::IdOrdMap;
use nexus_types::fm;
use nexus_types::inventory;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;
use slog::Logger;

mod case;
pub use case::{AllCases, CaseBuilder};
pub(crate) mod rng;
pub use rng::SitrepBuilderRng;

#[derive(Debug)]
pub struct SitrepBuilder<'a> {
    pub log: Logger,
    pub inventory: &'a inventory::Collection,
    pub parent_sitrep: Option<&'a fm::Sitrep>,
    pub sitrep_id: SitrepUuid,
    pub cases: case::AllCases,
    /// The analysis input this builder was constructed from; `cases` is
    /// seeded from its open cases.
    input: &'a analysis_input::Input,
    closed_cases_copied_forward: &'a IdOrdMap<fm::Case>,
    alerts_changed: bool,
    support_bundles_changed: bool,
    comment: String,
}

/// Generation counters inherited from the parent sitrep, or the initial
/// generation on the first-ever sitrep. Read once by [`SitrepBuilder::build`]
/// to seed the stamped generations on the child.
#[derive(Debug, Clone, Copy)]
struct ParentGenerations {
    alert_generation: Generation,
    support_bundle_generation: Generation,
}

impl ParentGenerations {
    /// Initial generations for the first sitrep, which has no parent.
    fn new() -> Self {
        Self {
            alert_generation: Generation::new(),
            support_bundle_generation: Generation::new(),
        }
    }
}

impl<'a> SitrepBuilder<'a> {
    pub fn new(log: &Logger, inputs: &'a analysis_input::Input) -> Self {
        Self::new_with_rng(log, inputs, SitrepBuilderRng::from_entropy())
    }

    pub fn new_with_rng(
        log: &Logger,
        inputs: &'a analysis_input::Input,
        mut rng: SitrepBuilderRng,
    ) -> Self {
        let parent_sitrep = inputs.parent_sitrep();
        let inventory = inputs.inventory();

        // TODO(eliza): should the RNG also be seeded with the parent sitrep
        // UUID and/or the Omicron zone UUID? Hmm.
        let sitrep_id = rng.sitrep_id();
        let log = log.new(slog::o!(
            "sitrep_id" => format!("{sitrep_id:?}"),
            "parent_sitrep_id" => format!("{:?}", parent_sitrep.as_ref().map(|s| s.id())),
            "inv_collection_id" => format!("{:?}", inventory.id),
        ));

        let cases = case::AllCases::new(log.clone(), sitrep_id, inputs, rng);
        let closed_cases_copied_forward = inputs.closed_cases_copied_forward();

        slog::info!(
            &log,
            "building sitrep {sitrep_id:?}";
            "existing_open_cases" => cases.len(),
            "closed_cases_copied_forward" => closed_cases_copied_forward.len(),
        );

        SitrepBuilder {
            log,
            sitrep_id,
            inventory,
            parent_sitrep,
            comment: String::new(),
            input: inputs,
            closed_cases_copied_forward,
            alerts_changed: inputs.alerts_changed(),
            support_bundles_changed: inputs.support_bundles_changed(),
            cases,
        }
    }

    /// The analysis input this builder was constructed from.
    ///
    /// The returned reference borrows the input (lifetime `'a`), not the
    /// builder, so callers may hold it while mutating the builder.
    pub fn input(&self) -> &'a analysis_input::Input {
        self.input
    }

    pub fn comment(&self) -> &str {
        &self.comment
    }

    pub fn comment_mut(&mut self) -> &mut String {
        &mut self.comment
    }

    /// The parent sitrep's generations, or the initial generation on the
    /// first-ever sitrep.
    fn parent_generations(&self) -> ParentGenerations {
        self.parent_sitrep
            .map(|p| ParentGenerations {
                alert_generation: p.metadata.alert_generation,
                support_bundle_generation: p.metadata.support_bundle_generation,
            })
            .unwrap_or_else(ParentGenerations::new)
    }

    pub fn build(
        self,
        creator_id: OmicronZoneUuid,
        time_created: chrono::DateTime<chrono::Utc>,
    ) -> (fm::Sitrep, fm::analysis_reports::AnalysisReport) {
        let parent = self.parent_generations();
        let alert_generation =
            if self.cases.alert_set_changed() || self.alerts_changed {
                parent.alert_generation.next()
            } else {
                parent.alert_generation
            };
        let support_bundle_generation =
            if self.cases.support_bundle_set_changed()
                || self.support_bundles_changed
            {
                parent.support_bundle_generation.next()
            } else {
                parent.support_bundle_generation
            };

        let mut ereports_by_id = iddqd::IdOrdMap::new();
        let mut report_cases = IdOrdMap::new();
        let cases = self
            .cases
            .cases
            .into_iter()
            // Note that entries are only pushed to `report_cases` for open
            // cases, as the closed cases which are just being copied forward
            // into the next sitrep have, by construction, not been changed in
            // this builder, since they weren't exposed for modification by the
            // builder API. Thus, we really don't have anything new to say about
            // them that's worth including in the report, as the fact that they
            // were copied forward will be recorded in the input report.
            .map(|case_builder| {
                let (case, report) = case_builder.build();
                report_cases.insert_unique(report).expect(
                    "we are iterating over an IdOrdMap, so the entries \
                     should already be unique",
                );
                case
            })
            .chain(self.closed_cases_copied_forward.iter().cloned())
            .inspect(|case| {
                ereports_by_id
                    .extend(case.ereports.iter().map(|ce| ce.ereport.clone()));
            })
            .collect();
        let report = fm::analysis_reports::AnalysisReport {
            sitrep_id: self.sitrep_id,
            comment: self.comment.clone(),
            cases: report_cases,
        };
        let sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: self.sitrep_id,
                parent_sitrep_id: self.parent_sitrep.map(|s| s.metadata.id),
                inv_collection_id: self.inventory.id,
                creator_id,
                comment: self.comment,
                time_created,
                // When creating a new sitrep that is a child of this sitrep,
                // the input inventory collection must either be the same
                // inventory as this sitrep, or have started after this sitrep's
                // inventory collection ended.
                next_inv_min_time_started: self.inventory.time_done,
                alert_generation,
                support_bundle_generation,
            },
            cases,
            ereports_by_id,
        };
        (sitrep, report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_input::Input;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::alert::AlertClass;
    use nexus_types::alert::test_alerts;
    use nexus_types::fm;
    use nexus_types::fm::SitrepVersion;
    use nexus_types::fm::case::AlertRequest;
    use nexus_types::fm::case::SupportBundleRequest;
    use nexus_types::inventory;
    use nexus_types::support_bundle::BundleDataSelection;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SupportBundleUuid;
    use std::sync::Arc;

    /// Build an empty inventory `Collection`. The id is irrelevant to these
    /// tests, so we let `CollectionBuilder` pick one.
    fn make_collection() -> Arc<inventory::Collection> {
        Arc::new(CollectionBuilder::new("test").build())
    }

    /// Finalize the builder with throwaway values for `creator_id` and
    /// `time_created` (which these tests don't exercise).
    fn build_sitrep(builder: SitrepBuilder<'_>) -> fm::Sitrep {
        let (sitrep, _) =
            builder.build(OmicronZoneUuid::new_v4(), chrono::Utc::now());
        sitrep
    }

    /// Build a minimal `Input` with no parent sitrep and an empty inventory.
    fn make_input() -> Input {
        let (input, _) =
            Input::builder(None, make_collection(), Arc::new(IdOrdMap::new()))
                .expect("no parent sitrep, so builder should succeed")
                .build();
        input
    }

    /// Build a minimal `Input` whose parent sitrep is stamped with the given
    /// generations, simulating a prior builder run that bumped them. The
    /// parent and child share an inventory so the freshness check trivially
    /// passes.
    fn make_input_with_parent_generations(gens: ParentGenerations) -> Input {
        let inv = make_collection();
        let parent_id = SitrepUuid::new_v4();
        let parent = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_id,
                parent_sitrep_id: None,
                inv_collection_id: inv.id,
                next_inv_min_time_started: inv.time_done,
                creator_id: OmicronZoneUuid::new_v4(),
                comment: String::new(),
                time_created: chrono::Utc::now(),
                alert_generation: gens.alert_generation,
                support_bundle_generation: gens.support_bundle_generation,
            },
            cases: IdOrdMap::new(),
            ereports_by_id: IdOrdMap::new(),
        };
        let parent_version = SitrepVersion {
            id: parent_id,
            version: 1,
            time_made_current: chrono::Utc::now(),
        };
        let (input, _) = Input::builder(
            Some(Arc::new((parent_version, parent))),
            inv,
            Arc::new(IdOrdMap::new()),
        )
        .expect("parent and child share an inventory")
        .build();
        input
    }

    #[test]
    fn first_sitrep_with_no_requests_stamps_initial_generation() {
        let logctx = dev::test_setup_log(
            "first_sitrep_with_no_requests_stamps_initial_generation",
        );
        let inputs = make_input();
        let sitrep = build_sitrep(SitrepBuilder::new(&logctx.log, &inputs));
        assert_eq!(sitrep.metadata.alert_generation, Generation::new());
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::new()
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn first_sitrep_with_alert_request_bumps_alert_generation() {
        let logctx = dev::test_setup_log(
            "first_sitrep_with_alert_request_bumps_alert_generation",
        );
        let inputs = make_input();
        let mut builder = SitrepBuilder::new(&logctx.log, &inputs);
        {
            let mut case =
                builder.cases.open_case(fm::DiagnosisEngineKind::PowerShelf);
            case.request_alert(&test_alerts::Foo(serde_json::json!({})), "")
                .unwrap();
        }
        let sitrep = build_sitrep(builder);
        assert_eq!(sitrep.metadata.alert_generation, Generation::new().next());
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::new()
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn first_sitrep_with_support_bundle_request_bumps_bundle_generation() {
        let logctx = dev::test_setup_log(
            "first_sitrep_with_support_bundle_request_bumps_bundle_generation",
        );
        let inputs = make_input();
        let mut builder = SitrepBuilder::new(&logctx.log, &inputs);
        {
            let mut case =
                builder.cases.open_case(fm::DiagnosisEngineKind::PowerShelf);
            case.request_support_bundle(Default::default(), "");
        }
        let sitrep = build_sitrep(builder);
        assert_eq!(sitrep.metadata.alert_generation, Generation::new());
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::new().next()
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn generation_stable_when_no_new_requests_with_parent() {
        let logctx = dev::test_setup_log(
            "generation_stable_when_no_new_requests_with_parent",
        );
        let inputs = make_input_with_parent_generations(ParentGenerations {
            alert_generation: Generation::from_u32(1),
            support_bundle_generation: Generation::from_u32(2),
        });
        let sitrep = build_sitrep(SitrepBuilder::new(&logctx.log, &inputs));
        assert_eq!(sitrep.metadata.alert_generation, Generation::from_u32(1));
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::from_u32(2)
        );
        logctx.cleanup_successful();
    }

    /// Verifies that the alert bump is relative to the parent sitrep's
    /// generation (not restarted from a fixed initial value), and that the
    /// support bundle generation is left untouched.
    #[test]
    fn child_sitrep_with_new_alert_bumps_alert_generation_only() {
        let logctx = dev::test_setup_log(
            "child_sitrep_with_new_alert_bumps_alert_generation_only",
        );
        let inputs = make_input_with_parent_generations(ParentGenerations {
            alert_generation: Generation::from_u32(5),
            support_bundle_generation: Generation::from_u32(7),
        });
        let mut builder = SitrepBuilder::new(&logctx.log, &inputs);
        {
            let mut case =
                builder.cases.open_case(fm::DiagnosisEngineKind::PowerShelf);
            case.request_alert(&test_alerts::Foo(serde_json::json!({})), "")
                .unwrap();
        }
        let sitrep = build_sitrep(builder);
        assert_eq!(sitrep.metadata.alert_generation, Generation::from_u32(6));
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::from_u32(7)
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn child_sitrep_with_new_bundle_bumps_bundle_generation_only() {
        let logctx = dev::test_setup_log(
            "child_sitrep_with_new_bundle_bumps_bundle_generation_only",
        );
        let inputs = make_input_with_parent_generations(ParentGenerations {
            alert_generation: Generation::from_u32(5),
            support_bundle_generation: Generation::from_u32(7),
        });
        let mut builder = SitrepBuilder::new(&logctx.log, &inputs);
        {
            let mut case =
                builder.cases.open_case(fm::DiagnosisEngineKind::PowerShelf);
            case.request_support_bundle(Default::default(), "");
        }
        let sitrep = build_sitrep(builder);
        assert_eq!(sitrep.metadata.alert_generation, Generation::from_u32(5));
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::from_u32(8)
        );
        logctx.cleanup_successful();
    }

    /// Verifies that the bump is relative to the parent sitrep's generation,
    /// not restarted from a fixed initial value.
    #[test]
    fn child_sitrep_with_both_new_requests_bumps_both_generations() {
        let logctx = dev::test_setup_log(
            "child_sitrep_with_both_new_requests_bumps_both_generations",
        );
        let inputs = make_input_with_parent_generations(ParentGenerations {
            alert_generation: Generation::from_u32(5),
            support_bundle_generation: Generation::from_u32(7),
        });
        let mut builder = SitrepBuilder::new(&logctx.log, &inputs);
        {
            let mut case =
                builder.cases.open_case(fm::DiagnosisEngineKind::PowerShelf);
            case.request_alert(&test_alerts::Foo(serde_json::json!({})), "")
                .unwrap();
            case.request_support_bundle(Default::default(), "");
        }
        let sitrep = build_sitrep(builder);
        assert_eq!(sitrep.metadata.alert_generation, Generation::from_u32(6));
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::from_u32(8)
        );
        logctx.cleanup_successful();
    }

    /// A closed case with an outstanding alert request becomes "satisfied" via
    /// marker presence, carry-forward drops it, and alert_generation bumps even
    /// though no open-case builder mutations happened.
    #[test]
    fn carry_forward_drop_bumps_alert_generation() {
        let logctx =
            dev::test_setup_log("carry_forward_drop_bumps_alert_generation");
        let inv = make_collection();
        let alert_id = AlertUuid::new_v4();
        let case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let parent_id = SitrepUuid::new_v4();

        let closed_case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_id,
                closed_sitrep_id: Some(parent_id),
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: String::new(),
            },
            ereports: IdOrdMap::new(),
            alerts_requested: [AlertRequest {
                id: alert_id,
                class: AlertClass::TestFoo,
                version: 0,
                payload: serde_json::json!({}),
                requested_sitrep_id: parent_id,
                comment: String::new(),
            }]
            .into_iter()
            .collect(),
            support_bundles_requested: IdOrdMap::new(),
            facts: IdOrdMap::new(),
        };

        let parent = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_id,
                parent_sitrep_id: None,
                inv_collection_id: inv.id,
                next_inv_min_time_started: inv.time_done,
                creator_id: OmicronZoneUuid::new_v4(),
                comment: String::new(),
                time_created: chrono::Utc::now(),
                alert_generation: Generation::new(),
                support_bundle_generation: Generation::new(),
            },
            cases: [closed_case].into_iter().collect(),
            ereports_by_id: IdOrdMap::new(),
        };
        let parent_version = SitrepVersion {
            id: parent_id,
            version: 1,
            time_made_current: chrono::Utc::now(),
        };
        let mut builder_inputs = crate::analysis_input::Input::builder(
            Some(Arc::new((parent_version, parent))),
            inv,
            Arc::new(IdOrdMap::new()),
        )
        .unwrap();
        // Marker exists, so carry-forward will drop the case.
        builder_inputs.add_marked_alert_requests([alert_id]);
        let (input, _) = builder_inputs.build();

        let sitrep = build_sitrep(SitrepBuilder::new(&logctx.log, &input));
        assert_eq!(
            sitrep.metadata.alert_generation,
            Generation::new().next(),
            "dropping a case with alerts from the set of closed cases being \
             carried forwards must bump alert_generation past the parent's"
        );
        logctx.cleanup_successful();
    }

    /// A closed case with an outstanding support-bundle request becomes
    /// "satisfied" via marker presence, carry-forward drops it, and
    /// support_bundle_generation bumps even though no open-case builder
    /// mutations happened.
    #[test]
    fn carry_forward_drop_bumps_support_bundle_generation() {
        let logctx = dev::test_setup_log(
            "carry_forward_drop_bumps_support_bundle_generation",
        );
        let inv = make_collection();
        let bundle_id = SupportBundleUuid::new_v4();
        let case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let parent_id = SitrepUuid::new_v4();

        let closed_case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_id,
                closed_sitrep_id: Some(parent_id),
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: String::new(),
            },
            ereports: IdOrdMap::new(),
            alerts_requested: IdOrdMap::new(),
            support_bundles_requested: [SupportBundleRequest {
                id: bundle_id,
                requested_sitrep_id: parent_id,
                data_selection: BundleDataSelection::default(),
                comment: String::new(),
            }]
            .into_iter()
            .collect(),
            facts: IdOrdMap::new(),
        };

        let parent = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_id,
                parent_sitrep_id: None,
                inv_collection_id: inv.id,
                next_inv_min_time_started: inv.time_done,
                creator_id: OmicronZoneUuid::new_v4(),
                comment: String::new(),
                time_created: chrono::Utc::now(),
                alert_generation: Generation::new(),
                support_bundle_generation: Generation::new(),
            },
            cases: [closed_case].into_iter().collect(),
            ereports_by_id: IdOrdMap::new(),
        };
        let parent_version = SitrepVersion {
            id: parent_id,
            version: 1,
            time_made_current: chrono::Utc::now(),
        };
        let mut builder_inputs = crate::analysis_input::Input::builder(
            Some(Arc::new((parent_version, parent))),
            inv,
            Arc::new(IdOrdMap::new()),
        )
        .unwrap();
        // Marker exists, so carry-forward will drop the case.
        builder_inputs.add_marked_support_bundle_requests([bundle_id]);
        let (input, _) = builder_inputs.build();

        let sitrep = build_sitrep(SitrepBuilder::new(&logctx.log, &input));
        assert_eq!(
            sitrep.metadata.support_bundle_generation,
            Generation::new().next(),
            "dropping a case with support bundle requests from the set of \
             closed cases being carried forwards must bump \
             support_bundle_generation past the parent's"
        );
        logctx.cleanup_successful();
    }
}
