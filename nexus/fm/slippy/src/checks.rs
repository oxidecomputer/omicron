// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks over a single sitrep

mod physical_disk;

use crate::slippy::CaseKind;
use crate::slippy::Severity;
use crate::slippy::SitrepKind;
use crate::slippy::Slippy;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseEreportUuid;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::FactUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Run every check that can be evaluated from this sitrep alone, without
/// reference to its parent.
pub(crate) fn perform_single_sitrep_checks(slippy: &mut Slippy<'_>) {
    check_ereport_index(slippy);
    check_cross_case_id_uniqueness(slippy);
    check_fact_payload_engines(slippy);
    physical_disk::check_physical_disk_cases(slippy);
}

/// `ereports_by_id` is a derived index of every ereport referenced by a case.
/// Check that it is exactly that: every case ereport appears in the index
/// with identical content, and every index entry is referenced by some case.
fn check_ereport_index(slippy: &mut Slippy<'_>) {
    let sitrep = slippy.sitrep();
    let mut referenced = BTreeSet::new();
    for case in &sitrep.cases {
        for case_ereport in &case.ereports {
            let ereport_id = *case_ereport.ereport_id();
            referenced.insert(ereport_id);
            match sitrep.ereports_by_id.get(&ereport_id) {
                None => {
                    slippy.push_sitrep_note(
                        Severity::Fatal,
                        SitrepKind::EreportMissingFromIndex {
                            case_id: case.id,
                            ereport_id,
                        },
                    );
                }
                // Compare contents: a sitrep loaded from the database can hold
                // distinct allocations, and two cases can hold different
                // ereports under the same ID.
                Some(indexed) if **indexed != *case_ereport.ereport => {
                    slippy.push_sitrep_note(
                        Severity::Fatal,
                        SitrepKind::IndexedEreportContentMismatch {
                            case_id: case.id,
                            ereport_id,
                        },
                    );
                }
                Some(_) => {}
            }
        }
    }
    for indexed in &sitrep.ereports_by_id {
        if !referenced.contains(&indexed.id) {
            slippy.push_sitrep_note(
                Severity::Fatal,
                SitrepKind::OrphanedIndexedEreport { ereport_id: indexed.id },
            );
        }
    }
}

/// Ereport assignment, fact, alert request, and support bundle request IDs
/// must be unique across the whole sitrep: each becomes a database primary
/// key, and the alert/support-bundle IDs additionally key rendezvous work.
/// Uniqueness within one case is already guaranteed by the map keying, so
/// only cross-case collisions are checked for.
fn check_cross_case_id_uniqueness(slippy: &mut Slippy<'_>) {
    let mut ereport_ids: BTreeMap<CaseEreportUuid, CaseUuid> = BTreeMap::new();
    let mut fact_ids: BTreeMap<FactUuid, CaseUuid> = BTreeMap::new();
    let mut alert_ids: BTreeMap<AlertUuid, CaseUuid> = BTreeMap::new();
    let mut bundle_ids: BTreeMap<SupportBundleUuid, CaseUuid> = BTreeMap::new();

    let mut notes = Vec::new();
    for case in &slippy.sitrep().cases {
        for case_ereport in &case.ereports {
            if let Some(case1) = ereport_ids.insert(case_ereport.id, case.id)
                && case1 != case.id
            {
                notes.push(SitrepKind::DuplicateCaseEreportId {
                    id: case_ereport.id,
                    case1,
                    case2: case.id,
                });
            }
        }
        for fact in &case.facts {
            if let Some(case1) = fact_ids.insert(fact.metadata.id, case.id)
                && case1 != case.id
            {
                notes.push(SitrepKind::DuplicateFactId {
                    fact_id: fact.metadata.id,
                    case1,
                    case2: case.id,
                });
            }
        }
        for alert in &case.alerts_requested {
            if let Some(case1) = alert_ids.insert(alert.id, case.id)
                && case1 != case.id
            {
                notes.push(SitrepKind::DuplicateAlertId {
                    alert_id: alert.id,
                    case1,
                    case2: case.id,
                });
            }
        }
        for bundle in &case.support_bundles_requested {
            if let Some(case1) = bundle_ids.insert(bundle.id, case.id)
                && case1 != case.id
            {
                notes.push(SitrepKind::DuplicateSupportBundleId {
                    bundle_id: bundle.id,
                    case1,
                    case2: case.id,
                });
            }
        }
    }
    for kind in notes {
        slippy.push_sitrep_note(Severity::Fatal, kind);
    }
}

/// Every fact on every case must carry the payload variant owned by the
/// case's diagnosis engine. This applies to closed cases too: a closed case
/// may still have rendezvous work pending, so its data validity matters.
/// On a closed case the violation is reported as `Quarantined` rather than
/// `Fatal`, since the engine contains a corrupt case by closing it.
fn check_fact_payload_engines(slippy: &mut Slippy<'_>) {
    let mut notes = Vec::new();
    for case in &slippy.sitrep().cases {
        let severity = if case.is_open() {
            Severity::Fatal
        } else {
            Severity::Quarantined
        };
        for fact in &case.facts {
            let payload_de = fact.payload.engine();
            if payload_de != case.metadata.de {
                notes.push((
                    case.id,
                    severity,
                    CaseKind::ForeignFactPayload {
                        fact_id: fact.metadata.id,
                        case_de: case.metadata.de,
                        payload_de,
                    },
                ));
            }
        }
    }
    for (case_id, severity, kind) in notes {
        slippy.push_case_note(case_id, severity, kind);
    }
}

/// Fixtures shared between this module's tests and the per-engine check
/// tests in submodules.
#[cfg(test)]
pub(crate) mod test_helpers {
    use crate::report::SlippyReportSortKey;
    use crate::slippy::CaseKind;
    use crate::slippy::Kind;
    use crate::slippy::Note;
    use crate::slippy::Severity;
    use crate::slippy::SitrepKind;
    use crate::slippy::Slippy;
    use nexus_fm::test_util::{
        FmTest, build_input, mk_in_service, run_analyze, set_health,
    };
    use nexus_types::fm;
    use nexus_types::fm::Sitrep;
    use nexus_types::inventory::ZpoolHealth;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::ZpoolUuid;

    /// Produce a clean sitrep the way the diagnosis engines do: build the
    /// example system, degrade one zpool, and run analysis. The result has
    /// one open physical-disk case carrying one `ZpoolUnhealthy` fact.
    pub(crate) fn clean_sitrep(
        test_name: &'static str,
    ) -> (dev::LogContext, Sitrep) {
        let (fm_test, logctx) = FmTest::new_with_logctx(test_name);
        let (example, _bp) = fm_test.system_builder.build();
        let mut collection = example.collection;
        let zpools: Vec<ZpoolUuid> = collection
            .sled_agents
            .iter()
            .flat_map(|sa| sa.zpools.iter().map(|z| z.id))
            .collect();
        assert!(
            !zpools.is_empty(),
            "example system should have at least one zpool"
        );
        set_health(&mut collection, zpools[0], ZpoolHealth::Degraded);
        let in_service = mk_in_service(zpools.iter().copied());
        let input = build_input(collection, None, in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        (logctx, sitrep)
    }

    /// The notes slippy produces for `sitrep`, without parent cross-checks.
    pub(crate) fn notes_for(sitrep: &Sitrep) -> Vec<Note> {
        Slippy::new_sitrep_only(sitrep)
            .into_report(SlippyReportSortKey::Kind)
            .notes()
            .to_vec()
    }

    pub(crate) fn case_note(case_id: CaseUuid, kind: CaseKind) -> Note {
        Note {
            severity: Severity::Fatal,
            kind: Kind::Case { case_id, kind: Box::new(kind) },
        }
    }

    pub(crate) fn quarantined_case_note(
        case_id: CaseUuid,
        kind: CaseKind,
    ) -> Note {
        Note {
            severity: Severity::Quarantined,
            kind: Kind::Case { case_id, kind: Box::new(kind) },
        }
    }

    pub(crate) fn sitrep_note(kind: SitrepKind) -> Note {
        Note { severity: Severity::Fatal, kind: Kind::Sitrep(kind) }
    }

    /// The ID of the sole case in `sitrep`.
    pub(crate) fn sole_case_id(sitrep: &Sitrep) -> CaseUuid {
        assert_eq!(sitrep.cases.len(), 1, "expected exactly one case");
        sitrep.cases.iter().next().unwrap().id
    }

    /// The sole fact on the sole case in `sitrep`.
    pub(crate) fn sole_fact(sitrep: &Sitrep) -> fm::case::Fact {
        let case = sitrep.cases.iter().next().expect("sitrep has a case");
        assert_eq!(case.facts.len(), 1, "expected exactly one fact");
        case.facts.iter().next().unwrap().clone()
    }

    /// Mutate the case with `case_id` in place.
    pub(crate) fn modify_case(
        sitrep: &mut Sitrep,
        case_id: CaseUuid,
        f: impl FnOnce(&mut fm::Case),
    ) {
        let mut case =
            sitrep.cases.remove(&case_id).expect("case is in the sitrep");
        f(&mut case);
        sitrep.cases.insert_unique(case).expect("case ID is unchanged");
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use crate::slippy::CaseKind;
    use crate::slippy::SitrepKind;
    use chrono::Utc;
    use nexus_fm::test_util::{make_degraded_fact, make_disk_case};
    use nexus_types::fm;
    use nexus_types::fm::DiagnosisEngineKind;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::ereport::{
        Ena, Ereport, EreportData, EreportId, Reporter,
    };
    use nexus_types::inventory::SpType;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::CaseEreportUuid;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::EreporterRestartUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::sync::Arc;

    /// Make an ereport with an obviously-fake payload. `ena` distinguishes
    /// multiple ereports from the same fake reporter.
    fn mk_test_ereport(restart_id: EreporterRestartUuid, ena: u64) -> Ereport {
        Ereport::new(
            EreportId { restart_id, ena: Ena(ena) },
            chrono::DateTime::<Utc>::MIN_UTC,
            OmicronZoneUuid::new_v4(),
            EreportData {
                serial_number: Some("test-serial".to_string()),
                part_number: Some("test-part".to_string()),
                class: Some("test.class".to_string()),
                report: serde_json::Map::new().into(),
            },
            Reporter::Sp { sp_type: SpType::Power, slot: 0 },
        )
    }

    /// Assign `ereport` to the case with `case_id`. Unless `skip_index` is
    /// set, also add it to the sitrep's `ereports_by_id` index.
    fn assign_ereport(
        sitrep: &mut Sitrep,
        case_id: CaseUuid,
        ereport_assignment_id: CaseEreportUuid,
        ereport: Arc<Ereport>,
        skip_index: bool,
    ) {
        let assigned_sitrep_id = sitrep.metadata.id;
        modify_case(sitrep, case_id, |case| {
            case.ereports
                .insert_unique(fm::case::CaseEreport {
                    id: ereport_assignment_id,
                    ereport: ereport.clone(),
                    assigned_sitrep_id,
                    comment: String::new(),
                })
                .expect("ereport not already assigned to case");
        });
        if !skip_index {
            sitrep.ereports_by_id.insert_overwrite(ereport);
        }
    }

    #[test]
    fn engine_produced_sitrep_is_clean() {
        let (logctx, sitrep) = clean_sitrep("engine_produced_sitrep_is_clean");
        assert_eq!(notes_for(&sitrep), Vec::new());
        logctx.cleanup_successful();
    }

    #[test]
    fn ereport_missing_from_index_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("ereport_missing_from_index_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let ereport =
            Arc::new(mk_test_ereport(EreporterRestartUuid::new_v4(), 1));
        let ereport_id = ereport.id;
        assign_ereport(
            &mut sitrep,
            case_id,
            CaseEreportUuid::new_v4(),
            ereport,
            true,
        );
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::EreportMissingFromIndex {
                case_id,
                ereport_id,
            })]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn orphaned_indexed_ereport_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("orphaned_indexed_ereport_is_flagged");
        let ereport =
            Arc::new(mk_test_ereport(EreporterRestartUuid::new_v4(), 1));
        let ereport_id = ereport.id;
        sitrep.ereports_by_id.insert_overwrite(ereport);
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::OrphanedIndexedEreport { ereport_id })]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn indexed_ereport_content_mismatch_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("indexed_ereport_content_mismatch_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let restart_id = EreporterRestartUuid::new_v4();
        let ereport = Arc::new(mk_test_ereport(restart_id, 1));
        let ereport_id = ereport.id;
        assign_ereport(
            &mut sitrep,
            case_id,
            CaseEreportUuid::new_v4(),
            ereport,
            true,
        );
        // Index an ereport with the same ID but a different collector.
        let mut doppelganger = mk_test_ereport(restart_id, 1);
        doppelganger.collector_id = OmicronZoneUuid::new_v4();
        sitrep.ereports_by_id.insert_overwrite(Arc::new(doppelganger));
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::IndexedEreportContentMismatch {
                case_id,
                ereport_id,
            })]
        );
        logctx.cleanup_successful();
    }

    /// Add a second open disk case (about a fresh disk) to `sitrep`. Returns
    /// the existing case's ID and the new case's ID as `(case1, case2)`,
    /// ordered the way the cross-case checks visit them (ascending by
    /// `CaseUuid`), which is the orientation duplicate-ID notes report.
    fn add_second_case(sitrep: &mut Sitrep) -> (CaseUuid, CaseUuid) {
        let existing = sole_case_id(sitrep);
        let added = CaseUuid::new_v4();
        sitrep
            .cases
            .insert_unique(make_disk_case(
                added,
                sitrep.metadata.id,
                [make_degraded_fact(
                    sitrep.metadata.id,
                    sitrep.metadata.inv_collection_id,
                    PhysicalDiskUuid::new_v4(),
                    ZpoolUuid::new_v4(),
                )],
            ))
            .unwrap();
        if existing < added { (existing, added) } else { (added, existing) }
    }

    #[test]
    fn duplicate_case_ereport_id_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("duplicate_case_ereport_id_is_flagged");
        let (case1, case2) = add_second_case(&mut sitrep);
        // Two distinct ereports assigned under the same assignment ID.
        let restart_id = EreporterRestartUuid::new_v4();
        let assignment_id = CaseEreportUuid::new_v4();
        assign_ereport(
            &mut sitrep,
            case1,
            assignment_id,
            Arc::new(mk_test_ereport(restart_id, 1)),
            false,
        );
        assign_ereport(
            &mut sitrep,
            case2,
            assignment_id,
            Arc::new(mk_test_ereport(restart_id, 2)),
            false,
        );
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::DuplicateCaseEreportId {
                id: assignment_id,
                case1,
                case2,
            })]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn duplicate_fact_id_is_flagged() {
        let (logctx, mut sitrep) = clean_sitrep("duplicate_fact_id_is_flagged");
        let existing = sole_case_id(&sitrep);
        let fact_id = sole_fact(&sitrep).metadata.id;
        // A second case (about a different disk) reusing the same FactUuid.
        let mut dup_fact = make_degraded_fact(
            sitrep.metadata.id,
            sitrep.metadata.inv_collection_id,
            PhysicalDiskUuid::new_v4(),
            ZpoolUuid::new_v4(),
        );
        dup_fact.metadata.id = fact_id;
        let added = CaseUuid::new_v4();
        sitrep
            .cases
            .insert_unique(make_disk_case(
                added,
                sitrep.metadata.id,
                [dup_fact],
            ))
            .unwrap();
        let (case1, case2) = if existing < added {
            (existing, added)
        } else {
            (added, existing)
        };
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::DuplicateFactId {
                fact_id,
                case1,
                case2,
            })]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn duplicate_alert_id_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("duplicate_alert_id_is_flagged");
        let (case1, case2) = add_second_case(&mut sitrep);
        let alert_id = AlertUuid::new_v4();
        let sitrep_id = sitrep.metadata.id;
        for case_id in [case1, case2] {
            modify_case(&mut sitrep, case_id, |case| {
                case.alerts_requested
                    .insert_unique(fm::case::AlertRequest {
                        id: alert_id,
                        class: nexus_types::alert::AlertClass::TestFoo,
                        version: 0,
                        payload: serde_json::json!({}),
                        requested_sitrep_id: sitrep_id,
                        comment: String::new(),
                    })
                    .unwrap();
            });
        }
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::DuplicateAlertId {
                alert_id,
                case1,
                case2,
            })]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn duplicate_support_bundle_id_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("duplicate_support_bundle_id_is_flagged");
        let (case1, case2) = add_second_case(&mut sitrep);
        let bundle_id = omicron_uuid_kinds::SupportBundleUuid::new_v4();
        let sitrep_id = sitrep.metadata.id;
        for case_id in [case1, case2] {
            modify_case(&mut sitrep, case_id, |case| {
                case.support_bundles_requested
                    .insert_unique(fm::case::SupportBundleRequest {
                        id: bundle_id,
                        requested_sitrep_id: sitrep_id,
                        data_selection:
                            nexus_types::support_bundle::BundleDataSelection::all(),
                        comment: String::new(),
                    })
                    .unwrap();
            });
        }
        assert_eq!(
            notes_for(&sitrep),
            [sitrep_note(SitrepKind::DuplicateSupportBundleId {
                bundle_id,
                case1,
                case2,
            })]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn foreign_fact_payload_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("foreign_fact_payload_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let fact_id = sole_fact(&sitrep).metadata.id;
        // Only one payload variant exists today, so simulate the mismatch
        // from the case side: reassign the case to a different engine.
        modify_case(&mut sitrep, case_id, |case| {
            case.metadata.de = DiagnosisEngineKind::PowerShelf;
        });
        assert_eq!(
            notes_for(&sitrep),
            [case_note(
                case_id,
                CaseKind::ForeignFactPayload {
                    fact_id,
                    case_de: DiagnosisEngineKind::PowerShelf,
                    payload_de: DiagnosisEngineKind::PhysicalDisk,
                }
            )]
        );
        logctx.cleanup_successful();
    }

    /// Data-validity violations are flagged on closed cases too: a closed
    /// case may still have rendezvous work pending, so its data matters.
    /// The engine contained the violation by closing the case, so the note
    /// is Quarantined rather than Fatal.
    #[test]
    fn foreign_fact_payload_on_closed_case_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("foreign_fact_payload_on_closed_case_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let fact_id = sole_fact(&sitrep).metadata.id;
        modify_case(&mut sitrep, case_id, |case| {
            case.metadata.de = DiagnosisEngineKind::PowerShelf;
            case.metadata.closed_sitrep_id = Some(SitrepUuid::new_v4());
        });
        assert_eq!(
            notes_for(&sitrep),
            [quarantined_case_note(
                case_id,
                CaseKind::ForeignFactPayload {
                    fact_id,
                    case_de: DiagnosisEngineKind::PowerShelf,
                    payload_de: DiagnosisEngineKind::PhysicalDisk,
                }
            )]
        );
        logctx.cleanup_successful();
    }
}
