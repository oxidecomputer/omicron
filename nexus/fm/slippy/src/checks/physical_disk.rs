// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks specific to the physical-disk diagnosis engine's cases.

use crate::slippy::CaseKind;
use crate::slippy::PhysicalDiskCaseKind;
use crate::slippy::Severity;
use crate::slippy::Slippy;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::DiskFact;
use nexus_types::inventory::ZpoolHealth;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use std::collections::BTreeMap;

pub(super) fn check_physical_disk_cases(slippy: &mut Slippy<'_>) {
    let mut open_case_by_disk: BTreeMap<PhysicalDiskUuid, CaseUuid> =
        BTreeMap::new();
    let mut notes = Vec::new();

    for case in slippy
        .sitrep()
        .cases
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
    {
        // The DE for disks chooses to close cases that have some issues.
        // If we identify these issues on a case which has already been
        // closed, the problem has already been quarantined.
        let severity = if case.is_open() {
            Severity::Fatal
        } else {
            Severity::Quarantined
        };

        // The disk this case is about
        let mut case_disk_id: Option<PhysicalDiskUuid> = None;
        for fact in &case.facts {
            // A fact carrying another engine's payload is reported by the
            // generic ForeignFactPayload check; skip it here.
            let Some(disk_fact) = fact.payload.as_physical_disk() else {
                continue;
            };
            let disk_id = disk_fact.physical_disk_id();
            let expected = *case_disk_id.get_or_insert(disk_id);
            if disk_id != expected {
                notes.push((
                    case.id,
                    severity,
                    PhysicalDiskCaseKind::DisagreeingDisks {
                        expected,
                        found: disk_id,
                        fact_id: fact.metadata.id,
                    },
                ));
            }
            match disk_fact {
                DiskFact::ZpoolUnhealthy(payload) => {
                    if payload.last_seen_health == ZpoolHealth::Online {
                        notes.push((
                            case.id,
                            severity,
                            PhysicalDiskCaseKind::ZpoolUnhealthyFactClaimsOnline {
                                fact_id: fact.metadata.id,
                                zpool_id: payload.zpool_id,
                            },
                        ));
                    }
                }
            }
        }

        // Having no facts, and sharing a disk with another case, are only
        // invalid for open cases.
        if !case.is_open() {
            continue;
        }
        match case_disk_id {
            None => {
                notes.push((
                    case.id,
                    Severity::Fatal,
                    PhysicalDiskCaseKind::OpenCaseWithNoFacts,
                ));
            }
            Some(disk_id) => {
                if let Some(other_case) =
                    open_case_by_disk.insert(disk_id, case.id)
                {
                    notes.push((
                        case.id,
                        Severity::Fatal,
                        PhysicalDiskCaseKind::DuplicateOpenCaseForDisk {
                            other_case,
                            physical_disk_id: disk_id,
                        },
                    ));
                }
            }
        }
    }

    for (case_id, severity, kind) in notes {
        slippy.push_case_note(case_id, severity, CaseKind::PhysicalDisk(kind));
    }
}

#[cfg(test)]
mod tests {
    use crate::checks::test_helpers::*;
    use crate::slippy::CaseKind;
    use crate::slippy::PhysicalDiskCaseKind;
    use nexus_fm::test_util::{make_degraded_fact, make_disk_case};
    use nexus_types::fm;
    use nexus_types::fm::Sitrep;
    use nexus_types::inventory::ZpoolHealth;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::FactUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use omicron_uuid_kinds::ZpoolUuid;

    #[test]
    fn open_disk_case_with_no_facts_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("open_disk_case_with_no_facts_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let fact_id = sole_fact(&sitrep).metadata.id;
        modify_case(&mut sitrep, case_id, |case| {
            case.facts.remove(&fact_id).unwrap();
        });
        assert_eq!(
            notes_for(&sitrep),
            [case_note(
                case_id,
                CaseKind::PhysicalDisk(
                    PhysicalDiskCaseKind::OpenCaseWithNoFacts
                ),
            )]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn case_with_disagreeing_disks_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("case_with_disagreeing_disks_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let expected = sole_fact(&sitrep)
            .payload
            .as_physical_disk()
            .unwrap()
            .physical_disk_id();
        let found = PhysicalDiskUuid::new_v4();
        let intruder = make_degraded_fact(
            sitrep.metadata.id,
            sitrep.metadata.inv_collection_id,
            found,
            ZpoolUuid::new_v4(),
        );
        let intruder_id = intruder.metadata.id;
        modify_case(&mut sitrep, case_id, |case| {
            case.facts.insert_unique(intruder).unwrap();
        });
        let notes = notes_for(&sitrep);
        // The check takes the first fact (in FactUuid order) as the case's
        // disk and flags the second, so which fact gets flagged depends on
        // how the two UUIDs happened to sort.
        let sole_fact_first =
            sole_fact_sorts_first(&sitrep, case_id, intruder_id);
        let expected_note = if sole_fact_first {
            case_note(
                case_id,
                CaseKind::PhysicalDisk(
                    PhysicalDiskCaseKind::DisagreeingDisks {
                        expected,
                        found,
                        fact_id: intruder_id,
                    },
                ),
            )
        } else {
            let original =
                sole_fact_id_excluding(&sitrep, case_id, intruder_id);
            case_note(
                case_id,
                CaseKind::PhysicalDisk(
                    PhysicalDiskCaseKind::DisagreeingDisks {
                        expected: found,
                        found: expected,
                        fact_id: original,
                    },
                ),
            )
        };
        assert_eq!(notes, [expected_note]);
        logctx.cleanup_successful();
    }

    /// Whether some fact other than `intruder_id` sorts first among the
    /// facts on `case_id`.
    fn sole_fact_sorts_first(
        sitrep: &Sitrep,
        case_id: CaseUuid,
        intruder_id: FactUuid,
    ) -> bool {
        let case = sitrep.cases.get(&case_id).unwrap();
        case.facts.iter().next().unwrap().metadata.id != intruder_id
    }

    /// The ID of the one fact on `case_id` that is not `intruder_id`.
    fn sole_fact_id_excluding(
        sitrep: &Sitrep,
        case_id: CaseUuid,
        intruder_id: FactUuid,
    ) -> FactUuid {
        let case = sitrep.cases.get(&case_id).unwrap();
        let ids: Vec<FactUuid> = case
            .facts
            .iter()
            .map(|f| f.metadata.id)
            .filter(|id| *id != intruder_id)
            .collect();
        assert_eq!(ids.len(), 1);
        ids[0]
    }

    #[test]
    fn duplicate_open_case_for_disk_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("duplicate_open_case_for_disk_is_flagged");
        let case1 = sole_case_id(&sitrep);
        let physical_disk_id = sole_fact(&sitrep)
            .payload
            .as_physical_disk()
            .unwrap()
            .physical_disk_id();
        let case2 = CaseUuid::new_v4();
        sitrep
            .cases
            .insert_unique(make_disk_case(
                case2,
                sitrep.metadata.id,
                [make_degraded_fact(
                    sitrep.metadata.id,
                    sitrep.metadata.inv_collection_id,
                    physical_disk_id,
                    ZpoolUuid::new_v4(),
                )],
            ))
            .unwrap();
        // The note lands on whichever case is visited second (map order),
        // referencing the first.
        let (first, second) =
            if case1 < case2 { (case1, case2) } else { (case2, case1) };
        assert_eq!(
            notes_for(&sitrep),
            [case_note(
                second,
                CaseKind::PhysicalDisk(
                    PhysicalDiskCaseKind::DuplicateOpenCaseForDisk {
                        other_case: first,
                        physical_disk_id,
                    }
                ),
            )]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn zpool_unhealthy_fact_claiming_online_is_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("zpool_unhealthy_fact_claiming_online_is_flagged");
        let case_id = sole_case_id(&sitrep);
        let mut fact = sole_fact(&sitrep);
        let fact_id = fact.metadata.id;
        let fm::FactPayload::PhysicalDisk(fm::DiskFact::ZpoolUnhealthy(
            ref mut payload,
        )) = fact.payload;
        payload.last_seen_health = ZpoolHealth::Online;
        let zpool_id = payload.zpool_id;
        modify_case(&mut sitrep, case_id, |case| {
            case.facts.insert_overwrite(fact);
        });
        assert_eq!(
            notes_for(&sitrep),
            [case_note(
                case_id,
                CaseKind::PhysicalDisk(
                    PhysicalDiskCaseKind::ZpoolUnhealthyFactClaimsOnline {
                        fact_id,
                        zpool_id,
                    }
                ),
            )]
        );
        logctx.cleanup_successful();
    }

    /// Data-validity violations are flagged on closed cases too: a closed
    /// case may still have rendezvous work pending, so its data matters.
    /// The engine contained the violation by closing the case, so the note
    /// is Quarantined rather than Fatal.
    #[test]
    fn zpool_unhealthy_fact_claiming_online_on_closed_case_is_flagged() {
        let (logctx, mut sitrep) = clean_sitrep(
            "zpool_unhealthy_fact_claiming_online_on_closed_case_is_flagged",
        );
        let case_id = sole_case_id(&sitrep);
        let mut fact = sole_fact(&sitrep);
        let fact_id = fact.metadata.id;
        let fm::FactPayload::PhysicalDisk(fm::DiskFact::ZpoolUnhealthy(
            ref mut payload,
        )) = fact.payload;
        payload.last_seen_health = ZpoolHealth::Online;
        let zpool_id = payload.zpool_id;
        modify_case(&mut sitrep, case_id, |case| {
            case.facts.insert_overwrite(fact);
            case.metadata.closed_sitrep_id = Some(SitrepUuid::new_v4());
        });
        assert_eq!(
            notes_for(&sitrep),
            [quarantined_case_note(
                case_id,
                CaseKind::PhysicalDisk(
                    PhysicalDiskCaseKind::ZpoolUnhealthyFactClaimsOnline {
                        fact_id,
                        zpool_id,
                    }
                ),
            )]
        );
        logctx.cleanup_successful();
    }

    /// A closed case with no facts is the engine's correct disposal of an
    /// uninterpretable case, not a violation.
    #[test]
    fn closed_empty_case_is_not_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("closed_empty_case_is_not_flagged");
        let case_id = sole_case_id(&sitrep);
        let fact_id = sole_fact(&sitrep).metadata.id;
        modify_case(&mut sitrep, case_id, |case| {
            case.facts.remove(&fact_id).unwrap();
            case.metadata.closed_sitrep_id = Some(SitrepUuid::new_v4());
        });
        assert_eq!(notes_for(&sitrep), Vec::new());
        logctx.cleanup_successful();
    }

    /// A closed case sharing a disk with an open case is ordinary history
    /// (the disk failed, recovered, and failed again), not a violation.
    #[test]
    fn closed_case_for_same_disk_is_not_flagged() {
        let (logctx, mut sitrep) =
            clean_sitrep("closed_case_for_same_disk_is_not_flagged");
        let physical_disk_id = sole_fact(&sitrep)
            .payload
            .as_physical_disk()
            .unwrap()
            .physical_disk_id();
        let added = CaseUuid::new_v4();
        let mut closed_case = make_disk_case(
            added,
            sitrep.metadata.id,
            [make_degraded_fact(
                sitrep.metadata.id,
                sitrep.metadata.inv_collection_id,
                physical_disk_id,
                ZpoolUuid::new_v4(),
            )],
        );
        closed_case.metadata.closed_sitrep_id = Some(SitrepUuid::new_v4());
        sitrep.cases.insert_unique(closed_case).unwrap();
        assert_eq!(notes_for(&sitrep), Vec::new());
        logctx.cleanup_successful();
    }
}
