// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SMF service diagnosis engine.
//!
//! Opens one case per SMF service instance (sled, zone, FMRI) that inventory
//! reports in the `maintenance` state. Other non-online states (`offline`,
//! `degraded`, unrecognized) are deliberately ignored, as are services in
//! Propolis zones.

use crate::SitrepBuilder;
use iddqd::{BiHashItem, BiHashMap, bi_upcast};
use nexus_types::fm;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::{SmfServiceFact, SmfServiceFactPayload};
use nexus_types::inventory::is_propolis_zone;
use omicron_uuid_kinds::{CaseUuid, SledUuid};
use sled_agent_types::inventory::SvcEnabledNotOnlineState;
use sled_agent_types::inventory::SvcsEnabledNotOnlineResult;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

/// Identifies one SMF service instance. Each case concerns exactly one.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ServiceKey {
    sled_id: SledUuid,
    zone_name: String,
    fmri: String,
}

impl ServiceKey {
    fn from_payload(p: &SmfServiceFactPayload) -> Self {
        Self {
            sled_id: p.sled_id,
            zone_name: p.zone_name.clone(),
            fmri: p.fmri.clone(),
        }
    }
}

impl fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { sled_id, zone_name, fmri } = self;
        write!(f, "{fmri} in zone {zone_name} on sled {sled_id}")
    }
}

/// A parent-forwarded SMF service case, parsed into the form this engine
/// acts on.
struct ParsedCase {
    case_id: CaseUuid,
    service: ServiceKey,
}

/// Keying on the service makes "at most one case per service" an invariant
/// of the map: inserting a second case for a service already present fails.
impl BiHashItem for ParsedCase {
    type K1<'a> = CaseUuid;
    type K2<'a> = &'a ServiceKey;
    fn key1(&self) -> Self::K1<'_> {
        self.case_id
    }
    fn key2(&self) -> Self::K2<'_> {
        &self.service
    }
    bi_upcast!();
}

/// Why a parent-forwarded SMF service case could not be interpreted.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
enum UninterpretableCase {
    #[error(transparent)]
    ForeignFact(#[from] fm::case::ForeignFact),
    #[error("facts reference different services ({expected} and {found})")]
    DisagreeingServices { expected: ServiceKey, found: ServiceKey },
    #[error("case has no facts, so the service it concerns is unknown")]
    NoFacts,
}

fn parse_case(case: &fm::Case) -> Result<ParsedCase, UninterpretableCase> {
    let mut service: Option<ServiceKey> = None;
    for fact in case.facts.iter() {
        let SmfServiceFact::InMaintenance(payload) = fact.as_smf_service()?;
        let found = ServiceKey::from_payload(payload);
        match &service {
            None => service = Some(found),
            Some(expected) if *expected != found => {
                return Err(UninterpretableCase::DisagreeingServices {
                    expected: expected.clone(),
                    found,
                });
            }
            Some(_) => {}
        }
    }
    let service = service.ok_or(UninterpretableCase::NoFacts)?;
    Ok(ParsedCase { case_id: case.id, service })
}

/// What the current inventory says about one sled's SMF services.
enum SledObservation<'a> {
    /// The sled reported its services. `in_maintenance` holds the non-Propolis
    /// services in maintenance, keyed by `(zone_name, fmri)`.
    Observed {
        in_maintenance: BTreeSet<(&'a str, &'a str)>,
        /// Number of `svcs` output lines that could not be parsed.
        parse_errors: usize,
        time_of_status: chrono::DateTime<chrono::Utc>,
    },
    /// The sled is in inventory, but its service states are unknown.
    Unknown,
}

pub(super) fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    let input = builder.input();
    let inv = input.inventory();
    let in_service_sleds = input.in_service_sleds();

    let observations: BTreeMap<SledUuid, SledObservation<'_>> = inv
        .sled_agents
        .iter()
        .map(|sa| {
            let obs = match &sa.smf_services_enabled_not_online {
                SvcsEnabledNotOnlineResult::SvcsEnabledNotOnline(svcs) => {
                    SledObservation::Observed {
                        in_maintenance: svcs
                            .services
                            .iter()
                            .filter(|svc| {
                                svc.state
                                    == SvcEnabledNotOnlineState::Maintenance
                                    && !is_propolis_zone(&svc.zone)
                            })
                            .map(|svc| (svc.zone.as_str(), svc.fmri.as_str()))
                            .collect(),
                        parse_errors: svcs.errors.len(),
                        time_of_status: svcs.time_of_status,
                    }
                }
                SvcsEnabledNotOnlineResult::SvcsCmdError(_)
                | SvcsEnabledNotOnlineResult::DataUnavailable => {
                    SledObservation::Unknown
                }
            };
            (sa.sled_id, obs)
        })
        .collect();

    for (sled_id, obs) in &observations {
        if let SledObservation::Observed { parse_errors: n @ 1.., .. } = obs {
            builder
                .log_warning("sled reported unparseable SMF service status")
                .kv("sled_id", sled_id)
                .kv("parse_errors", n)
                .finish();
        }
    }

    // Index the SMF service cases from the parent sitrep, closing any we
    // cannot interpret and any duplicates.
    let mut cases = BiHashMap::<ParsedCase>::new();
    for case in input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::SmfService)
    {
        let parsed = match parse_case(case) {
            Ok(parsed) => parsed,
            Err(reason) => {
                builder
                    .log_warning("closing uninterpretable SMF service case")
                    .kv("case_id", case.id)
                    .kv("reason", reason.to_string())
                    .finish();
                builder
                    .cases
                    .case_mut(&case.id)
                    .expect("case_id came from builder's open cases")
                    .close(format!("cannot interpret case: {reason}"));
                continue;
            }
        };
        if let Err(dup) = cases.insert_unique(parsed) {
            let kept = dup.duplicates()[0];
            builder
                .cases
                .case_mut(&case.id)
                .expect("case_id came from builder's open cases")
                .close(format!(
                    "duplicate of case {} for {}",
                    kept.case_id, kept.service,
                ));
        }
    }

    // Close cases whose service has left maintenance or whose sled has left
    // service. A sled missing from inventory, or whose service states are
    // unknown, is not evidence of recovery.
    for parsed in cases.iter() {
        let service = &parsed.service;
        let reason = if !in_service_sleds.contains(&service.sled_id) {
            format!("sled {} no longer in service", service.sled_id)
        } else {
            match observations.get(&service.sled_id) {
                // Unparseable lines could hide this service, so absence from
                // a partial listing is not evidence of recovery.
                Some(SledObservation::Observed {
                    in_maintenance,
                    parse_errors: 0,
                    ..
                }) if !in_maintenance.contains(&(
                    service.zone_name.as_str(),
                    service.fmri.as_str(),
                )) =>
                {
                    format!("{service} no longer reported in maintenance")
                }
                _ => continue,
            }
        };
        builder
            .cases
            .case_mut(&parsed.case_id)
            .expect("case_id came from builder's open cases")
            .close(reason);
    }

    // Open a case for each service in maintenance on an in-service sled that
    // doesn't already have one. Parsed maintenance lines are trustworthy even
    // when other lines failed to parse, so this ignores `parse_errors`.
    for (sled_id, obs) in &observations {
        if !in_service_sleds.contains(sled_id) {
            continue;
        }
        let SledObservation::Observed {
            in_maintenance, time_of_status, ..
        } = obs
        else {
            continue;
        };
        for (zone_name, fmri) in in_maintenance {
            let service = ServiceKey {
                sled_id: *sled_id,
                zone_name: zone_name.to_string(),
                fmri: fmri.to_string(),
            };
            if cases.contains_key2(&&service) {
                continue;
            }
            let mut case = builder.cases.open_case(
                DiagnosisEngineKind::SmfService,
                format!("opened because {service} is in maintenance"),
            );
            case.add_fact(
                SmfServiceFact::InMaintenance(SmfServiceFactPayload {
                    sled_id: *sled_id,
                    zone_name: zone_name.to_string(),
                    fmri: fmri.to_string(),
                    observed_in_inv: inv.id,
                    time_observed: *time_of_status,
                }),
                format!("{service} in maintenance"),
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_input::Input;
    use crate::test_util::{
        FmTest, make_parent_sitrep, make_zpool_degraded_fact, run_analyze,
    };
    use chrono::Utc;
    use iddqd::IdOrdMap;
    use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
    use nexus_types::fm::Sitrep;
    use nexus_types::inventory;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        CollectionUuid, FactUuid, PhysicalDiskUuid, PropolisUuid, SitrepUuid,
        ZpoolUuid,
    };
    use sled_agent_types::inventory::{
        SvcEnabledNotOnline, SvcsEnabledNotOnline, SvcsError,
    };
    use std::sync::Arc;

    const ZONE: &str = "oxz_ntp_aaaa";
    const FMRI: &str = "svc:/oxide/ntp:default";

    /// Returns the example system's collection, with every sled reporting
    /// no services in a non-online state, and the IDs of its sleds.
    fn setup(
        test_name: &'static str,
    ) -> (dev::LogContext, inventory::Collection, Vec<SledUuid>) {
        let (fm_test, logctx) = FmTest::new_with_logctx(test_name);
        let (example, _bp) = fm_test.system_builder.build();
        let mut collection = example.collection;
        let sled_ids: Vec<SledUuid> =
            collection.sled_agents.iter().map(|sa| sa.sled_id).collect();
        assert!(sled_ids.len() >= 2, "example system should have 2+ sleds");
        for sled_id in &sled_ids {
            set_services(&mut collection, *sled_id, &[], &[]);
        }
        (logctx, collection, sled_ids)
    }

    fn set_result(
        collection: &mut inventory::Collection,
        sled_id: SledUuid,
        result: SvcsEnabledNotOnlineResult,
    ) {
        collection
            .sled_agents
            .get_mut(sled_id)
            .expect("sled is in collection")
            .smf_services_enabled_not_online = result;
    }

    fn set_services(
        collection: &mut inventory::Collection,
        sled_id: SledUuid,
        services: &[(&str, &str, SvcEnabledNotOnlineState)],
        errors: &[&str],
    ) {
        set_result(
            collection,
            sled_id,
            SvcsEnabledNotOnlineResult::SvcsEnabledNotOnline(
                SvcsEnabledNotOnline {
                    services: services
                        .iter()
                        .map(|(zone, fmri, state)| SvcEnabledNotOnline {
                            fmri: fmri.to_string(),
                            zone: zone.to_string(),
                            state: *state,
                        })
                        .collect(),
                    errors: errors.iter().map(|e| e.to_string()).collect(),
                    time_of_status: Utc::now(),
                },
            ),
        );
    }

    fn set_maintenance(
        collection: &mut inventory::Collection,
        sled_id: SledUuid,
    ) {
        set_services(
            collection,
            sled_id,
            &[(ZONE, FMRI, SvcEnabledNotOnlineState::Maintenance)],
            &[],
        );
    }

    fn build_input(
        collection: inventory::Collection,
        parent: Option<Sitrep>,
        in_service_sleds: impl IntoIterator<Item = SledUuid>,
    ) -> Input {
        let parent = parent.map(|s| {
            Arc::new((
                fm::SitrepVersion {
                    id: s.id(),
                    version: 0,
                    time_made_current: Utc::now(),
                },
                s,
            ))
        });
        let (input, _report) = Input::builder(parent, Arc::new(collection))
            .expect("input builder should accept inventory")
            .in_service_sleds(Arc::new(in_service_sleds.into_iter().collect()))
            .with_empty_defaults()
            .build()
            .expect("all inputs provided");
        input
    }

    fn make_fact(
        parent_sitrep_id: SitrepUuid,
        inv_id: CollectionUuid,
        sled_id: SledUuid,
        fmri: &str,
    ) -> fm::case::Fact {
        fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: FactUuid::new_v4(),
                created_sitrep_id: parent_sitrep_id,
                comment: String::new(),
            },
            payload: SmfServiceFact::InMaintenance(SmfServiceFactPayload {
                sled_id,
                zone_name: ZONE.to_string(),
                fmri: fmri.to_string(),
                observed_in_inv: inv_id,
                time_observed: Utc::now(),
            })
            .into(),
        }
    }

    fn make_case(
        parent_sitrep_id: SitrepUuid,
        facts: impl IntoIterator<Item = fm::case::Fact>,
    ) -> fm::Case {
        let mut fact_map = IdOrdMap::new();
        for fact in facts {
            fact_map.insert_unique(fact).unwrap();
        }
        fm::Case {
            id: CaseUuid::new_v4(),
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: None,
                de: DiagnosisEngineKind::SmfService,
                comment: "an SMF service case".to_string(),
            },
            ereports: Default::default(),
            alerts_requested: Default::default(),
            support_bundles_requested: Default::default(),
            problems: Default::default(),
            facts: fact_map,
        }
    }

    /// Returns a parent sitrep holding one open case for `FMRI` on
    /// `sled_id`, and that case's ID.
    fn parent_with_case(
        collection: &inventory::Collection,
        sled_id: SledUuid,
    ) -> (Sitrep, CaseUuid) {
        let parent_id = SitrepUuid::new_v4();
        let case = make_case(
            parent_id,
            [make_fact(parent_id, collection.id, sled_id, FMRI)],
        );
        let case_id = case.id;
        (make_parent_sitrep(parent_id, collection.id, [case]), case_id)
    }

    fn smf_cases(sitrep: &Sitrep) -> Vec<&fm::Case> {
        sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::SmfService)
            .collect()
    }

    fn open_smf_cases(sitrep: &Sitrep) -> Vec<&fm::Case> {
        smf_cases(sitrep).into_iter().filter(|c| c.is_open()).collect()
    }

    fn is_open(sitrep: &Sitrep, case_id: CaseUuid) -> bool {
        sitrep.cases.get(&case_id).expect("case is in sitrep").is_open()
    }

    #[test]
    fn opens_case_for_service_in_maintenance() {
        let (logctx, mut collection, sleds) = setup("smf_opens_case");
        set_maintenance(&mut collection, sleds[0]);
        let inv_id = collection.id;
        let input = build_input(collection, None, sleds.clone());

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        let cases = open_smf_cases(&sitrep);
        assert_eq!(cases.len(), 1);
        let facts: Vec<_> = cases[0].facts.iter().collect();
        assert_eq!(facts.len(), 1);
        let SmfServiceFact::InMaintenance(payload) =
            facts[0].as_smf_service().unwrap();
        assert_eq!(payload.sled_id, sleds[0]);
        assert_eq!(payload.zone_name, ZONE);
        assert_eq!(payload.fmri, FMRI);
        assert_eq!(payload.observed_in_inv, inv_id);
        logctx.cleanup_successful();
    }

    #[test]
    fn ignores_other_states_and_propolis_zones() {
        let (logctx, mut collection, sleds) = setup("smf_ignores_others");
        let propolis_zone =
            format!("{PROPOLIS_ZONE_PREFIX}{}", PropolisUuid::new_v4());
        set_services(
            &mut collection,
            sleds[0],
            &[
                (ZONE, "svc:/a:default", SvcEnabledNotOnlineState::Offline),
                (ZONE, "svc:/b:default", SvcEnabledNotOnlineState::Degraded),
                (
                    ZONE,
                    "svc:/c:default",
                    SvcEnabledNotOnlineState::Unrecognized,
                ),
                (
                    &propolis_zone,
                    "svc:/d:default",
                    SvcEnabledNotOnlineState::Maintenance,
                ),
            ],
            &[],
        );
        let input = build_input(collection, None, sleds.clone());

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        assert!(smf_cases(&sitrep).is_empty());
        logctx.cleanup_successful();
    }

    #[test]
    fn does_not_open_case_on_sled_not_in_service() {
        let (logctx, mut collection, sleds) = setup("smf_not_in_service");
        set_maintenance(&mut collection, sleds[0]);
        let input =
            build_input(collection, None, sleds.iter().copied().skip(1));

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        assert!(smf_cases(&sitrep).is_empty());
        logctx.cleanup_successful();
    }

    #[test]
    fn keeps_case_while_still_in_maintenance() {
        let (logctx, mut collection, sleds) = setup("smf_keeps_case");
        set_maintenance(&mut collection, sleds[0]);
        let (parent, case_id) = parent_with_case(&collection, sleds[0]);
        let parent_case = parent.cases.get(&case_id).unwrap().clone();
        let input = build_input(collection, Some(parent), sleds.clone());

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        let cases = open_smf_cases(&sitrep);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].id, case_id);
        assert_eq!(cases[0].facts, parent_case.facts);
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_case_when_service_recovers() {
        let (logctx, collection, sleds) = setup("smf_closes_recovered");
        let (parent, case_id) = parent_with_case(&collection, sleds[0]);
        let input = build_input(collection, Some(parent), sleds.clone());

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        assert!(!is_open(&sitrep, case_id));
        assert!(open_smf_cases(&sitrep).is_empty());
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_case_when_sled_leaves_service() {
        let (logctx, mut collection, sleds) = setup("smf_closes_expunged");
        set_maintenance(&mut collection, sleds[0]);
        let (parent, case_id) = parent_with_case(&collection, sleds[0]);
        let input = build_input(
            collection,
            Some(parent),
            sleds.iter().copied().skip(1),
        );

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        assert!(!is_open(&sitrep, case_id));
        assert!(open_smf_cases(&sitrep).is_empty());
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_case_when_absent_sled_leaves_service() {
        let (logctx, mut collection, sleds) =
            setup("smf_closes_absent_expunged");
        let (parent, case_id) = parent_with_case(&collection, sleds[0]);
        collection.sled_agents.remove(sleds[0]);
        let input = build_input(
            collection,
            Some(parent),
            sleds.iter().copied().skip(1),
        );

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        assert!(!is_open(&sitrep, case_id));
        logctx.cleanup_successful();
    }

    #[test]
    fn keeps_case_when_status_unknown() {
        type MakeUnknown = fn(&mut inventory::Collection, SledUuid);
        let unknown: [(&str, MakeUnknown); 3] = [
            ("smf_unknown_absent", |c, s| {
                c.sled_agents.remove(s);
            }),
            ("smf_unknown_data_unavailable", |c, s| {
                set_result(c, s, SvcsEnabledNotOnlineResult::DataUnavailable)
            }),
            ("smf_unknown_cmd_error", |c, s| {
                set_result(
                    c,
                    s,
                    SvcsEnabledNotOnlineResult::SvcsCmdError(SvcsError {
                        error: "svcs failed".to_string(),
                        time_of_status: Utc::now(),
                    }),
                )
            }),
        ];
        for (test_name, make_unknown) in unknown {
            let (logctx, mut collection, sleds) = setup(test_name);
            let (parent, case_id) = parent_with_case(&collection, sleds[0]);
            make_unknown(&mut collection, sleds[0]);
            let input = build_input(collection, Some(parent), sleds.clone());

            let (sitrep, _) = run_analyze(&logctx.log, &input);
            assert!(is_open(&sitrep, case_id), "{test_name}");
            assert_eq!(open_smf_cases(&sitrep).len(), 1, "{test_name}");
            logctx.cleanup_successful();
        }
    }

    #[test]
    fn partial_listing_opens_but_does_not_close() {
        let (logctx, mut collection, sleds) = setup("smf_partial_listing");
        // The parent has a case for FMRI, which is absent from this partial
        // listing; a different service is newly in maintenance.
        let (parent, case_id) = parent_with_case(&collection, sleds[0]);
        let new_fmri = "svc:/oxide/other:default";
        set_services(
            &mut collection,
            sleds[0],
            &[(ZONE, new_fmri, SvcEnabledNotOnlineState::Maintenance)],
            &["Unexpected output line: maintenance"],
        );
        let input = build_input(collection, Some(parent), sleds.clone());

        let (sitrep, report) = run_analyze(&logctx.log, &input);
        assert!(is_open(&sitrep, case_id));
        let fmris: BTreeSet<_> = open_smf_cases(&sitrep)
            .iter()
            .flat_map(|c| c.facts.iter())
            .map(|f| f.as_smf_service().unwrap().payload().fmri.clone())
            .collect();
        assert_eq!(
            fmris,
            BTreeSet::from([FMRI.to_string(), new_fmri.to_string()])
        );
        let report = format!("{report:?}");
        assert!(
            report.contains("unparseable SMF service status"),
            "expected a warning in the report: {report}"
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn partial_listing_only_blocks_closing_on_its_own_sled() {
        let (logctx, mut collection, sleds) = setup("smf_partial_per_sled");
        let parent_id = SitrepUuid::new_v4();
        let inv_id = collection.id;
        let case_a = make_case(
            parent_id,
            [make_fact(parent_id, inv_id, sleds[0], FMRI)],
        );
        let case_b = make_case(
            parent_id,
            [make_fact(parent_id, inv_id, sleds[1], FMRI)],
        );
        let (id_a, id_b) = (case_a.id, case_b.id);
        let parent = make_parent_sitrep(parent_id, inv_id, [case_a, case_b]);
        set_services(&mut collection, sleds[0], &[], &["bad line"]);
        let input = build_input(collection, Some(parent), sleds.clone());

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        assert!(is_open(&sitrep, id_a));
        assert!(!is_open(&sitrep, id_b));
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_duplicate_and_uninterpretable_cases() {
        let (logctx, mut collection, sleds) = setup("smf_bad_cases");
        set_maintenance(&mut collection, sleds[0]);
        let parent_id = SitrepUuid::new_v4();
        let inv_id = collection.id;
        let first = make_case(
            parent_id,
            [make_fact(parent_id, inv_id, sleds[0], FMRI)],
        );
        let duplicate = make_case(
            parent_id,
            [make_fact(parent_id, inv_id, sleds[0], FMRI)],
        );
        let empty = make_case(parent_id, []);
        let foreign = make_case(
            parent_id,
            [make_zpool_degraded_fact(
                parent_id,
                inv_id,
                PhysicalDiskUuid::new_v4(),
                ZpoolUuid::new_v4(),
            )],
        );
        let disagreeing = make_case(
            parent_id,
            [
                make_fact(parent_id, inv_id, sleds[0], "svc:/x:default"),
                make_fact(parent_id, inv_id, sleds[1], "svc:/x:default"),
            ],
        );
        let ids =
            [first.id, duplicate.id, empty.id, disagreeing.id, foreign.id];
        let parent = make_parent_sitrep(
            parent_id,
            inv_id,
            [first, duplicate, empty, disagreeing, foreign],
        );
        let input = build_input(collection, Some(parent), sleds.clone());

        let (sitrep, _) = run_analyze(&logctx.log, &input);
        let open: Vec<_> =
            open_smf_cases(&sitrep).iter().map(|c| c.id).collect();
        // Open cases are visited in case ID order, so the lower ID survives.
        assert_eq!(open, vec![ids[0].min(ids[1])]);
        assert!(!is_open(&sitrep, ids[2]));
        assert!(!is_open(&sitrep, ids[3]));
        assert!(!is_open(&sitrep, ids[4]));
        logctx.cleanup_successful();
    }
}
