// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Certificate diagnosis engine.
//!
//! Nexus serves each silo's external API with the silo's TLS certificate
//! whose leaf `not_after` is latest (`ExternalEndpoint::best_certificate` in
//! Nexus). This engine predicts that choice from the analysis input and opens
//! a case (keyed by silo) when the certificate Nexus will serve:
//!  - expires within the configured warning window
//!    (`FmConfig::certificate_expiry_warning_days`), or
//!  - has already expired.
//!
//! Either condition means no later-expiring replacement is installed; once an
//! operator uploads one, the case closes. A silo with no certificates at all
//! opens no case.
//!
//! An alert is requested whenever the case's fact changes: entering the
//! warning window requests a `silo.certificate.expiring` alert, passing
//! `not_after` requests a `silo.certificate.expired` alert, and a different
//! certificate becoming the (still expiring or expired) best certificate
//! requests a fresh alert for it. Unchanged input requests nothing.
//!
//! Like `best_certificate`, this engine ignores `not_before`. If that rule
//! ever changes, it must change in both places together, or the engine will
//! reason about a certificate Nexus does not actually serve.

use crate::SitrepBuilder;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_types::alert::certificate as alert_types;
use nexus_types::fm;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::FmConfig;
use nexus_types::fm::{CertificateExpiryFactPayload, CertificateFact};
use nexus_types::observed_certificate::{
    ObservedCertificate, ObservedSiloCertificates,
};
use omicron_uuid_kinds::{CaseUuid, FactUuid};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use uuid::Uuid;

/// A parent-forwarded Certificate case, parsed into the form this engine acts
/// on. Every fact on a certificate case is about the same silo, and a case
/// carries exactly one fact.
struct ParsedCertificateCase {
    /// The silo `fact` is about.
    silo_id: Uuid,
    /// The fact to consider when advancing the case.
    fact: (FactUuid, CertificateFact),
    /// Facts that should not exist: any beyond the first. They carry no
    /// information the kept fact doesn't.
    duplicate_facts: Vec<FactUuid>,
}

/// Why a parent-forwarded Certificate case could not be interpreted.
///
/// Uninterpretable cases are closed by [`analyze`]: an open case this engine
/// cannot process would otherwise be carried forward into every future sitrep
/// with no path to closure.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
enum UninterpretableCase {
    #[error(transparent)]
    ForeignFact(#[from] fm::case::ForeignFact),
    #[error(
        "facts reference different silos ({expected} and {found}, 1 expected)"
    )]
    DisagreeingSilos { expected: Uuid, found: Uuid },
    #[error("case has no facts, so the silo it concerns cannot be determined")]
    NoFacts,
}

/// Parse one parent-forwarded Certificate case into a
/// [`ParsedCertificateCase`], or explain why it cannot be interpreted.
fn parse_case(
    case: &fm::Case,
) -> Result<ParsedCertificateCase, UninterpretableCase> {
    let mut kept: Option<(FactUuid, CertificateFact)> = None;
    let mut duplicate_facts = Vec::new();
    // `case.facts` iterates in fact UUID order, so the kept fact is
    // deterministically the one with the lowest UUID.
    for fact in case.facts.iter() {
        let cert_fact = fact.as_certificate()?;
        match &kept {
            None => kept = Some((fact.metadata.id, cert_fact.clone())),
            Some((_, first)) => {
                if first.silo_id() != cert_fact.silo_id() {
                    return Err(UninterpretableCase::DisagreeingSilos {
                        expected: first.silo_id(),
                        found: cert_fact.silo_id(),
                    });
                }
                duplicate_facts.push(fact.metadata.id);
            }
        }
    }
    let Some(fact) = kept else {
        return Err(UninterpretableCase::NoFacts);
    };
    Ok(ParsedCertificateCase {
        silo_id: fact.1.silo_id(),
        fact,
        duplicate_facts,
    })
}

pub(super) fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    let input = builder.input();
    // Expiry is judged against the input's deterministic "now"; see
    // `Input::reference_time`.
    let reference_time = input.reference_time();
    let window = warning_window(input.config());
    let silos = input.observed_silo_certificates();

    // Parse the Certificate cases copied forward from the parent sitrep. Every
    // case is about one silo, derived from its facts. Cases we cannot
    // interpret are closed inline, so they don't ride along as
    // open-but-unprocessable in every future sitrep. This is safe with
    // respect to fault coverage: detection below is independent of case
    // bookkeeping, so if a closed case concerned a silo that genuinely needs
    // attention, a fresh, well-formed case is opened in this same pass.
    let mut parent_cases: BTreeMap<CaseUuid, ParsedCertificateCase> =
        BTreeMap::new();
    for case in input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::Certificate)
    {
        match parse_case(case) {
            Ok(parsed_case) => {
                parent_cases.insert(case.id, parsed_case);
            }
            Err(reason) => {
                builder
                    .log_warning("closing uninterpretable Certificate case")
                    .kv("case_id", case.id)
                    .kv("reason", reason.to_string())
                    .finish();
                builder
                    .cases
                    .case_mut(&case.id)
                    .expect("case_id came from builder's open cases")
                    .close(format!("cannot interpret case: {reason}"));
            }
        }
    }

    // Inverse index: which parent case is about which silo. Cases are
    // per-silo, so a silo with two parent cases is already pathological. We
    // keep one and close the rest as duplicates. `parent_cases` iterates
    // ascending by CaseUuid, so we deterministically keep the lowest-ID case.
    let mut case_for_silo: BTreeMap<Uuid, CaseUuid> = BTreeMap::new();
    for (case_id, parsed_case) in &parent_cases {
        match case_for_silo.entry(parsed_case.silo_id) {
            Entry::Vacant(slot) => {
                slot.insert(*case_id);
            }
            Entry::Occupied(kept) => {
                let kept_case_id = *kept.get();
                builder
                    .log_warning("closing duplicate Certificate case")
                    .kv("case_id", case_id)
                    .kv("kept_case_id", kept_case_id)
                    .kv("silo_id", parsed_case.silo_id)
                    .finish();
                builder
                    .cases
                    .case_mut(case_id)
                    .expect("case_id came from builder's open cases")
                    .close(format!(
                        "duplicate of case {kept_case_id} for silo {}",
                        parsed_case.silo_id,
                    ));
            }
        }
    }

    // Close the surviving parent case for any silo that no longer exists or
    // whose best certificate is no longer expiring or expired. A silo whose
    // condition still holds has its fact reconciled in the next loop, which
    // owns all fact state for the silo.
    for (silo_id, case_id) in &case_for_silo {
        let mut case_mut = builder
            .cases
            .case_mut(case_id)
            .expect("case_id came from builder's open cases");
        let Some(silo) = silos.get(silo_id) else {
            case_mut.close(format!("silo {silo_id} no longer exists"));
            continue;
        };
        if desired_fact(silo, reference_time, window).is_some() {
            continue;
        }
        match silo.best_certificate() {
            None => case_mut.close(format!(
                "silo {} ({silo_id}) has no certificates installed; that \
                 is outside this engine's scope",
                silo.silo_name,
            )),
            Some(cert) => case_mut.close(format!(
                "silo {} ({silo_id}) now has a certificate, {} ({}), that \
                 expires at {}, outside the {} warning window",
                silo.silo_name,
                cert.name,
                cert.id,
                cert.not_after,
                omicron_common::format_time_delta(window),
            )),
        }
    }

    // For each silo whose best certificate is expiring or expired, ensure its
    // case carries exactly the fact matching the current observation: reuse
    // the parent-forwarded case if any (dropping duplicate and stale facts),
    // otherwise open a fresh case. This loop owns all fact state for a silo.
    for silo in silos.iter() {
        let Some((desired, best)) = desired_fact(silo, reference_time, window)
        else {
            continue;
        };

        let parent = case_for_silo
            .get(&silo.silo_id)
            .map(|case_id| (*case_id, &parent_cases[case_id]));

        let mut case_mut = match parent {
            Some((case_id, _)) => builder
                .cases
                .case_mut(&case_id)
                .expect("case_id came from builder's open cases"),
            None => builder.cases.open_case(
                DiagnosisEngineKind::Certificate,
                format!(
                    "the external TLS certificate for silo {} ({}) needs \
                     attention",
                    silo.silo_name, silo.silo_id,
                ),
            ),
        };

        // Duplicate facts carry no information the kept fact doesn't; remove
        // them whether or not the kept fact still matches the observation.
        if let Some((_, parsed_case)) = parent {
            for fact_id in &parsed_case.duplicate_facts {
                case_mut.remove_fact(*fact_id, "duplicate fact on the case");
            }
        }

        if let Some((_, parsed_case)) = parent {
            let (fact_id, carried) = &parsed_case.fact;
            if *carried == desired {
                continue;
            }
            case_mut.remove_fact(
                *fact_id,
                "fact no longer matches the silo's best certificate",
            );
        }

        // The comment is recorded once, when the fact is added, and carried
        // forward unchanged, so it names only the absolute expiration time
        // rather than a distance from a "now" that will go stale.
        let comment = match &desired {
            CertificateFact::BestCertificateExpiring(p) => format!(
                "best certificate {} ({}) expires at {}",
                best.name, best.id, p.not_after,
            ),
            CertificateFact::BestCertificateExpired(p) => format!(
                "best certificate {} ({}) expired at {}",
                best.name, best.id, p.not_after,
            ),
        };
        case_mut.add_fact(desired.clone(), comment.clone());

        // The fact is new in this sitrep, so the condition it describes is
        // new too (or concerns a different certificate than before): alert.
        let alert_silo = alert_types::AlertSilo {
            id: silo.silo_id,
            name: silo.silo_name.clone(),
        };
        let alert_cert = alert_types::AlertCertificate {
            id: best.id,
            name: best.name.clone(),
            not_after: best.not_after,
        };
        let alert_result = match &desired {
            CertificateFact::BestCertificateExpiring(_) => case_mut
                .request_alert(
                    &alert_types::SiloCertificateExpiringV0 {
                        silo: alert_silo,
                        certificate: alert_cert,
                        time: reference_time,
                    },
                    &comment,
                ),
            CertificateFact::BestCertificateExpired(_) => case_mut
                .request_alert(
                    &alert_types::SiloCertificateExpiredV0 {
                        silo: alert_silo,
                        certificate: alert_cert,
                        time: reference_time,
                    },
                    &comment,
                ),
        };
        if let Err(err) = alert_result {
            case_mut
                .log_warning("failed to request alert for certificate fact")
                .kv("silo_id", silo.silo_id)
                .kv("certificate_id", best.id)
                .kv("error", format_args!("{err}"));
        }
    }

    Ok(())
}

/// The warning window configured in `config`, as a duration.
fn warning_window(config: &FmConfig) -> TimeDelta {
    TimeDelta::days(i64::from(
        config.certificate_expiry_warning_days.value().get(),
    ))
}

/// The fact this silo's case should carry now, if any, paired with the
/// certificate it is about.
///
/// The certificate considered is the one Nexus serves: the silo's certificate
/// with the latest leaf `not_after` (see the module docs). A silo with no
/// certificates carries no fact.
fn desired_fact(
    silo: &ObservedSiloCertificates,
    reference_time: DateTime<Utc>,
    window: TimeDelta,
) -> Option<(CertificateFact, &ObservedCertificate)> {
    let best = silo.best_certificate()?;
    let payload = CertificateExpiryFactPayload {
        silo_id: silo.silo_id,
        certificate_id: best.id,
        not_after: best.not_after,
    };
    if best.not_after <= reference_time {
        Some((CertificateFact::BestCertificateExpired(payload), best))
    } else if best.not_after <= reference_time + window {
        Some((CertificateFact::BestCertificateExpiring(payload), best))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_input::Input;
    use crate::builder::{SitrepBuilder, SitrepBuilderRng};
    use crate::test_util::FmTest;
    use iddqd::IdOrdMap;
    use nexus_types::alert::AlertClass;
    use nexus_types::fm::config::Setting;
    use nexus_types::fm::{Sitrep, SitrepVersion};
    use nexus_types::inventory;
    use omicron_common::api::external::Name;
    use omicron_generation_kinds::{AlertGeneration, SupportBundleGeneration};
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, SitrepUuid};
    use std::num::NonZeroU32;
    use std::sync::Arc;

    const SILO_A: Uuid = Uuid::from_u128(0xA);
    const SILO_B: Uuid = Uuid::from_u128(0xB);
    const CERT_1: Uuid = Uuid::from_u128(0x1);
    const CERT_2: Uuid = Uuid::from_u128(0x2);

    /// Build a synthetic example collection (only used here for its
    /// `time_done`, which is the expiry reference time).
    fn setup(
        test_name: &'static str,
    ) -> (dev::LogContext, inventory::Collection) {
        let (fm_test, logctx) = FmTest::new_with_logctx(test_name);
        let (example, _bp) = fm_test.system_builder.build();
        (logctx, example.collection)
    }

    fn name(s: &str) -> Name {
        s.parse().expect("test names are valid")
    }

    fn mk_cert(
        id: Uuid,
        cert_name: &str,
        not_after: DateTime<Utc>,
    ) -> ObservedCertificate {
        ObservedCertificate {
            id,
            name: name(cert_name),
            not_before: not_after - TimeDelta::days(365),
            not_after,
        }
    }

    fn mk_silo(
        id: Uuid,
        silo_name: &str,
        certs: impl IntoIterator<Item = ObservedCertificate>,
    ) -> ObservedSiloCertificates {
        ObservedSiloCertificates {
            silo_id: id,
            silo_name: name(silo_name),
            certificates: certs.into_iter().collect(),
        }
    }

    fn silo_map(
        silos: impl IntoIterator<Item = ObservedSiloCertificates>,
    ) -> IdOrdMap<ObservedSiloCertificates> {
        silos.into_iter().collect()
    }

    /// The default config with the warning window overridden to `days`.
    fn config_with_window(days: u32) -> FmConfig {
        FmConfig {
            certificate_expiry_warning_days: Setting::new(
                NonZeroU32::new(days).unwrap(),
            ),
            ..FmConfig::default()
        }
    }

    fn build_input(
        collection: inventory::Collection,
        parent_sitrep: Option<Sitrep>,
        silos: IdOrdMap<ObservedSiloCertificates>,
        config: FmConfig,
    ) -> Input {
        let parent = parent_sitrep.map(|s| {
            Arc::new((
                SitrepVersion {
                    id: s.id(),
                    version: 0,
                    time_made_current: Utc::now(),
                },
                s,
            ))
        });
        let builder = Input::builder(parent, Arc::new(collection))
            .expect("input builder should accept fresh inventory")
            .observed_silo_certificates(Arc::new(silos))
            .config(config)
            .with_empty_defaults();
        builder.build().expect("all inputs provided").0
    }

    /// Runs the engine over `input`. `seed` drives the sitrep's deterministic
    /// UUID generation; multi-run tests pass a different seed per run so the
    /// two sitreps (and any facts they create) get distinct IDs.
    fn run_analyze(log: &slog::Logger, input: &Input, seed: &str) -> Sitrep {
        let mut builder = SitrepBuilder::new_with_rng(
            log,
            input,
            SitrepBuilderRng::from_seed(seed),
        );
        analyze(&mut builder).expect("analyze ok");
        let (sitrep, report) =
            builder.build(OmicronZoneUuid::new_v4(), Utc::now());
        eprintln!("\n--- analysis ---\n{}", report.display_multiline(0));
        for case in &sitrep.cases {
            eprintln!("{}", case.display_indented(0, None));
        }
        sitrep
    }

    fn cert_cases(sitrep: &Sitrep) -> Vec<&fm::Case> {
        sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::Certificate)
            .collect()
    }

    fn open_cert_cases(sitrep: &Sitrep) -> Vec<&fm::Case> {
        cert_cases(sitrep).into_iter().filter(|c| c.is_open()).collect()
    }

    #[track_caller]
    fn sole_open_case(sitrep: &Sitrep) -> &fm::Case {
        let cases = open_cert_cases(sitrep);
        assert_eq!(cases.len(), 1, "expected exactly one open case");
        cases[0]
    }

    /// The one certificate fact on `case`, with its decoded payload.
    #[track_caller]
    fn sole_fact(case: &fm::Case) -> (FactUuid, CertificateFact) {
        assert_eq!(
            case.facts.len(),
            1,
            "expected exactly one fact on case {}",
            case.id
        );
        let fact = case.facts.iter().next().unwrap();
        let cert_fact = fact.as_certificate().expect("fact is a cert fact");
        (fact.metadata.id, cert_fact.clone())
    }

    fn alert_count(case: &fm::Case, class: AlertClass) -> usize {
        case.alerts_requested.iter().filter(|a| a.class == class).count()
    }

    #[track_caller]
    fn assert_alert_counts(case: &fm::Case, expiring: usize, expired: usize) {
        assert_eq!(
            alert_count(case, AlertClass::SiloCertificateExpiring),
            expiring,
            "unexpected number of expiring alerts on case {}",
            case.id
        );
        assert_eq!(
            alert_count(case, AlertClass::SiloCertificateExpired),
            expired,
            "unexpected number of expired alerts on case {}",
            case.id
        );
    }

    fn mk_fact(
        parent_sitrep_id: SitrepUuid,
        payload: impl Into<fm::FactPayload>,
    ) -> fm::case::Fact {
        fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: FactUuid::new_v4(),
                created_sitrep_id: parent_sitrep_id,
                comment: "parent certificate fact".to_string(),
            },
            payload: payload.into(),
        }
    }

    fn make_certificate_case(
        case_id: CaseUuid,
        parent_sitrep_id: SitrepUuid,
        facts: impl IntoIterator<Item = fm::case::Fact>,
    ) -> fm::Case {
        let mut fact_map = IdOrdMap::new();
        for fact in facts {
            fact_map.insert_unique(fact).unwrap();
        }
        fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: None,
                de: DiagnosisEngineKind::Certificate,
                comment: "parent certificate case".to_string(),
            },
            ereports: Default::default(),
            alerts_requested: Default::default(),
            support_bundles_requested: Default::default(),
            facts: fact_map,
        }
    }

    fn make_parent_sitrep(
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        cases: impl IntoIterator<Item = fm::Case>,
    ) -> Sitrep {
        let mut case_map = IdOrdMap::new();
        for case in cases {
            case_map.insert_unique(case).unwrap();
        }
        Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id,
                creator_id: OmicronZoneUuid::new_v4(),
                parent_sitrep_id: None,
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                comment: String::new(),
                alert_generation: AlertGeneration::new(),
                support_bundle_generation: SupportBundleGeneration::new(),
            },
            cases: case_map,
            ereports_by_id: Default::default(),
        }
    }

    fn expiring_payload(not_after: DateTime<Utc>) -> CertificateFact {
        CertificateFact::BestCertificateExpiring(CertificateExpiryFactPayload {
            silo_id: SILO_A,
            certificate_id: CERT_1,
            not_after,
        })
    }

    /// A collection identical to `collection` but observed `later`.
    fn advance(
        collection: &inventory::Collection,
        later: TimeDelta,
    ) -> inventory::Collection {
        let mut c = collection.clone();
        c.time_done += later;
        c
    }

    #[test]
    fn expiring_best_cert_opens_case_and_alerts() {
        let (logctx, collection) =
            setup("expiring_best_cert_opens_case_and_alerts");
        let now = collection.time_done;
        let not_after = now + TimeDelta::days(10);
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", not_after)],
        )]);
        let input = build_input(collection, None, silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");

        let case = sole_open_case(&sitrep);
        let (_, fact) = sole_fact(case);
        assert_eq!(fact, expiring_payload(not_after));
        assert_alert_counts(case, 1, 0);

        let alert = case.alerts_requested.iter().next().unwrap();
        let payload = serde_json::from_value::<
            alert_types::SiloCertificateExpiringV0,
        >(alert.payload.clone())
        .expect("alert payload decodes");
        assert_eq!(payload.silo.id, SILO_A);
        assert_eq!(payload.silo.name, name("fake-silo-a"));
        assert_eq!(payload.certificate.id, CERT_1);
        assert_eq!(payload.certificate.name, name("fake-cert-1"));
        assert_eq!(payload.certificate.not_after, not_after);
        assert_eq!(payload.time, now);

        logctx.cleanup_successful();
    }

    #[test]
    fn expired_best_cert_opens_case_and_alerts() {
        let (logctx, collection) =
            setup("expired_best_cert_opens_case_and_alerts");
        let now = collection.time_done;
        let not_after = now - TimeDelta::days(1);
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", not_after)],
        )]);
        let input = build_input(collection, None, silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");

        let case = sole_open_case(&sitrep);
        let (_, fact) = sole_fact(case);
        assert_eq!(
            fact,
            CertificateFact::BestCertificateExpired(
                CertificateExpiryFactPayload {
                    silo_id: SILO_A,
                    certificate_id: CERT_1,
                    not_after,
                }
            )
        );
        assert_alert_counts(case, 0, 1);

        let alert = case.alerts_requested.iter().next().unwrap();
        let payload = serde_json::from_value::<
            alert_types::SiloCertificateExpiredV0,
        >(alert.payload.clone())
        .expect("alert payload decodes");
        assert_eq!(payload.certificate.id, CERT_1);
        assert_eq!(payload.time, now);

        logctx.cleanup_successful();
    }

    #[test]
    fn later_expiring_replacement_prevents_case() {
        let (logctx, collection) =
            setup("later_expiring_replacement_prevents_case");
        let now = collection.time_done;
        // The expiring (even expired) certificate is not what Nexus serves,
        // because a later-expiring one exists.
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [
                mk_cert(CERT_1, "fake-cert-1", now - TimeDelta::days(1)),
                mk_cert(CERT_2, "fake-cert-2", now + TimeDelta::days(400)),
            ],
        )]);
        let input = build_input(collection, None, silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");
        assert!(cert_cases(&sitrep).is_empty(), "no case should open");
        logctx.cleanup_successful();
    }

    #[test]
    fn silo_without_certificates_opens_no_case() {
        let (logctx, collection) =
            setup("silo_without_certificates_opens_no_case");
        let silos = silo_map([mk_silo(SILO_A, "fake-silo-a", [])]);
        let input = build_input(collection, None, silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");
        assert!(cert_cases(&sitrep).is_empty(), "no case should open");
        logctx.cleanup_successful();
    }

    #[test]
    fn distinct_silos_get_distinct_cases() {
        let (logctx, collection) = setup("distinct_silos_get_distinct_cases");
        let now = collection.time_done;
        let silos = silo_map([
            mk_silo(
                SILO_A,
                "fake-silo-a",
                [mk_cert(CERT_1, "fake-cert-1", now + TimeDelta::days(5))],
            ),
            mk_silo(
                SILO_B,
                "fake-silo-b",
                [mk_cert(CERT_2, "fake-cert-2", now - TimeDelta::days(5))],
            ),
        ]);
        let input = build_input(collection, None, silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");

        let cases = open_cert_cases(&sitrep);
        assert_eq!(cases.len(), 2);
        let mut silos_seen: Vec<Uuid> =
            cases.iter().map(|c| sole_fact(c).1.silo_id()).collect();
        silos_seen.sort();
        assert_eq!(silos_seen, vec![SILO_A, SILO_B]);
        logctx.cleanup_successful();
    }

    #[test]
    fn carried_expiring_case_rotates_to_expired_with_one_alert() {
        let (logctx, collection) =
            setup("carried_expiring_case_rotates_to_expired_with_one_alert");
        let now = collection.time_done;
        let not_after = now + TimeDelta::days(10);
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", not_after)],
        )]);

        let input1 = build_input(
            collection.clone(),
            None,
            silos.clone(),
            FmConfig::default(),
        );
        let sitrep1 = run_analyze(&logctx.log, &input1, "run-1");
        let case1_id = sole_open_case(&sitrep1).id;
        let (fact1_id, _) = sole_fact(sole_open_case(&sitrep1));

        // Twenty days later, the certificate has expired.
        let input2 = build_input(
            advance(&collection, TimeDelta::days(20)),
            Some(sitrep1),
            silos,
            FmConfig::default(),
        );
        let sitrep2 = run_analyze(&logctx.log, &input2, "run-2");

        let case = sole_open_case(&sitrep2);
        assert_eq!(case.id, case1_id, "the same case should be reused");
        let (fact2_id, fact) = sole_fact(case);
        assert_ne!(fact2_id, fact1_id, "the expired fact is a new fact");
        assert_eq!(
            case.facts.iter().next().unwrap().metadata.created_sitrep_id,
            sitrep2.id(),
            "the expired fact was created in the second sitrep"
        );
        assert_eq!(
            fact,
            CertificateFact::BestCertificateExpired(
                CertificateExpiryFactPayload {
                    silo_id: SILO_A,
                    certificate_id: CERT_1,
                    not_after,
                }
            )
        );
        // The expiring alert is carried from the first sitrep; exactly one
        // expired alert is new.
        assert_alert_counts(case, 1, 1);
        logctx.cleanup_successful();
    }

    #[test]
    fn carried_case_closes_when_replacement_installed() {
        let (logctx, collection) =
            setup("carried_case_closes_when_replacement_installed");
        let now = collection.time_done;
        let expiring =
            mk_cert(CERT_1, "fake-cert-1", now + TimeDelta::days(10));

        let input1 = build_input(
            collection.clone(),
            None,
            silo_map([mk_silo(SILO_A, "fake-silo-a", [expiring.clone()])]),
            FmConfig::default(),
        );
        let sitrep1 = run_analyze(&logctx.log, &input1, "run-1");
        let case1_id = sole_open_case(&sitrep1).id;

        let input2 = build_input(
            collection,
            Some(sitrep1),
            silo_map([mk_silo(
                SILO_A,
                "fake-silo-a",
                [
                    expiring,
                    mk_cert(CERT_2, "fake-cert-2", now + TimeDelta::days(400)),
                ],
            )]),
            FmConfig::default(),
        );
        let sitrep2 = run_analyze(&logctx.log, &input2, "run-2");

        assert!(open_cert_cases(&sitrep2).is_empty());
        let case = sitrep2.cases.get(&case1_id).expect("case carried");
        assert!(!case.is_open(), "case should be closed");
        assert_alert_counts(case, 1, 0);
        logctx.cleanup_successful();
    }

    #[test]
    fn carried_case_closes_when_silo_removed() {
        let (logctx, collection) =
            setup("carried_case_closes_when_silo_removed");
        let now = collection.time_done;
        let input1 = build_input(
            collection.clone(),
            None,
            silo_map([mk_silo(
                SILO_A,
                "fake-silo-a",
                [mk_cert(CERT_1, "fake-cert-1", now + TimeDelta::days(10))],
            )]),
            FmConfig::default(),
        );
        let sitrep1 = run_analyze(&logctx.log, &input1, "run-1");
        let case1_id = sole_open_case(&sitrep1).id;

        let input2 = build_input(
            collection,
            Some(sitrep1),
            silo_map([]),
            FmConfig::default(),
        );
        let sitrep2 = run_analyze(&logctx.log, &input2, "run-2");
        let case = sitrep2.cases.get(&case1_id).expect("case carried");
        assert!(!case.is_open(), "case should be closed");
        logctx.cleanup_successful();
    }

    #[test]
    fn new_best_cert_still_expiring_rotates_fact_and_realerts() {
        let (logctx, collection) =
            setup("new_best_cert_still_expiring_rotates_fact_and_realerts");
        let now = collection.time_done;
        let cert1 = mk_cert(CERT_1, "fake-cert-1", now + TimeDelta::days(10));

        let input1 = build_input(
            collection.clone(),
            None,
            silo_map([mk_silo(SILO_A, "fake-silo-a", [cert1.clone()])]),
            FmConfig::default(),
        );
        let sitrep1 = run_analyze(&logctx.log, &input1, "run-1");

        // An operator uploads a replacement that is itself inside the window.
        let cert2_not_after = now + TimeDelta::days(20);
        let input2 = build_input(
            collection,
            Some(sitrep1),
            silo_map([mk_silo(
                SILO_A,
                "fake-silo-a",
                [cert1, mk_cert(CERT_2, "fake-cert-2", cert2_not_after)],
            )]),
            FmConfig::default(),
        );
        let sitrep2 = run_analyze(&logctx.log, &input2, "run-2");

        let case = sole_open_case(&sitrep2);
        let (_, fact) = sole_fact(case);
        assert_eq!(
            fact,
            CertificateFact::BestCertificateExpiring(
                CertificateExpiryFactPayload {
                    silo_id: SILO_A,
                    certificate_id: CERT_2,
                    not_after: cert2_not_after,
                }
            )
        );
        assert_alert_counts(case, 2, 0);
        logctx.cleanup_successful();
    }

    #[test]
    fn rerun_with_unchanged_input_changes_nothing() {
        let (logctx, collection) =
            setup("rerun_with_unchanged_input_changes_nothing");
        let now = collection.time_done;
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", now + TimeDelta::days(10))],
        )]);

        let input1 = build_input(
            collection.clone(),
            None,
            silos.clone(),
            FmConfig::default(),
        );
        let sitrep1 = run_analyze(&logctx.log, &input1, "run-1");
        let case1 = sole_open_case(&sitrep1).clone();
        let (fact1_id, fact1) = sole_fact(&case1);

        let input2 =
            build_input(collection, Some(sitrep1), silos, FmConfig::default());
        let sitrep2 = run_analyze(&logctx.log, &input2, "run-2");

        let case2 = sole_open_case(&sitrep2);
        assert_eq!(case2.id, case1.id);
        let (fact2_id, fact2) = sole_fact(case2);
        assert_eq!(fact2_id, fact1_id, "the fact should be carried unchanged");
        assert_eq!(fact2, fact1);
        assert_eq!(
            case2.facts.iter().next().unwrap().metadata.created_sitrep_id,
            case1.metadata.created_sitrep_id,
            "the carried fact keeps its original creation sitrep"
        );
        assert_alert_counts(case2, 1, 0);
        assert_eq!(case2.alerts_requested, case1.alerts_requested);
        logctx.cleanup_successful();
    }

    /// Two certificates with the same `not_after` (say, the same PEM uploaded
    /// under two names) must not make the engine flip between them across
    /// sitreps: the fact and its alert are carried unchanged.
    #[test]
    fn equal_expiry_certificates_carry_fact_unchanged() {
        let (logctx, collection) =
            setup("equal_expiry_certificates_carry_fact_unchanged");
        let now = collection.time_done;
        let not_after = now + TimeDelta::days(10);
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [
                mk_cert(CERT_1, "fake-cert-1", not_after),
                mk_cert(CERT_2, "fake-cert-2", not_after),
            ],
        )]);

        let input1 = build_input(
            collection.clone(),
            None,
            silos.clone(),
            FmConfig::default(),
        );
        let sitrep1 = run_analyze(&logctx.log, &input1, "run-1");
        let case1 = sole_open_case(&sitrep1).clone();
        let (fact1_id, fact1) = sole_fact(&case1);
        assert_eq!(
            fact1,
            CertificateFact::BestCertificateExpiring(
                CertificateExpiryFactPayload {
                    silo_id: SILO_A,
                    certificate_id: CERT_2,
                    not_after,
                }
            ),
            "ties break toward the greatest certificate id"
        );

        let input2 =
            build_input(collection, Some(sitrep1), silos, FmConfig::default());
        let sitrep2 = run_analyze(&logctx.log, &input2, "run-2");

        let case2 = sole_open_case(&sitrep2);
        assert_eq!(case2.id, case1.id);
        let (fact2_id, fact2) = sole_fact(case2);
        assert_eq!(fact2_id, fact1_id, "the fact should be carried unchanged");
        assert_eq!(fact2, fact1);
        assert_alert_counts(case2, 1, 0);
        assert_eq!(case2.alerts_requested, case1.alerts_requested);
        logctx.cleanup_successful();
    }

    #[test]
    fn window_override_changes_outcome() {
        let (logctx, collection) = setup("window_override_changes_outcome");
        let now = collection.time_done;
        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", now + TimeDelta::days(45))],
        )]);

        // 45 days out is outside the default 30-day window...
        let input = build_input(
            collection.clone(),
            None,
            silos.clone(),
            FmConfig::default(),
        );
        let sitrep = run_analyze(&logctx.log, &input, "run-1");
        assert!(cert_cases(&sitrep).is_empty());

        // ...but inside a 60-day one.
        let input =
            build_input(collection, None, silos, config_with_window(60));
        let sitrep = run_analyze(&logctx.log, &input, "run-1");
        let case = sole_open_case(&sitrep);
        assert_alert_counts(case, 1, 0);
        logctx.cleanup_successful();
    }

    #[test]
    fn uninterpretable_parent_cases_are_closed_and_replaced() {
        let (logctx, collection) =
            setup("uninterpretable_parent_cases_are_closed_and_replaced");
        let now = collection.time_done;
        let not_after = now + TimeDelta::days(10);

        let parent_sitrep_id = SitrepUuid::new_v4();
        let foreign_case_id = CaseUuid::new_v4();
        let empty_case_id = CaseUuid::new_v4();
        let parent = {
            let mut parent = make_parent_sitrep(
                collection.id,
                [
                    // A Certificate case carrying another engine's fact.
                    make_certificate_case(
                        foreign_case_id,
                        parent_sitrep_id,
                        [mk_fact(
                            parent_sitrep_id,
                            fm::SagaFact::Abandoned(
                                fm::SagaAbandonedFactPayload {
                                    saga_id: steno::SagaId(Uuid::new_v4()),
                                },
                            ),
                        )],
                    ),
                    // A Certificate case with no facts at all.
                    make_certificate_case(empty_case_id, parent_sitrep_id, []),
                ],
            );
            parent.metadata.id = parent_sitrep_id;
            parent
        };

        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", not_after)],
        )]);
        let input =
            build_input(collection, Some(parent), silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");

        for id in [foreign_case_id, empty_case_id] {
            let case = sitrep.cases.get(&id).expect("case carried");
            assert!(!case.is_open(), "case {id} should be closed");
        }
        let case = sole_open_case(&sitrep);
        assert!(case.id != foreign_case_id && case.id != empty_case_id);
        let (_, fact) = sole_fact(case);
        assert_eq!(fact, expiring_payload(not_after));
        assert_alert_counts(case, 1, 0);
        logctx.cleanup_successful();
    }

    #[test]
    fn duplicate_cases_for_silo_keep_lowest_id() {
        let (logctx, collection) =
            setup("duplicate_cases_for_silo_keep_lowest_id");
        let now = collection.time_done;
        let not_after = now + TimeDelta::days(10);

        let parent_sitrep_id = SitrepUuid::new_v4();
        let low_id = CaseUuid::from_untyped_uuid(Uuid::from_u128(1));
        let high_id = CaseUuid::from_untyped_uuid(Uuid::from_u128(2));
        let mut parent = make_parent_sitrep(
            collection.id,
            [low_id, high_id].map(|id| {
                make_certificate_case(
                    id,
                    parent_sitrep_id,
                    [mk_fact(parent_sitrep_id, expiring_payload(not_after))],
                )
            }),
        );
        parent.metadata.id = parent_sitrep_id;

        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", not_after)],
        )]);
        let input =
            build_input(collection, Some(parent), silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");

        assert!(!sitrep.cases.get(&high_id).unwrap().is_open());
        let case = sole_open_case(&sitrep);
        assert_eq!(case.id, low_id);
        // The kept case's fact already matched, so nothing new was alerted.
        assert_alert_counts(case, 0, 0);
        logctx.cleanup_successful();
    }

    #[test]
    fn duplicate_facts_on_case_are_removed() {
        let (logctx, collection) = setup("duplicate_facts_on_case_are_removed");
        let now = collection.time_done;
        let not_after = now + TimeDelta::days(10);

        let parent_sitrep_id = SitrepUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let facts = [
            mk_fact(parent_sitrep_id, expiring_payload(not_after)),
            mk_fact(parent_sitrep_id, expiring_payload(not_after)),
        ];
        let lowest_fact_id = facts.iter().map(|f| f.metadata.id).min().unwrap();
        let mut parent = make_parent_sitrep(
            collection.id,
            [make_certificate_case(case_id, parent_sitrep_id, facts)],
        );
        parent.metadata.id = parent_sitrep_id;

        let silos = silo_map([mk_silo(
            SILO_A,
            "fake-silo-a",
            [mk_cert(CERT_1, "fake-cert-1", not_after)],
        )]);
        let input =
            build_input(collection, Some(parent), silos, FmConfig::default());
        let sitrep = run_analyze(&logctx.log, &input, "run-1");

        let case = sole_open_case(&sitrep);
        assert_eq!(case.id, case_id);
        let (fact_id, fact) = sole_fact(case);
        assert_eq!(fact_id, lowest_fact_id, "the lowest-UUID fact is kept");
        assert_eq!(fact, expiring_payload(not_after));
        assert_alert_counts(case, 0, 0);
        logctx.cleanup_successful();
    }
}
