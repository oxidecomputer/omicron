// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! "Currently installed external TLS certificates": the executed view from
//! the `silo` and `certificate` DB tables, reduced to what fault management
//! needs to reason about certificate expiry.
//!
//! Nexus serves each silo's external API with the silo's certificate whose
//! leaf `not_after` is latest (see `ExternalEndpoint::best_certificate` in
//! Nexus). The certificate diagnosis engine predicts that choice from this
//! view, so the view carries only the leaf certificate's validity window and
//! enough identity to name the certificate in a case or alert.

use chrono::{DateTime, Utc};
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_common::api::external::Name;
use uuid::Uuid;

/// One silo and every non-deleted external TLS certificate installed for it.
///
/// A silo with no certificates is still represented (with an empty
/// `certificates` map), so that consumers can tell "silo exists with no
/// certificates" apart from "silo does not exist".
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedSiloCertificates {
    pub silo_id: Uuid,
    pub silo_name: Name,
    pub certificates: IdOrdMap<ObservedCertificate>,
}

impl ObservedSiloCertificates {
    /// The certificate Nexus will serve for this silo: the one whose leaf
    /// `not_after` is latest, or `None` if the silo has no certificates.
    ///
    /// This must stay in lockstep with `ExternalEndpoint::best_certificate`
    /// in Nexus, which applies the same rule to choose the certificate
    /// actually presented to TLS clients. Like that function, this ignores
    /// `not_before`.
    ///
    /// When several certificates share the latest `not_after`, the one with
    /// the greatest id is returned. The tiebreak matters because the
    /// certificate diagnosis engine records the chosen certificate's id in
    /// its facts and treats a change of id as a new condition worth a fresh
    /// alert, so the choice must not vary between analyses of the same set of
    /// certificates. Nexus may serve a different one of the tied certificates,
    /// but it expires at the same time, so what the engine says about the
    /// expiration of the served certificate holds either way.
    pub fn best_certificate(&self) -> Option<&ObservedCertificate> {
        self.certificates.iter().max_by_key(|cert| (cert.not_after, cert.id))
    }
}

impl IdOrdItem for ObservedSiloCertificates {
    type Key<'a> = Uuid;
    fn key(&self) -> Self::Key<'_> {
        self.silo_id
    }
    id_upcast!();
}

/// One non-deleted external TLS certificate, reduced to its identity and the
/// validity window of its leaf certificate.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedCertificate {
    pub id: Uuid,
    pub name: Name,
    /// The leaf certificate's `not_before`. Recorded for reporting; the
    /// certificate diagnosis engine does not act on it.
    pub not_before: DateTime<Utc>,
    /// The leaf certificate's `not_after`.
    pub not_after: DateTime<Utc>,
}

impl IdOrdItem for ObservedCertificate {
    type Key<'a> = Uuid;
    fn key(&self) -> Self::Key<'_> {
        self.id
    }
    id_upcast!();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert(id: u128, not_after: DateTime<Utc>) -> ObservedCertificate {
        ObservedCertificate {
            id: Uuid::from_u128(id),
            name: format!("fake-cert-{id}").parse().unwrap(),
            not_before: not_after - chrono::TimeDelta::days(365),
            not_after,
        }
    }

    fn silo(
        certs: impl IntoIterator<Item = ObservedCertificate>,
    ) -> ObservedSiloCertificates {
        ObservedSiloCertificates {
            silo_id: Uuid::from_u128(0xA),
            silo_name: "fake-silo".parse().unwrap(),
            certificates: certs.into_iter().collect(),
        }
    }

    #[test]
    fn best_certificate_prefers_latest_not_after() {
        let t = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        // The later expiration wins even when it has the smaller id.
        let s = silo([cert(2, t), cert(1, t + chrono::TimeDelta::days(1))]);
        assert_eq!(s.best_certificate().unwrap().id, Uuid::from_u128(1));
        assert!(silo([]).best_certificate().is_none());
    }

    #[test]
    fn best_certificate_breaks_ties_toward_greatest_id() {
        let t = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        // Insertion order must not matter.
        for certs in [[cert(1, t), cert(2, t)], [cert(2, t), cert(1, t)]] {
            let s = silo(certs);
            assert_eq!(s.best_certificate().unwrap().id, Uuid::from_u128(2));
        }
    }
}
