// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of the certificate diagnosis engine's facts.
//!
//! Each certificate fact is stored as typed columns in the
//! `fm_fact_certificate` table. Every kind of certificate fact carries the
//! same payload, so the `kind` discriminant alone distinguishes them. See
//! [`nexus_types::fm::CertificateFact`] for semantics.

use crate::DbTypedUuid;
use crate::impl_enum_type;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_fact_certificate;
use nexus_types::fm;
use nexus_types::fm::case::FactMetadata;
use nexus_types::fm::{
    CertificateExpiryFactPayload, CertificateFact, FactPayload,
};
use omicron_uuid_kinds::{CaseKind, FactKind, SitrepKind};
use uuid::Uuid;

impl_enum_type!(
    FmFactCertificateKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum FmFactCertificateKind;

    BestCertificateExpiring => b"best_certificate_expiring"
    BestCertificateExpired => b"best_certificate_expired"
);

/// Diesel row for the `fm_fact_certificate` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_fact_certificate)]
pub struct FmFactCertificate {
    pub id: DbTypedUuid<FactKind>,
    /// The sitrep to which this fact belongs.
    ///
    /// This will change as the fact is carried forward from one sitrep to the
    /// next.
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
    /// Sitrep in which this fact was first added.
    ///
    /// Preserved unchanged when the fact is carried forward; debug-only.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    pub comment: String,

    /// The silo this fact is about.
    pub silo_id: Uuid,
    pub kind: FmFactCertificateKind,

    pub certificate_id: Uuid,
    pub not_after: DateTime<Utc>,
}

impl FmFactCertificate {
    /// Build a row from a fact's shared metadata (`metadata`) and its
    /// already-dispatched certificate payload (`cert_fact`).
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        metadata: &FactMetadata,
        cert_fact: &CertificateFact,
    ) -> Self {
        let FactMetadata { id, created_sitrep_id, comment } = metadata;
        let kind = match cert_fact {
            CertificateFact::BestCertificateExpiring(_) => {
                FmFactCertificateKind::BestCertificateExpiring
            }
            CertificateFact::BestCertificateExpired(_) => {
                FmFactCertificateKind::BestCertificateExpired
            }
        };
        let payload = cert_fact.payload();
        Self {
            id: (*id).into(),
            sitrep_id: sitrep_id.into(),
            case_id: case_id.into(),
            created_sitrep_id: (*created_sitrep_id).into(),
            comment: comment.clone(),
            silo_id: payload.silo_id,
            kind,
            certificate_id: payload.certificate_id,
            not_after: payload.not_after,
        }
    }

    /// Reconstruct an in-memory fact from a row.
    pub fn into_fact(self) -> fm::case::Fact {
        let payload = CertificateExpiryFactPayload {
            silo_id: self.silo_id,
            certificate_id: self.certificate_id,
            not_after: self.not_after,
        };
        let payload = match self.kind {
            FmFactCertificateKind::BestCertificateExpiring => {
                FactPayload::Certificate(
                    CertificateFact::BestCertificateExpiring(payload),
                )
            }
            FmFactCertificateKind::BestCertificateExpired => {
                FactPayload::Certificate(
                    CertificateFact::BestCertificateExpired(payload),
                )
            }
        };
        fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: self.id.into(),
                created_sitrep_id: self.created_sitrep_id.into(),
                comment: self.comment,
            },
            payload,
        }
    }
}
