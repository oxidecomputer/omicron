// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of the certificate diagnosis engine's facts.
//!
//! Each certificate fact is stored as typed columns in the
//! `fm_fact_certificate` table. The `kind` discriminant selects which payload
//! columns are populated; per-kind CHECK constraints enforce that the right
//! columns are non-NULL for each kind. See
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
use omicron_common::api::external::Error;
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
///
/// The payload columns are populated according to `kind`: a column is `Some`
/// if it belongs to that `kind`'s payload, and `None` otherwise.
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

    // Columns shared by both kinds.
    pub certificate_id: Option<Uuid>,
    pub not_after: Option<DateTime<Utc>>,
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
            certificate_id: Some(payload.certificate_id),
            not_after: Some(payload.not_after),
        }
    }

    /// Reconstruct an in-memory fact from a row.
    pub fn into_fact(self) -> Result<fm::case::Fact, Error> {
        let kind = self.kind;
        let payload = CertificateExpiryFactPayload {
            silo_id: self.silo_id,
            certificate_id: self
                .certificate_id
                .ok_or_else(|| missing_column(kind, "certificate_id"))?,
            not_after: self
                .not_after
                .ok_or_else(|| missing_column(kind, "not_after"))?,
        };
        let payload = match kind {
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
        Ok(fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: self.id.into(),
                created_sitrep_id: self.created_sitrep_id.into(),
                comment: self.comment,
            },
            payload,
        })
    }
}

fn missing_column(kind: FmFactCertificateKind, column: &str) -> Error {
    Error::InternalError {
        internal_message: format!(
            "fm_fact_certificate row of kind {kind:?} has a NULL {column}, \
             violating the CHECK constraint requiring it to be non-NULL for \
             this kind"
        ),
    }
}
