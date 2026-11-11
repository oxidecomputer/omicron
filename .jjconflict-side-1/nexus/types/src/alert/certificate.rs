// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo TLS certificate alert types.

use super::*;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::Name;
use serde::Deserialize;
use uuid::Uuid;

/// An alert indicating that the TLS certificate served for a silo's external
/// API is about to expire, and no later-expiring certificate is installed for
/// that silo.
///
/// Nexus serves the silo's certificate with the latest expiration time. Once
/// a certificate with a later expiration time is uploaded for the silo, this
/// condition is resolved.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SiloCertificateExpiringV0 {
    pub silo: AlertSilo,
    pub certificate: AlertCertificate,
    /// The time at which the condition was evaluated.
    pub time: DateTime<Utc>,
}

impl AlertPayload for SiloCertificateExpiringV0 {
    const CLASS: AlertClass = AlertClass::SiloCertificateExpiring;
    const VERSION: u32 = 0;
}

/// An alert indicating that the TLS certificate served for a silo's external
/// API has expired, and no later-expiring certificate is installed for that
/// silo.
///
/// Nexus continues to serve the expired certificate rather than none at all,
/// so that an operator can still connect (after choosing to trust it) and
/// upload a replacement. Once a certificate with a later expiration time is
/// uploaded for the silo, this condition is resolved.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SiloCertificateExpiredV0 {
    pub silo: AlertSilo,
    pub certificate: AlertCertificate,
    /// The time at which the condition was evaluated.
    pub time: DateTime<Utc>,
}

impl AlertPayload for SiloCertificateExpiredV0 {
    const CLASS: AlertClass = AlertClass::SiloCertificateExpired;
    const VERSION: u32 = 0;
}

/// Describes the silo involved in a certificate alert.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AlertSilo {
    pub id: Uuid,
    pub name: Name,
}

/// Describes the certificate involved in a certificate alert.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AlertCertificate {
    pub id: Uuid,
    pub name: Name,
    /// The time after which the certificate is no longer valid.
    pub not_after: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alert::tests::expectorate_alert_schema;

    #[test]
    fn silo_certificate_expiring_v0_schema() {
        expectorate_alert_schema::<SiloCertificateExpiringV0>();
    }

    #[test]
    fn silo_certificate_expired_v0_schema() {
        expectorate_alert_schema::<SiloCertificateExpiredV0>();
    }
}
