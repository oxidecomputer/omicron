// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::schema::certificate;
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use uuid::Uuid;

/// Representation of x509 certificates used by services.
#[derive(Queryable, Insertable, Clone, Selectable, Resource)]
#[diesel(table_name = certificate)]
pub struct Certificate {
    #[diesel(embed)]
    identity: CertificateIdentity,

    pub service: ServiceKind,

    pub cert: Vec<u8>,
    pub key: Vec<u8>,
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certificate")
            .field("identity", &self.identity)
            .field("service", &self.service)
            .field("cert", &self.cert)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl Certificate {
    pub fn new(
        id: Uuid,
        service: ServiceKind,
        params: params::CertificateCreate,
    ) -> Self {
        Self {
            identity: CertificateIdentity::new(id, params.identity),
            service,
            cert: params.cert,
            key: params.key,
        }
    }
}

impl From<Certificate> for views::Certificate {
    fn from(cert: Certificate) -> Self {
        Self { identity: cert.identity() }
    }
}
