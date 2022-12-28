// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::schema::certificate;
use db_macros::Asset;
use uuid::Uuid;

/// Representation of x509 certificates used by services.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = certificate)]
pub struct Certificate {
    #[diesel(embed)]
    identity: CertificateIdentity,

    pub service: ServiceKind,

    pub cert: Vec<u8>,
    pub key: Vec<u8>,
}

impl Certificate {
    pub fn new(
        id: Uuid,
        service: ServiceKind,
        cert: Vec<u8>,
        key: Vec<u8>,
    ) -> Self {
        Self { identity: CertificateIdentity::new(id), service, cert, key }
    }
}
