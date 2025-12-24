// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Certificate types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The service intended to use this certificate.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceUsingCertificate {
    /// This certificate is intended for access to the external API.
    ExternalApi,
}

// VIEWS

/// View of a Certificate
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Certificate {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The service using this certificate
    pub service: ServiceUsingCertificate,
    /// PEM-formatted string containing public certificate chain
    pub cert: String,
}

// PARAMS

/// Create-time parameters for a `Certificate`
#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct CertificateCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// PEM-formatted string containing public certificate chain
    pub cert: String,
    /// PEM-formatted string containing private key
    pub key: String,
    /// The service using this certificate
    pub service: ServiceUsingCertificate,
}

impl std::fmt::Debug for CertificateCreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateCreate")
            .field("identity", &self.identity)
            .field("cert", &self.cert)
            .field("key", &"<redacted>")
            .finish()
    }
}
