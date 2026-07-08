// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;

// This is a summary of the subset of `RackInitializeRequest` that is sensitive;
// we only report a summary instead of returning actual data.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentRssUserConfigSensitive {
    pub num_external_certificates: usize,
    pub recovery_silo_password_set: bool,
    // We define GetBgpAuthKeyInfoResponse in wicket-common and use a
    // progenitor replace directive for it, because we don't want typify to
    // turn the BTreeMap into a HashMap. Use the same struct here to piggyback
    // on that.
    pub bgp_auth_keys: GetBgpAuthKeyInfoResponse,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentRssUserConfig {
    pub sensitive: CurrentRssUserConfigSensitive,
    pub insensitive: CurrentRssUserConfigInsensitive,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CertificateUploadResponse {
    /// The key has been uploaded, but we're waiting on its corresponding
    /// certificate chain.
    WaitingOnCert,
    /// The cert chain has been uploaded, but we're waiting on its corresponding
    /// private key.
    WaitingOnKey,
    /// A cert chain and its key have been accepted.
    CertKeyAccepted,
    /// A cert chain and its key are valid, but have already been uploaded.
    CertKeyDuplicateIgnored,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PutRssRecoveryUserPasswordHash {
    pub hash: omicron_passwords::NewPasswordHash,
}
