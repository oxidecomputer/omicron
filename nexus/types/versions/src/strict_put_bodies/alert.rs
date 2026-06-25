// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Alert types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParamsStrict;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

/// Parameters to update a webhook configuration.
///
/// A `PUT` replaces the resource, so `name`, `description`, and `endpoint` must
/// all be present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookReceiverUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,

    /// The URL that webhook notification requests should be sent to
    pub endpoint: Url,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<WebhookReceiverUpdate>
    for crate::v2025_11_20_00::alert::WebhookReceiverUpdate
{
    fn from(new: WebhookReceiverUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
            endpoint: Some(new.endpoint),
        }
    }
}
