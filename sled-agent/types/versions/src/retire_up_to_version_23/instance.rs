// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::PropolisUuid;
use schemars::JsonSchema;
use serde::Deserialize;

/// Path parameters for VMM requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmPathParam {
    pub propolis_id: PropolisUuid,
}

