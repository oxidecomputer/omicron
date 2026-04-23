// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;

/// Path parameters for an instance-scoped request.
#[derive(Deserialize, JsonSchema)]
pub struct InstancePathParam {
    pub instance_id: InstanceUuid,
}
