// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use propolis_api_types::instance_spec::Component;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct VmmDiskAttachBody {
    pub components: HashMap<String, Component>,
}
