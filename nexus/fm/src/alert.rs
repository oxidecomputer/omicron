// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Alert messages.

use nexus_types::fm::AlertClass;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod power_shelf;

pub trait Alert: Serialize + JsonSchema + std::fmt::Debug {
    const CLASS: AlertClass;
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpdIdentity {
    pub part_number: Option<String>,
    pub revision: Option<String>,
    pub serial_number: Option<String>,
}
