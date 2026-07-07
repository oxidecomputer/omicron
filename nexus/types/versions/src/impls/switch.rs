// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for switch types.

use crate::latest::switch::SwitchLinkState;

impl SwitchLinkState {
    pub fn new(
        link: serde_json::Value,
        monitors: Option<serde_json::Value>,
    ) -> Self {
        Self { link, monitors }
    }
}
