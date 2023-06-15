// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Actions for state stateful property based tests.

use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use uuid::Uuid;

/// A test action to drive the test forward
#[derive(Debug)]
pub enum Action {
    Initialize {
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        initial_members: BTreeSet<Baseboard>,
    },
}
