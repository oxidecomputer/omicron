// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Updatable components ordered by dependencies (RFD 565).

use nexus_sled_agent_shared::inventory::ZoneKind;

/// Update sequence as defined by RFD 565 §6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum OrderedComponent {
    HostOs,
    SpRot,
    OmicronZone(ZoneKind),
}

impl From<ZoneKind> for OrderedComponent {
    fn from(zone_kind: ZoneKind) -> Self {
        Self::OmicronZone(zone_kind)
    }
}
