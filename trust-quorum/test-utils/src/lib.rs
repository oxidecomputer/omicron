// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for use by our proptests and tqdb

mod event;
mod event_log;
pub mod nexus;
mod state;

pub use event::Event;
pub use event_log::EventLog;
pub use state::TqState;

use trust_quorum::BaseboardId;

/// All possible members used in a test
pub fn member_universe(size: usize) -> Vec<BaseboardId> {
    (0..size)
        .map(|serial| BaseboardId {
            part_number: "test".into(),
            serial_number: serial.to_string(),
        })
        .collect()
}
