// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus functionality, divided into capability groups

use nexus_db_queries::db;
use slog::Logger;

mod sled_agent;

pub use sled_agent::NexusSledAgentBaseCapabilities;
pub use sled_agent::NexusSledAgentCapabilities;

pub trait NexusBaseCapabilities {
    fn log(&self) -> &Logger;
    fn datastore(&self) -> &db::DataStore;
}
