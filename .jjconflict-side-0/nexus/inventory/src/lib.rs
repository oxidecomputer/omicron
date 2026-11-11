// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron component inventory
//!
//! This module provides [`Collector`], an interface for collecting a complete
//! hardware/software inventory in a running Omicron deployment
//!
//! This is really just the collection part.  For separation of concerns, this
//! module doesn't know anything about storing these collections into the
//! database.  That's provided by the datastore.  The types associated with
//! collections are in `nexus_types::inventory` so they can be shared with other
//! parts of Nexus (like the datastore).
//!
//! This module lives inside Nexus but it has few dependencies on other parts of
//! Nexus.  It could be incorporated into other components.  (The corresponding
//! types in `nexus_types` might have to move, too)

mod builder;
mod collector;
pub mod examples;
mod sled_agent_enumerator;

// only exposed for test code to construct collections
pub use builder::CollectionBuilder;
pub use builder::CollectionBuilderRng;
pub use builder::CollectorBug;
pub use builder::InventoryError;

pub use builder::now_db_precision;

pub use collector::Collector;

pub use sled_agent_enumerator::SledAgentEnumerator;
pub use sled_agent_enumerator::StaticSledAgentEnumerator;
