// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::InventoryError;
use futures::future::BoxFuture;
use futures::FutureExt;

/// Describes how to find the list of sled agents to collect from
///
/// In a real system, this queries the database to list all sleds.  But for
/// testing the `StaticSledAgentEnumerator` below can be used to avoid a
/// database dependency.
pub trait SledAgentEnumerator {
    /// Returns a list of URLs for Sled Agent HTTP endpoints
    fn list_sled_agents(
        &self,
    ) -> BoxFuture<'_, Result<Vec<String>, InventoryError>>;
}

/// Used to provide an explicit list of sled agents to a `Collector`
///
/// This is mainly used for testing.
pub struct StaticSledAgentEnumerator {
    agents: Vec<String>,
}

impl StaticSledAgentEnumerator {
    pub fn new(iter: impl IntoIterator<Item = String>) -> Self {
        StaticSledAgentEnumerator { agents: iter.into_iter().collect() }
    }

    pub fn empty() -> Self {
        Self::new(std::iter::empty())
    }
}

impl SledAgentEnumerator for StaticSledAgentEnumerator {
    fn list_sled_agents(
        &self,
    ) -> BoxFuture<'_, Result<Vec<String>, InventoryError>> {
        futures::future::ready(Ok(self.agents.clone())).boxed()
    }
}
