// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::InventoryError;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

/// Describes how to find the list of sled agents to collect from
pub trait SledAgentEnumerator {
    fn list_sled_agents(
        &self,
    ) -> BoxFuture<
        '_,
        Result<Vec<Arc<sled_agent_client::Client>>, InventoryError>,
    >;
}

/// Used to provide an explicit list of sled agents to a `Collector`
///
/// This is mainly used for testing.
pub struct StaticSledAgentEnumerator {
    agents: Vec<Arc<sled_agent_client::Client>>,
}

impl StaticSledAgentEnumerator {
    pub fn new(
        iter: impl IntoIterator<Item = Arc<sled_agent_client::Client>>,
    ) -> Box<dyn SledAgentEnumerator + Send> {
        Box::new(StaticSledAgentEnumerator {
            agents: iter.into_iter().collect(),
        })
    }

    pub fn empty() -> Box<dyn SledAgentEnumerator + Send> {
        Self::new(std::iter::empty())
    }
}

impl SledAgentEnumerator for StaticSledAgentEnumerator {
    fn list_sled_agents(
        &self,
    ) -> BoxFuture<
        '_,
        Result<Vec<Arc<sled_agent_client::Client>>, InventoryError>,
    > {
        futures::future::ready(Ok(self.agents.clone())).boxed()
    }
}
