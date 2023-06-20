// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User provided dropshot server context

use std::sync::Arc;
use std::sync::Mutex;

use crate::bootstrap_addrs::BootstrapPeers;
use crate::rss_config::CurrentRssConfig;
use crate::update_tracker::UpdateTracker;
use crate::MgsHandle;
use sled_hardware::Baseboard;

/// Shared state used by API handlers
pub struct ServerContext {
    pub mgs_handle: MgsHandle,
    pub mgs_client: gateway_client::Client,
    pub(crate) bootstrap_peers: BootstrapPeers,
    pub(crate) update_tracker: Arc<UpdateTracker>,
    pub(crate) baseboard: Option<Baseboard>,
    pub(crate) rss_config: Mutex<CurrentRssConfig>,
}
