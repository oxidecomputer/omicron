// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types and functionality shared among bootstrap agent related crates

use bootstore::schemes::v0 as bootstore;
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_agent_measurements::MeasurementsHandle;
use slog::Logger;
use sprockets_tls::keys::SprocketsConfig;
use std::net::Ipv6Addr;
use std::sync::Arc;

/// Functionality necessary to run rack initialization or multirack cluster join
///
/// This is usually input to some constructor function
#[derive(Clone)]
pub struct RssContext {
    pub base_log: Logger,
    pub global_zone_bootstrap_ip: Ipv6Addr,
    pub internal_disks_rx: InternalDisksReceiver,
    pub bootstore_node_handle: bootstore::NodeHandle,
    pub sprockets_config: SprocketsConfig,
    pub trust_quorum_handle: trust_quorum::NodeTaskHandle,
    pub measurements: Arc<MeasurementsHandle>,
}
