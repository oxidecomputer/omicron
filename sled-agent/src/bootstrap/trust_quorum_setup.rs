// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for configuring and starting a [`trust_quorum::NodeTask`] during
//! bootstrap agent startup.

use super::config::TRUST_QUORUM_PORT;
use super::server::StartError;
use camino::Utf8PathBuf;
use sled_agent_types::sled::BaseboardId;
use sled_storage::dataset::CLUSTER_DATASET;
use sprockets_tls::keys::SprocketsConfig;
use std::net::{Ipv6Addr, SocketAddrV6};

const TRUST_QUORUM_STATE_FILE: &str = "trust-quorum-state.json";
const TRUST_QUORUM_NETWORK_CONFIG_FILE: &str =
    "trust-quorum-network-config.json";

/// Certain active bits, such as running trust quorum initialization as a result of RSS,
/// should remain disabled until the full integration is complete.
///
/// This is a constant, since we expect to remove the gating all at once,
/// and because it's hard to manage features with full a4x2 and racklette
/// deployments. The flag can be changed for local a4x2 builds during iniitial
/// testing.
pub const TRUST_QUORUM_INTEGRATION_ENABLED: bool = false;

pub fn new_trust_quorum_config(
    cluster_dataset_paths: &[Utf8PathBuf],
    baseboard_id: BaseboardId,
    global_zone_bootstrap_ip: Ipv6Addr,
    sprockets_config: SprocketsConfig,
) -> Result<trust_quorum::Config, StartError> {
    Ok(trust_quorum::Config {
        baseboard_id,
        listen_addr: SocketAddrV6::new(
            global_zone_bootstrap_ip,
            TRUST_QUORUM_PORT,
            0,
            0,
        ),
        tq_ledger_paths: trust_quorum_paths(
            cluster_dataset_paths,
            TRUST_QUORUM_STATE_FILE,
        )?,
        network_config_ledger_paths: trust_quorum_paths(
            cluster_dataset_paths,
            TRUST_QUORUM_NETWORK_CONFIG_FILE,
        )?,
        sprockets: sprockets_config,
    })
}

fn trust_quorum_paths(
    cluster_dataset_paths: &[Utf8PathBuf],
    filename: &str,
) -> Result<Vec<Utf8PathBuf>, StartError> {
    let paths: Vec<_> =
        cluster_dataset_paths.iter().map(|p| p.join(filename)).collect();

    if paths.is_empty() {
        return Err(StartError::MissingM2Paths(CLUSTER_DATASET));
    }
    Ok(paths)
}
