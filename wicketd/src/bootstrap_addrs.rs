// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for for discovering peer sleds on the bootstrap network via DDM
//! and the bootstrap agent.

use sled_hardware_types::BaseboardId;
use slog::Logger;
use slog::warn;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::task::JoinHandle;

pub(crate) struct BootstrapPeersFromDdm {
    // We use a standard mutex here, not a tokio mutex, even though this is
    // shared with a tokio task. We only keep it locked long enough to insert a
    // new entry or clone it.
    sleds: Arc<Mutex<BTreeMap<BaseboardId, Ipv6Addr>>>,
    inner_task: JoinHandle<()>,
}

impl Drop for BootstrapPeersFromDdm {
    fn drop(&mut self) {
        self.inner_task.abort();
    }
}

impl BootstrapPeersFromDdm {
    pub(crate) fn new(
        log: &Logger,
        bootstrap_agent_lockstep_address: SocketAddrV6,
    ) -> Self {
        let log = log.new(slog::o!("component" => "BootstrapPeers"));
        let sleds = Arc::default();
        let inner_task = tokio::spawn(scan_for_peers(
            Arc::clone(&sleds),
            log,
            bootstrap_agent_lockstep_address,
        ));
        Self { sleds, inner_task }
    }

    pub(crate) fn sleds(&self) -> BTreeMap<BaseboardId, Ipv6Addr> {
        self.sleds.lock().unwrap().clone()
    }
}

async fn scan_for_peers(
    sleds: Arc<Mutex<BTreeMap<BaseboardId, Ipv6Addr>>>,
    log: Logger,
    bootstrap_agent_lockstep_address: SocketAddrV6,
) {
    // How frequently do we attempt to refresh the set of peers?
    const SLEEP_BETWEEN_REFRESH: Duration = Duration::from_secs(30);

    // We only share `sleds` with the `BootstrapPeers` that created us, and it
    // only ever reads the current value: we are the only one that changes it.
    // We keep the previous version we set it to so if the set of addresses
    // remains unchanged (which we expect nearly all the time), we don't bother
    // locking and copying the new set in.
    let mut prev_sleds = None;
    loop {
        // Query the local bootstrap agent for all known peers
        let client = bootstrap_agent_lockstep_client::Client::new(
            &format!("http://{bootstrap_agent_lockstep_address}"),
            log.clone(),
        );

        match client.baseboard_ids().await {
            Ok(baseboard_ids) => {
                let all_sleds: BTreeMap<BaseboardId, Ipv6Addr> = baseboard_ids
                    .into_inner()
                    .data
                    .into_iter()
                    .map(|id_and_ip| (id_and_ip.id, id_and_ip.ip))
                    .collect();

                // Did our set of peers change? If so, update both `sleds`
                // (shared with our parent `BootstrapPeers`) and `prev_sleds`
                // (our local cache).
                if Some(&all_sleds) != prev_sleds.as_ref() {
                    *sleds.lock().unwrap() = all_sleds.clone();
                    prev_sleds = Some(all_sleds);
                }
            }
            Err(err) => {
                // We don't update our set of peers here, because we don't know
                // what has changed. We don't want to report an empty set in the
                // case of a transient failure.
                warn!(
                    log, "Failed to get baseboard IDs";
                    "err" => #%err,
                );
            }
        };

        tokio::time::sleep(SLEEP_BETWEEN_REFRESH).await;
    }
}
