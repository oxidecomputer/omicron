// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use futures::stream::FuturesUnordered;
use omicron_ddm_admin_client::Client as DdmAdminClient;
use sled_hardware_types::Baseboard;
use sled_hardware_types::underlay::BootstrapInterface;
use slog::Logger;
use slog::warn;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

pub(crate) struct BootstrapPeers {
    // We use a standard mutex here, not a tokio mutex, even though this is
    // shared with a tokio task. We only keep it locked long enough to insert a
    // new entry or clone it.
    sleds: Arc<Mutex<BTreeMap<Baseboard, Ipv6Addr>>>,
    inner_task: JoinHandle<()>,
}

impl Drop for BootstrapPeers {
    fn drop(&mut self) {
        self.inner_task.abort();
    }
}

impl BootstrapPeers {
    pub(crate) fn new(log: &Logger) -> Self {
        let log = log.new(slog::o!("component" => "BootstrapPeers"));
        let sleds = Arc::default();
        let inner_task = tokio::spawn(scan_for_peers(Arc::clone(&sleds), log));
        Self { sleds, inner_task }
    }

    pub(crate) fn sleds(&self) -> BTreeMap<Baseboard, Ipv6Addr> {
        self.sleds.lock().unwrap().clone()
    }
}

async fn scan_for_peers(
    sleds: Arc<Mutex<BTreeMap<Baseboard, Ipv6Addr>>>,
    log: Logger,
) {
    // How frequently do we attempt to refresh the set of peers? This does not
    // count the time it takes to do each refresh, which is potentially
    // significant if any prefixes reported by DDM are not running responsive
    // sled-agents, since we'll wait for them to time out.
    const SLEEP_BETWEEN_REFRESH: Duration = Duration::from_secs(30);

    let ddm_client = make_ddm_admin_client(&log).await;

    // We only share `sleds` with the `BootstrapPeers` that created us, and it
    // only ever reads the current value: we are the only one that changes it.
    // We keep the previous version we set it to so if the set of addresses
    // remains unchanged (which we expect nearly all the time), we don't bother
    // locking and copying the new set in.
    let mut prev_sleds = None;
    loop {
        // Ask mg-ddm for a list of bootstrap address prefixes.
        let addrs = possible_sled_agent_addrs(&ddm_client, &log).await;

        // Query the sled-agent on each prefix for its baseboard, dropping any
        // addresses that fail to return.
        let mut addrs_to_sleds = addrs
            .map(|ip| {
                let log = &log;
                async move {
                    let client = bootstrap_agent_client::Client::new(
                        &format!("http://[{ip}]"),
                        log.clone(),
                    );
                    let result = client.baseboard_get().await;

                    (ip, result)
                }
            })
            .collect::<FuturesUnordered<_>>();

        let mut all_sleds = BTreeMap::new();
        while let Some((ip, result)) = addrs_to_sleds.next().await {
            match result {
                Ok(baseboard) => {
                    // Convert from progenitor type back to `sled-hardware`
                    // type.
                    let baseboard = match baseboard.into_inner() {
                        bootstrap_agent_client::types::Baseboard::Gimlet {
                            identifier,
                            model,
                            revision,
                        } => Baseboard::new_gimlet(identifier, model, revision),
                        bootstrap_agent_client::types::Baseboard::Unknown => {
                            Baseboard::unknown()
                        }
                        bootstrap_agent_client::types::Baseboard::Pc {
                            identifier,
                            model,
                        } => Baseboard::new_pc(identifier, model),
                    };

                    all_sleds.insert(baseboard, ip);
                }
                Err(err) => {
                    warn!(
                        log, "Failed to get baseboard for {ip}";
                        "err" => #%err,
                    );
                }
            }
        }

        // Did our set of peers change? If so, update both `sleds` (shared with
        // our parent `BootstrapPeers`) and `prev_sleds` (our local cache).
        if Some(&all_sleds) != prev_sleds.as_ref() {
            *sleds.lock().unwrap() = all_sleds.clone();
            prev_sleds = Some(all_sleds);
        }

        tokio::time::sleep(SLEEP_BETWEEN_REFRESH).await;
    }
}

async fn possible_sled_agent_addrs(
    ddm_client: &DdmAdminClient,
    log: &Logger,
) -> impl Iterator<Item = Ipv6Addr> + use<> {
    // We're talking to a service's admin interface on localhost within our own
    // switch zone, and we're only asking for its current state. We use a retry
    // in a loop instead of `backoff`.
    const RETRY: Duration = Duration::from_secs(5);

    loop {
        match ddm_client
            .derive_bootstrap_addrs_from_prefixes(&[
                BootstrapInterface::GlobalZone,
            ])
            .await
        {
            Ok(addrs) => return addrs,
            Err(err) => {
                warn!(
                    log, "Failed to get prefixes from ddm";
                    "err" => #%err,
                );
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

async fn make_ddm_admin_client(log: &Logger) -> DdmAdminClient {
    const DDM_CONSTRUCT_RETRY: Duration = Duration::from_secs(1);

    // We don't really expect this to fail ever, so just keep retrying
    // indefinitely if it does.
    loop {
        match DdmAdminClient::localhost(log) {
            Ok(client) => return client,
            Err(err) => {
                warn!(
                    log, "Failed to construct DdmAdminClient";
                    "err" => #%err,
                );
                tokio::time::sleep(DDM_CONSTRUCT_RETRY).await;
            }
        }
    }
}
