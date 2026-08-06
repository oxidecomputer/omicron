// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Support Shell (`sush`) server that runs in the global zone.
//!
//! See RFD 620. This server runs jobs targeted at this sled on behalf of Oxide
//! support, and is deliberately independent of the rest of the control plane:
//! it is one of the tools you reach for when the control plane is the thing
//! that is broken. It therefore starts during bootstrap, before this sled knows
//! whether it is part of a rack at all.
//!
//! Startup is in two parts, because the pieces become available at different
//! times:
//!
//! * [`spawn_sush_tasks`] builds the job manager as soon as we know our
//!   baseboard, and is called from `long_running_tasks`.
//! * [`SushHandles::start_api`] starts the HTTP API, and is called from
//!   `server` once this sled has been told its underlay address.
//!
//! Gossip runs over sprockets on the bootstrap network, so jobs and
//! sessions are shared across sleds. A universe is a gossip network
//! identity; peers that meet merge into one by a dominance rule.
//! Session state is not yet persisted, so a restart forgets the
//! current session and re-seeds its universe.

use crate::config::SushConfig;
use dropshot::{ConfigDropshot, HandlerTaskMode, HttpServer, ServerBuilder};
use omicron_common::address::{SUSH_API_PORT, SUSH_GOSSIP_PORT};
use sled_agent_config_reconciler::AvailableDatasetsReceiver;
use sled_agent_measurements::MeasurementsHandle;
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use sprockets_tls::keys::SprocketsConfig;
use std::collections::BTreeSet;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use sush_server::executor::PathIsolation;
use sush_server::gossip::{GossipConfig, isolated, spawn_gossip};
use sush_server::link::CorpusSource;
use sush_server::output::{JobOutputDir, OutputDirs};
use sush_server::server::ApiServer;
use sush_server::{JobManager, seed_gossip};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Subdirectory of an encrypted dataset that job output is recorded in.
const SUSH_OUTPUT_SUBDIR: &str = "sush";

/// Maximum size of a request body the API will accept. The largest thing a
/// client sends is a signed job request or a certificate, both small.
const REQUEST_MAX_BODY_BYTES: usize = 0xFFFF;

/// Handles to the Support Shell server's tasks.
#[derive(Clone)]
pub struct SushHandles {
    log: Logger,
    manager: Arc<JobManager>,
    shutdown: CancellationToken,
}

impl SushHandles {
    /// Start serving the Support Shell API on the underlay.
    pub fn start_api(
        &self,
        underlay_ip: Ipv6Addr,
    ) -> Result<HttpServer<Arc<JobManager>>, String> {
        let bind_address =
            SocketAddr::V6(SocketAddrV6::new(underlay_ip, SUSH_API_PORT, 0, 0));
        let api = sush_api::sush_api_mod::api_description::<ApiServer>()
            .map_err(|err| format!("failed to describe sush API: {err}"))?;
        let server = ServerBuilder::new(
            api,
            Arc::clone(&self.manager),
            self.log.new(o!("component" => "dropshot (sush)")),
        )
        .config(ConfigDropshot {
            bind_address,
            default_request_body_max_bytes: REQUEST_MAX_BODY_BYTES,
            // An interactive job holds a websocket open for as long as it runs,
            // so a handler must outlive the request that created it.
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
            compression: Default::default(),
        })
        .start()
        .map_err(|err| err.to_string())?;
        info!(
            self.log, "started sush server";
            "address" => %bind_address,
        );
        Ok(server)
    }

    /// Stop the Support Shell server's tasks.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// What gossip needs from the sled: its sprockets identity, its reference
/// measurements, the bootstrap address to listen on, and where to find
/// its peers.
pub struct GossipInputs {
    pub sprockets: SprocketsConfig,
    pub measurements: Arc<MeasurementsHandle>,
    pub bootstrap_ip: Ipv6Addr,
    pub peers: watch::Receiver<BTreeSet<SocketAddrV6>>,
}

/// Start the Support Shell server's tasks, or return `None` if we can't.
pub async fn spawn_sush_tasks(
    log: &Logger,
    config: &SushConfig,
    own_baseboard: BaseboardId,
    gossip: GossipInputs,
    available_datasets_rx: AvailableDatasetsReceiver,
) -> Option<SushHandles> {
    let log = log.new(o!("component" => "sush"));

    // Job output starts out on the ramdisk, because an encrypted dataset can't
    // be mounted until trust quorum has been established, and we intend to be
    // useful before that.
    if let Err(err) = tokio::fs::create_dir_all(&config.ramdisk_dir).await {
        error!(
            log,
            "not starting sush server: could not create job output directory";
            "directory" => %config.ramdisk_dir,
            "error" => InlineErrorChain::new(&err),
        );
        return None;
    }
    let (output_dirs_tx, output_dirs_rx) = watch::channel(OutputDirs::new(
        config.ramdisk_dir.as_std_path(),
        mb_to_bytes(config.ramdisk_max_output_mb),
    ));

    if config.roots.is_empty() {
        warn!(log, "sush has no root certificates, so no job will ever run");
    }
    let shutdown = CancellationToken::new();

    // Gossip runs on the bootstrap network with the sled's sprockets
    // identity, and the attestation corpus is re-read per handshake because
    // updates change it. A sled that cannot gossip still serves local jobs.
    let GossipInputs { sprockets, measurements, bootstrap_ip, peers } = gossip;
    let corpus: CorpusSource = Arc::new({
        let log = log.clone();
        move || match measurements.current_measurements() {
            Ok(corpus) => corpus,
            Err(e) => {
                error!(log, "measurement error"; e);
                vec![]
            }
        }
    });
    let listen_addr = SocketAddrV6::new(bootstrap_ip, SUSH_GOSSIP_PORT, 0, 0);
    let universe = match spawn_gossip(
        &log,
        GossipConfig::default(),
        sprockets,
        corpus,
        listen_addr,
        peers,
        seed_gossip(),
        shutdown.clone(),
    )
    .await
    {
        Ok((_, universe)) => universe,
        Err(err) => {
            warn!(
                log,
                "gossip disabled, this sled serves local jobs only";
                "error" => InlineErrorChain::new(&err),
            );
            isolated(seed_gossip())
        }
    };

    let mut manager = match JobManager::new(
        log.clone(),
        PathIsolation::Enable,
        JobOutputDir::new(output_dirs_rx),
        own_baseboard,
        universe,
        &config.roots,
        shutdown.clone(),
    )
    .await
    {
        Ok(manager) => manager,
        Err(err) => {
            error!(log, "not starting sush server"; "error" => InlineErrorChain::new(&err));
            return None;
        }
    };

    // The state manager runs until shutdown and nothing waits on it, so all we
    // can usefully do with its handle is notice when it stops.
    if let Some(join) = manager.take_join_handle() {
        let log = log.clone();
        tokio::spawn(async move {
            match join.await {
                Ok(()) => info!(log, "sush state manager stopped"),
                Err(err) => error!(
                    log,
                    "sush state manager failed";
                    "error" => InlineErrorChain::new(&err),
                ),
            }
        });
    }

    tokio::spawn(promote_output_dir(
        log.clone(),
        available_datasets_rx,
        output_dirs_tx,
        mb_to_bytes(config.max_output_mb),
    ));

    info!(log, "started sush job manager");
    Some(SushHandles { log, manager: Arc::new(manager), shutdown })
}

/// Record new job output on an encrypted dataset as soon as one is
/// mounted, with a raised size limit. Output already recorded on the
/// ramdisk stays there, readable until reboot.
///
/// The debug datasets live on the U.2s under `crypt`, so they are encrypted at
/// rest and are not mounted until the keys are available. Waiting for one to
/// appear is how we wait for trust quorum without having to know anything about
/// trust quorum.
async fn promote_output_dir(
    log: Logger,
    mut available_datasets_rx: AvailableDatasetsReceiver,
    output_dirs_tx: watch::Sender<OutputDirs>,
    max_output_bytes: u64,
) {
    loop {
        if let Some(dataset) =
            available_datasets_rx.all_mounted_debug_datasets().first()
        {
            let dir = dataset.path.join(SUSH_OUTPUT_SUBDIR);
            match tokio::fs::create_dir_all(&dir).await {
                Ok(()) => {
                    output_dirs_tx.send_modify(|dirs| {
                        *dirs =
                            dirs.moved_to(dir.as_std_path(), max_output_bytes)
                    });
                    info!(
                        log, "recording job output on encrypted dataset";
                        "directory" => %dir,
                    );
                    return;
                }
                Err(err) => error!(
                    log,
                    "could not create job output directory on encrypted dataset, leaving it on the ramdisk";
                    "directory" => %dir,
                    "error" => InlineErrorChain::new(&err),
                ),
            }
        }
        available_datasets_rx.changed().await;
    }
}

fn mb_to_bytes(mb: u32) -> u64 {
    u64::from(mb) * 1024 * 1024
}
