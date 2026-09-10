// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Support Shell (`sush`) server that runs in the global zone.
//!
//! See RFD 620. This server runs jobs targeted at this sled on behalf of
//! Oxide support. A job arrives signed, and this sled runs it only when
//! the signature chains to one of the root certificates in its config.
//! The server is deliberately independent of the rest of the control plane,
//! because it is one of the tools you reach for when the control plane is
//! the thing that is broken. It therefore starts during bootstrap, before
//! this sled knows whether it is part of a rack at all.
//!
//! Startup is in two parts, because its inputs become available at different
//! times:
//!
//! * [`spawn_sush_tasks`] builds the job manager as soon as we know our
//!   baseboard, and is called from `long_running_tasks`. It serves the
//!   API on the bootstrap network, so the switch zone proxy can reach
//!   this sled before RSS runs.
//! * [`SushHandles::start_api`] serves the API again on the underlay,
//!   and is called from `server` once this sled has been told its
//!   underlay address.
//!
//! Gossip runs over sprockets on the bootstrap network. A job may be
//! addressed to a single sled, but the messages carrying jobs,
//! sessions, and events spread to every sled. The set of causally
//! related messages is called a _universe_: peers in the same universe
//! can gossip and converge on its contents; peers in different
//! universes cannot gossip at all. Every identity in a universe
//! descends from a single seed. On the first gossip sync, a deterministic
//! rule decides whose universe wins; the loser joins the winner's
//! universe and adopts a slice of its identity space. A restarted sled
//! therefore rejoins the rack's universe and replays what it missed
//! without re-executing previously run jobs. Each sled stores its identity in
//! a record called a _bookmark_, which is persisted to disk in order to survive
//! the sled restarting.
//!
//! The bookmark, like every record sush must trust across reboots, lives
//! in the [sush locker], described in the [storage section of RFD 620].
//! This is similar to the [`omicron_ledger::Ledger`], which also holds
//! records across both M.2s, but handles disagreements between the two
//! drives differently.
//!
//! [sush locker]:
//!   https://github.com/oxidecomputer/sush/blob/main/server/src/locker.rs
//! [storage section of RFD 620]:
//!   https://rfd.shared.oxide.computer/rfd/0620#_storage

use crate::config::SushConfig;
use anyhow::Context;
use camino::Utf8PathBuf;
use dropshot::{ConfigDropshot, HandlerTaskMode, HttpServer, ServerBuilder};
use gateway_client::Client as MgsClient;
use gateway_types::component::SpType;
use omicron_common::address::{
    MGS_PORT, SUSH_API_PORT, SUSH_GOSSIP_PORT, get_switch_zone_address,
};
use omicron_common::api::external::ByteCount;
use omicron_ddm_admin_client::Client as DdmClient;
use sled_agent_config_reconciler::AvailableDatasetsReceiver;
use sled_agent_measurements::MeasurementsHandle;
use sled_hardware_types::BaseboardId;
use slog::{Logger, debug, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use sprockets_tls::keys::SprocketsConfig;
use std::collections::BTreeSet;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::create_dir_all;
use tokio::spawn;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use sush_common::targets::{Cubbies, MAX_CUBBY};
use sush_server::executor::PathIsolation;
use sush_server::gossip::{
    GossipConfig, LinkedBaseboards, Universe, spawn_gossip,
};
use sush_server::link::CorpusSource;
use sush_server::locker::Locker;
use sush_server::output::{JobOutputDir, OutputDirs};
use sush_server::server::ApiServer;
use sush_server::{JobManager, seed_gossip};

/// Subdirectory of an encrypted dataset that job output is recorded in.
const SUSH_OUTPUT_SUBDIR: &str = "sush";

/// How often to refresh the cubby map from MGS.
const MGS_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for an MGS candidate to answer.
const MGS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum size of a request body the API will accept. The protocol
/// defines no message size limits, so we chose this cap rather than
/// deriving it. The largest bodies a client sends are the command in
/// a signed job request and a PEM-encoded certificate. Neither has
/// exceeded a few KB in practice, so this leaves an order of magnitude
/// of headroom while bounding how much the server needs to buffer for
/// any one request. We should raise it if we start seeing significantly
/// larger requests or certs in the wild.
const REQUEST_MAX_BODY_BYTES: usize = 0xFFFF;

/// Handles to the Support Shell server's tasks.
#[derive(Clone)]
pub struct SushHandles {
    log: Logger,
    manager: Arc<JobManager>,
    shutdown: CancellationToken,
}

impl SushHandles {
    /// Start serving the Support Shell API at `ip`.
    pub fn start_api(
        &self,
        ip: Ipv6Addr,
    ) -> anyhow::Result<HttpServer<Arc<JobManager>>> {
        let bind_address =
            SocketAddr::V6(SocketAddrV6::new(ip, SUSH_API_PORT, 0, 0));
        let api = sush_api::sush_api_mod::api_description::<ApiServer>()
            .context("describing the sush API")?;
        let server = ServerBuilder::new(
            api,
            Arc::clone(&self.manager),
            self.log.new(o!("component" => "dropshot (sush)")),
        )
        .config(ConfigDropshot {
            bind_address,
            default_request_body_max_bytes: REQUEST_MAX_BODY_BYTES,
            // While an interactive job is running, the corresponding websocket
            // connection must remain open, so HTTP request handlers must 
            // outlive the requests that created them.
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
            compression: Default::default(),
        })
        .start()
        .context("starting the sush API server")?;
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

pub struct GossipInputs {
    pub sprockets: SprocketsConfig,
    pub measurements: Arc<MeasurementsHandle>,
    pub bootstrap_ip: Ipv6Addr,
    pub peers: watch::Receiver<BTreeSet<SocketAddrV6>>,
}

/// Start the Support Shell server's tasks, or log the reason and return
/// `None` when they cannot start. The locker gets one slot on each of
/// `cluster_datasets`.
pub async fn spawn_sush_tasks(
    log: &Logger,
    config: &SushConfig,
    own_baseboard: BaseboardId,
    cluster_datasets: Vec<Utf8PathBuf>,
    gossip: GossipInputs,
    available_datasets_rx: AvailableDatasetsReceiver,
) -> Option<SushHandles> {
    let log = log.new(o!("component" => "sush"));

    // Job output starts on the ramdisk, because an encrypted dataset cannot be
    // mounted until trust quorum is established, and sush must be useful
    // before then.
    if let Err(err) = create_dir_all(&config.ramdisk_dir).await {
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
        ByteCount::from_mebibytes_u32(config.ramdisk_max_output_mb).to_bytes(),
    ));

    let shutdown = CancellationToken::new();

    // A slot that cannot be created is still handed to the locker, which
    // decides what a failed slot means for each load and store.
    let slots: Vec<Utf8PathBuf> =
        cluster_datasets.iter().map(|dataset| dataset.join("sush")).collect();
    for slot in &slots {
        if let Err(err) = create_dir_all(slot).await {
            warn!(
                log, "could not create a locker slot";
                "slot" => %slot,
                "error" => InlineErrorChain::new(&err),
            );
        }
    }
    let locker = Locker::new(&log, slots);

    let GossipInputs { sprockets, measurements, bootstrap_ip, peers } = gossip;

    // On every gossip protocol handshake, we must re-read the attestation
    // corpus, because a software update may have changed it. This closure
    // is invoked in order to do that.
    let corpus: CorpusSource = Arc::new({
        let log = log.clone();
        move || match measurements.current_measurements() {
            Ok(corpus) => corpus,
            Err(e) => {
                // If reading the measurements fails, this sled cannot
                // participate in the gossip protocol, but can still serve
                // local jobs.
                error!(log, "measurement error"; e);
                vec![]
            }
        }
    });
    let listen_addr = SocketAddrV6::new(bootstrap_ip, SUSH_GOSSIP_PORT, 0, 0);
    let (universe, linked) = match spawn_gossip(
        &log,
        GossipConfig::default(),
        sprockets,
        corpus,
        listen_addr,
        peers,
        seed_gossip(&log, &locker).await,
        shutdown.clone(),
    )
    .await
    {
        Ok((_, universe, linked)) => (universe, linked),
        Err(err) => {
            warn!(
                log,
                "gossip disabled, this sled serves local jobs only";
                "error" => InlineErrorChain::new(&err),
            );
            // This seed never reaches a peer, so it is not worth
            // storing. The null locker stores nothing, leaving the
            // bookmark on the M.2s untouched for the next boot.
            let seed = seed_gossip(&log, &Locker::null()).await;
            (Universe::isolated(seed.into_rumors()), LinkedBaseboards::lonely())
        }
    };

    let (tx_cubbies, rx_cubbies) = watch::channel(Cubbies::new());
    let mut manager = match JobManager::new(
        log.clone(),
        PathIsolation::Enable,
        JobOutputDir::new(output_dirs_rx),
        own_baseboard,
        rx_cubbies,
        universe,
        linked,
        &locker,
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
    spawn(poll_mgs_for_cubbies(
        log.new(o!("component" => "cubby map")),
        tx_cubbies,
    ));

    // The state manager runs until shutdown and nothing waits on it,
    // so all we can usefully do with its handle is notice when it stops.
    if let Some(join) = manager.take_join_handle() {
        let log = log.clone();
        spawn(async move {
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

    spawn(promote_output_dir(
        log.clone(),
        available_datasets_rx,
        output_dirs_tx,
        ByteCount::from_mebibytes_u32(config.max_output_mb).to_bytes(),
    ));

    info!(log, "started sush job manager");
    let handles = SushHandles { log, manager: Arc::new(manager), shutdown };

    match handles.start_api(bootstrap_ip) {
        Ok(server) => {
            let shutdown = handles.shutdown.clone();
            spawn(async move {
                shutdown.cancelled().await;
                server.close().await.ok();
            });
        }
        Err(err) => warn!(
            handles.log,
            "sush is not serving on the bootstrap network";
            "error" => #%err,
        ),
    }

    Some(handles)
}

/// Start recording new job output on an encrypted debug dataset as soon
/// as one is mounted, with a raised size limit. Output already recorded
/// on the ramdisk stays there, readable until reboot.
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
            match create_dir_all(&dir).await {
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
                    "could not create job output directory on encrypted dataset, \
                     leaving it on the ramdisk";
                    "directory" => %dir,
                    "error" => InlineErrorChain::new(&err),
                ),
            }
        }
        if available_datasets_rx.changed().await.is_err() {
            warn!(
                log,
                "no new datasets will appear, so job output will stay \
                 on the ramdisk",
            );
            return;
        }
    }
}

/// Discovers the baseboard identity in each cubby in the rack and publishes the
/// map over the provided watch channel.
///
/// This map is used to resolve the sled identity when a job specifies its 
/// target sled cubby number. 
///
/// This function may start while one or more MGS services are not available,
/// and must handle failures of both the MGS service or the entire scrimlet
/// gracefully. Therefore, on every poll, this function will attempt to use the
/// fixed MGS address on every subnet curretly known to `ddmd`, and accepts any
/// responses it receives. If a response that contains at least one sled is
/// received, the previously-discovered map is overwritten to avoid leaving
/// behind stale entries for sleds that are no longer present. Multiple
/// responses received within the same poll are merged to produce a single map.
/// If we cannot contact any MGS instance during a poll, the map does not change
/// until a subsequent poll receives a response.
async fn poll_mgs_for_cubbies(log: Logger, cubbies: watch::Sender<Cubbies>) {
    let ddm = match DdmClient::localhost(&log) {
        Ok(ddm) => ddm,
        Err(err) => {
            error!(
                log, "not polling MGS, cubby-targeted jobs will not run here";
                "error" => InlineErrorChain::new(&err),
            );
            return;
        }
    };
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(MGS_PROBE_TIMEOUT)
        .timeout(MGS_PROBE_TIMEOUT)
        .build()
        .expect("failed to build an HTTP client");
    loop {
        match ddm.derive_underlay_subnets_from_prefixes().await {
            Ok(subnets) => {
                let mut map = Cubbies::new();
                for subnet in subnets {
                    let addr = SocketAddrV6::new(
                        get_switch_zone_address(subnet),
                        MGS_PORT,
                        0,
                        0,
                    );
                    let mgs = MgsClient::new_with_client(
                        &format!("http://{addr}"),
                        client.clone(),
                        log.clone(),
                    );
                    if mgs.sp_local_switch_id().await.is_err() {
                        continue;
                    }
                    for cubby in 0..=MAX_CUBBY {
                        match mgs.sp_get(&SpType::Sled, cubby.into()).await {
                            Ok(state) => {
                                let state = state.into_inner();
                                map.insert(
                                    cubby,
                                    BaseboardId {
                                        part_number: state.model,
                                        serial_number: state.serial_number,
                                    },
                                );
                            }
                            Err(err) => {
                                debug!(
                                    log, "no SP state for cubby";
                                    "cubby" => cubby, "error" => %err,
                                );
                            }
                        }
                    }
                }
                // If this code is running, there must be at least one sled in
                // the rack, so we reject any response that does not have at
                // least one sled.
                if !map.is_empty() {
                    cubbies.send_replace(map);
                }
            }
            Err(err) => {
                warn!(
                    log, "unable to fetch prefixes";
                    "error" => InlineErrorChain::new(&err),
                );
            }
        }
        sleep(MGS_POLL_INTERVAL).await;
    }
}
