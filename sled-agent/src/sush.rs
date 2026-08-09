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
//!   baseboard, and is called from `long_running_tasks`. It serves the
//!   API on the bootstrap network, so the switch zone proxy can reach
//!   this sled before RSS runs.
//! * [`SushHandles::start_api`] serves the API again on the underlay,
//!   and is called from `server` once this sled has been told its
//!   underlay address.
//!
//! Gossip runs over sprockets on the bootstrap network, so jobs and
//! sessions are shared across sleds. A universe is a gossip network
//! identity; peers that meet merge into one by a dominance rule.
//! Session state is not yet persisted, so a restart forgets the
//! current session and re-seeds its universe.

use crate::config::SushConfig;
use anyhow::Context;
use camino::Utf8Path;
use dropshot::{ConfigDropshot, HandlerTaskMode, HttpServer, ServerBuilder};
use omicron_common::address::{SUSH_API_PORT, SUSH_GOSSIP_PORT};
use sha3::{Digest as _, Sha3_256};
use sled_agent_config_reconciler::AvailableDatasetsReceiver;
use sled_agent_measurements::MeasurementsHandle;
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use sprockets_tls::ipcc::Ipcc;
use sprockets_tls::keys::SprocketsConfig;
use std::collections::BTreeSet;
use std::io;
use std::iter::once;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use x509_cert::Certificate;
use x509_cert::der::oid::db::rfc8410::ID_ED_25519;
use x509_cert::der::{Decode as _, Reader as _, SliceReader};
use x509_cert::spki::AlgorithmIdentifierOwned;
use x509_cert::time::Validity;

use sush_common::keys::{EphemeralKey, KeyType, pem_cert_chain};
use sush_server::executor::PathIsolation;
use sush_server::gossip::{GossipConfig, isolated, spawn_gossip};
use sush_server::link::CorpusSource;
use sush_server::output::{JobOutputDir, OutputDirs};
use sush_server::server::ApiServer;
use sush_server::{JobManager, seed_gossip};

/// Subdirectory of an encrypted dataset that job output is recorded in.
const SUSH_OUTPUT_SUBDIR: &str = "sush";

/// Path inside the switch zone to the sush proxy's TLS private key.
pub const SUSH_PROXY_KEY_PATH: &str = "/etc/sush-proxy/key.pem";

/// Path inside the switch zone to the sush proxy's TLS certificate chain.
pub const SUSH_PROXY_CERT_CHAIN_PATH: &str = "/etc/sush-proxy/chain.pem";

/// How long a minted proxy identity claims to be valid. Nothing checks
/// expiry today, and the identity is re-minted at every zone startup.
const SUSH_PROXY_CERT_VALIDITY: Duration =
    Duration::from_secs(365 * 24 * 60 * 60);

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
    /// Start serving the Support Shell API at `ip`.
    pub fn start_api(
        &self,
        ip: Ipv6Addr,
    ) -> Result<HttpServer<Arc<JobManager>>, String> {
        let bind_address =
            SocketAddr::V6(SocketAddrV6::new(ip, SUSH_API_PORT, 0, 0));
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
    let handles = SushHandles { log, manager: Arc::new(manager), shutdown };

    // Serve the API on the bootstrap network from the start, so the
    // switch zone proxy can reach this sled before RSS runs. The
    // underlay API starts separately, once this sled has an address.
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
            "error" => err,
        ),
    }

    Some(handles)
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

/// Mint the switch zone proxy's TLS identity: an ephemeral key whose
/// certificate the RoT signs once, in its signing convention (Ed25519
/// over the SHA3-256 digest of the TBS certificate). The key and chain
/// are written as PEM under `zone_root` for the proxy to serve with.
pub async fn mint_proxy_identity(
    log: &Logger,
    zone_root: &Utf8Path,
) -> anyhow::Result<()> {
    // IPCC requests are ioctls, i.e., blocking I/O.
    let (key_pem, chain_pem) = spawn_blocking(mint_proxy_pems).await??;
    let key_path = format!("{zone_root}{SUSH_PROXY_KEY_PATH}");
    let chain_path = format!("{zone_root}{SUSH_PROXY_CERT_CHAIN_PATH}");
    let dir = Utf8Path::new(&key_path).parent().expect("key path has a parent");
    create_dir_all(dir).await.with_context(|| format!("creating {dir}"))?;
    write_private(&key_path, key_pem.as_bytes())
        .await
        .with_context(|| format!("writing {key_path}"))?;
    tokio::fs::write(&chain_path, chain_pem.as_bytes())
        .await
        .with_context(|| format!("writing {chain_path}"))?;
    info!(log, "minted sush proxy TLS identity"; "key" => key_path);
    Ok(())
}

/// The proxy's private key and certificate chain, PEM-encoded.
fn mint_proxy_pems() -> anyhow::Result<(String, String)> {
    let ipcc = Ipcc::new().context("opening IPCC")?;
    let chain_der =
        ipcc.rot_get_tq_cert_chain().context("fetching the TQ cert chain")?;
    // The RoT returns the chain leaf first, as sprockets assumes too.
    let platform = der_cert_chain(&chain_der)?;
    let issuer = platform
        .first()
        .context("the TQ cert chain is empty")?
        .tbs_certificate
        .subject
        .clone();
    let leaf = EphemeralKey::new_delegated(
        KeyType::Ed25519,
        "CN=sush-proxy".parse().context("parsing the subject")?,
        issuer,
        Validity::from_now(SUSH_PROXY_CERT_VALIDITY)
            .context("computing validity")?,
        AlgorithmIdentifierOwned { oid: ID_ED_25519, parameters: None },
        |tbs| ipcc.rot_tq_sign(&Sha3_256::digest(tbs)),
    )
    .context("minting the proxy key")?;
    let key_pem = leaf.private_key_pem().context("encoding the proxy key")?;
    let chain = once(leaf.cert().clone()).chain(platform).collect::<Vec<_>>();
    let chain_pem = pem_cert_chain(chain).context("encoding the chain")?;
    Ok((key_pem, chain_pem))
}

/// Parse a concatenated series of DER certs, as the RoT returns.
fn der_cert_chain(bytes: &[u8]) -> anyhow::Result<Vec<Certificate>> {
    let mut chain = Vec::new();
    let mut reader =
        SliceReader::new(bytes).context("reading the TQ cert chain")?;
    while !reader.is_finished() {
        chain.push(
            Certificate::decode(&mut reader)
                .context("parsing the TQ cert chain")?,
        );
    }
    Ok(chain)
}

/// Write a file readable only by the owner.
async fn write_private(path: &str, contents: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .await?;
    file.write_all(contents).await?;
    file.flush().await
}
