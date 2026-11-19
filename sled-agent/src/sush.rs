// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Support Shell (`sush`) server that runs in the global zone.
//!
//! See [RFD 620]. The server runs jobs targeted at this sled on behalf of
//! Oxide support. Jobs are authorized by signatures, and the server runs
//! a job only when its signature validates against one of the trust
//! anchors in its config; see `SushConfig::trust_anchors` for how those
//! are selected.
//!
//! Jobs are distributed among servers using a gossip network implemented
//! by the [rumors] library, running over sprockets on the bootstrap network.
//! _Peers_ (servers) exchange messages that are causally ordered. Various
//! pieces of gossip network state are persisted to the M.2s using a
//! [sush locker], described in the _Storage_ section of [RFD 620]. This
//! is similar to an [`omicron_ledger::Ledger`], which also holds records
//! across M.2s, but handles disagreements between the two drives
//! differently.
//!
//! The gossip network is invisible to clients, which speak only the
//! `sush` HTTP API; the server relays requests and events. That API is
//! also served on the bootstrap network only; clients, including those
//! tunneled through Nexus, reach it through a small proxy in the switch
//! zone. It is currently served over unencrypted HTTP, pending intra-rack
//! mTLS support.
//!
//! [RFD 620]:
//!   https://rfd.shared.oxide.computer/rfd/0620
//! [rumors]:
//!   https://github.com/oxidecomputer/rumors
//! [sush locker]:
//!   https://github.com/oxidecomputer/sush/blob/main/server/src/locker.rs

use crate::config::SushConfig;
use anyhow::{Context, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use dice_verifier::verify_cert_chain as verify_platform_cert_chain;
use dropshot::{ConfigDropshot, HandlerTaskMode, HttpServer, ServerBuilder};
use gateway_client::Client as MgsClient;
use omicron_common::address::{
    MGS_PORT, SUSH_API_PORT, SUSH_GOSSIP_PORT, get_switch_zone_address,
};
use omicron_common::api::external::ByteCount;
use omicron_ddm_admin_client::Client as DdmClient;
use omicron_sush_proxy::cubbies::{merge_cubbies, poll_cubbies};
use sled_agent_config_reconciler::AvailableDatasetsReceiver;
use sled_agent_measurements::MeasurementsHandle;
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use sprockets_tls::ipcc::Ipcc;
use sprockets_tls::keys::SprocketsConfig;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::read;
use std::io;
use std::iter::once;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use strum::Display;
use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt as _;
use tokio::spawn;
use tokio::sync::watch;
use tokio::task::{JoinSet, spawn_blocking};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use x509_cert::Certificate;
use x509_cert::der::{Decode as _, DecodePem as _, Reader as _, SliceReader};
use x509_cert::time::{Time, Validity};

use sush_common::keys::{EphemeralKey, KeyType, pem_cert_chain};
use sush_common::targets::Cubbies;
use sush_server::executor::PathIsolation;
use sush_server::gossip::{ConnectedBaseboards, GossipConfig, spawn_gossip};
use sush_server::locker::Locker;
use sush_server::output::{JobOutputDir, OutputDirs};
use sush_server::server::ApiServer;
use sush_server::sprockets::CorpusSource;
use sush_server::{JobManager, seed_gossip};

/// Subdirectory of an encrypted dataset that job output is recorded in.
const SUSH_OUTPUT_SUBDIR: &str = "sush";

/// Path inside the switch zone to the sush proxy's TLS private key.
/// `write_private` makes it readable by the owner (root) only.
pub const SUSH_PROXY_KEY_PATH: &str = "/etc/sush-proxy/key.pem";

/// Path inside the switch zone to the sush proxy's TLS certificate chain.
pub const SUSH_PROXY_CERT_CHAIN_PATH: &str = "/etc/sush-proxy/chain.pem";

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
    fn start_api(
        &self,
        ip: Ipv6Addr,
    ) -> anyhow::Result<HttpServer<Arc<JobManager>>> {
        let bind_address =
            SocketAddr::V6(SocketAddrV6::new(ip, SUSH_API_PORT, 0, 0));
        let api = sush_api::sush_api_mod::api_description::<ApiServer>()
            .context("failed to describe the sush API")?;
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
        .context("failed to start the sush API server")?;
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

    // Job output starts on the ramdisk, because an encrypted dataset cannot
    // be mounted until trust quorum is established, and sush must be useful
    // before then; see `promote_output_dir`.
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
    let locker = match Locker::new(&log, own_baseboard.clone(), slots) {
        Ok(locker) => locker,
        Err(err) => {
            error!(
                log, "locker is locked, not starting sush server";
                "error" => InlineErrorChain::new(&err),
            );
            return None;
        }
    };

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
    let (network, connected) = match spawn_gossip(
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
        Ok((_, network, connected)) => (network, connected),
        Err(err) => {
            warn!(
                log,
                "gossip disabled, this sled will run local jobs only";
                "error" => InlineErrorChain::new(&err),
            );

            // Don't persist an isolated identity.
            let locker = Locker::null();
            let seed = seed_gossip(&log, &locker).await;
            (seed.without_gossip(), ConnectedBaseboards::empty())
        }
    };

    let anchors = config.trust_anchors(&log).await;
    let (tx_cubbies, rx_cubbies) = watch::channel(Cubbies::new());
    let mut manager = match JobManager::new(
        log.clone(),
        PathIsolation::Enable,
        JobOutputDir::new(output_dirs_rx),
        own_baseboard,
        rx_cubbies,
        network,
        connected,
        &locker,
        &anchors,
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
    spawn(update_cubbies(log.new(o!("component" => "cubby map")), tx_cubbies));

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
/// on the ramdisk currently stays there, but should be migrated to the
/// encrypted storage; see sush#69.
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
            let dir = dataset.path().join(SUSH_OUTPUT_SUBDIR);
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

/// Periodically poll each MGS for the sled in each cubby, and merge the
/// answer from the first MGS to respond into the cubby map. Look for MGS
/// at the switch zone address in every underlay subnet that DDM knows.
/// Try every address at once, because most subnets have no switch zone,
/// and a request to an address with no MGS hangs until it times out.
async fn update_cubbies(log: Logger, tx_cubbies: watch::Sender<Cubbies>) {
    let ddm = match DdmClient::localhost(&log) {
        Ok(ddm) => ddm,
        Err(error) => {
            error!(
                log, "not polling MGS: cannot build a DDM client";
                "error" => InlineErrorChain::new(&error),
            );
            return;
        }
    };
    let client = match reqwest::ClientBuilder::new()
        .connect_timeout(MGS_PROBE_TIMEOUT)
        .timeout(MGS_PROBE_TIMEOUT)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            error!(
                log, "not polling MGS: cannot build an HTTP client";
                "error" => InlineErrorChain::new(&error),
            );
            return;
        }
    };
    loop {
        match ddm.derive_underlay_subnets_from_prefixes().await {
            Ok(subnets) => {
                let mut polls = JoinSet::new();
                for subnet in subnets {
                    let log = log.clone();
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
                    polls.spawn(async move {
                        (addr, poll_cubbies(&log, &mgs).await)
                    });
                }

                let mut errors = BTreeMap::new();
                let mut updated = false;
                while let Some(joined) = polls.join_next().await {
                    match joined {
                        Ok((_, Ok(states))) => {
                            tx_cubbies.send_modify(|cubbies| {
                                merge_cubbies(cubbies, states)
                            });
                            updated = true;
                            break;
                        }
                        Ok((addr, Err(error))) => {
                            errors.insert(addr, error);
                        }
                        Err(error) => {
                            warn!(
                                log, "MGS probe task failed";
                                "error" => InlineErrorChain::new(&error),
                            );
                        }
                    }
                }
                if !updated {
                    warn!(
                        log, "no MGS answered, cubby map unchanged";
                        "previous_sleds" => tx_cubbies.borrow().len(),
                    );
                    for (addr, error) in &errors {
                        warn!(
                            log, "MGS probe failed";
                            "addr" => %addr,
                            "error" => InlineErrorChain::new(error),
                        );
                    }
                }
            }
            Err(error) => {
                warn!(
                    log, "cannot fetch underlay prefixes from DDM";
                    "error" => InlineErrorChain::new(&error),
                );
            }
        }
        sleep(MGS_POLL_INTERVAL).await;
    }
}

/// Generate the switch zone proxy's TLS identity. This is an ephemeral,
/// self-signed certificate carrying the RoT's voucher for its key
/// (a Trust Quorum signature over the tagged SPKI digest; see RFD 620
/// §4.6.2.1). The key and chain are written as PEM under `zone_root`
/// for the proxy to serve with.
pub async fn generate_proxy_identity(
    log: &Logger,
    zone_root: &Utf8Path,
) -> anyhow::Result<()> {
    let (key_pem, chain_pem) = spawn_blocking(generate_proxy_pems).await??;
    let key_path = format!("{zone_root}{SUSH_PROXY_KEY_PATH}");
    let chain_path = format!("{zone_root}{SUSH_PROXY_CERT_CHAIN_PATH}");
    let dir = Utf8Path::new(&key_path).parent().expect("key path has a parent");
    create_dir_all(dir)
        .await
        .with_context(|| format!("failed to create {dir}"))?;
    write_private(&key_path, key_pem.as_bytes())
        .await
        .with_context(|| format!("failed to write {key_path}"))?;
    tokio::fs::write(&chain_path, chain_pem.as_bytes())
        .await
        .with_context(|| format!("failed to write {chain_path}"))?;
    info!(log, "generated sush proxy TLS identity"; "key" => key_path);
    Ok(())
}

/// Produce the proxy's private key and certificate chain, both PEM
/// encoded. The chain is leaf first, the way the client expects it.
/// Uses IPCC, so performs blocking I/O. The vouched certificate never
/// expires, because (a) we probably don't know what time it actually is
/// when we generate it, and (b) it's ephemeral and regenerated at every
/// switch zone startup.
fn generate_proxy_pems() -> anyhow::Result<(String, String)> {
    let ipcc = Ipcc::new().context("failed to open IPCC")?;
    let chain_der = ipcc
        .rot_get_tq_cert_chain()
        .context("failed to fetch the TQ cert chain")?;
    let platform = der_cert_chain(&chain_der)?;
    let vouched = EphemeralKey::new_vouched(
        KeyType::Ed25519,
        "CN=sush-proxy,O=Oxide Computer Company,C=US"
            .parse()
            .context("failed to parse the proxy cert subject")?,
        Validity {
            not_before: Time::try_from(SystemTime::now())
                .context("failed to compute the proxy cert validity")?,
            not_after: Time::INFINITY,
        },
        |digest| ipcc.rot_tq_sign(digest),
    )
    .context("failed to generate the proxy key")?;
    let key_pem =
        vouched.private_key_pem().context("failed to encode the proxy key")?;
    let chain =
        once(vouched.cert().clone()).chain(platform).collect::<Vec<_>>();
    let chain_pem = pem_cert_chain(chain)
        .context("failed to encode the proxy cert chain")?;
    Ok((key_pem, chain_pem))
}

impl SushConfig {
    /// Choose the certificates this sled will trust to sign sush jobs.
    /// Every sled trusts the production anchors, and sleds whose platform
    /// identity (fetched here) chains to the staging root trust the staging
    /// anchors as well. This allows, e.g., support to run production-signed
    /// jobs on lab systems, but does not allow engineers to run
    /// staging-signed jobs on production systems.
    async fn trust_anchors(&self, log: &Logger) -> Vec<Utf8PathBuf> {
        let SushConfig {
            production_platform_root,
            staging_platform_root,
            production_trust_anchors,
            staging_trust_anchors,
            ramdisk_dir: _,
            ramdisk_max_output_mb: _,
            max_output_mb: _,
        } = self.clone();
        let mut anchors = production_trust_anchors;
        match spawn_blocking(move || {
            platform_identity_signer(
                &production_platform_root,
                &staging_platform_root,
            )
        })
        .await
        .context("failed to fetch the platform identity")
        .flatten()
        {
            Ok(signer) => {
                info!(log, "platform identity"; "signer" => %signer);
                if signer == PlatformIdentitySigner::Staging {
                    anchors.extend(staging_trust_anchors);
                }
            }
            Err(err) => warn!(
                log, "unknown platform identity, trusting only production job signers";
                "error" => InlineErrorChain::new(err.as_ref()),
            ),
        }
        anchors
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Display)]
#[strum(serialize_all = "lowercase")]
enum PlatformIdentitySigner {
    Production,
    Staging,
}

/// Read the platform identity from the RoT via IPCC (using blocking I/O),
/// and return which signer was used.
fn platform_identity_signer(
    production: &Utf8Path,
    staging: &Utf8Path,
) -> anyhow::Result<PlatformIdentitySigner> {
    let ipcc = Ipcc::new().context("failed to open IPCC")?;
    let chain_der = ipcc
        .rot_get_tq_cert_chain()
        .context("failed to fetch the TQ cert chain")?;
    let chain = der_cert_chain(&chain_der)?;
    let production_root = read_cert(production)?;
    let staging_root = read_cert(staging)?;
    ensure!(is_staging_cert(&staging_root), "{staging} is not a staging root");
    ensure!(
        !is_staging_cert(&production_root),
        "{production} is a staging root, not production"
    );
    let roots = [production_root, staging_root.clone()];
    let root = verify_platform_cert_chain(&chain, Some(&roots))
        .context("failed to verify the platform identity")?;
    Ok(if *root == staging_root {
        PlatformIdentitySigner::Staging
    } else {
        PlatformIdentitySigner::Production
    })
}

/// Whether a platform identity root's subject includes "Staging", e.g.,
/// "Platform Identity Staging Root A". (Production roots' subjects do
/// not include "Production"). This is used only to fail closed in case
/// roots are accidentally swapped in the config file; it is not used as
/// a verification criterion.
fn is_staging_cert(root: &Certificate) -> bool {
    root.tbs_certificate.subject.to_string().contains("Staging")
}

/// Read a PEM file holding one certificate.
fn read_cert(path: &Utf8Path) -> anyhow::Result<Certificate> {
    let pem = read(path).with_context(|| format!("failed to read {path}"))?;
    Certificate::from_pem(&pem).with_context(|| {
        format!("failed to parse {path} as a PEM-encoded certificate")
    })
}

/// Parse a concatenated series of DER certs, as the RoT returns.
fn der_cert_chain(bytes: &[u8]) -> anyhow::Result<Vec<Certificate>> {
    let mut chain = Vec::new();
    let mut reader =
        SliceReader::new(bytes).context("failed to read the TQ cert chain")?;
    while !reader.is_finished() {
        chain.push(
            Certificate::decode(&mut reader)
                .context("failed to parse the TQ cert chain")?,
        );
    }
    ensure!(!chain.is_empty(), "the TQ cert chain is empty");
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
