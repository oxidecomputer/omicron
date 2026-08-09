// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Support Shell proxy for the switch zone (RFD 620).
//!
//! Terminates technician-port connections and routes each request
//! to a sled's `sush` server. Sleds are discovered by probing the
//! addresses behind the bootstrap and underlay prefixes DDM
//! advertises, and the cubby numbering comes from MGS.
//!
//! The verification counterpart of the proxy's TLS identity lives
//! in sush's `client/src/tls.rs`.

use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use gateway_client::Client as MgsClient;
use gateway_types::component::SpType;
use omicron_common::address::SUSH_API_PORT;
use omicron_ddm_admin_client::Client as DdmClient;
use sled_hardware_types::BaseboardId;
use sled_hardware_types::underlay::BootstrapInterface;
use slog::{Logger, debug, warn};
use sprockets_tls::keys::ResolveSetting;
use sush_common::targets::{Cubbies, MAX_CUBBY};
use sush_server::ProxyServer;
use sush_server::proxy::{Targets, platform_tls};
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const POLL_INTERVAL: Duration = Duration::from_secs(30);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How the proxy authenticates itself to clients.
#[derive(Clone, Debug)]
pub enum Tls {
    /// The sled's platform identity: an ephemeral key minted by
    /// sled-agent and signed once by the RoT, served from local files.
    Platform { priv_key: Utf8PathBuf, cert_chain: Utf8PathBuf },
    /// None. For development images, which have no RoT to mint an
    /// identity with.
    Insecure,
}

pub struct Config {
    pub address: SocketAddr,
    pub mgs_address: SocketAddrV6,
    pub tls: Tls,
}

/// Serve the proxy and its discovery loops.
pub async fn run(log: &Logger, config: Config) -> Result<()> {
    let tls = match config.tls {
        Tls::Platform { priv_key, cert_chain } => Some(
            platform_tls(log, ResolveSetting::Local { priv_key, cert_chain })
                .context("loading the TLS identity")?,
        ),
        Tls::Insecure => None,
    };
    let (tx_targets, rx_targets) = watch::channel(Targets::default());
    let _proxy = ProxyServer::start(
        log,
        config.address,
        tls,
        rx_targets,
        CancellationToken::new(),
    )
    .await
    .context("starting the proxy")?;

    let ddm = DdmClient::localhost(log).context("reaching ddmd")?;
    let mgs = mgs_client(log, config.mgs_address);
    tokio::join!(sleds(log, ddm, &tx_targets), cubbies(log, mgs, &tx_targets),);
    unreachable!("discovery loops never return");
}

fn mgs_client(log: &Logger, address: SocketAddrV6) -> MgsClient {
    let client = reqwest::ClientBuilder::new()
        .connect_timeout(PROBE_TIMEOUT)
        .timeout(PROBE_TIMEOUT)
        .build()
        .expect("failed to build an HTTP client");
    MgsClient::new_with_client(
        &format!("http://{address}"),
        client,
        log.clone(),
    )
}

/// Keep `Targets::sleds` current: every sled address advertised via
/// DDM that answers `/target` is routable. Sleds answer on their
/// bootstrap addresses from boot and on their underlay addresses once
/// RSS assigns them; we probe both, and underlay wins.
async fn sleds(log: &Logger, ddm: DdmClient, targets: &watch::Sender<Targets>) {
    let probe = reqwest::ClientBuilder::new()
        .connect_timeout(PROBE_TIMEOUT)
        .timeout(PROBE_TIMEOUT)
        .build()
        .expect("failed to build an HTTP client");
    loop {
        let mut sleds = BTreeMap::new();
        match ddm
            .derive_bootstrap_addrs_from_prefixes(&[
                BootstrapInterface::GlobalZone,
            ])
            .await
        {
            Ok(addrs) => {
                let addrs =
                    addrs.map(|ip| SocketAddrV6::new(ip, SUSH_API_PORT, 0, 0));
                discover(log, &probe, addrs, &mut sleds).await;
            }
            Err(err) => {
                warn!(log, "unable to fetch bootstrap prefixes"; "error" => %err)
            }
        }
        match ddm.derive_sled_addrs_from_prefixes().await {
            Ok(addrs) => {
                let addrs = addrs
                    .map(|a| SocketAddrV6::new(*a.ip(), SUSH_API_PORT, 0, 0));
                discover(log, &probe, addrs, &mut sleds).await;
            }
            Err(err) => warn!(log, "unable to fetch prefixes"; "error" => %err),
        }
        targets.send_modify(|t| t.sleds = sleds);
        sleep(POLL_INTERVAL).await;
    }
}

/// Probe candidate addresses and record the sleds that answer.
async fn discover(
    log: &Logger,
    probe: &reqwest::Client,
    addrs: impl Iterator<Item = SocketAddrV6>,
    sleds: &mut BTreeMap<BaseboardId, SocketAddr>,
) {
    for addr in addrs {
        match target(probe, addr).await {
            Ok(baseboard) => {
                sleds.insert(baseboard, SocketAddr::V6(addr));
            }
            Err(err) => {
                debug!(log, "sled did not answer"; "addr" => %addr, "error" => %err);
            }
        }
    }
}

/// Ask a sush server which baseboard it serves.
async fn target(
    probe: &reqwest::Client,
    addr: SocketAddrV6,
) -> Result<BaseboardId, reqwest::Error> {
    probe
        .get(format!("http://{addr}/target"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}

/// Keep `Targets::cubbies` current from MGS's view of the SPs.
/// Answers merge over what we already know, so a probe outage never
/// erases the map.
async fn cubbies(
    log: &Logger,
    mgs: MgsClient,
    targets: &watch::Sender<Targets>,
) {
    loop {
        let mut cubbies = Cubbies::new();
        for cubby in 0..=MAX_CUBBY {
            match mgs.sp_get(&SpType::Sled, cubby.into()).await {
                Ok(state) => {
                    let state = state.into_inner();
                    cubbies.insert(
                        cubby,
                        BaseboardId {
                            part_number: state.model,
                            serial_number: state.serial_number,
                        },
                    );
                }
                Err(err) => {
                    debug!(log, "no SP state for cubby"; "cubby" => cubby, "error" => %err);
                }
            }
        }
        targets.send_modify(|t| t.cubbies.extend(cubbies));
        sleep(POLL_INTERVAL).await;
    }
}
