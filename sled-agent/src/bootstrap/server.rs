// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::config::Config;
use super::http_entrypoints::ba_api as http_api;
use crate::config::Config as SledConfig;
use crate::sp::SpHandle;
use dropshot::HttpServer;
use slog::Drain;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;

/// Wraps a [Agent] object, and provides helper methods for exposing it
/// via an HTTP interface.
pub struct Server {
    bootstrap_agent: Arc<Agent>,
    http_server: dropshot::HttpServer<Arc<Agent>>,
}

impl Server {
    pub async fn start(
        address: Ipv6Addr,
        config: Config,
        sled_config: SledConfig,
    ) -> Result<Self, String> {
        let (drain, registration) = slog_dtrace::with_drain(
            config.log.to_logger("SledAgent").map_err(|message| {
                format!("initializing logger: {}", message)
            })?,
        );
        let log = slog::Logger::root(drain.fuse(), slog::o!());
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("Failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            return Err(msg);
        } else {
            debug!(log, "registered DTrace probes");
        }

        info!(log, "detecting (real or simulated) SP");
        let sp = SpHandle::detect(&config.sp_config, &sled_config, &log)
            .await
            .map_err(|err| format!("Failed to detect local SP: {err}"))?;

        info!(log, "setting up bootstrap agent server");
        let bootstrap_agent = Arc::new(
            Agent::new(log.clone(), sled_config, address, sp.clone())
                .await
                .map_err(|e| e.to_string())?,
        );

        let ba = Arc::clone(&bootstrap_agent);
        let dropshot_log = log.new(o!("component" => "dropshot (Bootstrap)"));
        let http_server = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_api(),
            ba,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        // Are connections to our bootstrap dropshot server being tunneled
        // through a sprockets proxy? If so, start up our half.
        if let Some(sprockets_proxy_bind_addr) =
            config.sprockets_proxy_bind_addr
        {
            spawn_sprockets_proxy(
                &sp,
                &http_server,
                sprockets_proxy_bind_addr,
                &log,
            )
            .await?;
        }

        let server = Server { bootstrap_agent, http_server };

        // Initialize the bootstrap agent *after* the server has started.
        // This ordering allows the bootstrap agent to communicate with
        // other bootstrap agents on the rack during the initialization
        // process.
        if let Err(e) = server.bootstrap_agent.initialize(&config).await {
            let _ = server.close().await;
            return Err(e.to_string());
        }

        Ok(server)
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        self.http_server.await
    }

    pub async fn close(self) -> Result<(), String> {
        self.http_server.close().await
    }
}

pub fn run_openapi() -> Result<(), String> {
    http_api()
        .openapi("Oxide Bootstrap Agent API", "0.0.1")
        .description("API for interacting with bootstrapping agents")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

async fn spawn_sprockets_proxy(
    sp: &Option<SpHandle>,
    http_server: &HttpServer<Arc<Agent>>,
    sprockets_proxy_bind_addr: SocketAddrV6,
    log: &Logger,
) -> Result<(), String> {
    // We can only start a sprockets proxy if we have an SP.
    let sp = sp.as_ref().ok_or(
        "Misconfiguration: cannot start a sprockets proxy without an SP",
    )?;

    // If we're running a sprockets proxy, our dropshot server should be
    // listening on localhost.
    let dropshot_addr = http_server.local_addr();
    if !dropshot_addr.ip().is_loopback() {
        return Err(concat!(
            "Misconfiguration: bootstrap dropshot IP address should ",
            "be loopback when using a sprockets proxy"
        )
        .into());
    }

    let proxy_config = sprockets_proxy::Config {
        bind_address: SocketAddr::V6(sprockets_proxy_bind_addr),
        target_address: dropshot_addr,
        role: sprockets_proxy::Role::Server,
    };
    let proxy_log = log.new(o!("component" => "sprockets-proxy (Bootstrap)"));

    // TODO-cleanup The `Duration` passed to `Proxy::new()` is the timeout
    // for communicating with the RoT. Currently it can be set to anything
    // at all (our simulated RoT always responds immediately). Should the
    // value move to our config?
    let proxy = sprockets_proxy::Proxy::new(
        &proxy_config,
        sp.manufacturing_public_key(),
        sp.rot_handle(),
        sp.rot_certs(),
        Duration::from_secs(5),
        proxy_log,
    )
    .await
    .map_err(|err| format!("Failed to start sprockets proxy: {err}"))?;

    tokio::spawn(async move {
        // TODO-robustness `proxy.run()` only fails if `accept()`ing on our
        // already-bound listening socket fails, which means something has
        // gone very wrong. Do we have any recourse other than panicking?
        // What does dropshot do if `accept()` fails?
        proxy.run().await.expect("sprockets server proxy failed");
    });

    Ok(())
}
