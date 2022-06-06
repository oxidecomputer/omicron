// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent's Bootstrap API.
//! This is not its own crate because the only intended consumer is other
//! bootstrap peers within the cluster.

use crate::sp::SpHandle;
use slog::Logger;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed to start sprockets proxy: {0}")]
    SprocketsProxy(#[from] sprockets_proxy::Error),

    #[error("Error making HTTP request to Bootstrap Agent: {0}")]
    BootstrapApi(#[from] generated::Error<generated::types::Error>),
}

pub(crate) struct Client {
    inner: generated::Client,
    sprockets_proxy: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        // If we spawned a sprockets proxy task when we were created, kill it
        // when we're dropped.
        if let Some(task) = self.sprockets_proxy.take() {
            task.abort();
        }
    }
}

impl Client {
    pub(crate) async fn new(
        bootstrap_addr: SocketAddrV6,
        sp: &Option<SpHandle>,
        log: &Logger,
    ) -> Result<Self, Error> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;

        let (url, sprockets_proxy) = if let Some(sp) = sp.as_ref() {
            // We have an SP; spawn a sprockets proxy for this connection.
            let proxy_config = sprockets_proxy::Config {
                bind_address: "[::1]:0".parse().unwrap(),
                target_address: SocketAddr::V6(bootstrap_addr),
                role: sprockets_proxy::Role::Client,
            };
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
                log.new(o!("BootstrapAgentClientSprocketsProxy"
                        => proxy_config.target_address)),
            )
            .await?;

            let proxy_addr = proxy.local_addr();

            let proxy_task = tokio::spawn(async move {
                // `proxy.run()` only fails if `accept()`ing on our
                // already-bound listening socket fails, which means something
                // has gone very wrong; panic if it fails.
                proxy.run().await.expect("sprockets client proxy failed");
            });

            info!(
                log, "Sending request to peer agent via sprockets proxy";
                "peer" => %bootstrap_addr,
                "sprockets_proxy" => %proxy_addr,
            );
            (format!("http://{}", proxy_addr), Some(proxy_task))
        } else {
            // We have no SP; connect directly.
            info!(
                log, "Sending request to peer agent";
                "peer" => %bootstrap_addr,
            );
            (format!("http://{}", bootstrap_addr), None)
        };

        let inner = generated::Client::new_with_client(
            &url,
            client,
            log.new(o!("BootstrapAgentClient" => url.clone())),
        );

        Ok(Self { inner, sprockets_proxy })
    }

    pub(crate) async fn start_sled(
        &self,
        request: &types::SledAgentRequest,
    ) -> Result<ResponseValue<types::SledAgentResponse>, Error> {
        Ok(self.inner.start_sled(request).await?)
    }
}

mod generated {
    use omicron_common::generate_logging_api;

    generate_logging_api!("../openapi/bootstrap-agent.json");
}

pub(crate) use generated::types;
pub(crate) use generated::ResponseValue;
