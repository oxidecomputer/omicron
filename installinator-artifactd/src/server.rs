// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! The installinator artifact server.

use std::net::SocketAddrV6;

use anyhow::{anyhow, Result};
use dropshot::{HandlerTaskMode, HttpServer};

use crate::{
    context::ServerContext,
    store::{ArtifactGetter, ArtifactStore},
};

/// The installinator artifact server.
#[derive(Debug)]
pub struct ArtifactServer {
    address: SocketAddrV6,
    log: slog::Logger,
    store: ArtifactStore,
}

impl ArtifactServer {
    /// Creates a new artifact server with the given address.
    pub fn new<Getter: ArtifactGetter>(
        getter: Getter,
        address: SocketAddrV6,
        log: &slog::Logger,
    ) -> Self {
        let log = log.new(slog::o!("component" => "installinator artifactd"));
        let store = ArtifactStore::new(getter, &log);
        Self { address, log, store }
    }

    /// Starts the artifact server.
    ///
    /// This returns an `HttpServer`, which can be awaited to completion.
    pub fn start(self) -> Result<HttpServer<ServerContext>> {
        let context = ServerContext { artifact_store: self.store };

        let dropshot_config = dropshot::ConfigDropshot {
            bind_address: std::net::SocketAddr::V6(self.address),
            // Even though the installinator sets an upper bound on the number
            // of items in a progress report, they can get pretty large if they
            // haven't gone through for a bit. Ensure that hitting the max
            // request size won't cause a failure by setting a generous upper
            // bound for the request size.
            //
            // TODO: replace with an endpoint-specific option once
            // https://github.com/oxidecomputer/dropshot/pull/618 lands and is
            // available in omicron.
            request_body_max_bytes: 4 * 1024 * 1024,
            default_handler_task_mode: HandlerTaskMode::Detached,
        };

        let api = crate::http_entrypoints::api();
        let server = dropshot::HttpServerStarter::new(
            &dropshot_config,
            api,
            context,
            &self.log,
        )
        .map_err(|error| {
            anyhow!(error)
                .context("failed to create installinator artifact server")
        })?;

        Ok(server.start())
    }
}
