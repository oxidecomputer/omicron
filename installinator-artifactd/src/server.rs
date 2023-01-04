// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! The installinator artifact server.

use std::net::SocketAddrV6;

use anyhow::{anyhow, Result};

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
    pub async fn start(self) -> Result<()> {
        let context = ServerContext { artifact_store: self.store };

        let dropshot_config = dropshot::ConfigDropshot {
            bind_address: std::net::SocketAddr::V6(self.address),
            ..Default::default()
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

        server
            .start()
            .await
            .map_err(|error| anyhow!(error).context("failed to run server"))
    }
}
