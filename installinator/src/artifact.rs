// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddrV6;

use anyhow::{Context, Result};
use clap::Args;
use futures::StreamExt;
use installinator_artifact_client::ClientError;
use installinator_common::ProgressReport;
use ipcc_key_value::{InstallinatorImageId, Ipcc};
use omicron_common::update::{ArtifactHash, ArtifactHashId};
use uuid::Uuid;

use crate::peers::FetchSender;

#[derive(Clone, Debug, Eq, PartialEq, Args)]
pub(crate) struct ArtifactIdOpts {
    /// Retrieve artifact ID from IPCC
    #[clap(long, required_unless_present_any = ["update_id", "host_phase_2", "control_plane"])]
    from_ipcc: bool,

    #[clap(
        long,
        conflicts_with = "from_ipcc",
        required_unless_present = "from_ipcc"
    )]
    update_id: Option<Uuid>,

    #[clap(
        long,
        conflicts_with = "from_ipcc",
        required_unless_present = "from_ipcc"
    )]
    host_phase_2: Option<ArtifactHash>,

    #[clap(
        long,
        conflicts_with = "from_ipcc",
        required_unless_present = "from_ipcc"
    )]
    control_plane: Option<ArtifactHash>,
}

impl ArtifactIdOpts {
    pub(crate) fn resolve(&self) -> Result<InstallinatorImageId> {
        if self.from_ipcc {
            let ipcc = Ipcc::open().context("error opening IPCC")?;
            ipcc.installinator_image_id()
                .context("error retrieving installinator image ID")
        } else {
            let update_id = self.update_id.unwrap();
            let host_phase_2 = self.host_phase_2.unwrap();
            let control_plane = self.control_plane.unwrap();

            Ok(InstallinatorImageId { update_id, host_phase_2, control_plane })
        }
    }
}

#[derive(Debug)]
pub(crate) struct ArtifactClient {
    log: slog::Logger,
    client: installinator_artifact_client::Client,
}

impl ArtifactClient {
    pub(crate) fn new(addr: SocketAddrV6, log: &slog::Logger) -> Self {
        let endpoint = format!("http://[{}]:{}", addr.ip(), addr.port());
        let log = log.new(
            slog::o!("component" => "ArtifactClient", "peer" => addr.to_string()),
        );
        let client =
            installinator_artifact_client::Client::new(&endpoint, log.clone());
        Self { log, client }
    }

    pub(crate) async fn fetch(
        &self,
        artifact_hash_id: ArtifactHashId,
        sender: FetchSender,
    ) {
        let artifact_bytes = match self
            .client
            .get_artifact_by_hash(
                artifact_hash_id.kind.as_str(),
                &artifact_hash_id.hash.to_string(),
            )
            .await
        {
            Ok(artifact_bytes) => artifact_bytes,
            Err(error) => {
                _ = sender.send(Err(error)).await;
                return;
            }
        };

        slog::debug!(
            &self.log,
            "preparing to receive {:?} bytes from artifact",
            artifact_bytes.content_length(),
        );

        let mut bytes = artifact_bytes.into_inner_stream();
        while let Some(item) = bytes.next().await {
            if let Err(_) = sender.send(item.map_err(Into::into)).await {
                // The sender was dropped, which indicates that the job was cancelled.
                return;
            }
        }
    }

    pub(crate) async fn report_progress(
        &self,
        update_id: Uuid,
        report: ProgressReport,
    ) -> Result<(), ClientError> {
        self.client
            .report_progress(&update_id, &report.into())
            .await
            .map(|resp| resp.into_inner())
    }
}
