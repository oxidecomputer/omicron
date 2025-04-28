// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::Args;
use futures::StreamExt;
use installinator_client::ClientError;
use installinator_common::EventReport;
use ipcc::{InstallinatorImageId, Ipcc};
use tokio::sync::mpsc;
use tufaceous_artifact::{ArtifactHash, ArtifactHashId};
use uuid::Uuid;

use crate::{errors::HttpError, fetch::FetchReceiver};

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
            let ipcc = Ipcc::new().context("error opening IPCC")?;
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
    client: installinator_client::Client,
}

impl ArtifactClient {
    pub(crate) fn new(addr: SocketAddr, log: &slog::Logger) -> Self {
        // NOTE: the production code path is always IPv6. IPv4 is supported for
        // testing only.
        let endpoint = match addr {
            SocketAddr::V4(addr) => {
                format!("http://{}:{}", addr.ip(), addr.port())
            }
            SocketAddr::V6(addr) => {
                format!("http://[{}]:{}", addr.ip(), addr.port())
            }
        };
        let log = log.new(
            slog::o!("component" => "ArtifactClient", "peer" => addr.to_string()),
        );
        let client = installinator_client::Client::new(&endpoint, log.clone());
        Self { log, client }
    }

    pub(crate) async fn fetch(
        &self,
        artifact_hash_id: ArtifactHashId,
    ) -> Result<(u64, FetchReceiver), HttpError> {
        let artifact_bytes = self
            .client
            .get_artifact_by_hash(
                artifact_hash_id.kind.as_str(),
                &artifact_hash_id.hash.to_string(),
            )
            .await?;

        slog::debug!(
            &self.log,
            "preparing to receive {:?} bytes from artifact",
            artifact_bytes.content_length(),
        );

        // We expect servers to set a Content-Length header.
        let content_length =
            match artifact_bytes.headers().get(http::header::CONTENT_LENGTH) {
                Some(v) => {
                    let s = v
                        .to_str()
                        .map_err(|_| HttpError::InvalidContentLength)?;
                    s.parse().map_err(|_| HttpError::InvalidContentLength)?
                }
                None => return Err(HttpError::MissingContentLength),
            };

        let (fetch_sender, fetch_receiver) = mpsc::channel(8);

        tokio::spawn(async move {
            let mut bytes = artifact_bytes.into_inner_stream();
            while let Some(item) = bytes.next().await {
                if let Err(_) =
                    fetch_sender.send(item.map_err(Into::into)).await
                {
                    // The sender was dropped, which indicates that the job was cancelled.
                    return;
                }
            }
        });

        Ok((content_length, fetch_receiver))
    }

    pub(crate) async fn report_progress(
        &self,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        self.client
            .report_progress(&update_id, &report)
            .await
            .map(|resp| resp.into_inner())
    }
}
