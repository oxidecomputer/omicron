// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Args;
use futures::StreamExt;
use installinator_client::ClientError;
use installinator_common::EventReport;
use ipcc::{InstallinatorImageId, Ipcc};
use omicron_uuid_kinds::MupdateUuid;
use tokio::sync::mpsc;
use tufaceous_artifact::{
    ArtifactHash, ArtifactHashId, ArtifactKind, KnownArtifactKind,
};

use crate::{errors::HttpError, fetch::FetchReceiver};

#[derive(Clone, Debug, Eq, PartialEq, Args)]
pub(crate) struct ArtifactIdOpts {
    /// Retrieve artifact ID from IPCC
    #[clap(
        long,
        required_unless_present_any = ["update_id", "installinator_doc"]
    )]
    from_ipcc: bool,

    #[clap(
        long,
        conflicts_with = "from_ipcc",
        required_unless_present = "from_ipcc"
    )]
    update_id: Option<MupdateUuid>,

    #[clap(
        long,
        conflicts_with_all = ["from_ipcc"],
        required_unless_present_any = ["from_ipcc"],
    )]
    installinator_doc: Option<ArtifactHash>,
}

impl ArtifactIdOpts {
    pub(crate) fn resolve(&self) -> Result<LookupId> {
        if self.from_ipcc {
            let ipcc = Ipcc::new().context("error opening IPCC")?;
            let image_id = ipcc
                .installinator_image_id()
                .context("error retrieving installinator image ID")?;
            LookupId::from_image_id(&image_id)
        } else {
            let update_id = self.update_id.unwrap();
            let document = self
                .installinator_doc
                .context("error retrieving installinator doc hash")?;

            Ok(LookupId { update_id, document })
        }
    }
}

/// Identifiers used by installinator to retrieve artifacts.
pub(crate) struct LookupId {
    pub(crate) update_id: MupdateUuid,
    pub(crate) document: ArtifactHash,
}

impl LookupId {
    fn from_image_id(image_id: &InstallinatorImageId) -> Result<Self> {
        // This sentinel hash is used to indicate that the host phase 2 hash is
        // actually the hash to the installinator document.

        if image_id.control_plane != ArtifactHash([0; 32]) {
            bail!("non-zero control plane hash, this isn't using doc format");
        }

        Ok(Self {
            update_id: image_id.update_id,
            document: image_id.host_phase_2,
        })
    }
}

/// The host phase 2 and control plane hashes to download.
#[derive(Clone, Debug)]
pub(crate) struct ArtifactsToDownload {
    pub(crate) host_phase_2: ArtifactHash,
    pub(crate) control_plane: ArtifactHash,
}

impl ArtifactsToDownload {
    pub(crate) fn host_phase_2_id(&self) -> ArtifactHashId {
        ArtifactHashId {
            kind: ArtifactKind::HOST_PHASE_2,
            hash: self.host_phase_2,
        }
    }

    pub(crate) fn control_plane_id(&self) -> ArtifactHashId {
        ArtifactHashId {
            kind: KnownArtifactKind::ControlPlane.into(),
            hash: self.control_plane,
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

        // Set a connect timeout of 15 seconds (the progenitor default), and a
        // total timeout of 5 minutes. The progenitor default for the total
        // timeout is 15 seconds, which can easily be exceeded for large
        // downloads. (Don't set the total timeout to be too long, though,
        // because we're fetching ~2GiB artifacts over a LAN which really should
        // take less than 5 minutes.)
        //
        // Do not set a read timeout here -- instead, read timeouts are handled
        // by the fetch loop. (Why is the read timeout handled by the fetch
        // loop? So that it can also apply to the mock peer backend, and logic
        // shared across both.)
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(5 * 60))
            .build()
            .expect("installinator artifact client created");
        let client = installinator_client::Client::new_with_client(
            &endpoint,
            client,
            log.clone(),
        );

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
        update_id: MupdateUuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        self.client
            .report_progress(&update_id, &report.into_generic())
            .await
            .map(|resp| resp.into_inner())
    }
}
