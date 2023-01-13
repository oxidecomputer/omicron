// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for uploading artifacts to wicketd.

use std::net::SocketAddrV6;

use anyhow::{Context, Result};
use clap::Args;
use tokio::io::AsyncReadExt;

use crate::wicketd::create_wicketd_client;

#[derive(Debug, Args)]
pub(crate) struct UploadArgs {
    /// Artifact name to upload
    name: String,

    /// Artifact version to upload
    version: String,

    /// Do not perform the upload to wicketd.
    #[clap(long)]
    no_upload: bool,
}

impl UploadArgs {
    pub(crate) fn exec(
        self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        let runtime =
            tokio::runtime::Runtime::new().context("creating tokio runtime")?;
        runtime.block_on(self.do_upload(log, wicketd_addr))
    }

    async fn do_upload(
        &self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        // Read the entire artifact from stdin into memory.
        let mut artifact_bytes = Vec::new();
        tokio::io::stdin()
            .read_to_end(&mut artifact_bytes)
            .await
            .with_context(|| {
                format!(
                    "error reading artifact {}:{} from stdin",
                    self.name, self.version
                )
            })?;

        let artifact_bytes_len = artifact_bytes.len();

        slog::info!(
            log,
            "read artifact {}:{} ({artifact_bytes_len} bytes) from stdin",
            self.name,
            self.version,
        );

        // TODO: perform validation on the artifact

        if self.no_upload {
            slog::info!(
                log,
                "not uploading artifact to wicketd (--no-upload passed in)"
            );
        } else {
            slog::info!(log, "uploading artifact to wicketd");
            let wicketd_client = create_wicketd_client(&log, wicketd_addr);

            wicketd_client
                .put_artifact(&self.name, &self.version, artifact_bytes)
                .await
                .with_context(|| {
                    format!(
                        "error uploading artifact {}:{} to wicketd",
                        self.name, self.version,
                    )
                })?;

            slog::info!(
                log,
                "successfully uploaded {}:{} ({artifact_bytes_len} bytes) to wicketd",
                self.name,
                self.version,
            );
        }

        Ok(())
    }
}
