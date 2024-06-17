// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for uploading artifacts to wicketd.

use std::{
    convert::Infallible,
    net::SocketAddrV6,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use buf_list::BufList;
use clap::Args;
use futures::StreamExt;
use reqwest::Body;
use tokio_util::io::ReaderStream;

// We have observed wicketd running in a switch zone under load take ~60 seconds
// to accept a repository; set the timeout to double that to give some headroom.
const WICKETD_UPLOAD_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Args)]
pub(crate) struct UploadArgs {
    /// Do not upload to wicketd.
    #[clap(long)]
    no_upload: bool,
}

impl UploadArgs {
    pub(crate) async fn exec(
        self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        self.do_upload(log, wicketd_addr).await
    }

    async fn do_upload(
        &self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        let repo_bytes = Self::read_repository_from_stdin(&log).await?;
        let repository_bytes_len = repo_bytes.num_bytes();

        // Repository validation is performed by wicketd.

        if self.no_upload {
            slog::info!(
                log,
                "not uploading repository to wicketd (--no-upload passed in)"
            );
        } else {
            slog::info!(log, "uploading repository to wicketd");
            let wicketd_client = wicketd_client::Client::new_with_client(
                &format!("http://{wicketd_addr}"),
                shared_client::timeout::<{ WICKETD_UPLOAD_TIMEOUT.as_secs() }>(
                ),
                log.clone(),
            );

            let body = Body::wrap_stream(futures::stream::iter(
                repo_bytes.into_iter().map(Ok::<_, Infallible>),
            ));
            wicketd_client
                .put_repository(body)
                .await
                .context("error uploading repository to wicketd")?;

            slog::info!(
                log,
                "successfully uploaded repository ({repository_bytes_len} bytes) to wicketd",
            );
        }

        Ok(())
    }

    async fn read_repository_from_stdin(log: &slog::Logger) -> Result<BufList> {
        const PROGRESS_INTERVAL: Duration = Duration::from_secs(2);
        slog::info!(log, "beginning read of repository from stdin");

        let mut last_progress_report = Instant::now();

        // Read the entire repository from stdin into memory.
        let mut stream = ReaderStream::new(tokio::io::stdin());
        let mut repo_bytes = BufList::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("error reading repository from stdin")?;

            repo_bytes.push_chunk(chunk);

            let now = Instant::now();
            if now.duration_since(last_progress_report) > PROGRESS_INTERVAL {
                slog::info!(
                    log,
                    "continuing to read repository from stdin \
                     ({} bytes read so far)",
                    repo_bytes.num_bytes(),
                );
                last_progress_report = now;
            }
        }

        slog::info!(
            log,
            "finished reading repository from stdin ({} bytes)",
            repo_bytes.num_bytes(),
        );

        Ok(repo_bytes)
    }
}
