// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for uploading artifacts to wicketd.

use std::{convert::Infallible, net::SocketAddrV6, time::Duration};

use anyhow::{Context, Result};
use buf_list::BufList;
use clap::Args;
use futures::TryStreamExt;
use reqwest::Body;
use tokio_util::io::ReaderStream;

use crate::wicketd::create_wicketd_client;

const WICKETD_UPLOAD_TIMEOUT: Duration = Duration::from_millis(30_000);

#[derive(Debug, Args)]
pub(crate) struct UploadArgs {
    /// Do not upload to wicketd.
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
        // Read the entire repository from stdin into memory.
        let repo_bytes: BufList = ReaderStream::new(tokio::io::stdin())
            .try_collect()
            .await
            .context("error reading repository from stdin")?;

        let repository_bytes_len = repo_bytes.num_bytes();

        slog::info!(
            log,
            "read repository ({repository_bytes_len} bytes) from stdin",
        );

        // Repository validation is performed by wicketd.

        if self.no_upload {
            slog::info!(
                log,
                "not uploading repository to wicketd (--no-upload passed in)"
            );
        } else {
            slog::info!(log, "uploading repository to wicketd");
            let wicketd_client = create_wicketd_client(
                &log,
                wicketd_addr,
                WICKETD_UPLOAD_TIMEOUT,
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
}
