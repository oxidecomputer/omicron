// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result};
use buf_list::BufList;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use slog::Drain;
use tokio::io::AsyncWriteExt;

use crate::{
    errors::DiscoverPeersError,
    peers::{ArtifactId, ExamplePeers, FetchedArtifact, Peers},
};

/// Installinator app.
#[derive(Debug, Parser)]
#[command(version)]
pub struct InstallinatorApp {
    #[clap(subcommand)]
    subcommand: InstallinatorCommand,
}

impl InstallinatorApp {
    /// Executes the app.
    pub async fn exec(self) -> Result<()> {
        let log = Self::setup_log("/tmp/installinator.log").unwrap();

        match self.subcommand {
            InstallinatorCommand::DebugDiscover(opts) => opts.exec(log).await,
            InstallinatorCommand::Install(opts) => opts.exec(log).await,
        }
    }

    fn setup_log(path: impl AsRef<Utf8Path>) -> anyhow::Result<slog::Logger> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        let file_decorator = slog_term::PlainDecorator::new(file);
        let file_drain =
            slog_term::FullFormat::new(file_decorator).build().fuse();

        let stderr_decorator =
            slog_term::PlainDecorator::new(std::io::stderr());
        let stderr_drain =
            slog_term::FullFormat::new(stderr_decorator).build().fuse();

        let drain = slog::Duplicate::new(file_drain, stderr_drain).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        Ok(slog::Logger::root(drain, slog::o!()))
    }
}

#[derive(Debug, Subcommand)]
enum InstallinatorCommand {
    /// Discover peers on the bootstrap network.
    DebugDiscover(DebugDiscoverOpts),
    /// Perform the installation.
    Install(InstallOpts),
}

#[derive(Debug, Args)]
#[command(version)]
struct DebugDiscoverOpts {
    // TODO: add options here
}

impl DebugDiscoverOpts {
    async fn exec(self, log: slog::Logger) -> Result<()> {
        let peers = Peers::new(
            &log,
            Box::new(ExamplePeers::new(&log)?),
            Duration::from_secs(10),
        );
        println!("discovered peers: {}", peers.display());
        Ok(())
    }
}

#[derive(Debug, Args)]
#[command(version)]
struct InstallOpts {
    // TODO: fetch this
    artifact_id: ArtifactId,

    // TODO: checksum?

    // The destination to write to.
    destination: Utf8PathBuf,
}

impl InstallOpts {
    async fn exec(self, log: slog::Logger) -> Result<()> {
        // TODO: is buffering up the entire artifact in memory OK?
        let artifact = FetchedArtifact::loop_fetch_from_peers(
            &log,
            || {
                // TODO: discover nodes via the bootstrap network
                async {
                    Ok(Peers::new(
                        &log,
                        Box::new(
                            ExamplePeers::new(&log)
                                .map_err(DiscoverPeersError::Retry)?,
                        ),
                        Duration::from_secs(10),
                    ))
                }
            },
            &self.artifact_id,
        )
        .await?;

        slog::info!(
            log,
            "for artifact {}, fetched {} bytes from {}, writing to {}",
            self.artifact_id,
            artifact.artifact.num_bytes(),
            artifact.addr,
            self.destination,
        );

        // TODO: add retries to this?
        write_artifact(&self.artifact_id, artifact.artifact, &self.destination)
            .await?;

        Ok(())
    }
}

async fn write_artifact(
    artifact_id: &ArtifactId,
    mut artifact: BufList,
    destination: &Utf8Path,
) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        // TODO: do we want create = true? Maybe only if writing to a file and not an M.2.
        .create(true)
        .write(true)
        .truncate(true)
        .open(destination)
        .await
        .with_context(|| {
            format!("failed to open destination `{destination}` for writing")
        })?;

    let num_bytes = artifact.num_bytes();

    file.write_all_buf(&mut artifact).await.with_context(|| {
        format!("failed to write artifact {artifact_id} ({num_bytes} bytes) to destination `{destination}`")
    })?;

    Ok(())
}
