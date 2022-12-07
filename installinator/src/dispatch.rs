// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use slog::Drain;

use crate::peers::{ArtifactId, Peers};

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
            InstallinatorCommand::Discover(opts) => opts.exec(log).await,
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
    Discover(DiscoverOpts),
    /// Perform the installation.
    Install(InstallOpts),
}

#[derive(Debug, Args)]
#[command(version)]
struct DiscoverOpts {
    //
}

impl DiscoverOpts {
    async fn exec(self, log: slog::Logger) -> Result<()> {
        let peers = Peers::discover(&log).await?;
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
        // Discover the nodes via the bootstrap network.
        let discovered_peers =
            Peers::discover(&log).await.context("error discovering peers")?;

        // TODO: is buffering up the entire artifact in memory is OK?
        let (peer, artifact) = discovered_peers
            .fetch_artifact(&self.artifact_id)
            .await
            .context("error fetching artifact")?;

        slog::info!(
            log,
            "for artifact {}, fetched {} bytes from {peer}, writing to {}",
            self.artifact_id,
            artifact.len(),
            self.destination,
        );

        std::fs::write(&self.destination, &artifact).with_context(|| {
            format!(
                "error writing {} to {}",
                self.artifact_id, self.destination
            )
        })?;

        Ok(())
    }
}
