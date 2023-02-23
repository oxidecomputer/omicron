// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result};
use buf_list::BufList;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use installinator_common::ReportEventKind;
use omicron_common::{
    api::internal::nexus::KnownArtifactKind,
    update::{ArtifactHashId, ArtifactKind},
};
use slog::Drain;
use tokio::{io::AsyncWriteExt, sync::mpsc};

use crate::{
    artifact::ArtifactIdOpts,
    peers::{DiscoveryMechanism, FetchedArtifact, Peers},
    reporter::EventReporter,
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

        let stderr_drain = stderr_env_drain("RUST_LOG");

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

/// Perform discovery of peers.
#[derive(Debug, Args)]
#[command(version)]
struct DebugDiscoverOpts {
    #[command(flatten)]
    opts: DiscoverOpts,
}

impl DebugDiscoverOpts {
    async fn exec(self, log: slog::Logger) -> Result<()> {
        let peers = Peers::new(
            &log,
            self.opts.mechanism.discover_peers(&log).await?,
            Duration::from_secs(10),
        );
        println!("discovered peers: {}", peers.display());
        Ok(())
    }
}

/// Options shared by both [`DebugDiscoverOpts`] and [`InstallOpts`].
#[derive(Debug, Args)]
struct DiscoverOpts {
    /// The mechanism by which to discover peers: bootstrap or list:[::1]:8000
    #[clap(long, default_value_t = DiscoveryMechanism::Bootstrap)]
    mechanism: DiscoveryMechanism,
}

#[derive(Debug, Args)]
#[command(version)]
struct InstallOpts {
    #[command(flatten)]
    discover_opts: DiscoverOpts,

    /// Artifact ID options
    #[command(flatten)]
    artifact_ids: ArtifactIdOpts,

    // TODO: checksum?

    // The destination to write to.
    destination: Utf8PathBuf,
}

impl InstallOpts {
    async fn exec(self, log: slog::Logger) -> Result<()> {
        let image_id = self.artifact_ids.resolve()?;

        let host_phase_2_id = ArtifactHashId {
            // TODO: currently we're assuming that wicket will unpack the host
            // phase 2 image. We may instead have the installinator do it.
            // kind: KnownArtifactKind::Host.into(),
            kind: ArtifactKind::HOST_PHASE_2,
            hash: image_id.host_phase_2,
        };

        let discovery = self.discover_opts.mechanism.clone();
        let discovery_log = log.clone();
        let (event_reporter, event_sender) =
            EventReporter::new(&log, image_id.update_id, move || {
                let log = discovery_log.clone();
                let discovery = discovery.clone();
                async move {
                    Ok(Peers::new(
                        &log,
                        discovery.discover_peers(&log).await?,
                        Duration::from_secs(10),
                    ))
                }
            });
        let event_handle = event_reporter.start();

        let host_phase_2_artifact = fetch_artifact(
            &host_phase_2_id,
            &self.discover_opts.mechanism,
            &log,
            &event_sender,
        )
        .await?;

        let control_plane_id = ArtifactHashId {
            kind: KnownArtifactKind::ControlPlane.into(),
            hash: image_id.control_plane,
        };

        let control_plane_artifact = fetch_artifact(
            &control_plane_id,
            &self.discover_opts.mechanism,
            &log,
            &event_sender,
        )
        .await?;

        // TODO: figure out the actual destination.

        // TODO: add retries to this process?
        std::fs::create_dir_all(&self.destination).with_context(|| {
            format!("error creating directories at {}", self.destination)
        })?;

        write_artifact(
            &host_phase_2_id,
            host_phase_2_artifact.artifact,
            &self.destination.join("host_phase_2.bin"),
        )
        .await?;

        write_artifact(
            &control_plane_id,
            control_plane_artifact.artifact,
            &self.destination.join("control_plane.bin"),
        )
        .await?;

        // Wait for all events to be sent.
        event_handle
            .await
            .context("progress reporting task failed to complete")?;

        Ok(())
    }
}

async fn fetch_artifact(
    id: &ArtifactHashId,
    discovery: &DiscoveryMechanism,
    log: &slog::Logger,
    event_sender: &mpsc::Sender<ReportEventKind>,
) -> Result<FetchedArtifact> {
    // TODO: Not sure why slog::o!("artifact" => ?id) isn't working, figure it
    // out at some point.
    let log = log.new(slog::o!("artifact" => format!("{id:?}")));
    let artifact = FetchedArtifact::loop_fetch_from_peers(
        &log,
        || async {
            Ok(Peers::new(
                &log,
                discovery.discover_peers(&log).await?,
                Duration::from_secs(10),
            ))
        },
        id,
        event_sender,
    )
    .await
    .with_context(|| format!("error fetching image with id {id:?}"))?;

    slog::info!(
        log,
        "fetched {} bytes from {}",
        artifact.artifact.num_bytes(),
        artifact.addr,
    );

    Ok(artifact)
}

async fn write_artifact(
    artifact_id: &ArtifactHashId,
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
        format!("failed to write artifact {artifact_id:?} ({num_bytes} bytes) to destination `{destination}`")
    })?;

    Ok(())
}

pub(crate) fn stderr_env_drain(
    env_var: &str,
) -> impl Drain<Ok = (), Err = slog::Never> {
    let stderr_decorator = slog_term::TermDecorator::new().build();
    let stderr_drain =
        slog_term::FullFormat::new(stderr_decorator).build().fuse();
    let mut builder = slog_envlogger::LogBuilder::new(stderr_drain);
    if let Ok(s) = std::env::var(env_var) {
        builder = builder.parse(&s);
    } else {
        // Log at the info level by default.
        builder = builder.filter(None, slog::FilterLevel::Info);
    }
    builder.build()
}
