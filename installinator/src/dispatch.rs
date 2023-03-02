// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use installinator_common::CompletionEventKind;
use omicron_common::{
    api::internal::nexus::KnownArtifactKind,
    update::{ArtifactHashId, ArtifactKind},
};
use slog::{info, warn, Drain};
use tokio::sync::mpsc;

use crate::{
    artifact::ArtifactIdOpts,
    hardware::Hardware,
    peers::{DiscoveryMechanism, FetchedArtifact, Peers},
    reporter::{ProgressReporter, ReportEvent},
    write::{write_artifact, WriteDestination},
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
            InstallinatorCommand::DebugHardwareScan(opts) => {
                opts.exec(log).await
            }
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
    /// Scan hardware to find the target M.2 device.
    DebugHardwareScan(DebugHardwareScan),
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

/// Perform a scan of the device hardware looking for the target M.2.
#[derive(Debug, Args)]
#[command(version)]
struct DebugHardwareScan {}

impl DebugHardwareScan {
    async fn exec(self, log: slog::Logger) -> Result<()> {
        let hardware = Hardware::scan(&log)?;

        for disk in hardware.m2_disks() {
            match disk.boot_image_devfs_path() {
                Ok(boot_image_path) => {
                    info!(
                        log, "found M.2 disk";
                        "identity" => ?disk.identity(),
                        "path" => disk.devfs_path().display(),
                        "slot" => disk.slot(),
                        "boot_image_path" => boot_image_path.display(),
                        "zpool" => %disk.zpool_name(),
                    );
                }
                Err(err) => {
                    warn!(
                        log, "found M.2 disk but failed to find boot image path";
                        "identity" => ?disk.identity(),
                        "path" => disk.devfs_path().display(),
                        "slot" => disk.slot(),
                        "boot_image_path_err" => %err,
                        "zpool" => %disk.zpool_name(),
                    );
                }
            }
        }

        Ok(())
    }
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
        let (progress_reporter, event_sender) =
            ProgressReporter::new(&log, image_id.update_id, move || {
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
        let progress_handle = progress_reporter.start();

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
        let destination = WriteDestination::in_directory(&self.destination)?;

        write_artifact(
            &log,
            &host_phase_2_id,
            host_phase_2_artifact.artifact,
            &destination.host_phase_2,
            &event_sender,
        )
        .await;

        write_artifact(
            &log,
            &control_plane_id,
            control_plane_artifact.artifact,
            &destination.control_plane,
            &event_sender,
        )
        .await;

        // TODO: verify artifact was correctly written out to disk.

        // Drop the event sender: this signals completion.
        _ = event_sender
            .send(ReportEvent::Completion(CompletionEventKind::Completed))
            .await;
        std::mem::drop(event_sender);

        // Wait for all progress reports to be sent.
        progress_handle.await.context("progress reporter to complete")?;

        Ok(())
    }
}

async fn fetch_artifact(
    id: &ArtifactHashId,
    discovery: &DiscoveryMechanism,
    log: &slog::Logger,
    event_sender: &mpsc::Sender<ReportEvent>,
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
