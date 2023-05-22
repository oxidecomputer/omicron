// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use buf_list::Cursor;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use installinator_common::{
    InstallinatorCompletionMetadata, InstallinatorComponent, InstallinatorSpec,
    InstallinatorStepId, StepContext, StepHandle, StepProgress, UpdateEngine,
};
use omicron_common::{
    api::internal::nexus::KnownArtifactKind,
    update::{ArtifactHashId, ArtifactKind},
};
use slog::Drain;
use tufaceous_lib::ControlPlaneZoneImages;
use update_engine::StepResult;

use crate::{
    artifact::ArtifactIdOpts,
    peers::{DiscoveryMechanism, FetchedArtifact, Peers},
    reporter::ProgressReporter,
    write::{ArtifactWriter, WriteDestination},
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
    pub async fn exec(self, log: &slog::Logger) -> Result<()> {
        match self.subcommand {
            InstallinatorCommand::DebugDiscover(opts) => opts.exec(log).await,
            InstallinatorCommand::DebugHardwareScan(opts) => {
                opts.exec(log).await
            }
            InstallinatorCommand::Install(opts) => opts.exec(log).await,
        }
    }

    pub fn setup_log(
        path: impl AsRef<Utf8Path>,
    ) -> anyhow::Result<slog::Logger> {
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
    async fn exec(self, log: &slog::Logger) -> Result<()> {
        let peers = Peers::new(
            log,
            self.opts.mechanism.discover_peers(log).await?,
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
    async fn exec(self, log: &slog::Logger) -> Result<()> {
        // Finding the write destination from the gimlet hardware logs details
        // about what it's doing sufficiently for this subcommand; just create a
        // write destination and then discard it.
        _ = WriteDestination::from_hardware(log)?;
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

    /// If true, perform sled-agent-like bootstrapping operations on startup
    /// (e.g., configure and enable maghemite).
    #[clap(long)]
    bootstrap_sled: bool,

    /// If true, do not exit after successful completion (e.g., to continue
    /// running as an smf service).
    #[clap(long)]
    stay_alive: bool,

    /// Install on a gimlet's M.2 drives, found via scanning for hardware.
    ///
    /// WARNING: This will overwrite the boot image slice of both M.2 drives, if
    /// present!
    #[clap(long)]
    install_on_gimlet: bool,

    // TODO: checksum?

    // The destination to write to.
    #[clap(
        required_unless_present = "install_on_gimlet",
        conflicts_with = "install_on_gimlet"
    )]
    destination: Option<Utf8PathBuf>,
}

impl InstallOpts {
    async fn exec(self, log: &slog::Logger) -> Result<()> {
        if self.bootstrap_sled {
            crate::bootstrap::bootstrap_sled(log.clone()).await?;
        }

        let image_id = self.artifact_ids.resolve()?;

        let discovery = self.discover_opts.mechanism.clone();
        let discovery_log = log.clone();
        let (progress_reporter, event_sender) =
            ProgressReporter::new(log, image_id.update_id, move || {
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
        let discovery = &self.discover_opts.mechanism;

        let engine = UpdateEngine::new(log, event_sender);

        let host_phase_2_id = ArtifactHashId {
            // TODO: currently we're assuming that wicketd will unpack the host
            // phase 2 image. We may instead have the installinator do it.
            kind: ArtifactKind::HOST_PHASE_2,
            hash: image_id.host_phase_2,
        };
        let host_2_phase_id_2 = host_phase_2_id.clone();
        let host_phase_2_artifact = engine
            .new_step(
                InstallinatorComponent::HostPhase2,
                InstallinatorStepId::Download,
                "Downloading host phase 2 artifact",
                |cx| async move {
                    let host_phase_2_artifact =
                        fetch_artifact(&cx, &host_phase_2_id, discovery, log)
                            .await?;

                    let address = host_phase_2_artifact.addr;

                    StepResult::success(
                        host_phase_2_artifact,
                        InstallinatorCompletionMetadata::Download { address },
                    )
                },
            )
            .register();

        let control_plane_id = ArtifactHashId {
            kind: KnownArtifactKind::ControlPlane.into(),
            hash: image_id.control_plane,
        };
        let control_plane_id_2 = control_plane_id.clone();
        let control_plane_artifact = engine
            .new_step(
                InstallinatorComponent::ControlPlane,
                InstallinatorStepId::Download,
                "Downloading control plane artifact",
                |cx| async move {
                    let control_plane_artifact =
                        fetch_artifact(&cx, &control_plane_id, discovery, log)
                            .await?;

                    let address = control_plane_artifact.addr;

                    StepResult::success(
                        control_plane_artifact,
                        InstallinatorCompletionMetadata::Download { address },
                    )
                },
            )
            .register();

        let destination = if self.install_on_gimlet {
            let log = log.clone();
            engine
                .new_step(
                    InstallinatorComponent::Both,
                    InstallinatorStepId::Scan,
                    "Scanning hardware to find M.2 disks",
                    move |cx| async move {
                        scan_hardware_with_retries(&cx, &log).await
                    },
                )
                .register()
        } else {
            // clap ensures `self.destination` is not `None` if
            // `install_on_gimlet` is false.
            let destination = self.destination.as_ref().unwrap();
            StepHandle::ready(WriteDestination::in_directory(destination)?)
        };

        let control_plane_zones = engine
            .new_step(
                InstallinatorComponent::ControlPlane,
                InstallinatorStepId::UnpackControlPlaneArtifact,
                "Unpacking composite control plane artifact",
                move |cx| async move {
                    let control_plane_artifact =
                        control_plane_artifact.into_value(cx.token()).await;
                    let zones = tokio::task::spawn_blocking(|| {
                        ControlPlaneZoneImages::extract(Cursor::new(
                            control_plane_artifact.artifact,
                        ))
                    })
                    .await
                    .unwrap()?;

                    let zones_to_install = zones.zones.len();
                    StepResult::success(
                        zones,
                        InstallinatorCompletionMetadata::ControlPlaneZones {
                            zones_to_install,
                        },
                    )
                },
            )
            .register();

        engine
            .new_step(
                InstallinatorComponent::Both,
                InstallinatorStepId::Write,
                "Writing host and control plane artifacts",
                |cx| async move {
                    let destination = destination.into_value(cx.token()).await;
                    let host_phase_2_artifact =
                        host_phase_2_artifact.into_value(cx.token()).await;
                    let control_plane_zones =
                        control_plane_zones.into_value(cx.token()).await;

                    let mut writer = ArtifactWriter::new(
                        &host_2_phase_id_2,
                        &host_phase_2_artifact.artifact,
                        &control_plane_id_2,
                        &control_plane_zones,
                        destination,
                    );

                    // TODO: verify artifact was correctly written out to disk.

                    let write_output = writer.write(&cx, log).await;
                    let slots_not_written = write_output.slots_not_written();

                    let metadata = InstallinatorCompletionMetadata::Write {
                        output: write_output,
                    };

                    if slots_not_written.is_empty() {
                        StepResult::success((), metadata)
                    } else {
                        // Some slots were not properly written out.
                        let mut message =
                            "Some M.2 slots were not written: ".to_owned();
                        for (i, slot) in slots_not_written.iter().enumerate() {
                            message.push_str(&format!("{slot}"));
                            if i + 1 < slots_not_written.len() {
                                message.push_str(", ");
                            }
                        }
                        StepResult::warning((), metadata, message)
                    }
                },
            )
            .register();

        engine.execute().await.context("failed to execute installinator")?;

        // Wait for all progress reports to be sent.
        progress_handle.await.context("progress reporter to complete")?;

        if self.stay_alive {
            loop {
                slog::info!(log, "installation complete; sleeping");
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }

        Ok(())
    }
}

async fn scan_hardware_with_retries(
    cx: &StepContext,
    log: &slog::Logger,
) -> Result<StepResult<WriteDestination, InstallinatorSpec>> {
    // Scanning for our disks is inherently racy: we have to wait for the disks
    // to attach. This should take milliseconds in general; we'll set a hard cap
    // at retrying for ~30 seconds.
    const HARDWARE_RETRIES: usize = 60;
    const HARDWARE_RETRY_DELAY: Duration = Duration::from_millis(500);

    // We always expect to find two M.2s! If we do not, fail, so an operator can
    // flag this sled and fix it before we proceed.
    const EXPECTED_NUM_DISKS: usize = 2;

    let mut attempt = 0;
    loop {
        attempt += 1;
        let log = log.clone();
        let result = tokio::task::spawn_blocking(move || {
            WriteDestination::from_hardware(&log)
        })
        .await
        .unwrap();

        match result {
            Ok(destination) => {
                let disks_found = destination.num_target_disks();
                if disks_found == EXPECTED_NUM_DISKS {
                    return StepResult::success(
                        destination,
                        InstallinatorCompletionMetadata::HardwareScan {
                            disks_found,
                        },
                    );
                } else if attempt < HARDWARE_RETRIES {
                    cx.send_progress(StepProgress::retry(format!(
                        "hardware scan {attempt} failed:
                         found {disks_found} but expected {EXPECTED_NUM_DISKS}"
                    )))
                    .await;
                    tokio::time::sleep(HARDWARE_RETRY_DELAY).await;
                } else {
                    bail!(
                        "hardware scan failed:
                         found {disks_found} but expected {EXPECTED_NUM_DISKS}"
                    );
                }
            }
            Err(error) => {
                if attempt < HARDWARE_RETRIES {
                    cx.send_progress(StepProgress::retry(format!(
                        "hardware scan {attempt} failed: {error:#}"
                    )))
                    .await;
                    tokio::time::sleep(HARDWARE_RETRY_DELAY).await;
                } else {
                    return Err(error);
                }
            }
        }
    }
}

async fn fetch_artifact(
    cx: &StepContext,
    id: &ArtifactHashId,
    discovery: &DiscoveryMechanism,
    log: &slog::Logger,
) -> Result<FetchedArtifact> {
    // TODO: Not sure why slog::o!("artifact" => ?id) isn't working, figure it
    // out at some point.
    let log = log.new(slog::o!("artifact" => format!("{id:?}")));
    let artifact = FetchedArtifact::loop_fetch_from_peers(
        cx,
        &log,
        || async {
            Ok(Peers::new(
                &log,
                discovery.discover_peers(&log).await?,
                Duration::from_secs(10),
            ))
        },
        id,
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
