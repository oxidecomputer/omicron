// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use buf_list::{BufList, Cursor};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use installinator_common::{
    InstallinatorCompletionMetadata, InstallinatorComponent, InstallinatorSpec,
    InstallinatorStepId, StepContext, StepHandle, StepProgress, StepSuccess,
    StepWarning, UpdateEngine,
};
use omicron_common::FileKv;
use sha2::{Digest, Sha256};
use slog::{Drain, error, warn};
use tufaceous_artifact::{
    ArtifactHash, ArtifactHashId, ArtifactKind, KnownArtifactKind,
};
use tufaceous_lib::ControlPlaneZoneImages;
use update_engine::StepResult;

use crate::{
    ArtifactWriter, WriteDestination,
    artifact::ArtifactIdOpts,
    fetch::{FetchArtifactBackend, FetchedArtifact, HttpFetchBackend},
    peers::DiscoveryMechanism,
    reporter::{HttpProgressBackend, ProgressReporter, ReportProgressBackend},
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
        Ok(slog::Logger::root(drain, slog::o!(FileKv)))
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
        let backend = FetchArtifactBackend::new(
            log,
            Box::new(HttpFetchBackend::new(
                &log,
                self.opts.mechanism.discover_peers(&log).await?,
            )),
            Duration::from_secs(10),
        );
        println!("discovered peers: {}", backend.peers().display());
        Ok(())
    }
}

/// Options shared by both [`DebugDiscoverOpts`] and [`InstallOpts`].
#[derive(Debug, Args)]
struct DiscoverOpts {
    /// The mechanism by which to discover peers: bootstrap or `list:[::1]:8000`
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
        _ = WriteDestination::from_hardware(log).await?;
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

    //TODO this probably needs to get plumbed somewhere instead of relying
    //on a default.
    /// The first gimlet data link to use.
    #[clap(long, default_value = "cxgbe0")]
    data_link0: String,

    //TODO this probably needs to get plumbed somewhere instead of relying
    //on a default.
    /// The second gimlet data link to use.
    #[clap(long, default_value = "cxgbe1")]
    data_link1: String,

    // TODO: checksum?
    /// The first destination to write to, representing M.2 slot A.
    ///
    /// This is mutually exclusive with --install-on-gimlet. At least one of the
    /// two must be provided.
    #[clap(
        required_unless_present = "install_on_gimlet",
        conflicts_with = "install_on_gimlet"
    )]
    a_destination: Option<Utf8PathBuf>,

    /// The second destination to write to, representing M.2 slot B.
    ///
    /// This is mutually exclusive with --install-on-gimlet, and is optional to
    /// provide.
    #[clap(conflicts_with = "install_on_gimlet")]
    b_destination: Option<Utf8PathBuf>,
}

impl InstallOpts {
    async fn exec(self, log: &slog::Logger) -> Result<()> {
        if self.bootstrap_sled {
            let data_links = [self.data_link0.clone(), self.data_link1.clone()];
            crate::bootstrap::bootstrap_sled(&data_links, log.clone()).await?;
        }

        let image_id = self.artifact_ids.resolve()?;

        let discovery = self.discover_opts.mechanism.clone();
        let (progress_reporter, event_sender) = ProgressReporter::new(
            log,
            image_id.update_id,
            ReportProgressBackend::new(
                log,
                HttpProgressBackend::new(log, discovery),
            ),
        );
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
                async move |cx| {
                    let host_phase_2_artifact =
                        fetch_artifact(&cx, &host_phase_2_id, discovery, log)
                            .await?;

                    // Check that the sha256 of the data we got from wicket
                    // matches the data we asked for. If this fails, we fail the
                    // entire installation rather than trying to fetch the
                    // artifact again, because we're fetching data from wicketd
                    // (in memory) over TCP to ourselves (in memory), so the
                    // only cases where this could fail are disturbing enough
                    // (memory corruption, corruption under TCP, or wicketd gave
                    // us something other than what we requested) we want to
                    // know immediately and not retry: it's likely an operator
                    // could miss any warnings we emit if a retry succeeds.
                    check_downloaded_artifact_hash(
                        "host phase 2",
                        host_phase_2_artifact.artifact.clone(),
                        host_phase_2_id.hash,
                    )
                    .await?;

                    let address = host_phase_2_artifact.peer.address();

                    StepSuccess::new(host_phase_2_artifact)
                        .with_metadata(
                            InstallinatorCompletionMetadata::Download {
                                address,
                            },
                        )
                        .into()
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
                async move |cx| {
                    let control_plane_artifact =
                        fetch_artifact(&cx, &control_plane_id, discovery, log)
                            .await?;

                    // Check that the sha256 of the data we got from wicket
                    // matches the data we asked for. We do not retry this for
                    // the same reasons described above when checking the
                    // downloaded host phase 2 artifact.
                    check_downloaded_artifact_hash(
                        "control plane",
                        control_plane_artifact.artifact.clone(),
                        control_plane_id.hash,
                    )
                    .await?;

                    let address = control_plane_artifact.peer.address();

                    StepSuccess::new(control_plane_artifact)
                        .with_metadata(
                            InstallinatorCompletionMetadata::Download {
                                address,
                            },
                        )
                        .into()
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
                    async move |cx| scan_hardware_with_retries(&cx, &log).await,
                )
                .register()
        } else {
            // clap ensures `self.a_destination` is not `None` if
            // `install_on_gimlet` is false.
            let a_destination = self.a_destination.as_ref().unwrap();
            StepHandle::ready(WriteDestination::in_directories(
                a_destination,
                self.b_destination.as_deref(),
            )?)
        };

        let control_plane_zones = engine
            .new_step(
                InstallinatorComponent::ControlPlane,
                InstallinatorStepId::UnpackControlPlaneArtifact,
                "Unpacking composite control plane artifact",
                async move |cx| {
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
                    StepSuccess::new(zones).with_metadata(InstallinatorCompletionMetadata::ControlPlaneZones {
                        zones_to_install,
                    }).into()
                },
            )
            .register();

        engine
            .new_step(
                InstallinatorComponent::Both,
                InstallinatorStepId::Write,
                "Writing host and control plane artifacts",
                async move |cx| {
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
                        StepSuccess::new(()).with_metadata(metadata).into()
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
                        StepWarning::new((), message)
                            .with_metadata(metadata)
                            .into()
                    }
                },
            )
            .register();

        // Wait for both the engine to complete and all progress reports to be
        // sent, then possibly return an error to our caller if either failed.
        // Use `join_then_try!` to ensure this.
        cancel_safe_futures::join_then_try!(
            async {
                engine
                    .execute()
                    .await
                    .context("failed to execute installinator")
            },
            async { progress_handle.await.context("progress reporter failed") },
        )?;

        if self.stay_alive {
            loop {
                slog::info!(log, "installation complete; sleeping");
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }

        Ok(())
    }
}

async fn check_downloaded_artifact_hash(
    name: &'static str,
    data: BufList,
    expected_hash: ArtifactHash,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut hasher = Sha256::new();
        for chunk in data.iter() {
            hasher.update(chunk);
        }
        let computed_hash = ArtifactHash(hasher.finalize().into());
        if expected_hash != computed_hash {
            bail!(
                "downloaded {name} checksum failure: \
                 expected {} but calculated {}",
                hex::encode(&expected_hash),
                hex::encode(&computed_hash)
            );
        }
        Ok(())
    })
    .await
    .unwrap()
}

async fn scan_hardware_with_retries(
    cx: &StepContext,
    log: &slog::Logger,
) -> Result<StepResult<WriteDestination, InstallinatorSpec>> {
    // Scanning for our disks is inherently racy: we have to wait for the disks
    // to attach. This should take milliseconds in general; we'll set a hard cap
    // at retrying for ~10 seconds. (In practice if we're failing, this will
    // take much longer than 10 seconds, because each failed attempt takes a
    // nontrivial amount of time.)
    const HARDWARE_RETRIES: usize = 20;
    const HARDWARE_RETRY_DELAY: Duration = Duration::from_millis(500);

    let mut retry = 0;
    let result = loop {
        let log = log.clone();
        let result = WriteDestination::from_hardware(&log).await;

        match result {
            Ok(destination) => break Ok(destination),
            Err(error) => {
                if retry < HARDWARE_RETRIES {
                    warn!(
                        log,
                        "hardware scan failed; will retry after {:?} \
                         (attempt {} of {})",
                        HARDWARE_RETRY_DELAY, retry + 1, HARDWARE_RETRIES;
                        "err" => #%error,
                    );
                    cx.send_progress(StepProgress::retry(format!(
                        "hardware scan {retry} failed: {error:#}"
                    )))
                    .await;
                    retry += 1;
                    tokio::time::sleep(HARDWARE_RETRY_DELAY).await;
                    continue;
                } else {
                    error!(
                        log, "hardware scan failed (retries exhausted)";
                        "err" => #%error,
                    );
                    break Err(error);
                }
            }
        }
    };

    let destination = result?;
    let disks_found = destination.num_target_disks();
    StepSuccess::new(destination)
        .with_metadata(InstallinatorCompletionMetadata::HardwareScan {
            disks_found,
        })
        .into()
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
            Ok(FetchArtifactBackend::new(
                &log,
                Box::new(HttpFetchBackend::new(
                    &log,
                    discovery.discover_peers(&log).await?,
                )),
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
        artifact.peer,
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
