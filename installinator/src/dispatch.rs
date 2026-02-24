// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
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
    ArtifactHash, InstallinatorArtifactId, InstallinatorArtifactKind,
    InstallinatorArtifactKindId, InstallinatorDocument,
};
use update_engine::StepResult;

use crate::{
    ArtifactWriter, MeasurementToWrite, WriteDestination, ZoneToWrite,
    artifact::ArtifactIdOpts,
    fetch::{FetchArtifactBackend, FetchedArtifact, HttpFetchBackend},
    peers::{DiscoveryMechanism, LastKnownPeer},
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
    Install(Box<InstallOpts>),
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
            None,
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

        let lookup_id = self.artifact_ids.resolve()?;

        let discovery = self.discover_opts.mechanism.clone();
        let (progress_reporter, event_sender) = ProgressReporter::new(
            log,
            lookup_id.update_id,
            ReportProgressBackend::new(
                log,
                HttpProgressBackend::new(log, discovery),
            ),
        );
        let progress_handle = progress_reporter.start();
        let discovery = &self.discover_opts.mechanism;
        let last_known_peer = LastKnownPeer::new();
        let last_known_peer = &last_known_peer;

        let engine = UpdateEngine::new(log, event_sender);

        let artifacts = engine
            .new_step(
                InstallinatorComponent::InstallinatorDocument,
                InstallinatorStepId::Download,
                "Downloading installinator document",
                async move |cx| {
                    let installinator_doc_id = InstallinatorArtifactId {
                        kind:
                            InstallinatorArtifactKindId::INSTALLINATOR_DOCUMENT,
                        hash: lookup_id.document,
                    };
                    let installinator_doc = fetch_artifact(
                        &cx,
                        installinator_doc_id,
                        discovery,
                        &last_known_peer,
                        log,
                    )
                    .await?;

                    // Read the document as JSON.
                    //
                    // Going via the reader is slightly less efficient
                    // than operating purely in-memory, but serde_json
                    // doesn't have a good way to pass in a BufList
                    // directly.
                    let json: InstallinatorDocument = serde_json::from_reader(
                        buf_list::Cursor::new(&installinator_doc.artifact),
                    )
                    .context("error deserializing installinator document")?;

                    // We expect there is exactly one host phase 2 image.
                    let host_phase_2_count = json
                        .artifacts
                        .iter()
                        .filter(|artifact| {
                            matches!(
                                artifact.kind,
                                InstallinatorArtifactKind::HostPhase2
                            )
                        })
                        .count();
                    ensure!(
                        host_phase_2_count == 1,
                        "expected a single host phase 2 \
                        image but found {host_phase_2_count}"
                    );

                    StepSuccess::new(json.artifacts).into()
                },
            )
            .register();

        let downloads = engine
            .new_step(
                InstallinatorComponent::All,
                InstallinatorStepId::Download,
                "Downloading artifacts",
                async move |cx| {
                    let artifacts = artifacts.into_value(cx.token()).await;
                    let mut downloads = Vec::new();
                    cx.with_nested_engine(|engine| {
                        // 80-col lines vs rustfmt, name a more iconic duo
                        use InstallinatorCompletionMetadata as Metadata;

                        for artifact in artifacts {
                            let component = match &artifact.kind {
                                InstallinatorArtifactKind::MeasurementCorpus => {
                                    InstallinatorComponent::MeasurementCorpus
                                }
                                InstallinatorArtifactKind::HostPhase2 => {
                                    InstallinatorComponent::HostPhase2
                                }
                                InstallinatorArtifactKind::Zone { .. } => {
                                    InstallinatorComponent::ControlPlaneZone
                                }
                            };
                            let id = artifact.downgrade();
                            let step = engine
                                .new_step(
                                    component,
                                    InstallinatorStepId::Download,
                                    format!(
                                        "Downloading {}",
                                        artifact.file_name
                                    ),
                                    async move |cx2| {
                                        let fetched = fetch_artifact(
                                            &cx2,
                                            id,
                                            discovery,
                                            last_known_peer,
                                            log,
                                        )
                                        .await?;
                                        let metadata = Metadata::Download {
                                            address: fetched.peer.address(),
                                        };
                                        StepSuccess::new(fetched.artifact)
                                            .with_metadata(metadata)
                                            .into()
                                    },
                                )
                                .register();
                            downloads.push((artifact, step));
                        }
                        Ok(())
                    })
                    .await?;
                    StepSuccess::new(downloads).into()
                },
            )
            .register();

        let destination = if self.install_on_gimlet {
            let log = log.clone();
            engine
                .new_step(
                    InstallinatorComponent::All,
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

        engine
            .new_step(
                InstallinatorComponent::All,
                InstallinatorStepId::Write,
                "Writing host, control plane, and measurement artifacts",
                async move |cx| {
                    let destination = destination.into_value(cx.token()).await;

                    let mut host_phase_2 = None;
                    let mut zones = Vec::new();
                    let mut measurement_corpus = Vec::new();
                    for (artifact, step) in
                        downloads.into_value(cx.token()).await
                    {
                        let data = step.into_value(cx.token()).await;
                        match &artifact.kind {
                            InstallinatorArtifactKind::MeasurementCorpus => {
                                measurement_corpus.push(MeasurementToWrite {
                                    name: artifact.file_name,
                                    artifact: data,
                                });
                            }
                            InstallinatorArtifactKind::HostPhase2 => {
                                host_phase_2 =
                                    Some((artifact.downgrade(), data));
                            }
                            InstallinatorArtifactKind::Zone { .. } => {
                                zones.push(ZoneToWrite {
                                    id: artifact.downgrade(),
                                    file_name: artifact.file_name,
                                    data,
                                });
                            }
                        }
                    }
                    // We checked there is at least one host phase 2 artifact
                    // when we read the installinator document.
                    let (host_phase_2_id, host_phase_2_data) = host_phase_2
                        .expect(
                            "already checked presence of host phase 2 artifact",
                        );

                    let mut writer = ArtifactWriter::new(
                        lookup_id.update_id,
                        &host_phase_2_id,
                        &host_phase_2_data,
                        &zones,
                        &measurement_corpus,
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
    id: InstallinatorArtifactId,
    discovery: &DiscoveryMechanism,
    last_known_peer: &LastKnownPeer,
    log: &slog::Logger,
) -> Result<FetchedArtifact> {
    // TODO: Not sure why slog::o!("artifact" => ?id) isn't working, figure it
    // out at some point.
    let log = log.new(slog::o!("artifact" => format!("{id:?}")));
    let fetched = FetchedArtifact::loop_fetch_from_peers(
        cx,
        &log,
        || async {
            let preferred = last_known_peer.get();
            Ok(FetchArtifactBackend::new(
                &log,
                Box::new(HttpFetchBackend::new(
                    &log,
                    discovery.discover_peers(&log).await?,
                )),
                Duration::from_secs(10),
                preferred,
            ))
        },
        &id,
    )
    .await
    .with_context(|| format!("error fetching artifact with id {id:?}"))?;

    // Check that the sha256 of the data we got from wicket matches the data we
    // asked for.
    //
    // If this fails, we fail the entire installation rather than trying to
    // fetch the artifact again, because we're fetching data from wicketd
    // (cached in a temp dir) over TCP to ourselves (in memory), so the only
    // cases where this could fail are disturbing enough (memory corruption,
    // corruption under TCP, or wicketd gave us something other than what we
    // requested) we want to know immediately and not retry: it's likely an
    // operator could miss any warnings we emit if a retry succeeds.
    let fetched = tokio::task::spawn_blocking(move || {
        let mut hasher = Sha256::new();
        for chunk in &fetched.artifact {
            hasher.update(&chunk);
        }
        let computed_hash = ArtifactHash(hasher.finalize().into());
        if id.hash != computed_hash {
            bail!(
                "downloaded {:?} checksum failure: \
                 expected {} but calculated {}",
                id.kind,
                hex::encode(&id.hash),
                hex::encode(&computed_hash)
            );
        }
        Ok(fetched)
    })
    .await
    .unwrap()?;

    last_known_peer.set(fetched.peer);

    slog::info!(
        log,
        "fetched {} bytes from {}",
        fetched.artifact.num_bytes(),
        fetched.peer,
    );

    Ok(fetched)
}

pub(crate) fn stderr_env_drain(
    env_var: &str,
) -> impl Drain<Ok = (), Err = slog::Never> + use<> {
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
