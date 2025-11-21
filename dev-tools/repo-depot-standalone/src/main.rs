// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Serve the Repo Depot API from one or more Omicron TUF repos

use anyhow::Context;
use anyhow::anyhow;
use buf_list::BufList;
use bytes::Buf;
use camino::Utf8PathBuf;
use clap::Parser;
use dropshot::FreeformBody;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use futures::StreamExt;
use futures::stream::TryStreamExt;
use libc::SIGINT;
use repo_depot_api::ArtifactPathParams;
use repo_depot_api::RepoDepotApi;
use signal_hook_tokio::Signals;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactHashId;
use update_common::artifacts::{
    ArtifactsWithPlan, ControlPlaneZonesMode, VerificationMode,
};

fn main() -> Result<(), anyhow::Error> {
    oxide_tokio_rt::run(async {
        let args = RepoDepotStandalone::parse();

        if let Err(error) = args.exec().await {
            eprintln!("error: {:#}", error);
            std::process::exit(1);
        }

        Ok(())
    })
}

/// Serve the Repo Depot API from one or more Omicron TUF repos
#[derive(Debug, Parser)]
struct RepoDepotStandalone {
    /// log level filter
    #[arg(
        env,
        long,
        value_parser = parse_dropshot_log_level,
        default_value = "info",
    )]
    log_level: dropshot::ConfigLoggingLevel,

    /// address on which to serve the API
    #[arg(long, default_value = "[::]:0")]
    listen_addr: SocketAddr,

    /// paths to Omicron TUF repositories (zip files)
    #[arg(required = true, num_args = 1..)]
    zip_files: Vec<Utf8PathBuf>,
}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}

impl RepoDepotStandalone {
    async fn exec(self) -> Result<(), anyhow::Error> {
        let log = dropshot::ConfigLogging::StderrTerminal {
            level: self.log_level.clone(),
        }
        .to_logger("repo-depot-standalone")
        .context("failed to create logger")?;

        // Gracefully handle SIGINT so that we clean up the files that got
        // extracted to a temporary directory.
        let signals =
            Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
        let mut signal_stream = signals.fuse();

        let mut ctx = RepoMetadata::new();
        for repo_path in &self.zip_files {
            let file = std::fs::File::open(repo_path)
                .with_context(|| format!("open {:?}", repo_path))?;
            let buf = std::io::BufReader::new(file);
            info!(
                &log,
                "extracting Omicron TUF repository";
                "path" => %repo_path
            );
            let plan = ArtifactsWithPlan::from_zip(
                buf,
                None,
                ArtifactHash([0; 32]),
                ControlPlaneZonesMode::Split,
                VerificationMode::BlindlyTrustAnything,
                &log,
            )
            .await
            .with_context(|| format!("load {:?}", repo_path))?;
            ctx.load_repo(plan)
                .context("loading artifacts from repository at {repo_path}")?;
            info!(&log, "loaded Omicron TUF repository"; "path" => %repo_path);
        }

        let my_api = repo_depot_api::repo_depot_api_mod::api_description::<
            StandaloneApiImpl,
        >()
        .unwrap();

        let server = ServerBuilder::new(my_api, Arc::new(ctx), log.clone())
            .config(dropshot::ConfigDropshot {
                bind_address: self.listen_addr,
                ..Default::default()
            })
            .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
                dropshot::ClientSpecifiesVersionInHeader::new(
                    omicron_common::api::VERSION_HEADER,
                    repo_depot_api::latest_version(),
                ),
            )))
            .start()
            .context("failed to create server")?;

        // Wait for a signal.
        let caught_signal = signal_stream.next().await;
        assert_eq!(caught_signal.unwrap(), SIGINT);
        info!(
            &log,
            "caught signal, shutting down and removing \
            temporary directories"
        );

        // The temporary files are deleted by `Drop` handlers so all we need to
        // do is shut down gracefully.
        server
            .close()
            .await
            .map_err(|e| anyhow!("error closing HTTP server: {e}"))
    }
}

/// Keeps metadata that allows us to fetch a target from any of the TUF repos
/// based on its hash.
struct RepoMetadata {
    repos: Vec<ArtifactsWithPlan>,
    targets_by_hash: BTreeMap<ArtifactHash, (usize, ArtifactHashId)>,
}

impl RepoMetadata {
    pub fn new() -> RepoMetadata {
        RepoMetadata { repos: Vec::new(), targets_by_hash: BTreeMap::new() }
    }

    pub fn load_repo(&mut self, plan: ArtifactsWithPlan) -> anyhow::Result<()> {
        let repo_index = self.repos.len();

        for artifact_meta in &plan.description().artifacts {
            let artifact_hash = artifact_meta.hash;
            let artifact_id = &artifact_meta.id;
            let artifact_hash_id = ArtifactHashId {
                kind: artifact_id.kind.clone(),
                hash: artifact_hash,
            };

            // Some hashes appear multiple times, whether in the same repo or
            // different repos.  That's fine.  They all have the same contents
            // so we can serve any of them when this hash is requested.
            self.targets_by_hash
                .insert(artifact_meta.hash, (repo_index, artifact_hash_id));
        }

        self.repos.push(plan);
        Ok(())
    }

    pub async fn data_for_hash(
        &self,
        requested_sha: &ArtifactHash,
    ) -> Option<anyhow::Result<ReaderStream<impl AsyncRead + use<>>>> {
        let (repo_index, artifact_hash_id) =
            self.targets_by_hash.get(requested_sha)?;
        let repo = &self.repos[*repo_index];
        Some(
            repo.get_by_hash(artifact_hash_id)
                .unwrap_or_else(|| {
                    panic!(
                        "artifact hash unexpectedly missing from the repo that \
                         we recorded having found it in"
                    )
                })
                .reader_stream()
                .await,
        )
    }
}

struct StandaloneApiImpl;

impl RepoDepotApi for StandaloneApiImpl {
    type Context = Arc<RepoMetadata>;

    async fn artifact_get_by_sha256(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParams>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
        let repo_metadata = rqctx.context();
        let requested_sha = &path_params.into_inner().sha256;
        let reader = repo_metadata
            .data_for_hash(requested_sha)
            .await
            .ok_or_else(|| {
                HttpError::for_not_found(
                    None,
                    String::from("found no target with this hash"),
                )
            })?
            .map_err(|error| {
                HttpError::for_internal_error(format!(
                    "loading file from TUF repo: {}",
                    InlineErrorChain::new(&*error),
                ))
            })?;
        let mut buf_list =
            reader.try_collect::<BufList>().await.map_err(|error| {
                HttpError::for_internal_error(format!(
                    "reading target from TUF repo: {}",
                    InlineErrorChain::new(&error),
                ))
            })?;
        let body = dropshot::Body::with_content(
            buf_list.copy_to_bytes(buf_list.num_bytes()),
        );
        Ok(HttpResponseOk(FreeformBody::from(body)))
    }
}
