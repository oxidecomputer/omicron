// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Serve the Repo Depot API from one or more extracted TUF repos

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
use futures::stream::TryStreamExt;
use repo_depot_api::ArtifactPathParams;
use repo_depot_api::RepoDepotApi;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tough::Repository;
use tough::TargetName;
use tufaceous_artifact::ArtifactHash;
use tufaceous_lib::OmicronRepo;

fn main() -> Result<(), anyhow::Error> {
    omicron_runtime::run(async {
        let args = RepoDepotStandalone::parse();

        if let Err(error) = args.exec().await {
            eprintln!("error: {:#}", error);
            std::process::exit(1);
        }

        Ok(())
    })
}

/// Serve the Repo Depot API from one or more extracted TUF repos
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

    /// paths to local extracted Omicron TUF repositories
    #[arg(required = true, num_args = 1..)]
    repo_paths: Vec<Utf8PathBuf>,
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

        let mut ctx = RepoMetadata::new();
        for repo_path in &self.repo_paths {
            let omicron_repo =
                OmicronRepo::load_untrusted_ignore_expiration(&log, repo_path)
                    .await
                    .with_context(|| {
                        format!("loading repository at {repo_path}")
                    })?;
            ctx.load_repo(omicron_repo)
                .context("loading artifacts from repository at {repo_path}")?;
            info!(&log, "loaded Omicron TUF repository"; "path" => %repo_path);
        }

        let my_api = repo_depot_api::repo_depot_api_mod::api_description::<
            StandaloneApiImpl,
        >()
        .unwrap();

        let server = ServerBuilder::new(my_api, Arc::new(ctx), log)
            .config(dropshot::ConfigDropshot {
                bind_address: self.listen_addr,
                ..Default::default()
            })
            .start()
            .context("failed to create server")?;

        server.await.map_err(|error| anyhow!("server shut down: {error}"))
    }
}

/// Keeps metadata that allows us to fetch a target from any of the TUF repos
/// based on its hash.
struct RepoMetadata {
    repos: Vec<OmicronRepo>,
    targets_by_hash: BTreeMap<ArtifactHash, (usize, TargetName)>,
}

impl RepoMetadata {
    pub fn new() -> RepoMetadata {
        RepoMetadata { repos: Vec::new(), targets_by_hash: BTreeMap::new() }
    }

    pub fn load_repo(
        &mut self,
        omicron_repo: OmicronRepo,
    ) -> anyhow::Result<()> {
        let repo_index = self.repos.len();

        let tuf_repo = omicron_repo.repo();
        for (target_name, target) in &tuf_repo.targets().signed.targets {
            let target_hash: &[u8] = &target.hashes.sha256;
            let target_hash_array: [u8; 32] = target_hash
                .try_into()
                .context("sha256 hash wasn't 32 bytes")?;
            let artifact_hash = ArtifactHash(target_hash_array);
            self.targets_by_hash
                .insert(artifact_hash, (repo_index, target_name.clone()));
        }

        self.repos.push(omicron_repo);
        Ok(())
    }

    pub fn repo_and_target_name_for_hash(
        &self,
        requested_sha: &ArtifactHash,
    ) -> Option<(&Repository, &TargetName)> {
        let (repo_index, target_name) =
            self.targets_by_hash.get(requested_sha)?;
        let omicron_repo = &self.repos[*repo_index];
        Some((omicron_repo.repo(), target_name))
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
        let (tuf_repo, target_name) = repo_metadata
            .repo_and_target_name_for_hash(requested_sha)
            .ok_or_else(|| {
                HttpError::for_not_found(
                    None,
                    String::from("found no target with this hash"),
                )
            })?;

        let reader = tuf_repo
            .read_target(&target_name)
            .await
            .map_err(|error| {
                HttpError::for_internal_error(format!(
                    "failed to read target from TUF repo: {}",
                    InlineErrorChain::new(&error),
                ))
            })?
            .ok_or_else(|| {
                // We already checked above that the hash is present in the TUF
                // repo so this should not be a 404.
                HttpError::for_internal_error(String::from(
                    "missing target from TUF repo",
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
