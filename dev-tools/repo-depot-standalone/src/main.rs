// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Serve the Repo Depot API from an extracted TUF repo

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
use std::net::SocketAddr;
use std::sync::Arc;
use tufaceous_lib::OmicronRepo;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = RepoDepotStandalone::parse();

    if let Err(error) = args.exec().await {
        eprintln!("error: {:#}", error);
        std::process::exit(1);
    }

    Ok(())
}

/// Serve the Repo Depot API from an extracted TUF repo
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

    /// path to local extracted Omicron TUF repository
    repo_path: Utf8PathBuf,
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

        let repo_path = &self.repo_path;
        let repo =
            OmicronRepo::load_untrusted_ignore_expiration(&log, repo_path)
                .await
                .with_context(|| {
                    format!("loading repository at {repo_path}")
                })?;
        info!(&log, "loaded Omicron TUF repository"; "path" => %repo_path);

        let my_api = repo_depot_api::repo_depot_api_mod::api_description::<
            StandaloneApiImpl,
        >()
        .unwrap();

        let server = ServerBuilder::new(my_api, Arc::new(repo), log)
            .config(dropshot::ConfigDropshot {
                bind_address: self.listen_addr,
                ..Default::default()
            })
            .start()
            .context("failed to create server")?;

        server.await.map_err(|error| anyhow!("server shut down: {error}"))
    }
}

struct StandaloneApiImpl;

impl RepoDepotApi for StandaloneApiImpl {
    type Context = Arc<OmicronRepo>;

    async fn artifact_get_by_sha256(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParams>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
        let omicron_repo = rqctx.context();
        let tuf_repo = omicron_repo.repo();
        let fetch_sha = path_params.into_inner().sha256;
        let target_name = tuf_repo
            .targets()
            .signed
            .targets
            .iter()
            .find(|(_, target)| {
                let fetch_array: &[u8] = fetch_sha.as_ref();
                let target_array: &[u8] = &target.hashes.sha256;
                fetch_array == target_array
            })
            .ok_or_else(|| {
                HttpError::for_not_found(
                    None,
                    String::from("found no target with this hash"),
                )
            })?
            .0;
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
