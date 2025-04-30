// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ArtifactCache;
use anyhow::Context;
use dropshot::{FreeformBody, HttpError, HttpResponseOk, RequestContext};
use dropshot::{HttpServer, ServerBuilder};
use hubtools::{CabooseBuilder, HubrisArchiveBuilder};
use qorb::resolver::Resolver;
use qorb::resolvers::fixed::FixedResolver;
use repo_depot_api::ArtifactPathParams;
use sha2::Digest;
use sp_sim::SIM_GIMLET_BOARD;
use std::collections::BTreeMap;
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;

type ArtifactData = BTreeMap<ArtifactHash, Vec<u8>>;
type InMemoryRepoDepotServerContext = Arc<ArtifactData>;

pub struct TestArtifacts {
    pub caboose: hubtools::Caboose,
    pub artifact_cache: Arc<ArtifactCache>,
    resolver: FixedResolver,
    repo_depot_server: HttpServer<InMemoryRepoDepotServerContext>,
}

impl TestArtifacts {
    pub async fn new(log: &slog::Logger) -> anyhow::Result<TestArtifacts> {
        let caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_GIMLET_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();

        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(caboose.as_slice()).unwrap();
        let artifact = builder.build_to_vec().unwrap();
        let artifact_hash = {
            let mut digest = sha2::Sha256::default();
            digest.update(&artifact);
            ArtifactHash(digest.finalize().into())
        };
        let artifact_data =
            std::iter::once((artifact_hash, artifact)).collect();

        let repo_depot_server = {
            let log = log.new(slog::o!("component" => "RepoDepotServer"));
            let my_api = repo_depot_api::repo_depot_api_mod::api_description::<
                InMemoryRepoDepotServerImpl,
            >()
            .unwrap();

            ServerBuilder::new(my_api, Arc::new(artifact_data), log)
                .start()
                .context("failed to create server")?
        };

        let mut resolver =
            FixedResolver::new(std::iter::once(repo_depot_server.local_addr()));
        let artifact_cache = Arc::new(ArtifactCache::new(
            log.new(slog::o!("component" => "ArtifactCache")),
            resolver.monitor(),
        ));

        Ok(TestArtifacts {
            caboose,
            artifact_cache,
            resolver,
            repo_depot_server,
        })
    }

    pub async fn teardown(mut self) {
        self.resolver.terminate().await;
        let _ = self.repo_depot_server.close().await;
    }
}

struct InMemoryRepoDepotServerImpl;
impl repo_depot_api::RepoDepotApi for InMemoryRepoDepotServerImpl {
    type Context = InMemoryRepoDepotServerContext;

    async fn artifact_get_by_sha256(
        rqctx: RequestContext<Self::Context>,
        path_params: dropshot::Path<ArtifactPathParams>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
        let artifact_data = rqctx.context();
        let artifact_hash = path_params.into_inner().sha256;
        let artifact_contents =
            artifact_data.get(&artifact_hash).ok_or_else(|| {
                HttpError::for_not_found(
                    None,
                    String::from("no such artifact id"),
                )
            })?;
        let body = dropshot::Body::with_content(artifact_contents.clone());
        Ok(HttpResponseOk(FreeformBody::from(body)))
    }
}
