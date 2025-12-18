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
use sp_sim::{SIM_GIMLET_BOARD, SIM_SIDECAR_BOARD};
use std::collections::BTreeMap;
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;

type ArtifactData = BTreeMap<ArtifactHash, Vec<u8>>;
type InMemoryRepoDepotServerContext = Arc<ArtifactData>;

/// Facilities for working with the artifacts needed when running SP update
/// tests
///
/// `TestArtifacts` does a few things:
///
/// - it creates some specific useful test artifacts (SP, RoT, RoT bootloader,
///   and host OS images)
/// - it provides the hashes and cabooses used for these images
/// - it serves these images via an in-memory Repo Depot server
///
/// Together, this makes it easy to write SP update tests that use these
/// artifacts.
pub struct TestArtifacts {
    pub sp_gimlet_artifact_hash: ArtifactHash,
    pub sp_sidecar_artifact_hash: ArtifactHash,
    pub rot_gimlet_artifact_hash: ArtifactHash,
    pub rot_sidecar_artifact_hash: ArtifactHash,
    pub rot_bootloader_gimlet_artifact_hash: ArtifactHash,
    pub rot_bootloader_sidecar_artifact_hash: ArtifactHash,
    pub host_phase_1_artifact_hash: ArtifactHash,
    pub artifact_cache: Arc<ArtifactCache>,
    deployed_cabooses: BTreeMap<ArtifactHash, hubtools::Caboose>,
    resolver: FixedResolver,
    repo_depot_server: HttpServer<InMemoryRepoDepotServerContext>,
}

impl TestArtifacts {
    pub async fn new(log: &slog::Logger) -> anyhow::Result<TestArtifacts> {
        // Make an SP update artifact for SimGimlet.
        let sp_gimlet_artifact_caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_GIMLET_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();
        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(sp_gimlet_artifact_caboose.as_slice()).unwrap();
        let sp_gimlet_artifact = builder.build_to_vec().unwrap();
        let sp_gimlet_artifact_hash =
            ArtifactHash(sha2::Sha256::digest(&sp_gimlet_artifact).into());

        // Make an SP update artifact for SimSidecar
        let sp_sidecar_artifact_caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_SIDECAR_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();
        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(sp_sidecar_artifact_caboose.as_slice()).unwrap();
        let sp_sidecar_artifact = builder.build_to_vec().unwrap();
        let sp_sidecar_artifact_hash =
            ArtifactHash(sha2::Sha256::digest(&sp_sidecar_artifact).into());

        // Make an RoT update artifact for SimGimlet.
        let rot_gimlet_artifact_caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_GIMLET_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();
        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(rot_gimlet_artifact_caboose.as_slice()).unwrap();
        let rot_gimlet_artifact = builder.build_to_vec().unwrap();
        let rot_gimlet_artifact_hash =
            ArtifactHash(sha2::Sha256::digest(&rot_gimlet_artifact).into());

        // Make an RoT update artifact for SimSidecar
        let rot_sidecar_artifact_caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_SIDECAR_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();
        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(rot_sidecar_artifact_caboose.as_slice()).unwrap();
        let rot_sidecar_artifact = builder.build_to_vec().unwrap();
        let rot_sidecar_artifact_hash =
            ArtifactHash(sha2::Sha256::digest(&rot_sidecar_artifact).into());

        // Make an RoT bootloader update artifact for SimGimlet.
        let rot_bootloader_gimlet_artifact_caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_GIMLET_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();
        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder
            .write_caboose(rot_bootloader_gimlet_artifact_caboose.as_slice())
            .unwrap();
        let rot_bootloader_gimlet_artifact = builder.build_to_vec().unwrap();
        let rot_bootloader_gimlet_artifact_hash = ArtifactHash(
            sha2::Sha256::digest(&rot_bootloader_gimlet_artifact).into(),
        );

        // Make an RoT bootloader update artifact for SimSidecar
        let rot_bootloader_sidecar_artifact_caboose = CabooseBuilder::default()
            .git_commit("fake-git-commit")
            .board(SIM_SIDECAR_BOARD)
            .version("0.0.0")
            .name("fake-name")
            .build();
        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder
            .write_caboose(rot_bootloader_sidecar_artifact_caboose.as_slice())
            .unwrap();
        let rot_bootloader_sidecar_artifact = builder.build_to_vec().unwrap();
        let rot_bootloader_sidecar_artifact_hash = ArtifactHash(
            sha2::Sha256::digest(&rot_bootloader_sidecar_artifact).into(),
        );

        // Make a fake host OS phase 1 image. This is not a hubris archive and
        // does not have a caboose, and in practice is entirely opaque (so we
        // just use completely bogus data here).
        let mut host_phase_1_artifact =
            b"nexus-mgs-updates test phase 1".to_vec();

        // Pad the phase 1 artifact so it's not so small that it uploads in a
        // single UDP packet to the sp simulator. Real images are 32 MiB, and
        // "fits in one packet" interferes with our tests that try to step
        // through the update process and pause at various points, because we
        // skip from "upload started" to "upload done" without seeing "upload in
        // progress".
        host_phase_1_artifact.resize(2048, b'.');
        let host_phase_1_artifact_hash =
            ArtifactHash(sha2::Sha256::digest(&host_phase_1_artifact).into());

        // Assemble a map of artifact hash to artifact contents.
        let artifact_data = [
            (sp_gimlet_artifact_hash, sp_gimlet_artifact),
            (sp_sidecar_artifact_hash, sp_sidecar_artifact),
            (rot_gimlet_artifact_hash, rot_gimlet_artifact),
            (rot_sidecar_artifact_hash, rot_sidecar_artifact),
            (
                rot_bootloader_gimlet_artifact_hash,
                rot_bootloader_gimlet_artifact,
            ),
            (
                rot_bootloader_sidecar_artifact_hash,
                rot_bootloader_sidecar_artifact,
            ),
            (host_phase_1_artifact_hash, host_phase_1_artifact),
        ]
        .into_iter()
        .collect();

        // Assemble a map of artifact hash to generated caboose.
        let deployed_cabooses = [
            (sp_gimlet_artifact_hash, sp_gimlet_artifact_caboose),
            (sp_sidecar_artifact_hash, sp_sidecar_artifact_caboose),
            (rot_gimlet_artifact_hash, rot_gimlet_artifact_caboose),
            (rot_sidecar_artifact_hash, rot_sidecar_artifact_caboose),
            (
                rot_bootloader_gimlet_artifact_hash,
                rot_bootloader_gimlet_artifact_caboose,
            ),
            (
                rot_bootloader_sidecar_artifact_hash,
                rot_bootloader_sidecar_artifact_caboose,
            ),
        ]
        .into_iter()
        .collect();

        // Start a Repo Depot server that will serve these artifacts.
        let repo_depot_server = {
            let log = log.new(slog::o!("component" => "RepoDepotServer"));
            let my_api = repo_depot_api::repo_depot_api_mod::api_description::<
                InMemoryRepoDepotServerImpl,
            >()
            .unwrap();

            ServerBuilder::new(my_api, Arc::new(artifact_data), log)
                .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
                    dropshot::ClientSpecifiesVersionInHeader::new(
                        omicron_common::api::VERSION_HEADER,
                        repo_depot_api::latest_version(),
                    ),
                )))
                .start()
                .context("failed to create server")?
        };

        // Create an ArtifactCache pointed at our Repo Depot server.
        // This can be used directly by the caller for doing SP updates.
        let mut resolver =
            FixedResolver::new(std::iter::once(repo_depot_server.local_addr()));
        let artifact_cache = Arc::new(ArtifactCache::new(
            log.new(slog::o!("component" => "ArtifactCache")),
            resolver.monitor(),
        ));

        Ok(TestArtifacts {
            sp_gimlet_artifact_hash,
            sp_sidecar_artifact_hash,
            rot_gimlet_artifact_hash,
            rot_sidecar_artifact_hash,
            rot_bootloader_gimlet_artifact_hash,
            rot_bootloader_sidecar_artifact_hash,
            host_phase_1_artifact_hash,
            artifact_cache,
            deployed_cabooses,
            resolver,
            repo_depot_server,
        })
    }

    /// Return the caboose that was used to generate the given artifact
    pub fn deployed_caboose(
        &self,
        hash: &ArtifactHash,
    ) -> Option<&hubtools::Caboose> {
        self.deployed_cabooses.get(hash)
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
