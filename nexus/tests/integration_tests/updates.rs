// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO(iliana):
// - refactor `test_update_end_to_end` into a test setup function
// - test that an unknown artifact returns 404, not 500
// - tests around target names and artifact names that contain dangerous paths like `../`

use async_trait::async_trait;
use camino_tempfile::Utf8TempDir;
use chrono::{Duration, Utc};
use dropshot::test_util::LogContext;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpServerStarter, Path,
    RequestContext,
};
use http::{Method, Response, StatusCode};
use hyper::Body;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::{load_test_config, test_setup, test_setup_with_config};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use omicron_common::nexus_config::UpdatesConfig;
use omicron_common::update::{Artifact, ArtifactKind, ArtifactsDocument};
use omicron_sled_agent::sim;
use ring::pkcs8::Document;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::Ed25519KeyPair;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::fs::File;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::PathBuf;
use tempfile::{NamedTempFile, TempDir};
use tough::editor::signed::{PathExists, SignedRole};
use tough::editor::RepositoryEditor;
use tough::key_source::KeySource;
use tough::schema::{KeyHolder, RoleKeys, RoleType, Root};
use tough::sign::Sign;

const UPDATE_COMPONENT: &'static str = "omicron-test-component";

#[tokio::test]
async fn test_update_end_to_end() {
    let mut config = load_test_config();
    let logctx = LogContext::new("test_update_end_to_end", &config.pkg.log);

    // build the TUF repo
    let rng = SystemRandom::new();
    let tuf_repo = new_tuf_repo(&rng).await;
    slog::info!(logctx.log, "TUF repo created at {}", tuf_repo.path());

    // serve it over HTTP
    let dropshot_config = Default::default();
    let mut api = ApiDescription::new();
    api.register(static_content).unwrap();
    let context = FileServerContext { base: tuf_repo.path().to_owned().into() };
    let server =
        HttpServerStarter::new(&dropshot_config, api, context, &logctx.log)
            .unwrap()
            .start();
    let local_addr = server.local_addr();

    // stand up the test environment
    config.pkg.updates = Some(UpdatesConfig {
        trusted_root: tuf_repo.path().join("metadata").join("1.root.json"),
        default_base_url: format!("http://{}/", local_addr),
    });
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_update_end_to_end",
        &mut config,
        sim::SimMode::Explicit,
        None,
    )
    .await;
    let client = &cptestctx.external_client;

    // call /v1/system/update/refresh on nexus
    // - download and verify the repo
    // - return 204 Non Content
    // - tells sled agent to do the thing
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/v1/system/update/refresh")
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    let artifact_path = cptestctx.sled_agent_storage.path();
    let component_path = artifact_path.join(UPDATE_COMPONENT);
    // check sled agent did the thing
    assert_eq!(tokio::fs::read(component_path).await.unwrap(), TARGET_CONTENTS);

    server.close().await.expect("failed to shut down dropshot server");
    cptestctx.teardown().await;
    logctx.cleanup_successful();
}

// =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=

struct FileServerContext {
    base: PathBuf,
}

#[derive(Deserialize, JsonSchema)]
struct AllPath {
    path: Vec<String>,
}

#[endpoint(method = GET, path = "/{path:.*}", unpublished = true)]
async fn static_content(
    rqctx: RequestContext<FileServerContext>,
    path: Path<AllPath>,
) -> Result<Response<Body>, HttpError> {
    // NOTE: this is a particularly brief and bad implementation of this to keep the test shorter.
    // see https://github.com/oxidecomputer/dropshot/blob/main/dropshot/examples/file_server.rs for
    // something more robust!
    let mut fs_path = rqctx.context().base.clone();
    for component in path.into_inner().path {
        fs_path.push(component);
    }
    let body = tokio::fs::read(fs_path).await.map_err(|e| {
        // tough 0.15+ depend on ENOENT being translated into 404.
        if e.kind() == std::io::ErrorKind::NotFound {
            HttpError::for_not_found(None, e.to_string())
        } else {
            HttpError::for_bad_request(None, e.to_string())
        }
    })?;
    Ok(Response::builder().status(StatusCode::OK).body(body.into())?)
}

// =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=

const TARGET_CONTENTS: &[u8] = b"hello world".as_slice();

async fn new_tuf_repo(rng: &(dyn SecureRandom + Sync)) -> Utf8TempDir {
    let version =
        NonZeroU64::new(Utc::now().timestamp().try_into().unwrap()).unwrap();
    let expires = Utc::now() + Duration::minutes(5);

    // create the key
    let key_data = Ed25519KeyPair::generate_pkcs8(rng).unwrap();
    let key = Ed25519KeyPair::from_pkcs8(key_data.as_ref()).unwrap();
    let tuf_key = key.tuf_key();
    let key_id = tuf_key.key_id().unwrap();

    // create the root role
    let mut root = Root {
        spec_version: "1.0.0".to_string(),
        consistent_snapshot: true,
        version: NonZeroU64::new(1).unwrap(),
        expires,
        keys: HashMap::new(),
        roles: HashMap::new(),
        _extra: HashMap::new(),
    };
    root.keys.insert(key_id.clone(), tuf_key);
    for role in [
        RoleType::Root,
        RoleType::Snapshot,
        RoleType::Targets,
        RoleType::Timestamp,
    ] {
        root.roles.insert(
            role,
            RoleKeys {
                keyids: vec![key_id.clone()],
                threshold: NonZeroU64::new(1).unwrap(),
                _extra: HashMap::new(),
            },
        );
    }

    let signing_keys =
        vec![Box::new(KeyKeySource(key_data)) as Box<dyn KeySource + 'static>];

    // self-sign the root role
    let signed_root = SignedRole::new(
        root.clone(),
        &KeyHolder::Root(root),
        &signing_keys,
        rng,
    )
    .await
    .unwrap();

    // TODO(iliana): there's no way to create a `RepositoryEditor` without having the root.json on
    // disk. this is really unergonomic. write and upstream a fix
    let mut root_tmp = NamedTempFile::new().unwrap();
    root_tmp.as_file_mut().write_all(signed_root.buffer()).unwrap();
    let mut editor = RepositoryEditor::new(&root_tmp).await.unwrap();
    root_tmp.close().unwrap();

    editor
        .targets_version(version)
        .unwrap()
        .targets_expires(expires)
        .unwrap()
        .snapshot_version(version)
        .snapshot_expires(expires)
        .timestamp_version(version)
        .timestamp_expires(expires);
    let (targets_dir, target_names) = generate_targets();
    for target in target_names {
        editor.add_target_path(targets_dir.path().join(target)).await.unwrap();
    }

    let signed_repo = editor.sign(&signing_keys).await.unwrap();

    let repo = Utf8TempDir::new().unwrap();
    signed_repo.write(repo.path().join("metadata")).await.unwrap();
    signed_repo
        .copy_targets(
            targets_dir,
            repo.path().join("targets"),
            PathExists::Fail,
        )
        .await
        .unwrap();

    repo
}

// Returns a temporary directory of targets and the list of filenames in it.
fn generate_targets() -> (TempDir, Vec<&'static str>) {
    let dir = TempDir::new().unwrap();

    // The update artifact. This will someday be a tarball of some variety.
    std::fs::write(
        dir.path().join(format!("{UPDATE_COMPONENT}-1")),
        TARGET_CONTENTS,
    )
    .unwrap();

    // artifacts.json, which describes all available artifacts.
    let artifacts = ArtifactsDocument {
        system_version: "1.0.0".parse().unwrap(),
        artifacts: vec![Artifact {
            name: UPDATE_COMPONENT.into(),
            version: "0.0.0".parse().unwrap(),
            kind: ArtifactKind::from_known(KnownArtifactKind::ControlPlane),
            target: format!("{UPDATE_COMPONENT}-1"),
        }],
    };
    let f = File::create(dir.path().join("artifacts.json")).unwrap();
    serde_json::to_writer_pretty(f, &artifacts).unwrap();

    (dir, vec!["omicron-test-component-1", "artifacts.json"])
}

// =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=

// Wrapper struct so that we can use an in-memory key as a key source.
// TODO(iliana): this should just be in tough with a lot less hacks
struct KeyKeySource(Document);

impl Debug for KeyKeySource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("KeyKeySource").finish()
    }
}

#[async_trait]
impl KeySource for KeyKeySource {
    async fn as_sign(
        &self,
    ) -> Result<Box<dyn Sign>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        // this is a really ugly hack, because tough doesn't `impl Sign for &'a T where T: Sign`.
        // awslabs/tough#446
        Ok(Box::new(Ed25519KeyPair::from_pkcs8(self.0.as_ref()).unwrap()))
    }

    async fn write(
        &self,
        _value: &str,
        _key_id_hex: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        unimplemented!();
    }
}

// =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=

// Tests that ".." paths are disallowed by dropshot.
#[tokio::test]
async fn test_download_with_dots_fails() {
    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_download_with_dots_fails")
            .await;
    let client = &cptestctx.internal_client;

    let filename = "hey/can/you/look/../../../../up/the/directory/tree";
    let artifact_get_url = format!("/artifacts/{}", filename);

    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::GET,
        &artifact_get_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    cptestctx.teardown().await;
}
