// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::tuf_v2::SharedEditor;
use anyhow::Context as _;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use camino::Utf8PathBuf;
use camino_tempfile::NamedUtf8TempFile;
use camino_tempfile::Utf8TempPath;
use fs_err::tokio as fs;
use fs_err::tokio::File;
use futures::StreamExt;
use sha2::Digest as _;
use sha2::Sha256;
use sigstore_verify::IdentityMatcher;
use sigstore_verify::VerificationPolicy;
use sigstore_verify::VerificationResult;
use sigstore_verify::Verifier;
use sigstore_verify::trust_root::TrustedRoot;
use sigstore_verify::trust_root::TufConfig;
use sigstore_verify::types::Bundle;
use sigstore_verify::types::Sha256Hash;
use sigstore_verify::types::SignatureContent;
use slog::Logger;
use slog::debug;
use slog::warn;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::time::Duration;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncSeekExt as _;
use tokio::io::AsyncWriteExt as _;
use tufaceous_artifact::RotSlot;
use tufaceous_artifact_v1::ArtifactVersion;
use tufaceous_artifact_v1::KnownArtifactKind;
use tufaceous_lib_v1::assemble::DeserializedArtifactData;
use tufaceous_lib_v1::assemble::DeserializedArtifactSource;
use tufaceous_lib_v1::assemble::DeserializedFileArtifactSource;

pub(crate) async fn fetch_hubris(
    log: Logger,
    client: reqwest::Client,
    attestation: Utf8PathBuf,
    output_dir: Utf8PathBuf,
    editor: SharedEditor,
) -> Result<()> {
    fs::create_dir_all(&output_dir).await?;

    let bundle: Bundle =
        serde_json::from_slice(&fs::read(&attestation).await?)?;
    let fetcher = Fetcher::new(log, client, &bundle, &output_dir).await?;

    let mut artifacts: BTreeMap<_, Vec<_>> = BTreeMap::new();

    let metadata = fetcher.get_omicron_metadata().await?;
    let version = ArtifactVersion::new(&metadata.version)?;
    for (name, board) in &metadata.boards {
        let source = match board {
            OmicronMetadataBoard::Sp { archive } => {
                let output = output_dir.join(archive);
                fetcher.download_attested(archive).await?.persist(&output)?;

                editor.add_sp_archive(output.clone()).await?;
                DeserializedArtifactSource::File { path: output }
            }
            OmicronMetadataBoard::Bootleby { archive } => {
                let output = output_dir.join(archive);
                fetcher.download_attested(archive).await?.persist(&output)?;

                editor.add_rot_bootloader_archive(output.clone()).await?;
                DeserializedArtifactSource::File { path: output }
            }
            OmicronMetadataBoard::Rot { archive_a, archive_b } => {
                let a = output_dir.join(archive_a);
                fetcher.download_attested(archive_a).await?.persist(&a)?;

                let b = output_dir.join(archive_b);
                fetcher.download_attested(archive_b).await?.persist(&b)?;

                editor.add_rot_archive(RotSlot::A, a.clone()).await?;
                editor.add_rot_archive(RotSlot::B, b.clone()).await?;
                DeserializedArtifactSource::CompositeRot {
                    archive_a: DeserializedFileArtifactSource::File { path: a },
                    archive_b: DeserializedFileArtifactSource::File { path: b },
                }
            }
        };

        artifacts.entry(kind_from_board_name(name)?).or_default().push(
            DeserializedArtifactData {
                name: name.clone(),
                version: version.clone(),
                source,
            },
        );
    }

    if metadata.has_corim {
        let path = output_dir.join("corim.cbor");
        fetcher.download("corim.cbor").await?.persist(&path)?;

        editor.add_measurement_corpus(path.clone()).await?;
        artifacts.entry(KnownArtifactKind::MeasurementCorpus).or_default().push(
            DeserializedArtifactData {
                name: format!("staging-{}", fetcher.tag),
                version,
                source: DeserializedArtifactSource::File { path },
            },
        )
    }

    // Pass the list of artifacts to the code generating Tufaceous v1 repos.
    // This won't be needed from Tufaceous v2 onwards.
    fs::write(
        output_dir.join("artifacts.json"),
        serde_json::to_string_pretty(&artifacts)?.into_bytes(),
    )
    .await?;

    Ok(())
}

// This is temporary until we drop support for Tufaceous v1.
fn kind_from_board_name(board: &str) -> Result<KnownArtifactKind> {
    if board.starts_with("gimlet-")
        || board.starts_with("cosmo-")
        || board.starts_with("metro-")
    {
        Ok(KnownArtifactKind::GimletSp)
    } else if board.starts_with("sidecar-") {
        Ok(KnownArtifactKind::SwitchSp)
    } else if board.starts_with("psc-") || board.starts_with("observer-") {
        Ok(KnownArtifactKind::PscSp)
    } else {
        bail!("unknown board name: {board}");
    }
}

struct Fetcher<'a> {
    log: Logger,
    client: reqwest::Client,
    verifier: Verifier,
    output_dir: &'a Utf8PathBuf,
    release_bundle: &'a Bundle,
    tag: String,
    repo: String,
    commit: String,
}

impl<'a> Fetcher<'a> {
    async fn new(
        log: Logger,
        client: reqwest::Client,
        bundle: &'a Bundle,
        output_dir: &'a Utf8PathBuf,
    ) -> Result<Self> {
        let SignatureContent::DsseEnvelope(envelope) = &bundle.content else {
            bail!("not a release attestation (the content is not DSSE)");
        };
        ensure!(
            envelope.payload_type == IN_TOTO_PAYLOAD_TYPE,
            "not a release attestation (payload type is {})",
            envelope.payload_type,
        );
        let in_toto: ReleaseStatement =
            serde_json::from_slice(envelope.payload.as_bytes())
                .context("failed to parse in-toto statement")?;

        // GitHub migrated over time from predicate version 0.1 to 0.2. For the
        // fields we care about, the difference between the two doesn't matter.
        ensure!(
            RELEASE_PREDICATE_TYPES.contains(&in_toto.predicate_type.as_str()),
            "not a release attestation (bad predicate type {})",
            in_toto.predicate_type,
        );
        let repo = in_toto
            .predicate
            .repository
            .ok_or_else(|| anyhow!("missing repo in predicate"))?;
        let tag = in_toto
            .predicate
            .tag
            .ok_or_else(|| anyhow!("missing tag in predicate"))?;

        // The commit of the published release is not stored as a predicate for
        // some reason, but rather as a subject???
        let release_uri = format!("pkg:github/{repo}@{tag}");
        let commit = in_toto
            .subject
            .iter()
            .find(|sub| sub.uri.as_deref() == Some(release_uri.as_str()))
            .ok_or_else(|| anyhow!("missing commit in in-toto subject"))?
            .digest
            .sha1
            .clone()
            .ok_or_else(|| anyhow!("missing commit sha1 in in-toto subject"))?;

        let verifier = Verifier::new(&fetch_trusted_root(&client).await?)
            .context("failed to prepare the sigstore trusted root")?;

        Ok(Fetcher {
            log,
            client,
            verifier,
            output_dir,
            release_bundle: bundle,
            tag,
            repo,
            commit,
        })
    }

    async fn get_omicron_metadata(&self) -> Result<OmicronMetadata> {
        let file = self.download("omicron-metadata.json").await?;
        let raw = fs::read(&file).await?;

        // We first deserialize a struct containing only the metadata_version
        // field to provide better error messages when the version is bumped.
        let version: MetadataVersion = serde_json::from_slice(&raw)?;
        if version.metadata_version != EXPECTED_OMICRON_METADATA_VERSION {
            bail!(
                "omicron-metadata.json's version is {}, expected {}",
                version.metadata_version,
                EXPECTED_OMICRON_METADATA_VERSION,
            );
        }

        Ok(serde_json::from_slice(&raw)?)
    }

    async fn download_attested(&self, name: &str) -> Result<Utf8TempPath> {
        let path = self.download(name).await?;

        // Retrieve the attestation for the artifact.
        let sigstore = self.download(&format!("{name}.sigstore.json")).await?;
        let bundle =
            serde_json::from_slice::<Bundle>(&fs::read(&sigstore).await?)
                .with_context(|| {
                    format!("failed to parse attestation for {name}")
                })?;

        // Verify that the attestation for the artifact is valid, and that it
        // was issued to GitHub Actions.
        let digest = sha256_file(&mut File::open(&path).await?).await?;
        let outcome = self
            .verify(
                &digest,
                &bundle,
                verification_policy().require_issuer(GITHUB_ACTIONS_ISSUER),
            )
            .with_context(|| {
                format!("failed to verify attestation of {name}")
            })?;
        let claims = &outcome
            .certificate()
            .ok_or_else(|| anyhow!("attestation of {name} has no certificate"))?
            .ci_claims;

        // Verify that the artifact was built by GitHub Actions in the correct
        // repository with the correct source commit (preventing a release from
        // containing artifacts built elsewhere).
        let check_claim = |claim, expected: &str, found: &Option<String>| {
            if Some(expected) != found.as_deref() {
                bail!(
                    "expected attestation claim {claim} of {name} \
                     to be {expected}, found {found:?}"
                );
            } else {
                Ok(())
            }
        };
        check_claim(
            "source_repository_uri",
            &format!("https://github.com/{}", self.repo),
            &claims.source_repository_uri,
        )?;
        check_claim(
            "source_repository_digest",
            &self.commit,
            &claims.source_repository_digest,
        )?;

        Ok(path)
    }

    async fn download(&self, name: &str) -> Result<Utf8TempPath> {
        let url = format!(
            "https://github.com/{}/releases/download/{}/{name}",
            self.repo, self.tag
        );

        // Handle spurious GitHub failures by retrying.
        let mut retries = 0;
        let mut file = loop {
            match self.fetch_url(&url).await {
                Ok(file) => break file,
                Err(err) => {
                    retries += 1;
                    if retries > crate::RETRY_ATTEMPTS {
                        return Err(err);
                    } else {
                        warn!(self.log, "retrying fetching {url}: {err}");
                        tokio::time::sleep(Duration::from_secs(retries as _))
                            .await;
                    }
                }
            }
        };

        // Verify that the downloaded file is part of the release.
        //
        // This uses GitHub's release attestation, which contains the hashes of
        // all released files in the in-toto subject.
        let digest = sha256_file(file.as_file_mut()).await?;
        self.verify(
            &digest,
            self.release_bundle,
            verification_policy().require_identity(IdentityMatcher::Uri(
                GITHUB_RELEASES_IDENTITY.into(),
            )),
        )
        .with_context(|| {
            format!("{name} is not part of the attested GitHub release")
        })?;

        Ok(file.into_temp_path())
    }

    fn verify(
        &self,
        digest: &Sha256Hash,
        bundle: &Bundle,
        policy: VerificationPolicy,
    ) -> Result<VerificationResult> {
        Ok(self.verifier.verify(digest, bundle, &policy)?)
    }

    async fn fetch_url(&self, url: &str) -> Result<NamedUtf8TempFile<File>> {
        let (file, path) =
            NamedUtf8TempFile::new_in(&self.output_dir)?.into_parts();
        let mut file = File::from_std(fs_err::File::from_parts(file, &path));

        debug!(self.log, "downloading {url}");

        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to request {url}"))?;
        if !resp.status().is_success() {
            bail!("request to {url} failed with {}", resp.status());
        }

        // Files can potentially be arbitrarily large, stream them rather than
        // buffering the whole thing into memory.
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?).await?;
        }
        file.seek(SeekFrom::Start(0)).await?;

        Ok(NamedUtf8TempFile::from_parts(file, path))
    }
}

/// OIDC issuer of the certificates GitHub Actions workflows sign with.
const GITHUB_ACTIONS_ISSUER: &str =
    "https://token.actions.githubusercontent.com";

/// Certificate identity GitHub uses to sign release attestations.
const GITHUB_RELEASES_IDENTITY: &str = "https://dotcom.releases.github.com";

const IN_TOTO_PAYLOAD_TYPE: &str = "application/vnd.in-toto+json";

const RELEASE_PREDICATE_TYPES: &[&str] = &[
    "https://in-toto.io/attestation/release/v0.1",
    "https://in-toto.io/attestation/release/v0.2",
];

/// The verification policy shared by every attestation we check.
///
/// GitHub's release attestations (and attestations for private repositories)
/// are signed by GitHub's own Sigstore instance, which has no public
/// transparency log and whose certificates carry no SCTs. Signing times are
/// instead established by the RFC 3161 timestamps in the bundles, which are
/// still verified against the trusted root.
fn verification_policy() -> VerificationPolicy {
    VerificationPolicy::any_identity().skip_tlog_unsafe().skip_sct()
}

/// The subset of a GitHub release in-toto statement we care about.
///
/// `sigstore_verify::types::Statement` does not expose the subject `uri`
/// field, which is where GitHub stores the commit of the release.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseStatement {
    subject: Vec<ReleaseSubject>,
    predicate_type: String,
    predicate: ReleasePredicate,
}

#[derive(Debug, serde::Deserialize)]
struct ReleaseSubject {
    uri: Option<String>,
    digest: ReleaseSubjectDigest,
}

#[derive(Debug, serde::Deserialize)]
struct ReleaseSubjectDigest {
    sha1: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ReleasePredicate {
    repository: Option<String>,
    tag: Option<String>,
}

async fn sha256_file(file: &mut File) -> Result<Sha256Hash> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(Sha256Hash::new(hasher.finalize().into()))
}

async fn fetch_trusted_root(client: &reqwest::Client) -> Result<TrustedRoot> {
    // releng is designed to run in an ephemeral CI environment, so the TUF
    // caching is not actually needed.
    let fetch = |config: TufConfig| async move {
        TrustedRoot::from_tuf(
            config.without_cache().with_http_client(client.clone()),
        )
        .await
    };

    let mut trusted_root = fetch(TufConfig::production())
        .await
        .context("failed to fetch the Sigstore public good trusted root")?;
    let github = fetch(TufConfig::github())
        .await
        .context("failed to fetch the GitHub trusted root")?;

    trusted_root.tlogs.extend(github.tlogs);
    trusted_root.certificate_authorities.extend(github.certificate_authorities);
    trusted_root.ctlogs.extend(github.ctlogs);
    trusted_root.timestamp_authorities.extend(github.timestamp_authorities);

    Ok(trusted_root)
}

const EXPECTED_OMICRON_METADATA_VERSION: u32 = 4;

#[derive(Debug, serde::Deserialize)]
struct MetadataVersion {
    metadata_version: u32,
}

#[derive(Debug, serde::Deserialize)]
struct OmicronMetadata {
    version: String,
    has_corim: bool,
    boards: BTreeMap<String, OmicronMetadataBoard>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OmicronMetadataBoard {
    Sp { archive: String },
    Bootleby { archive: String },
    Rot { archive_a: String, archive_b: String },
}
