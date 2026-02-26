use anyhow::Context as _;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use camino::Utf8PathBuf;
use camino_tempfile::NamedUtf8TempFile;
use camino_tempfile::Utf8TempDir;
use camino_tempfile::Utf8TempPath;
use futures::StreamExt;
use sigstore_verify::GITHUB_DOT_COM;
use sigstore_verify::SIGSTORE_PUBLIC_GOOD;
use sigstore_verify::repr::bundle::Bundle;
use sigstore_verify::repr::bundle::BundleContent;
use sigstore_verify::repr::in_toto::InTotoPredicate;
use sigstore_verify::repr::in_toto::InTotoStatement;
use sigstore_verify::repr::trusted_root::TrustedRoot;
use slog::Logger;
use slog::debug;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt as _;
use tokio::io::AsyncWriteExt as _;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::assemble::DeserializedArtifactData;
use tufaceous_lib::assemble::DeserializedArtifactSource;
use tufaceous_lib::assemble::DeserializedFileArtifactSource;

pub(crate) async fn fetch_hubris(
    log: Logger,
    client: reqwest::Client,
    attestation: Utf8PathBuf,
    output_dir: Utf8PathBuf,
) -> Result<()> {
    std::fs::create_dir_all(&output_dir)?;

    let bundle: Bundle = serde_json::from_slice(&std::fs::read(&attestation)?)?;
    let fetcher = Fetcher::new(log, client, &bundle, &output_dir).await?;

    let mut artifacts: BTreeMap<_, Vec<_>> = BTreeMap::new();

    let metadata = fetcher.get_omicron_metadata().await?;
    let version = ArtifactVersion::new(&metadata.version)?;
    for (name, board) in &metadata.boards {
        let source = match board {
            OmicronMetadataBoard::Single { archive } => {
                let output = output_dir.join(archive);
                fetcher.download_attested(archive).await?.persist(&output)?;

                DeserializedArtifactSource::File { path: output }
            }
            OmicronMetadataBoard::Ab { archive_a, archive_b } => {
                let a = output_dir.join(archive_a);
                fetcher.download_attested(archive_a).await?.persist(&a)?;

                let b = output_dir.join(archive_b);
                fetcher.download_attested(archive_b).await?.persist(&b)?;

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

    if fetcher.release_contains("corim.cbor") {
        let path = output_dir.join("corim.cbor");
        fetcher.download("corim.cbor").await?.persist(&path)?;
        artifacts.entry(KnownArtifactKind::MeasurementCorpus).or_default().push(
            DeserializedArtifactData {
                name: format!("staging-{}", fetcher.tag),
                version,
                source: DeserializedArtifactSource::File { path },
            },
        )
    }

    tokio::fs::write(
        output_dir.join("artifacts.json"),
        serde_json::to_string_pretty(&artifacts)?.into_bytes(),
    )
    .await?;

    Ok(())
}

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
    trusted_root: TrustedRoot,
    output_dir: &'a Utf8PathBuf,
    release_bundle: &'a Bundle,
    in_toto: &'a InTotoStatement,
    tag: &'a str,
    repo: &'a str,
}

impl<'a> Fetcher<'a> {
    async fn new(
        log: Logger,
        client: reqwest::Client,
        bundle: &'a Bundle,
        output_dir: &'a Utf8PathBuf,
    ) -> Result<Self> {
        let BundleContent::DsseEnvelope(envelope) = &bundle.content else {
            bail!("not a release attestation (the content is not DSSE)");
        };
        let in_toto = &envelope.payload.parsed;

        let (repo, tag) = match &in_toto.predicate {
            InTotoPredicate::Release01(release) => (
                release.predicate.repository.as_deref(),
                release.predicate.tag.as_deref(),
            ),
            InTotoPredicate::Release02(release) => (
                release.predicate.repository.as_deref(),
                release.predicate.tag.as_deref(),
            ),
            _ => bail!("not a release attestation (bad predicate)"),
        };

        Ok(Fetcher {
            log,
            client,
            trusted_root: fetch_trusted_root().await?,
            output_dir,
            release_bundle: bundle,
            in_toto,
            tag: tag.ok_or_else(|| anyhow!("missing tag in predicate"))?,
            repo: repo.ok_or_else(|| anyhow!("missing repo in predicate"))?,
        })
    }

    async fn get_omicron_metadata(&self) -> Result<OmicronMetadata> {
        let file = self.download("omicron-metadata.json").await?;
        let raw = tokio::fs::read(&file).await?;

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

    fn release_contains(&self, name: &str) -> bool {
        self.in_toto
            .subject
            .iter()
            .find(|sub| sub.name.as_deref() == Some(name))
            .is_some()
    }

    async fn download_attested(&self, name: &str) -> Result<Utf8TempPath> {
        let path = self.download(name).await?;
        // TODO: verify their own attestation
        Ok(path)
    }

    async fn download(&self, name: &str) -> Result<Utf8TempPath> {
        let (file, path) =
            NamedUtf8TempFile::new_in(&self.output_dir)?.into_parts();
        let mut file = tokio::fs::File::from_std(file);

        let url = format!(
            "https://github.com/{}/releases/download/{}/{name}",
            self.repo, self.tag
        );

        debug!(self.log, "downloading {url}");
        let resp = self
            .client
            .get(&url)
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

        // Verify that the downloaded file is part of the release.
        //
        // This uses GitHub's release attestation, which contains the hashes of
        // all released files in the in-toto subject.
        file.seek(SeekFrom::Start(0)).await?;
        let claims = sigstore_verify::verify_no_tlog_async(
            &self.trusted_root,
            &self.release_bundle,
            &mut file,
        )
        .await?;
        ensure!(
            &["https://dotcom.releases.github.com"]
                == &claims.claims.subject_alternative_names_uris[..],
            "attestation is not for a GitHub release"
        );

        Ok(path)
    }
}

async fn fetch_trusted_root() -> Result<TrustedRoot> {
    // releng is designed to run in an ephemeral CI environment, so the TUF
    // caching is not actually needed. "Store" it in a temporary directory.
    let cache_dir = Utf8TempDir::new()?;

    let mut trusted_root = TrustedRoot::empty();
    trusted_root.merge(
        sigstore_verify::fetch_trusted_root(
            cache_dir.as_ref(),
            &SIGSTORE_PUBLIC_GOOD,
        )
        .await?,
    );
    trusted_root.merge(
        sigstore_verify::fetch_trusted_root(
            cache_dir.as_ref(),
            &GITHUB_DOT_COM,
        )
        .await?,
    );

    Ok(trusted_root)
}

const EXPECTED_OMICRON_METADATA_VERSION: u32 = 3;

#[derive(Debug, serde::Deserialize)]
struct MetadataVersion {
    metadata_version: u32,
}

#[derive(Debug, serde::Deserialize)]
struct OmicronMetadata {
    version: String,
    boards: BTreeMap<String, OmicronMetadataBoard>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OmicronMetadataBoard {
    Single { archive: String },
    Ab { archive_a: String, archive_b: String },
}
