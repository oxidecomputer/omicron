// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Constructor for the `UpdatePlan` wicketd and Nexus use to drive sled
//! mupdates.
//!
//! This is a "plan" in name only: it is a strict list of which artifacts to
//! apply to which components; the ordering and application of the plan lives
//! elsewhere.

use super::ArtifactIdData;
use super::Board;
use super::ExtractedArtifactDataHandle;
use super::ExtractedArtifacts;
use super::HashingNamedUtf8TempFile;
use crate::errors::RepositoryError;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use hubtools::RawHubrisArchive;
use omicron_common::api::external::SemverVersion;
use omicron_common::api::external::TufArtifactMeta;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use omicron_common::update::ArtifactHash;
use omicron_common::update::ArtifactHashId;
use omicron_common::update::ArtifactId;
use omicron_common::update::ArtifactKind;
use slog::info;
use slog::Logger;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use tokio::io::AsyncReadExt;
use tufaceous_lib::HostPhaseImages;
use tufaceous_lib::RotArchives;

/// Artifacts with their hashes and sources, as obtained from an uploaded
/// repository.
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub system_version: SemverVersion,
    pub gimlet_sp: BTreeMap<Board, ArtifactIdData>,
    pub gimlet_rot_a: Vec<ArtifactIdData>,
    pub gimlet_rot_b: Vec<ArtifactIdData>,
    pub psc_sp: BTreeMap<Board, ArtifactIdData>,
    pub psc_rot_a: Vec<ArtifactIdData>,
    pub psc_rot_b: Vec<ArtifactIdData>,
    pub sidecar_sp: BTreeMap<Board, ArtifactIdData>,
    pub sidecar_rot_a: Vec<ArtifactIdData>,
    pub sidecar_rot_b: Vec<ArtifactIdData>,

    // Note: The Trampoline image is broken into phase1/phase2 as part of our
    // update plan (because they go to different destinations), but the two
    // phases will have the _same_ `ArtifactId` (i.e., the ID of the Host
    // artifact from the TUF repository.
    //
    // The same would apply to the host phase1/phase2, but we don't actually
    // need the `host_phase_2` data as part of this plan (we serve it from the
    // artifact server instead).
    pub host_phase_1: ArtifactIdData,
    pub trampoline_phase_1: ArtifactIdData,
    pub trampoline_phase_2: ArtifactIdData,

    // We need to send installinator the hash of the host_phase_2 data it should
    // fetch from us; we compute it while generating the plan.
    pub host_phase_2_hash: ArtifactHash,

    // We also need to send installinator the hash of the control_plane image it
    // should fetch from us. This is already present in the TUF repository, but
    // we record it here for use by the update process.
    pub control_plane_hash: ArtifactHash,
}

// Used to represent the information extracted from signed RoT images. This
// is used when going from `UpdatePlanBuilder` -> `UpdatePlan` to check
// the versions on the RoT images and also to generate the map of
// ArtifactId -> Sign hashes for checking artifacts
#[derive(Debug, Eq, Hash, PartialEq)]
struct RotSignData {
    kind: KnownArtifactKind,
    sign: Vec<u8>,
}

/// `UpdatePlanBuilder` mirrors all the fields of `UpdatePlan`, but they're all
/// optional: it can be filled in as we read a TUF repository.
/// [`UpdatePlanBuilder::build()`] will (fallibly) convert from the builder to
/// the final plan.
#[derive(Debug)]
pub struct UpdatePlanBuilder<'a> {
    // fields that mirror `UpdatePlan`
    system_version: SemverVersion,
    gimlet_sp: BTreeMap<Board, ArtifactIdData>,
    gimlet_rot_a: Vec<ArtifactIdData>,
    gimlet_rot_b: Vec<ArtifactIdData>,
    psc_sp: BTreeMap<Board, ArtifactIdData>,
    psc_rot_a: Vec<ArtifactIdData>,
    psc_rot_b: Vec<ArtifactIdData>,
    sidecar_sp: BTreeMap<Board, ArtifactIdData>,
    sidecar_rot_a: Vec<ArtifactIdData>,
    sidecar_rot_b: Vec<ArtifactIdData>,

    // We always send phase 1 images (regardless of host or trampoline) to the
    // SP via MGS, so we retain their data.
    host_phase_1: Option<ArtifactIdData>,
    trampoline_phase_1: Option<ArtifactIdData>,

    // Trampoline phase 2 images must be sent to MGS so that the SP is able to
    // fetch it on demand while the trampoline OS is booting, so we need the
    // data to send to MGS when we start an update.
    trampoline_phase_2: Option<ArtifactIdData>,

    // In contrast to the trampoline phase 2 image, the host phase 2 image and
    // the control plane are fetched by installinator from us over the bootstrap
    // network. The only information we have to send to the SP via MGS is the
    // hash of these two artifacts; we still hold the data in our `by_hash` map
    // we build below, but we don't need the data when driving an update.
    host_phase_2_hash: Option<ArtifactHash>,
    control_plane_hash: Option<ArtifactHash>,

    // The by_id and by_hash maps, and metadata, used in `ArtifactsWithPlan`.
    by_id: BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
    by_hash: HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
    artifacts_meta: Vec<TufArtifactMeta>,

    // map for RoT signing information, used in `ArtifactsWithPlan`
    rot_by_sign: HashMap<RotSignData, Vec<ArtifactId>>,

    // extra fields we use to build the plan
    extracted_artifacts: ExtractedArtifacts,
    log: &'a Logger,
}

impl<'a> UpdatePlanBuilder<'a> {
    pub fn new(
        system_version: SemverVersion,
        log: &'a Logger,
    ) -> Result<Self, RepositoryError> {
        let extracted_artifacts = ExtractedArtifacts::new(log)?;
        Ok(Self {
            system_version,
            gimlet_sp: BTreeMap::new(),
            gimlet_rot_a: Vec::new(),
            gimlet_rot_b: Vec::new(),
            psc_sp: BTreeMap::new(),
            psc_rot_a: Vec::new(),
            psc_rot_b: Vec::new(),
            sidecar_sp: BTreeMap::new(),
            sidecar_rot_a: Vec::new(),
            sidecar_rot_b: Vec::new(),
            host_phase_1: None,
            trampoline_phase_1: None,
            trampoline_phase_2: None,
            host_phase_2_hash: None,
            control_plane_hash: None,

            by_id: BTreeMap::new(),
            by_hash: HashMap::new(),
            rot_by_sign: HashMap::new(),
            artifacts_meta: Vec::new(),

            extracted_artifacts,
            log,
        })
    }

    /// Adds an artifact with these contents to the by_id and by_hash maps.
    pub async fn add_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_hash: ArtifactHash,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        // If we don't know this artifact kind, we'll still serve it up by hash,
        // but we don't do any further processing on it.
        let Some(artifact_kind) = artifact_id.kind.to_known() else {
            return self
                .add_unknown_artifact(artifact_id, artifact_hash, stream)
                .await;
        };

        // If we do know the artifact kind, we may have additional work to do,
        // so we break each out into its own method. The particulars of that
        // work varies based on the kind of artifact; for example, we have to
        // unpack RoT artifacts into the A and B images they contain.
        match artifact_kind {
            KnownArtifactKind::GimletSp
            | KnownArtifactKind::PscSp
            | KnownArtifactKind::SwitchSp => {
                self.add_sp_artifact(
                    artifact_id,
                    artifact_kind,
                    artifact_hash,
                    stream,
                )
                .await
            }
            KnownArtifactKind::GimletRot
            | KnownArtifactKind::PscRot
            | KnownArtifactKind::SwitchRot => {
                self.add_rot_artifact(artifact_id, artifact_kind, stream).await
            }
            KnownArtifactKind::Host => {
                self.add_host_artifact(artifact_id, stream)
            }
            KnownArtifactKind::Trampoline => {
                self.add_trampoline_artifact(artifact_id, stream)
            }
            KnownArtifactKind::ControlPlane => {
                self.add_control_plane_artifact(
                    artifact_id,
                    artifact_hash,
                    stream,
                )
                .await
            }
        }
    }

    async fn add_sp_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_kind: KnownArtifactKind,
        artifact_hash: ArtifactHash,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        let sp_map = match artifact_kind {
            KnownArtifactKind::GimletSp => &mut self.gimlet_sp,
            KnownArtifactKind::PscSp => &mut self.psc_sp,
            KnownArtifactKind::SwitchSp => &mut self.sidecar_sp,
            // We're only called with an SP artifact kind.
            KnownArtifactKind::GimletRot
            | KnownArtifactKind::Host
            | KnownArtifactKind::Trampoline
            | KnownArtifactKind::ControlPlane
            | KnownArtifactKind::PscRot
            | KnownArtifactKind::SwitchRot => unreachable!(),
        };

        let mut stream = std::pin::pin!(stream);

        // SP images are small, and hubtools wants a `&[u8]` to parse, so we'll
        // read the whole thing into memory.
        let mut data = Vec::new();
        while let Some(res) = stream.next().await {
            let chunk = res.map_err(|error| RepositoryError::ReadArtifact {
                kind: artifact_kind.into(),
                error: Box::new(error),
            })?;
            data.extend_from_slice(&chunk);
        }

        let (artifact_id, board) =
            read_hubris_board_from_archive(artifact_id, data.clone())?;

        let slot = match sp_map.entry(board) {
            btree_map::Entry::Vacant(slot) => slot,
            btree_map::Entry::Occupied(slot) => {
                return Err(RepositoryError::DuplicateBoardEntry {
                    board: slot.key().0.clone(),
                    kind: artifact_kind,
                });
            }
        };

        let artifact_hash_id =
            ArtifactHashId { kind: artifact_kind.into(), hash: artifact_hash };
        let data = self
            .extracted_artifacts
            .store(
                artifact_hash_id,
                futures::stream::iter([Ok(Bytes::from(data))]),
            )
            .await?;
        slot.insert(ArtifactIdData {
            id: artifact_id.clone(),
            data: data.clone(),
        });

        self.record_extracted_artifact(
            artifact_id,
            data,
            artifact_kind.into(),
            self.log,
        )?;

        Ok(())
    }

    async fn add_rot_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_kind: KnownArtifactKind,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        let (rot_a, rot_a_kind, rot_b, rot_b_kind) = match artifact_kind {
            KnownArtifactKind::GimletRot => (
                &mut self.gimlet_rot_a,
                ArtifactKind::GIMLET_ROT_IMAGE_A,
                &mut self.gimlet_rot_b,
                ArtifactKind::GIMLET_ROT_IMAGE_B,
            ),
            KnownArtifactKind::PscRot => (
                &mut self.psc_rot_a,
                ArtifactKind::PSC_ROT_IMAGE_A,
                &mut self.psc_rot_b,
                ArtifactKind::PSC_ROT_IMAGE_B,
            ),
            KnownArtifactKind::SwitchRot => (
                &mut self.sidecar_rot_a,
                ArtifactKind::SWITCH_ROT_IMAGE_A,
                &mut self.sidecar_rot_b,
                ArtifactKind::SWITCH_ROT_IMAGE_B,
            ),
            // We're only called with an RoT artifact kind.
            KnownArtifactKind::GimletSp
            | KnownArtifactKind::Host
            | KnownArtifactKind::Trampoline
            | KnownArtifactKind::ControlPlane
            | KnownArtifactKind::PscSp
            | KnownArtifactKind::SwitchSp => unreachable!(),
        };

        let (rot_a_data, rot_b_data) = Self::extract_nested_artifact_pair(
            stream,
            &mut self.extracted_artifacts,
            artifact_kind,
            |reader, out_a, out_b| {
                RotArchives::extract_into(reader, out_a, out_b)
            },
        )?;

        // We need to get all the signing information now to properly check
        // version at builder time (builder time is not async)
        let image_a_stream = rot_a_data
            .reader_stream()
            .await
            .map_err(RepositoryError::CreateReaderStream)?;
        let mut image_a = Vec::with_capacity(rot_a_data.file_size());
        tokio_util::io::StreamReader::new(image_a_stream)
            .read_to_end(&mut image_a)
            .await
            .map_err(|error| RepositoryError::ReadExtractedArchive {
                artifact: ArtifactHashId {
                    kind: artifact_id.kind.clone(),
                    hash: rot_a_data.hash(),
                },
                error,
            })?;

        let (artifact_id, image_a_sign) =
            read_hubris_sign_from_archive(artifact_id, image_a)?;

        self.rot_by_sign
            .entry(RotSignData {
                kind: artifact_kind,
                sign: hex::decode(image_a_sign).expect("should decode"),
            })
            .or_default()
            .push(artifact_id.clone());

        let image_b_stream = rot_b_data
            .reader_stream()
            .await
            .map_err(RepositoryError::CreateReaderStream)?;
        let mut image_b = Vec::with_capacity(rot_b_data.file_size());
        tokio_util::io::StreamReader::new(image_b_stream)
            .read_to_end(&mut image_b)
            .await
            .map_err(|error| RepositoryError::ReadExtractedArchive {
                artifact: ArtifactHashId {
                    kind: artifact_id.kind.clone(),
                    hash: rot_b_data.hash(),
                },
                error,
            })?;

        let (artifact_id, image_b_sign) =
            read_hubris_sign_from_archive(artifact_id, image_b)?;

        self.rot_by_sign
            .entry(RotSignData {
                kind: artifact_kind,
                sign: hex::decode(image_b_sign).expect("should decode"),
            })
            .or_default()
            .push(artifact_id.clone());

        // Technically we've done all we _need_ to do with the RoT images. We
        // send them directly to MGS ourself, so don't expect anyone to ask for
        // them via `by_id` or `by_hash`. However, it's more convenient to
        // record them in `by_id` and `by_hash`: their addition will be
        // consistently logged the way other artifacts are, and they'll show up
        // in our dropshot endpoint that reports the artifacts we have.
        let rot_a_id = ArtifactId {
            name: artifact_id.name.clone(),
            version: artifact_id.version.clone(),
            kind: rot_a_kind.clone(),
        };
        let rot_b_id = ArtifactId {
            name: artifact_id.name.clone(),
            version: artifact_id.version.clone(),
            kind: rot_b_kind.clone(),
        };

        rot_a.push(ArtifactIdData { id: rot_a_id, data: rot_a_data.clone() });
        rot_b.push(ArtifactIdData { id: rot_b_id, data: rot_b_data.clone() });

        self.record_extracted_artifact(
            artifact_id.clone(),
            rot_a_data,
            rot_a_kind,
            self.log,
        )?;
        self.record_extracted_artifact(
            artifact_id,
            rot_b_data,
            rot_b_kind,
            self.log,
        )?;

        Ok(())
    }

    fn add_host_artifact(
        &mut self,
        artifact_id: ArtifactId,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        if self.host_phase_1.is_some() || self.host_phase_2_hash.is_some() {
            return Err(RepositoryError::DuplicateArtifactKind(
                KnownArtifactKind::Host,
            ));
        }

        let (phase_1_data, phase_2_data) = Self::extract_nested_artifact_pair(
            stream,
            &mut self.extracted_artifacts,
            KnownArtifactKind::Host,
            |reader, out_1, out_2| {
                HostPhaseImages::extract_into(reader, out_1, out_2)
            },
        )?;

        // Similarly to the RoT, we need to create new, non-conflicting artifact
        // IDs for each image.
        let phase_1_id = ArtifactId {
            name: artifact_id.name.clone(),
            version: artifact_id.version.clone(),
            kind: ArtifactKind::HOST_PHASE_1,
        };

        self.host_phase_1 =
            Some(ArtifactIdData { id: phase_1_id, data: phase_1_data.clone() });
        self.host_phase_2_hash = Some(phase_2_data.hash());

        self.record_extracted_artifact(
            artifact_id.clone(),
            phase_1_data,
            ArtifactKind::HOST_PHASE_1,
            self.log,
        )?;
        self.record_extracted_artifact(
            artifact_id,
            phase_2_data,
            ArtifactKind::HOST_PHASE_2,
            self.log,
        )?;

        Ok(())
    }

    fn add_trampoline_artifact(
        &mut self,
        artifact_id: ArtifactId,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        if self.trampoline_phase_1.is_some()
            || self.trampoline_phase_2.is_some()
        {
            return Err(RepositoryError::DuplicateArtifactKind(
                KnownArtifactKind::Trampoline,
            ));
        }

        let (phase_1_data, phase_2_data) = Self::extract_nested_artifact_pair(
            stream,
            &mut self.extracted_artifacts,
            KnownArtifactKind::Trampoline,
            |reader, out_1, out_2| {
                HostPhaseImages::extract_into(reader, out_1, out_2)
            },
        )?;

        // Similarly to the RoT, we need to create new, non-conflicting artifact
        // IDs for each image. We'll append a suffix to the name; keep the
        // version and kind the same.
        let phase_1_id = ArtifactId {
            name: artifact_id.name.clone(),
            version: artifact_id.version.clone(),
            kind: ArtifactKind::TRAMPOLINE_PHASE_1,
        };
        let phase_2_id = ArtifactId {
            name: artifact_id.name.clone(),
            version: artifact_id.version.clone(),
            kind: ArtifactKind::TRAMPOLINE_PHASE_2,
        };

        self.trampoline_phase_1 =
            Some(ArtifactIdData { id: phase_1_id, data: phase_1_data.clone() });
        self.trampoline_phase_2 =
            Some(ArtifactIdData { id: phase_2_id, data: phase_2_data.clone() });

        self.record_extracted_artifact(
            artifact_id.clone(),
            phase_1_data,
            ArtifactKind::TRAMPOLINE_PHASE_1,
            self.log,
        )?;
        self.record_extracted_artifact(
            artifact_id,
            phase_2_data,
            ArtifactKind::TRAMPOLINE_PHASE_2,
            self.log,
        )?;

        Ok(())
    }

    async fn add_control_plane_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_hash: ArtifactHash,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        if self.control_plane_hash.is_some() {
            return Err(RepositoryError::DuplicateArtifactKind(
                KnownArtifactKind::ControlPlane,
            ));
        }

        // The control plane artifact is the easiest one: we just need to copy
        // it into our tempdir and record it. Nothing to inspect or extract.
        let artifact_hash_id = ArtifactHashId {
            kind: artifact_id.kind.clone(),
            hash: artifact_hash,
        };

        let data =
            self.extracted_artifacts.store(artifact_hash_id, stream).await?;

        self.control_plane_hash = Some(data.hash());

        self.record_extracted_artifact(
            artifact_id,
            data,
            KnownArtifactKind::ControlPlane.into(),
            self.log,
        )?;

        Ok(())
    }

    async fn add_unknown_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_hash: ArtifactHash,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
    ) -> Result<(), RepositoryError> {
        let artifact_kind = artifact_id.kind.clone();
        let artifact_hash_id =
            ArtifactHashId { kind: artifact_kind.clone(), hash: artifact_hash };

        let data =
            self.extracted_artifacts.store(artifact_hash_id, stream).await?;

        self.record_extracted_artifact(
            artifact_id,
            data,
            artifact_kind,
            self.log,
        )?;

        Ok(())
    }

    /// A helper that converts a single artifact `stream` into a pair of
    /// extracted artifacts.
    ///
    /// RoT, host OS, and trampoline OS artifacts all contain a pair of
    /// artifacts we actually care about (RoT: A/B images; host/trampoline:
    /// phase1/phase2 images). This method is a helper to extract that.
    ///
    /// This method uses a `block_in_place` into synchronous code, because the
    /// value of changing tufaceous to do async tarball extraction is honestly
    /// pretty dubious.
    ///
    /// The main costs of this are that:
    /// 1. This code can only be used with multithreaded Tokio executors. (This
    ///    is OK for production, but does require that our tests use `flavor =
    ///    "multi_thread`.)
    /// 2. Parallelizing extraction is harder if we ever want to do that in the
    ///    future. (It can be done using the async-scoped crate, though.)
    ///
    /// Depending on how things shake out, we may want to revisit this in the
    /// future.
    fn extract_nested_artifact_pair<F>(
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
        extracted_artifacts: &mut ExtractedArtifacts,
        kind: KnownArtifactKind,
        extract: F,
    ) -> Result<
        (ExtractedArtifactDataHandle, ExtractedArtifactDataHandle),
        RepositoryError,
    >
    where
        F: FnOnce(
                &mut dyn io::BufRead,
                &mut HashingNamedUtf8TempFile,
                &mut HashingNamedUtf8TempFile,
            ) -> anyhow::Result<()>
            + Send,
    {
        // Since stream isn't guaranteed to be 'static, we have to use
        // block_in_place here, not spawn_blocking. This does mean that the
        // current task is taken over, and that this function can only be used
        // from a multithreaded Tokio runtime.
        //
        // An alternative would be to use the `async-scoped` crate. However:
        //
        // - We would only spawn one task there.
        // - The only safe use of async-scoped is with the `scope_and_block`
        //   call, which uses `tokio::task::block_in_place` anyway.
        // - async-scoped also requires a multithreaded Tokio runtime.
        //
        // If we ever want to parallelize extraction across all the different
        // artifacts, `async-scoped` would be a good fit.
        tokio::task::block_in_place(|| {
            let stream = std::pin::pin!(stream);
            let reader =
                tokio_util::io::StreamReader::new(stream.map_err(|error| {
                    // StreamReader requires a conversion from tough's errors to
                    // std::io::Error.
                    std::io::Error::new(io::ErrorKind::Other, error)
                }));

            // RotArchives::extract_into takes a synchronous reader, so we need
            // to use this bridge. The bridge can only be used from a blocking
            // context.
            let mut reader = tokio_util::io::SyncIoBridge::new(reader);

            Self::extract_nested_artifact_pair_impl(
                extracted_artifacts,
                kind,
                |out_a, out_b| extract(&mut reader, out_a, out_b),
            )
        })
    }

    fn extract_nested_artifact_pair_impl<F>(
        extracted_artifacts: &mut ExtractedArtifacts,
        kind: KnownArtifactKind,
        extract: F,
    ) -> Result<
        (ExtractedArtifactDataHandle, ExtractedArtifactDataHandle),
        RepositoryError,
    >
    where
        F: FnOnce(
            &mut HashingNamedUtf8TempFile,
            &mut HashingNamedUtf8TempFile,
        ) -> anyhow::Result<()>,
    {
        // Create two temp files for the pair of images we want to
        // extract from `reader`.
        let mut image1_out = extracted_artifacts.new_tempfile()?;
        let mut image2_out = extracted_artifacts.new_tempfile()?;

        // Extract the two images from `reader`.
        extract(&mut image1_out, &mut image2_out)
            .map_err(|error| RepositoryError::TarballExtract { kind, error })?;

        // Persist the two images we just extracted.
        let image1 =
            extracted_artifacts.store_tempfile(kind.into(), image1_out)?;
        let image2 =
            extracted_artifacts.store_tempfile(kind.into(), image2_out)?;

        Ok((image1, image2))
    }

    // Record an artifact in `by_id` and `by_hash`, or fail if either already has an
    // entry for this id/hash.
    fn record_extracted_artifact(
        &mut self,
        tuf_repo_artifact_id: ArtifactId,
        data: ExtractedArtifactDataHandle,
        data_kind: ArtifactKind,
        log: &Logger,
    ) -> Result<(), RepositoryError> {
        use std::collections::hash_map::Entry;

        let artifact_hash_id =
            ArtifactHashId { kind: data_kind.clone(), hash: data.hash() };

        let by_hash_slot = match self.by_hash.entry(artifact_hash_id) {
            Entry::Occupied(slot) => {
                return Err(RepositoryError::DuplicateHashEntry(
                    slot.key().clone(),
                ));
            }
            Entry::Vacant(slot) => slot,
        };

        info!(
            log, "added artifact";
            "name" => %tuf_repo_artifact_id.name,
            "kind" => %by_hash_slot.key().kind,
            "version" => %tuf_repo_artifact_id.version,
            "hash" => %by_hash_slot.key().hash,
            "length" => data.file_size(),
        );

        self.by_id
            .entry(tuf_repo_artifact_id.clone())
            .or_default()
            .push(by_hash_slot.key().clone());

        // In the artifacts_meta document, use the expanded artifact ID
        // (artifact kind = data_kind, and name and version from
        // tuf_repo_artifact_id).
        let artifacts_meta_id = ArtifactId {
            name: tuf_repo_artifact_id.name,
            version: tuf_repo_artifact_id.version,
            kind: data_kind,
        };
        self.artifacts_meta.push(TufArtifactMeta {
            id: artifacts_meta_id,
            hash: data.hash(),
            size: data.file_size() as u64,
        });
        by_hash_slot.insert(data);

        Ok(())
    }

    pub fn build(self) -> Result<UpdatePlanBuildOutput, RepositoryError> {
        // Ensure our multi-board-supporting kinds have at least one board
        // present.
        for (kind, no_artifacts) in [
            (KnownArtifactKind::GimletSp, self.gimlet_sp.is_empty()),
            (KnownArtifactKind::PscSp, self.psc_sp.is_empty()),
            (KnownArtifactKind::SwitchSp, self.sidecar_sp.is_empty()),
            (
                KnownArtifactKind::GimletRot,
                self.gimlet_rot_a.is_empty() || self.gimlet_rot_b.is_empty(),
            ),
            (
                KnownArtifactKind::PscRot,
                self.psc_rot_a.is_empty() || self.psc_rot_b.is_empty(),
            ),
            (
                KnownArtifactKind::SwitchRot,
                self.sidecar_rot_a.is_empty() || self.sidecar_rot_b.is_empty(),
            ),
        ] {
            if no_artifacts {
                return Err(RepositoryError::MissingArtifactKind(kind));
            }
        }

        // Ensure that all A/B RoT images for each board kind and same
        // signing key have the same version. (i.e. allow gimlet_rot signed
        // with a staging key to be a different version from gimlet_rot signed
        // with a production key)
        for (entry, versions) in &self.rot_by_sign {
            let kind = entry.kind;
            // This unwrap is safe because we check above that each of the types
            // has at least one entry
            let version = &versions.first().unwrap().version;
            match versions.iter().find(|x| x.version != *version) {
                None => continue,
                Some(v) => {
                    return Err(RepositoryError::MultipleVersionsPresent {
                        kind,
                        v1: version.clone(),
                        v2: v.version.clone(),
                    })
                }
            }
        }

        let mut rot_by_sign = HashMap::new();
        for (k, v) in self.rot_by_sign {
            for id in v {
                rot_by_sign.insert(id, k.sign.clone());
            }
        }

        // Repeat the same version check for all SP images. (This is a separate
        // loop because the types of the iterators don't match.)
        for (kind, mut single_board_sp_artifacts) in [
            (KnownArtifactKind::GimletSp, self.gimlet_sp.values()),
            (KnownArtifactKind::PscSp, self.psc_sp.values()),
            (KnownArtifactKind::SwitchSp, self.sidecar_sp.values()),
        ] {
            // We know each of these iterators has at least 1 element (checked
            // above) so we can safely unwrap the first.
            let version = &single_board_sp_artifacts.next().unwrap().id.version;
            for artifact in single_board_sp_artifacts {
                if artifact.id.version != *version {
                    return Err(RepositoryError::MultipleVersionsPresent {
                        kind,
                        v1: version.clone(),
                        v2: artifact.id.version.clone(),
                    });
                }
            }
        }

        let plan = UpdatePlan {
            system_version: self.system_version,
            gimlet_sp: self.gimlet_sp, // checked above
            gimlet_rot_a: self.gimlet_rot_a, // checked above
            gimlet_rot_b: self.gimlet_rot_b, // checked above
            psc_sp: self.psc_sp,       // checked above
            psc_rot_a: self.psc_rot_a, // checked above
            psc_rot_b: self.psc_rot_b, // checked above
            sidecar_sp: self.sidecar_sp, // checked above
            sidecar_rot_a: self.sidecar_rot_a, // checked above
            sidecar_rot_b: self.sidecar_rot_b, // checked above
            host_phase_1: self.host_phase_1.ok_or(
                RepositoryError::MissingArtifactKind(KnownArtifactKind::Host),
            )?,
            trampoline_phase_1: self.trampoline_phase_1.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::Trampoline,
                ),
            )?,
            trampoline_phase_2: self.trampoline_phase_2.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::Trampoline,
                ),
            )?,
            host_phase_2_hash: self.host_phase_2_hash.ok_or(
                RepositoryError::MissingArtifactKind(KnownArtifactKind::Host),
            )?,
            control_plane_hash: self.control_plane_hash.ok_or(
                RepositoryError::MissingArtifactKind(
                    KnownArtifactKind::ControlPlane,
                ),
            )?,
        };
        Ok(UpdatePlanBuildOutput {
            plan,
            by_id: self.by_id,
            by_hash: self.by_hash,
            rot_by_sign,
            artifacts_meta: self.artifacts_meta,
        })
    }
}

/// The output of [`UpdatePlanBuilder::build`].
pub struct UpdatePlanBuildOutput {
    pub plan: UpdatePlan,
    pub by_id: BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
    pub by_hash: HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
    pub rot_by_sign: HashMap<ArtifactId, Vec<u8>>,
    pub artifacts_meta: Vec<TufArtifactMeta>,
}

// We take id solely to be able to output error messages
fn read_hubris_sign_from_archive(
    id: ArtifactId,
    data: Vec<u8>,
) -> Result<(ArtifactId, Vec<u8>), RepositoryError> {
    let archive = match RawHubrisArchive::from_vec(data).map_err(Box::new) {
        Ok(archive) => archive,
        Err(error) => {
            return Err(RepositoryError::ParsingHubrisArchive { id, error });
        }
    };
    let caboose = match archive.read_caboose().map_err(Box::new) {
        Ok(caboose) => caboose,
        Err(error) => {
            return Err(RepositoryError::ReadHubrisCaboose { id, error });
        }
    };
    let sign = match caboose.sign() {
        Ok(sign) => sign,
        Err(error) => {
            return Err(RepositoryError::ReadHubrisCabooseBoard { id, error });
        }
    };
    Ok((id, sign.to_vec()))
}

// This function takes and returns `id` to avoid an unnecessary clone; `id` will
// be present in either the Ok tuple or the error.
fn read_hubris_board_from_archive(
    id: ArtifactId,
    data: Vec<u8>,
) -> Result<(ArtifactId, Board), RepositoryError> {
    let archive = match RawHubrisArchive::from_vec(data).map_err(Box::new) {
        Ok(archive) => archive,
        Err(error) => {
            return Err(RepositoryError::ParsingHubrisArchive { id, error });
        }
    };
    let caboose = match archive.read_caboose().map_err(Box::new) {
        Ok(caboose) => caboose,
        Err(error) => {
            return Err(RepositoryError::ReadHubrisCaboose { id, error });
        }
    };
    let board = match caboose.board() {
        Ok(board) => board,
        Err(error) => {
            return Err(RepositoryError::ReadHubrisCabooseBoard { id, error });
        }
    };
    let board = match std::str::from_utf8(board) {
        Ok(s) => s,
        Err(_) => {
            return Err(RepositoryError::ReadHubrisCabooseBoardUtf8(id));
        }
    };
    Ok((id, Board(board.to_string())))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use omicron_test_utils::dev::test_setup_log;
    use rand::{distributions::Standard, thread_rng, Rng};
    use sha2::{Digest, Sha256};
    use tufaceous_lib::{CompositeEntry, MtimeSource};

    fn make_random_bytes() -> Vec<u8> {
        thread_rng().sample_iter(Standard).take(128).collect()
    }

    struct RandomHostOsImage {
        phase1: Bytes,
        phase2: Bytes,
        tarball: Bytes,
    }

    fn make_random_host_os_image() -> RandomHostOsImage {
        use tufaceous_lib::CompositeHostArchiveBuilder;

        let phase1 = make_random_bytes();
        let phase2 = make_random_bytes();

        let mut builder =
            CompositeHostArchiveBuilder::new(Vec::new(), MtimeSource::Zero)
                .unwrap();
        builder
            .append_phase_1(CompositeEntry {
                data: &phase1,
                mtime_source: MtimeSource::Zero,
            })
            .unwrap();
        builder
            .append_phase_2(CompositeEntry {
                data: &phase2,
                mtime_source: MtimeSource::Zero,
            })
            .unwrap();

        let tarball = builder.finish().unwrap();

        RandomHostOsImage {
            phase1: Bytes::from(phase1),
            phase2: Bytes::from(phase2),
            tarball: Bytes::from(tarball),
        }
    }

    struct RandomRotImage {
        archive_a: Bytes,
        archive_b: Bytes,
        tarball: Bytes,
    }

    fn make_random_rot_image(sign: &str, board: &str) -> RandomRotImage {
        use tufaceous_lib::CompositeRotArchiveBuilder;

        let archive_a = make_fake_rot_image(sign, board);
        let archive_b = make_fake_rot_image(sign, board);

        let mut builder =
            CompositeRotArchiveBuilder::new(Vec::new(), MtimeSource::Zero)
                .unwrap();
        builder
            .append_archive_a(CompositeEntry {
                data: &archive_a,
                mtime_source: MtimeSource::Zero,
            })
            .unwrap();
        builder
            .append_archive_b(CompositeEntry {
                data: &archive_b,
                mtime_source: MtimeSource::Zero,
            })
            .unwrap();

        let tarball = builder.finish().unwrap();

        RandomRotImage {
            archive_a: Bytes::from(archive_a),
            archive_b: Bytes::from(archive_b),
            tarball: Bytes::from(tarball),
        }
    }

    fn make_fake_rot_image(sign: &str, board: &str) -> Vec<u8> {
        use hubtools::{CabooseBuilder, HubrisArchiveBuilder};

        let caboose = CabooseBuilder::default()
            .git_commit("this-is-fake-data")
            .board(board)
            .version("0.0.0")
            .name("rot-bord")
            .sign(sign)
            .build();

        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(caboose.as_slice()).unwrap();
        builder.build_to_vec().unwrap()
    }

    fn make_fake_sp_image(board: &str) -> Vec<u8> {
        use hubtools::{CabooseBuilder, HubrisArchiveBuilder};

        let caboose = CabooseBuilder::default()
            .git_commit("this-is-fake-data")
            .board(board)
            .version("0.0.0")
            .name(board)
            .build();

        let mut builder = HubrisArchiveBuilder::with_fake_image();
        builder.write_caboose(caboose.as_slice()).unwrap();
        builder.build_to_vec().unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bad_rot_versions() {
        const VERSION_0: SemverVersion = SemverVersion::new(0, 0, 0);
        const VERSION_1: SemverVersion = SemverVersion::new(0, 0, 1);

        let logctx = test_setup_log("test_multi_rot_version");

        let mut plan_builder =
            UpdatePlanBuilder::new(VERSION_0, &logctx.log).unwrap();

        // The control plane artifact can be arbitrary bytes; just populate it
        // with random data.
        {
            let kind = KnownArtifactKind::ControlPlane;
            let data = make_random_bytes();
            let hash = ArtifactHash(Sha256::digest(&data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(Bytes::from(data))]),
                )
                .await
                .unwrap();
        }

        // For each SP image, we'll insert two artifacts: these should end up in
        // the update plan's SP image maps keyed by their "board". Normally the
        // board is read from the archive itself via hubtools; we'll inject a
        // test function that returns the artifact ID name as the board instead.
        for (kind, boards) in [
            (KnownArtifactKind::GimletSp, ["test-gimlet-a", "test-gimlet-b"]),
            (KnownArtifactKind::PscSp, ["test-psc-a", "test-psc-b"]),
            (KnownArtifactKind::SwitchSp, ["test-switch-a", "test-switch-b"]),
        ] {
            for board in boards {
                let data = make_fake_sp_image(board);
                let hash = ArtifactHash(Sha256::digest(&data).into());
                let id = ArtifactId {
                    name: board.to_string(),
                    version: VERSION_0,
                    kind: kind.into(),
                };
                plan_builder
                    .add_artifact(
                        id,
                        hash,
                        futures::stream::iter([Ok(Bytes::from(data))]),
                    )
                    .await
                    .unwrap();
            }
        }

        // The Host, Trampoline, and RoT artifacts must be structed the way we
        // expect (i.e., .tar.gz's containing multiple inner artifacts).
        let host = make_random_host_os_image();
        let trampoline = make_random_host_os_image();

        for (kind, image) in [
            (KnownArtifactKind::Host, &host),
            (KnownArtifactKind::Trampoline, &trampoline),
        ] {
            let data = &image.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        let gimlet_rot = make_random_rot_image("gimlet", "gimlet");
        let psc_rot = make_random_rot_image("psc", "psc");
        let sidecar_rot = make_random_rot_image("sidecar", "sidecar");

        let gimlet_rot_2 = make_random_rot_image("gimlet", "gimlet-the second");

        for (kind, artifact) in [
            (KnownArtifactKind::GimletRot, &gimlet_rot),
            (KnownArtifactKind::PscRot, &psc_rot),
            (KnownArtifactKind::SwitchRot, &sidecar_rot),
        ] {
            let data = &artifact.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        let bad_kind = KnownArtifactKind::GimletRot;
        let data = &gimlet_rot_2.tarball;
        let hash = ArtifactHash(Sha256::digest(data).into());
        let id = ArtifactId {
            name: format!("{bad_kind:?}"),
            version: VERSION_1,
            kind: bad_kind.into(),
        };
        plan_builder
            .add_artifact(id, hash, futures::stream::iter([Ok(data.clone())]))
            .await
            .unwrap();

        match plan_builder.build() {
            Err(_) => (),
            Ok(_) => panic!("Added two artifacts with the same version"),
        }
        logctx.cleanup_successful();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_rot_version() {
        const VERSION_0: SemverVersion = SemverVersion::new(0, 0, 0);
        const VERSION_1: SemverVersion = SemverVersion::new(0, 0, 1);

        let logctx = test_setup_log("test_multi_rot_version");

        let mut plan_builder =
            UpdatePlanBuilder::new("0.0.0".parse().unwrap(), &logctx.log)
                .unwrap();

        // The control plane artifact can be arbitrary bytes; just populate it
        // with random data.
        {
            let kind = KnownArtifactKind::ControlPlane;
            let data = make_random_bytes();
            let hash = ArtifactHash(Sha256::digest(&data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(Bytes::from(data))]),
                )
                .await
                .unwrap();
        }

        // For each SP image, we'll insert two artifacts: these should end up in
        // the update plan's SP image maps keyed by their "board". Normally the
        // board is read from the archive itself via hubtools; we'll inject a
        // test function that returns the artifact ID name as the board instead.
        for (kind, boards) in [
            (KnownArtifactKind::GimletSp, ["test-gimlet-a", "test-gimlet-b"]),
            (KnownArtifactKind::PscSp, ["test-psc-a", "test-psc-b"]),
            (KnownArtifactKind::SwitchSp, ["test-switch-a", "test-switch-b"]),
        ] {
            for board in boards {
                let data = make_fake_sp_image(board);
                let hash = ArtifactHash(Sha256::digest(&data).into());
                let id = ArtifactId {
                    name: board.to_string(),
                    version: VERSION_0,
                    kind: kind.into(),
                };
                plan_builder
                    .add_artifact(
                        id,
                        hash,
                        futures::stream::iter([Ok(Bytes::from(data))]),
                    )
                    .await
                    .unwrap();
            }
        }

        // The Host, Trampoline, and RoT artifacts must be structed the way we
        // expect (i.e., .tar.gz's containing multiple inner artifacts).
        let host = make_random_host_os_image();
        let trampoline = make_random_host_os_image();

        for (kind, image) in [
            (KnownArtifactKind::Host, &host),
            (KnownArtifactKind::Trampoline, &trampoline),
        ] {
            let data = &image.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        let gimlet_rot = make_random_rot_image("gimlet", "gimlet");
        let psc_rot = make_random_rot_image("psc", "psc");
        let sidecar_rot = make_random_rot_image("sidecar", "sidecar");

        let gimlet_rot_2 = make_random_rot_image("gimlet2", "gimlet");
        let psc_rot_2 = make_random_rot_image("psc2", "psc");
        let sidecar_rot_2 = make_random_rot_image("sidecar2", "sidecar");

        for (kind, artifact) in [
            (KnownArtifactKind::GimletRot, &gimlet_rot),
            (KnownArtifactKind::PscRot, &psc_rot),
            (KnownArtifactKind::SwitchRot, &sidecar_rot),
        ] {
            let data = &artifact.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        for (kind, artifact) in [
            (KnownArtifactKind::GimletRot, &gimlet_rot_2),
            (KnownArtifactKind::PscRot, &psc_rot_2),
            (KnownArtifactKind::SwitchRot, &sidecar_rot_2),
        ] {
            let data = &artifact.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_1,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        let UpdatePlanBuildOutput { plan, .. } = plan_builder.build().unwrap();

        assert_eq!(plan.gimlet_rot_a.len(), 2);
        assert_eq!(plan.gimlet_rot_b.len(), 2);
        assert_eq!(plan.psc_rot_a.len(), 2);
        assert_eq!(plan.psc_rot_b.len(), 2);
        assert_eq!(plan.sidecar_rot_a.len(), 2);
        assert_eq!(plan.sidecar_rot_b.len(), 2);
        logctx.cleanup_successful();
    }

    // See documentation for extract_nested_artifact_pair for why multi_thread
    // is required.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_update_plan_from_artifacts() {
        const VERSION_0: SemverVersion = SemverVersion::new(0, 0, 0);

        let logctx = test_setup_log("test_update_plan_from_artifacts");

        let mut plan_builder =
            UpdatePlanBuilder::new("0.0.0".parse().unwrap(), &logctx.log)
                .unwrap();

        // Add a couple artifacts with kinds wicketd/nexus don't understand; it
        // should still ingest and serve them.
        let mut expected_unknown_artifacts = BTreeSet::new();

        for kind in ["test-kind-1", "test-kind-2"] {
            let data = make_random_bytes();
            let hash = ArtifactHash(Sha256::digest(&data).into());
            let id = ArtifactId {
                name: kind.to_string(),
                version: VERSION_0,
                kind: kind.parse().unwrap(),
            };
            expected_unknown_artifacts.insert(id.clone());
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(Bytes::from(data))]),
                )
                .await
                .unwrap();
        }

        // The control plane artifact can be arbitrary bytes; just populate it
        // with random data.
        {
            let kind = KnownArtifactKind::ControlPlane;
            let data = make_random_bytes();
            let hash = ArtifactHash(Sha256::digest(&data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(Bytes::from(data))]),
                )
                .await
                .unwrap();
        }

        // For each SP image, we'll insert two artifacts: these should end up in
        // the update plan's SP image maps keyed by their "board". Normally the
        // board is read from the archive itself via hubtools; we'll inject a
        // test function that returns the artifact ID name as the board instead.
        for (kind, boards) in [
            (KnownArtifactKind::GimletSp, ["test-gimlet-a", "test-gimlet-b"]),
            (KnownArtifactKind::PscSp, ["test-psc-a", "test-psc-b"]),
            (KnownArtifactKind::SwitchSp, ["test-switch-a", "test-switch-b"]),
        ] {
            for board in boards {
                let data = make_fake_sp_image(board);
                let hash = ArtifactHash(Sha256::digest(&data).into());
                let id = ArtifactId {
                    name: board.to_string(),
                    version: VERSION_0,
                    kind: kind.into(),
                };
                plan_builder
                    .add_artifact(
                        id,
                        hash,
                        futures::stream::iter([Ok(Bytes::from(data))]),
                    )
                    .await
                    .unwrap();
            }
        }

        // The Host, Trampoline, and RoT artifacts must be structed the way we
        // expect (i.e., .tar.gz's containing multiple inner artifacts).
        let host = make_random_host_os_image();
        let trampoline = make_random_host_os_image();

        for (kind, image) in [
            (KnownArtifactKind::Host, &host),
            (KnownArtifactKind::Trampoline, &trampoline),
        ] {
            let data = &image.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        let gimlet_rot = make_random_rot_image("gimlet", "gimlet");
        let psc_rot = make_random_rot_image("psc", "psc");
        let sidecar_rot = make_random_rot_image("sidecar", "sidecar");

        for (kind, artifact) in [
            (KnownArtifactKind::GimletRot, &gimlet_rot),
            (KnownArtifactKind::PscRot, &psc_rot),
            (KnownArtifactKind::SwitchRot, &sidecar_rot),
        ] {
            let data = &artifact.tarball;
            let hash = ArtifactHash(Sha256::digest(data).into());
            let id = ArtifactId {
                name: format!("{kind:?}"),
                version: VERSION_0,
                kind: kind.into(),
            };
            plan_builder
                .add_artifact(
                    id,
                    hash,
                    futures::stream::iter([Ok(data.clone())]),
                )
                .await
                .unwrap();
        }

        let UpdatePlanBuildOutput { plan, by_id, .. } =
            plan_builder.build().unwrap();

        assert_eq!(plan.gimlet_sp.len(), 2);
        assert_eq!(plan.psc_sp.len(), 2);
        assert_eq!(plan.sidecar_sp.len(), 2);

        for (id, hash_ids) in &by_id {
            let kind = match id.kind.to_known() {
                Some(kind) => kind,
                None => {
                    assert!(
                        expected_unknown_artifacts.remove(id),
                        "unexpected unknown artifact ID {id:?}"
                    );
                    continue;
                }
            };
            match kind {
                KnownArtifactKind::GimletSp => {
                    assert!(
                        id.name.starts_with("test-gimlet-"),
                        "unexpected id.name {:?}",
                        id.name
                    );
                    assert_eq!(hash_ids.len(), 1);
                    assert_eq!(
                        plan.gimlet_sp.get(&id.name).unwrap().data.hash(),
                        hash_ids[0].hash
                    );
                }
                KnownArtifactKind::ControlPlane => {
                    assert_eq!(hash_ids.len(), 1);
                    assert_eq!(plan.control_plane_hash, hash_ids[0].hash);
                }
                KnownArtifactKind::PscSp => {
                    assert!(
                        id.name.starts_with("test-psc-"),
                        "unexpected id.name {:?}",
                        id.name
                    );
                    assert_eq!(hash_ids.len(), 1);
                    assert_eq!(
                        plan.psc_sp.get(&id.name).unwrap().data.hash(),
                        hash_ids[0].hash
                    );
                }
                KnownArtifactKind::SwitchSp => {
                    assert!(
                        id.name.starts_with("test-switch-"),
                        "unexpected id.name {:?}",
                        id.name
                    );
                    assert_eq!(hash_ids.len(), 1);
                    assert_eq!(
                        plan.sidecar_sp.get(&id.name).unwrap().data.hash(),
                        hash_ids[0].hash
                    );
                }
                // These are special (we import their inner parts) and we'll
                // check them below.
                KnownArtifactKind::Host
                | KnownArtifactKind::Trampoline
                | KnownArtifactKind::GimletRot
                | KnownArtifactKind::PscRot
                | KnownArtifactKind::SwitchRot => {}
            }
        }

        // Check extracted host and trampoline data
        assert_eq!(read_to_vec(&plan.host_phase_1.data).await, host.phase1);
        assert_eq!(
            read_to_vec(&plan.trampoline_phase_1.data).await,
            trampoline.phase1
        );
        assert_eq!(
            read_to_vec(&plan.trampoline_phase_2.data).await,
            trampoline.phase2
        );

        let hash = Sha256::digest(&host.phase2);
        assert_eq!(plan.host_phase_2_hash.0, *hash);

        // Check extracted RoT data
        assert_eq!(
            read_to_vec(&plan.gimlet_rot_a[0].data).await,
            gimlet_rot.archive_a
        );
        assert_eq!(
            read_to_vec(&plan.gimlet_rot_b[0].data).await,
            gimlet_rot.archive_b
        );
        assert_eq!(
            read_to_vec(&plan.psc_rot_a[0].data).await,
            psc_rot.archive_a
        );
        assert_eq!(
            read_to_vec(&plan.psc_rot_b[0].data).await,
            psc_rot.archive_b
        );
        assert_eq!(
            read_to_vec(&plan.sidecar_rot_a[0].data).await,
            sidecar_rot.archive_a
        );
        assert_eq!(
            read_to_vec(&plan.sidecar_rot_b[0].data).await,
            sidecar_rot.archive_b
        );

        logctx.cleanup_successful();
    }

    async fn read_to_vec(data: &ExtractedArtifactDataHandle) -> Vec<u8> {
        let mut buf = Vec::with_capacity(data.file_size());
        let mut stream = data.reader_stream().await.unwrap();
        while let Some(data) = stream.next().await {
            let data = data.unwrap();
            buf.extend_from_slice(&data);
        }
        buf
    }
}
