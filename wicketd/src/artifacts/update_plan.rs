// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Constructor for the `UpdatePlan` wicketd uses to drive sled mupdates.
//!
//! This is a "plan" in name only: it is a strict list of which artifacts to
//! apply to which components; the ordering and application of the plan lives
//! elsewhere.

use super::error::RepositoryError;
use super::extracted_artifacts::ExtractedArtifacts;
use super::extracted_artifacts::HashingNamedUtf8TempFile;
use super::ArtifactIdData;
use super::Board;
use super::ExtractedArtifactDataHandle;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use hubtools::RawHubrisArchive;
use omicron_common::api::external::SemverVersion;
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
use tufaceous_lib::HostPhaseImages;
use tufaceous_lib::RotArchives;

/// The update plan currently in effect.
///
/// Exposed for testing.
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub(crate) system_version: SemverVersion,
    pub(crate) gimlet_sp: BTreeMap<Board, ArtifactIdData>,
    pub(crate) gimlet_rot_a: Vec<ArtifactIdData>,
    pub(crate) gimlet_rot_b: Vec<ArtifactIdData>,
    pub(crate) psc_sp: BTreeMap<Board, ArtifactIdData>,
    pub(crate) psc_rot_a: Vec<ArtifactIdData>,
    pub(crate) psc_rot_b: Vec<ArtifactIdData>,
    pub(crate) sidecar_sp: BTreeMap<Board, ArtifactIdData>,
    pub(crate) sidecar_rot_a: Vec<ArtifactIdData>,
    pub(crate) sidecar_rot_b: Vec<ArtifactIdData>,

    // Note: The Trampoline image is broken into phase1/phase2 as part of our
    // update plan (because they go to different destinations), but the two
    // phases will have the _same_ `ArtifactId` (i.e., the ID of the Host
    // artifact from the TUF repository.
    //
    // The same would apply to the host phase1/phase2, but we don't actually
    // need the `host_phase_2` data as part of this plan (we serve it from the
    // artifact server instead).
    pub(crate) host_phase_1: ArtifactIdData,
    pub(crate) trampoline_phase_1: ArtifactIdData,
    pub(crate) trampoline_phase_2: ArtifactIdData,

    // We need to send installinator the hash of the host_phase_2 data it should
    // fetch from us; we compute it while generating the plan.
    //
    // This is exposed for testing.
    pub host_phase_2_hash: ArtifactHash,

    // We also need to send installinator the hash of the control_plane image it
    // should fetch from us. This is already present in the TUF repository, but
    // we record it here for use by the update process.
    //
    // This is exposed for testing.
    pub control_plane_hash: ArtifactHash,
}

/// `UpdatePlanBuilder` mirrors all the fields of `UpdatePlan`, but they're all
/// optional: it can be filled in as we read a TUF repository.
/// [`UpdatePlanBuilder::build()`] will (fallibly) convert from the builder to
/// the final plan.
#[derive(Debug)]
pub(super) struct UpdatePlanBuilder<'a> {
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

    // extra fields we use to build the plan
    extracted_artifacts: ExtractedArtifacts,
    log: &'a Logger,
}

impl<'a> UpdatePlanBuilder<'a> {
    pub(super) fn new(
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

            extracted_artifacts,
            log,
        })
    }

    pub(super) async fn add_artifact(
        &mut self,
        artifact_id: ArtifactId,
        artifact_hash: ArtifactHash,
        stream: impl Stream<Item = Result<bytes::Bytes, tough::error::Error>> + Send,
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
    ) -> Result<(), RepositoryError> {
        // If we don't know this artifact kind, we'll still serve it up by hash,
        // but we don't do any further processing on it.
        let Some(artifact_kind) = artifact_id.kind.to_known() else {
            return self
                .add_unknown_artifact(
                    artifact_id,
                    artifact_hash,
                    stream,
                    by_id,
                    by_hash,
                )
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
                    by_id,
                    by_hash,
                )
                .await
            }
            KnownArtifactKind::GimletRot
            | KnownArtifactKind::PscRot
            | KnownArtifactKind::SwitchRot => {
                self.add_rot_artifact(
                    artifact_id,
                    artifact_kind,
                    stream,
                    by_id,
                    by_hash,
                )
                .await
            }
            KnownArtifactKind::Host => {
                self.add_host_artifact(artifact_id, stream, by_id, by_hash)
            }
            KnownArtifactKind::Trampoline => self.add_trampoline_artifact(
                artifact_id,
                stream,
                by_id,
                by_hash,
            ),
            KnownArtifactKind::ControlPlane => {
                self.add_control_plane_artifact(
                    artifact_id,
                    artifact_hash,
                    stream,
                    by_id,
                    by_hash,
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
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
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

        record_extracted_artifact(
            artifact_id,
            by_id,
            by_hash,
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
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
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

        record_extracted_artifact(
            artifact_id.clone(),
            by_id,
            by_hash,
            rot_a_data,
            rot_a_kind,
            self.log,
        )?;
        record_extracted_artifact(
            artifact_id,
            by_id,
            by_hash,
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
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
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

        record_extracted_artifact(
            artifact_id.clone(),
            by_id,
            by_hash,
            phase_1_data,
            ArtifactKind::HOST_PHASE_1,
            self.log,
        )?;
        record_extracted_artifact(
            artifact_id,
            by_id,
            by_hash,
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
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
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

        record_extracted_artifact(
            artifact_id.clone(),
            by_id,
            by_hash,
            phase_1_data,
            ArtifactKind::TRAMPOLINE_PHASE_1,
            self.log,
        )?;
        record_extracted_artifact(
            artifact_id,
            by_id,
            by_hash,
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
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
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

        record_extracted_artifact(
            artifact_id,
            by_id,
            by_hash,
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
        by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
        by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
    ) -> Result<(), RepositoryError> {
        let artifact_kind = artifact_id.kind.clone();
        let artifact_hash_id =
            ArtifactHashId { kind: artifact_kind.clone(), hash: artifact_hash };

        let data =
            self.extracted_artifacts.store(artifact_hash_id, stream).await?;

        record_extracted_artifact(
            artifact_id,
            by_id,
            by_hash,
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

    pub(super) fn build(self) -> Result<UpdatePlan, RepositoryError> {
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

        // Ensure that all A/B RoT images for each board kind have the same
        // version number.
        for (kind, mut single_board_rot_artifacts) in [
            (
                KnownArtifactKind::GimletRot,
                self.gimlet_rot_a.iter().chain(&self.gimlet_rot_b),
            ),
            (
                KnownArtifactKind::PscRot,
                self.psc_rot_a.iter().chain(&self.psc_rot_b),
            ),
            (
                KnownArtifactKind::SwitchRot,
                self.sidecar_rot_a.iter().chain(&self.sidecar_rot_b),
            ),
        ] {
            // We know each of these iterators has at least 2 elements (one from
            // the A artifacts and one from the B artifacts, checked above) so
            // we can safely unwrap the first.
            let version =
                &single_board_rot_artifacts.next().unwrap().id.version;
            for artifact in single_board_rot_artifacts {
                if artifact.id.version != *version {
                    return Err(RepositoryError::MultipleVersionsPresent {
                        kind,
                        v1: version.clone(),
                        v2: artifact.id.version.clone(),
                    });
                }
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

        Ok(UpdatePlan {
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
        })
    }
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

// Record an artifact in `by_id` and `by_hash`, or fail if either already has an
// entry for this id/hash.
fn record_extracted_artifact(
    tuf_repo_artifact_id: ArtifactId,
    by_id: &mut BTreeMap<ArtifactId, Vec<ArtifactHashId>>,
    by_hash: &mut HashMap<ArtifactHashId, ExtractedArtifactDataHandle>,
    data: ExtractedArtifactDataHandle,
    data_kind: ArtifactKind,
    log: &Logger,
) -> Result<(), RepositoryError> {
    use std::collections::hash_map::Entry;

    let artifact_hash_id =
        ArtifactHashId { kind: data_kind, hash: data.hash() };

    let by_hash_slot = match by_hash.entry(artifact_hash_id) {
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

    by_id
        .entry(tuf_repo_artifact_id)
        .or_default()
        .push(by_hash_slot.key().clone());
    by_hash_slot.insert(data);

    Ok(())
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

        let mut builder = CompositeHostArchiveBuilder::new(Vec::new()).unwrap();
        builder.append_phase_1(phase1.len(), phase1.as_slice()).unwrap();
        builder.append_phase_2(phase2.len(), phase2.as_slice()).unwrap();

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

    fn make_random_rot_image() -> RandomRotImage {
        use tufaceous_lib::CompositeRotArchiveBuilder;

        let archive_a = make_random_bytes();
        let archive_b = make_random_bytes();

        let mut builder = CompositeRotArchiveBuilder::new(Vec::new()).unwrap();
        builder
            .append_archive_a(archive_a.len(), archive_a.as_slice())
            .unwrap();
        builder
            .append_archive_b(archive_b.len(), archive_b.as_slice())
            .unwrap();

        let tarball = builder.finish().unwrap();

        RandomRotImage {
            archive_a: Bytes::from(archive_a),
            archive_b: Bytes::from(archive_b),
            tarball: Bytes::from(tarball),
        }
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

    // See documentation for extract_nested_artifact_pair for why multi_thread
    // is required.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_update_plan_from_artifacts() {
        const VERSION_0: SemverVersion = SemverVersion::new(0, 0, 0);

        let logctx = test_setup_log("test_update_plan_from_artifacts");

        let mut by_id = BTreeMap::new();
        let mut by_hash = HashMap::new();
        let mut plan_builder =
            UpdatePlanBuilder::new("0.0.0".parse().unwrap(), &logctx.log)
                .unwrap();

        // Add a couple artifacts with kinds wicketd doesn't understand; it
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
                    &mut by_id,
                    &mut by_hash,
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
                    &mut by_id,
                    &mut by_hash,
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
                        &mut by_id,
                        &mut by_hash,
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
                    &mut by_id,
                    &mut by_hash,
                )
                .await
                .unwrap();
        }

        let gimlet_rot = make_random_rot_image();
        let psc_rot = make_random_rot_image();
        let sidecar_rot = make_random_rot_image();

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
                    &mut by_id,
                    &mut by_hash,
                )
                .await
                .unwrap();
        }

        let plan = plan_builder.build().unwrap();

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
