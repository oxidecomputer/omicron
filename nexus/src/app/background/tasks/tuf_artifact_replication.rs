// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TUF Repo Depot: Artifact replication across sleds (RFD 424)
//!
//! `Nexus::updates_put_repository` accepts a TUF repository, which Nexus
//! unpacks, verifies, and reasons about the artifacts in. This uses temporary
//! storage within the Nexus zone. After that, the update artifacts have to
//! go somewhere else. We've settled for now on "everywhere": a copy of each
//! artifact is stored on each sled's M.2 devices.
//!
//! This background task is responsible for getting locally-stored artifacts
//! onto sleds, and ensuring all sleds have copies of all artifacts.
//! `Nexus::updates_put_repository` sends the [`ArtifactsWithPlan`] object to
//! this task via an [`mpsc`] channel and activates it. Once enough sleds have a
//! copy of each artifact in an `ArtifactsWithPlan`, the local copy is removed.
//!
//! # Task flow
//!
//! 1. The task moves `ArtifactsWithPlan` objects off the `mpsc` channel and
//!    into a `Vec` that represents the set of artifacts stored locally in this
//!    Nexus zone.
//! 2. The task fetches the list of artifacts from CockroachDB, and queries
//!    the list of artifacts stored on each sled. Sled artifact storage is
//!    content-addressed by SHA-256 checksum. Errors querying a sled are
//!    logged but otherwise ignored: the task proceeds as if that sled has no
//!    artifacts. (This means that the task will always be trying to replicate
//!    artifacts to that sled until it comes back or is pulled out of service.)
//! 3. The task builds a directory of all artifacts and where they can be found
//!    (local `ArtifactsWithPlan` and/or sled agents).
//! 4. If all the artifacts belonging to an `ArtifactsWithPlan` object have
//!    been replicated to at least `MIN_SLED_REPLICATION` sleds, the task drops
//!    the object from its `Vec` (thus cleaning up the local storage of those
//!    files).
//! 5. The task generates a list of requests that need to be sent:
//!    - PUT each locally-stored artifact not present on any sleds to
//!      `MIN_SLED_REPLICATION` random sleds.
//!    - For each partially-replicated artifact, choose a sled that is missing
//!      the artifact, and tell it (via `artifact_copy_from_depot`) to fetch the
//!      artifact from a random sled that has it.
//!    - DELETE all artifacts no longer tracked in CockroachDB from all sleds
//!      that have that artifact.
//!
//! # Rate limits
//!
//! To avoid doing everything at once, Nexus limits both concurrency
//! and the rate of its requests. Concurrency is controlled by
//! `MAX_REQUEST_CONCURRENCY`, which is intended to reduce bandwidth spikes for
//! PUT requests.
//!
//! TODO: (omicron#7400) In addition to Nexus concurrency rate limits, we should
//! also rate limit requests per second sent by Nexus, as well as limit the
//! number of ongoing copy requests being processed at once by Sled Agent.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use nexus_auth::context::OpContext;
use nexus_db_queries::db::{
    DataStore, datastore::SQL_BATCH_SIZE, pagination::Paginator,
};
use nexus_networking::sled_client_from_address;
use nexus_types::deployment::SledFilter;
use nexus_types::internal_api::background::{
    TufArtifactReplicationCounters, TufArtifactReplicationOperation,
    TufArtifactReplicationRequest, TufArtifactReplicationStatus,
};
use omicron_common::update::ArtifactHash;
use omicron_uuid_kinds::{GenericUuid, SledUuid};
use rand::seq::SliceRandom;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use update_common::artifacts::{
    ArtifactsWithPlan, ExtractedArtifactDataHandle,
};

use crate::app::background::BackgroundTask;

// The maximum number of requests to sleds to run at once. This is intended
// to reduce bandwidth spikes for PUT requests; other requests should return
// quickly.
const MAX_REQUEST_CONCURRENCY: usize = 8;
// The number of copies of an artifact we expect each sled to have. This is
// currently 2 because sleds store a copy of each artifact on each M.2 device.
const EXPECTED_COUNT: u32 = 2;
// How many recent requests to remember for debugging purposes. At 32 sleds
// and 64 artifacts per repo, this is enough to remember at least the most
// recent two repositories being replicated or deleted unless requests need to
// be retried.
const MAX_REQUEST_DEBUG_BUFFER_LEN: usize = 32 * 64 * 2;

#[derive(Debug)]
struct Sled {
    id: SledUuid,
    client: sled_agent_client::Client,
    depot_base_url: String,
}

#[derive(Debug)]
struct ArtifactPresence {
    /// The count of this artifact stored on each sled.
    sleds: BTreeMap<SledUuid, u32>,
    /// Handle to the artifact's local storage if present.
    local: Option<ArtifactHandle>,
    /// An artifact is wanted if it is listed in the `tuf_artifact` table.
    wanted: bool,
}

/// Wrapper enum for `ExtractedArtifactDataHandle` so that we don't need to
/// create and unpack TUF repos in unit tests. In `cfg(not(test))` this is
/// equivalent to a wrapper struct.
#[derive(Debug, Clone)]
enum ArtifactHandle {
    Extracted(ExtractedArtifactDataHandle),
    #[cfg(test)]
    Fake,
}

impl ArtifactHandle {
    async fn file(&self) -> std::io::Result<tokio::fs::File> {
        match self {
            ArtifactHandle::Extracted(handle) => handle.file().await,
            #[cfg(test)]
            ArtifactHandle::Fake => unimplemented!(),
        }
    }
}

#[derive(Debug, Default)]
struct Inventory(BTreeMap<ArtifactHash, ArtifactPresence>);

impl Inventory {
    fn into_requests<'a>(
        self,
        sleds: &'a [Sled],
        rng: &mut impl rand::Rng,
        min_sled_replication: usize,
    ) -> Requests<'a> {
        let mut requests = Requests::default();
        for (hash, presence) in self.0 {
            if presence.wanted {
                let (sleds_present, mut sleds_not_present) =
                    sleds.iter().partition::<Vec<_>, _>(|sled| {
                        presence
                            .sleds
                            .get(&sled.id)
                            .copied()
                            .unwrap_or_default()
                            > 0
                    });
                sleds_not_present.shuffle(rng);

                // If we have a local copy, PUT the artifact to more sleds until
                // we meet `MIN_SLED_REPLICATION`.
                let mut sled_puts = Vec::new();
                if let Some(handle) = presence.local {
                    let count = min_sled_replication
                        .saturating_sub(sleds_present.len());
                    for _ in 0..count {
                        let Some(sled) = sleds_not_present.pop() else {
                            break;
                        };
                        requests.put.push(Request::Put {
                            sled,
                            handle: handle.clone(),
                            hash,
                        });
                        sled_puts.push(sled);
                    }
                }

                // Tell each remaining sled missing the artifact to fetch it
                // from a random sled that has it.
                for target_sled in sleds_not_present {
                    if let Some(source_sled) =
                        sleds_present.choose(rng).copied()
                    {
                        requests.other.push(Request::CopyFromDepot {
                            target_sled,
                            source_sled,
                            hash,
                        });
                    } else {
                        // There are no sleds that currently have the artifact,
                        // but we might have PUT requests going out. Choose one
                        // of the sleds we are PUTting this artifact onto and
                        // schedule it after all PUT requests are done.
                        if let Some(source_sled) =
                            sled_puts.choose(rng).copied()
                        {
                            requests.copy_after_put.push(
                                Request::CopyFromDepot {
                                    target_sled,
                                    source_sled,
                                    hash,
                                },
                            );
                        }
                    }
                }

                // If there are sleds with 0 < n < `EXPECTED_COUNT` copies, tell
                // them to also fetch it from a random other sled.
                for target_sled in sleds {
                    let Some(count) = presence.sleds.get(&target_sled.id)
                    else {
                        continue;
                    };
                    if *count > 0 && *count < EXPECTED_COUNT {
                        let Ok(source_sled) = sleds_present
                            .choose_weighted(rng, |sled| {
                                if sled.id == target_sled.id { 0 } else { 1 }
                            })
                            .copied()
                        else {
                            break;
                        };
                        requests.recopy.push(Request::CopyFromDepot {
                            target_sled,
                            source_sled,
                            hash,
                        })
                    }
                }
            } else {
                // We don't want this artifact to be stored anymore, so tell all
                // sleds that have it to DELETE it.
                for sled in sleds {
                    if presence.sleds.contains_key(&sled.id) {
                        requests.other.push(Request::Delete { sled, hash });
                    }
                }
            }
        }

        requests.put.shuffle(rng);
        requests.copy_after_put.shuffle(rng);
        requests.recopy.shuffle(rng);
        requests.other.shuffle(rng);
        requests
    }
}

#[derive(Debug, Default)]
struct Requests<'a> {
    /// PUT requests are always highest priority.
    put: Vec<Request<'a>>,
    /// Copy requests are standard priority, but these copy requests depend on
    /// PUTs and need to be delayed until all PUTs are complete.
    copy_after_put: Vec<Request<'a>>,
    /// Requests to copy artifacts onto sleds that already have a copy, but not
    /// `EXPECTED_COUNT` copies, are lowest priority.
    recopy: Vec<Request<'a>>,
    /// Everything else; standard priority.
    other: Vec<Request<'a>>,
}

impl<'a> Requests<'a> {
    fn into_stream(
        self,
        log: &'a slog::Logger,
    ) -> impl Stream<
        Item = impl Future<Output = TufArtifactReplicationRequest> + use<'a>,
    > + use<'a> {
        // Create a `Semaphore` with `self.put.len()` permits. Each PUT
        // request is assigned a permit immediately, which we drop after
        // `Request::execute` returns. Before yielding any requests in
        // `copy_after_put` we acquire `self.put.len()` permits, thus waiting
        // until all PUT requests have completed.
        let put_len = self.put.len();
        let semaphore = Arc::new(Semaphore::new(put_len));
        let mut put_permits = Vec::new();
        for _ in 0..put_len {
            // This cannot fail: the semaphore is not closed and there are
            // `put_len` permits available.
            let permit = Arc::clone(&semaphore)
                .try_acquire_owned()
                .expect("semaphore should have an available permit");
            put_permits.push(permit);
        }

        let put = futures::stream::iter(self.put.into_iter().zip(put_permits))
            .map(|(request, permit)| request.execute(log, Some(permit)));
        let other =
            futures::stream::iter(self.other.into_iter().chain(self.recopy))
                .map(|request| request.execute(log, None));

        let copy_after_put = async move {
            // There's an awkward mix of usize and u32 in the `Semaphore`
            // API. If `put_len` somehow exceeds u32, don't run any of the
            // `copy_after_put` requests; we'll pick them up next time this
            // background task runs. (Otherwise we would run billions(?!) of
            // copy requests early.)
            let iter = if let Ok(put_len) = u32::try_from(put_len) {
                // Wait for all PUT requests to complete by acquiring `put_len`
                // permits from `semaphore`.
                let _permit = semaphore
                    .acquire_many(put_len)
                    .await
                    .expect("semaphore should not be closed");
                self.copy_after_put
            } else {
                Vec::new()
            };
            futures::stream::iter(iter)
                .map(|request| request.execute(log, None))
        }
        .flatten_stream();

        put.chain(futures::stream::select(copy_after_put, other))
    }
}

#[derive(Debug)]
enum Request<'a> {
    Put {
        sled: &'a Sled,
        handle: ArtifactHandle,
        hash: ArtifactHash,
    },
    CopyFromDepot {
        target_sled: &'a Sled,
        source_sled: &'a Sled,
        hash: ArtifactHash,
    },
    Delete {
        sled: &'a Sled,
        hash: ArtifactHash,
    },
}

impl Request<'_> {
    async fn execute(
        self,
        log: &slog::Logger,
        _permit: Option<OwnedSemaphorePermit>,
    ) -> TufArtifactReplicationRequest {
        let err: Option<Box<dyn std::error::Error>> = async {
            match &self {
                Request::Put { sled, handle, hash } => {
                    sled.client
                        .artifact_put(&hash.to_string(), handle.file().await?)
                        .await?;
                }
                Request::CopyFromDepot { target_sled, source_sled, hash } => {
                    target_sled
                    .client
                    .artifact_copy_from_depot(
                        &hash.to_string(),
                        &sled_agent_client::types::ArtifactCopyFromDepotBody {
                            depot_base_url: source_sled.depot_base_url.clone(),
                        },
                    )
                    .await?;
                }
                Request::Delete { sled, hash } => {
                    sled.client.artifact_delete(&hash.to_string()).await?;
                }
            };
            Ok(())
        }
        .await
        .err();

        let time = Utc::now();
        let (target_sled, hash) = match &self {
            Request::Put { sled, hash, .. }
            | Request::CopyFromDepot { target_sled: sled, hash, .. }
            | Request::Delete { sled, hash } => (sled, hash),
        };
        let msg = match (&self, err.is_some()) {
            (Request::Put { .. }, true) => "Failed to put artifact",
            (Request::Put { .. }, false) => "Successfully put artifact",
            (Request::CopyFromDepot { .. }, true) => {
                "Failed to request artifact copy from depot"
            }
            (Request::CopyFromDepot { .. }, false) => {
                "Successfully requested artifact copy from depot"
            }
            (Request::Delete { .. }, true) => "Failed to delete artifact",
            (Request::Delete { .. }, false) => "Successfully deleted artifact",
        };
        if let Some(ref err) = err {
            slog::warn!(
                log,
                "{msg}";
                "error" => InlineErrorChain::new(err.as_ref()),
                "sled" => target_sled.client.baseurl(),
                "sha256" => &hash.to_string(),
            );
        } else {
            slog::info!(
                log,
                "{msg}";
                "sled" => target_sled.client.baseurl(),
                "sha256" => &hash.to_string(),
            );
        }

        TufArtifactReplicationRequest {
            time,
            target_sled: target_sled.id,
            operation: match self {
                Request::Put { hash, .. } => {
                    TufArtifactReplicationOperation::Put { hash }
                }
                Request::CopyFromDepot { hash, source_sled, .. } => {
                    TufArtifactReplicationOperation::Copy {
                        hash,
                        source_sled: source_sled.id,
                    }
                }
                Request::Delete { hash, .. } => {
                    TufArtifactReplicationOperation::Delete { hash }
                }
            },
            error: err.map(|err| err.to_string()),
        }
    }
}

pub struct ArtifactReplication {
    datastore: Arc<DataStore>,
    local: Vec<ArtifactsWithPlan>,
    local_rx: mpsc::Receiver<ArtifactsWithPlan>,
    min_sled_replication: usize,
    /// List of recent requests for debugging.
    request_debug_ringbuf: Arc<VecDeque<TufArtifactReplicationRequest>>,
    lifetime_counters: TufArtifactReplicationCounters,
}

impl BackgroundTask for ArtifactReplication {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // Move any received artifacts from `local_rx` to `local`.
            loop {
                match self.local_rx.try_recv() {
                    Ok(artifacts) => self.local.push(artifacts),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        let err = "artifact replication receiver disconnected";
                        error!(&opctx.log, "{err}");
                        return json!({"error": err});
                    }
                }
            }

            let sleds = match self
                .list_sleds(opctx)
                .await
                .context("failed to list sleds")
            {
                Ok(sleds) => sleds,
                Err(err) => return json!({"error": format!("{err:#}")}),
            };
            let mut counters = TufArtifactReplicationCounters::default();
            let mut inventory = match self
                .list_artifacts_from_database(opctx)
                .await
                .context("failed to list artifacts from database")
            {
                Ok(inventory) => inventory,
                Err(err) => return json!({"error": format!("{err:#}")}),
            };
            self.list_artifacts_on_sleds(
                opctx,
                &sleds,
                &mut inventory,
                &mut counters,
            )
            .await;
            self.list_and_clean_up_local_artifacts(&mut inventory);

            let requests = inventory.into_requests(
                &sleds,
                &mut rand::thread_rng(),
                self.min_sled_replication,
            );
            let completed = requests
                .into_stream(&opctx.log)
                .buffer_unordered(MAX_REQUEST_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
            self.insert_debug_requests(completed, &mut counters);

            self.lifetime_counters += counters;
            serde_json::to_value(TufArtifactReplicationStatus {
                last_run_counters: counters,
                lifetime_counters: self.lifetime_counters,
                request_debug_ringbuf: self.request_debug_ringbuf.clone(),
                local_repos: self.local.len(),
            })
            .unwrap()
        }
        .boxed()
    }
}

impl ArtifactReplication {
    pub fn new(
        datastore: Arc<DataStore>,
        local_rx: mpsc::Receiver<ArtifactsWithPlan>,
        min_sled_replication: usize,
    ) -> ArtifactReplication {
        ArtifactReplication {
            datastore,
            local: Vec::new(),
            local_rx,
            min_sled_replication,
            request_debug_ringbuf: Arc::new(VecDeque::new()),
            lifetime_counters: TufArtifactReplicationCounters::default(),
        }
    }

    fn insert_debug_requests(
        &mut self,
        mut requests: Vec<TufArtifactReplicationRequest>,
        counters: &mut TufArtifactReplicationCounters,
    ) {
        for request in &requests {
            counters.inc(request);
        }

        // `Arc::make_mut` will either directly provide a mutable reference
        // if there are no other references, or clone it if there are. At this
        // point there should never be any other references; we only clone this
        // Arc when serializing the ringbuf to a `serde_json::Value`.
        let ringbuf = Arc::make_mut(&mut self.request_debug_ringbuf);
        let to_delete = (ringbuf.len() + requests.len())
            .saturating_sub(MAX_REQUEST_DEBUG_BUFFER_LEN);
        ringbuf.drain(0..to_delete);

        requests.sort();
        ringbuf.extend(requests);
    }

    async fn list_sleds(&self, opctx: &OpContext) -> Result<Vec<Sled>> {
        Ok(self
            .datastore
            .sled_list_all_batched(&opctx, SledFilter::TufArtifactReplication)
            .await?
            .into_iter()
            .map(|sled| Sled {
                id: SledUuid::from_untyped_uuid(sled.identity.id),
                client: sled_client_from_address(
                    sled.identity.id,
                    sled.address(),
                    &opctx.log,
                ),
                depot_base_url: format!(
                    "http://{}",
                    sled.address_with_port(sled.repo_depot_port.into())
                ),
            })
            .collect())
    }

    async fn list_artifacts_from_database(
        &self,
        opctx: &OpContext,
    ) -> Result<Inventory> {
        let mut inventory = Inventory::default();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .datastore
                .update_tuf_artifact_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|a| a.id.into_untyped_uuid());
            for artifact in batch {
                inventory.0.entry(artifact.sha256.0).or_insert_with(|| {
                    ArtifactPresence {
                        sleds: BTreeMap::new(),
                        local: None,
                        wanted: true,
                    }
                });
            }
        }
        Ok(inventory)
    }

    /// Ask all sled agents to list the artifacts they have, and mark those
    /// artifacts as present on those sleds.
    async fn list_artifacts_on_sleds(
        &mut self,
        opctx: &OpContext,
        sleds: &[Sled],
        inventory: &mut Inventory,
        counters: &mut TufArtifactReplicationCounters,
    ) {
        let responses =
            futures::future::join_all(sleds.iter().map(|sled| async move {
                let response = sled.client.artifact_list().await;
                (sled, Utc::now(), response)
            }))
            .await;
        let mut requests = Vec::new();
        for (sled, time, response) in responses {
            let mut error = None;
            match response {
                Ok(response) => {
                    info!(
                        &opctx.log,
                        "Successfully got artifact list";
                        "sled" => sled.client.baseurl(),
                    );
                    for (hash, count) in response.into_inner() {
                        let Ok(hash) = ArtifactHash::from_str(&hash) else {
                            error = Some(format!(
                                "sled reported bogus artifact hash {hash:?}"
                            ));
                            error!(
                                &opctx.log,
                                "Failed to get artifact list: \
                                sled reported bogus artifact hash";
                                "sled" => sled.client.baseurl(),
                                "bogus_hash" => hash,
                            );
                            continue;
                        };
                        let entry =
                            inventory.0.entry(hash).or_insert_with(|| {
                                ArtifactPresence {
                                    sleds: BTreeMap::new(),
                                    local: None,
                                    // If we're inserting, this artifact wasn't
                                    // listed in the database.
                                    wanted: false,
                                }
                            });
                        entry.sleds.insert(sled.id, count);
                    }
                }
                Err(err) => {
                    warn!(
                        &opctx.log,
                        "Failed to get artifact list";
                        "error" => InlineErrorChain::new(&err),
                        "sled" => sled.client.baseurl(),
                    );
                    error = Some(err.to_string());
                }
            };
            requests.push(TufArtifactReplicationRequest {
                time,
                target_sled: sled.id,
                operation: TufArtifactReplicationOperation::List,
                error,
            });
        }
        self.insert_debug_requests(requests, counters);
    }

    /// Fill in the `local` field on the values of `inventory` with any local
    /// artifacts, while removing the locally-stored `ArtifactsWithPlan` objects
    /// once they reach the minimum requirement to be considered replicated.
    fn list_and_clean_up_local_artifacts(&mut self, inventory: &mut Inventory) {
        self.local.retain(|plan| {
            let mut keep_plan = false;
            for hash_id in plan.by_id().values().flatten() {
                if let Some(handle) = plan.get_by_hash(hash_id) {
                    if let Some(presence) = inventory.0.get_mut(&hash_id.hash) {
                        presence.local =
                            Some(ArtifactHandle::Extracted(handle));
                        if presence.sleds.len() < self.min_sled_replication {
                            keep_plan = true;
                        }
                    }
                }
            }
            keep_plan
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use expectorate::assert_contents;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;

    const MIN_SLED_REPLICATION: usize = 3;

    /// Create a list of `Sled`s suitable for testing
    /// `Inventory::into_requests`. Neither the `client` or `depot_base_url`
    /// fields are ever used.
    fn fake_sleds(n: usize, rng: &mut impl rand::Rng) -> Vec<Sled> {
        (0..n)
            .map(|_| Sled {
                id: SledUuid::from_untyped_uuid(
                    uuid::Builder::from_random_bytes(rng.gen()).into_uuid(),
                ),
                client: sled_agent_client::Client::new(
                    "http://invalid.test",
                    slog::Logger::root(slog::Discard, slog::o!()),
                ),
                depot_base_url: String::new(),
            })
            .collect()
    }

    fn requests_to_string(requests: &Requests<'_>) -> String {
        let mut s = String::new();
        for vec in [
            &requests.put,
            &requests.copy_after_put,
            &requests.recopy,
            &requests.other,
        ] {
            for request in vec {
                match request {
                    Request::Put { sled, hash, .. } => {
                        writeln!(s, "- PUT {hash}\n  to {}", sled.id)
                    }
                    Request::CopyFromDepot {
                        target_sled,
                        source_sled,
                        hash,
                    } => {
                        writeln!(
                            s,
                            "- COPY {hash}\n  from {}\n  to {}",
                            source_sled.id, target_sled.id,
                        )
                    }
                    Request::Delete { sled, hash } => {
                        writeln!(s, "- DELETE {hash}\n  from {}", sled.id)
                    }
                }
                .unwrap();
            }
        }
        s
    }

    fn check_consistency(requests: &Requests<'_>) {
        // Everything in `put` should be `Put`.
        for request in &requests.put {
            assert!(
                matches!(request, Request::Put { .. }),
                "request in `put` is not `Put`: {request:?}"
            );
        }
        // Everything in `copy_after_put` should be `CopyFromDepot`, and should
        // refer to an artifact that was put onto that sled in `put`.
        for request in &requests.copy_after_put {
            if let Request::CopyFromDepot {
                source_sled, hash: copy_hash, ..
            } = request
            {
                assert!(
                    requests.put.iter().any(|request| match request {
                        Request::Put { sled, hash, .. } => {
                            sled.id == source_sled.id && *hash == *copy_hash
                        }
                        _ => false,
                    }),
                    "request in `copy_after_put` does not follow from \
                    any request in `put`: {request:?}"
                );
            } else {
                panic!(
                    "request in `copy_after_put` is not `CopyFromDepot`: \
                    {request:?}"
                );
            }
        }
        // Everything in `recopy` should be `Copy`.
        for request in &requests.recopy {
            assert!(
                matches!(request, Request::CopyFromDepot { .. }),
                "request in `recopy` is not `CopyFromDepot`: {request:?}"
            );
        }
        // Everything in `other` should be `Copy` or `Delete`.
        for request in &requests.other {
            assert!(
                matches!(
                    request,
                    Request::CopyFromDepot { .. } | Request::Delete { .. }
                ),
                "request in `other` is not `CopyFromDepot` or `Delete`: \
                {request:?}"
            );
        }
    }

    #[test]
    fn simple_replicate() {
        // Replicate 2 local artifacts onto some number of sleds.
        let mut rng = StdRng::from_seed(Default::default());
        let sleds = fake_sleds(MIN_SLED_REPLICATION + 13, &mut rng);
        let mut inventory = BTreeMap::new();
        for _ in 0..2 {
            inventory.insert(
                ArtifactHash(rng.gen()),
                ArtifactPresence {
                    sleds: BTreeMap::new(),
                    local: Some(ArtifactHandle::Fake),
                    wanted: true,
                },
            );
        }
        let requests = Inventory(inventory).into_requests(
            &sleds,
            &mut rng,
            MIN_SLED_REPLICATION,
        );
        check_consistency(&requests);
        assert_eq!(requests.put.len(), MIN_SLED_REPLICATION * 2);
        assert_eq!(requests.copy_after_put.len(), 13 * 2);
        assert_eq!(requests.recopy.len(), 0);
        assert_eq!(requests.other.len(), 0);
        assert_contents(
            "tests/tuf-replication/simple_replicate.txt",
            &requests_to_string(&requests),
        );
    }

    #[test]
    fn new_sled() {
        // Ten artifacts are replicated across 4 sleds, and 1 sled has no
        // artifacts.
        let mut rng = StdRng::from_seed(Default::default());
        let sleds = fake_sleds(5, &mut rng);
        let mut sled_presence = BTreeMap::new();
        for sled in &sleds[..4] {
            sled_presence.insert(sled.id, 2);
        }
        let mut inventory = BTreeMap::new();
        for _ in 0..10 {
            inventory.insert(
                ArtifactHash(rng.gen()),
                ArtifactPresence {
                    sleds: sled_presence.clone(),
                    local: None,
                    wanted: true,
                },
            );
        }
        let requests = Inventory(inventory).into_requests(
            &sleds,
            &mut rng,
            MIN_SLED_REPLICATION,
        );
        check_consistency(&requests);
        assert_eq!(requests.put.len(), 0);
        assert_eq!(requests.copy_after_put.len(), 0);
        assert_eq!(requests.recopy.len(), 0);
        assert_eq!(requests.other.len(), 10);
        assert_contents(
            "tests/tuf-replication/new_sled.txt",
            &requests_to_string(&requests),
        );
    }

    #[test]
    fn delete() {
        // 4 sleds have an artifact we don't want anymore.
        let mut rng = StdRng::from_seed(Default::default());
        let sleds = fake_sleds(4, &mut rng);
        let mut inventory = BTreeMap::new();
        inventory.insert(
            ArtifactHash(rng.gen()),
            ArtifactPresence {
                sleds: sleds.iter().map(|sled| (sled.id, 2)).collect(),
                local: None,
                wanted: false,
            },
        );
        let requests = Inventory(inventory).into_requests(
            &sleds,
            &mut rng,
            MIN_SLED_REPLICATION,
        );
        check_consistency(&requests);
        assert_eq!(requests.put.len(), 0);
        assert_eq!(requests.copy_after_put.len(), 0);
        assert_eq!(requests.recopy.len(), 0);
        assert_eq!(requests.other.len(), 4);
        assert!(
            requests
                .other
                .iter()
                .all(|request| matches!(request, Request::Delete { .. })),
            "not all requests are deletes"
        );
        assert_contents(
            "tests/tuf-replication/delete.txt",
            &requests_to_string(&requests),
        );
    }

    #[test]
    fn recopy() {
        // 3 sleds have two copies of an artifact; 1 has a single copy.
        let mut rng = StdRng::from_seed(Default::default());
        let sleds = fake_sleds(4, &mut rng);
        let mut inventory = BTreeMap::new();
        inventory.insert(
            ArtifactHash(rng.gen()),
            ArtifactPresence {
                sleds: sleds
                    .iter()
                    .enumerate()
                    .map(|(i, sled)| (sled.id, if i == 0 { 1 } else { 2 }))
                    .collect(),
                local: None,
                wanted: true,
            },
        );
        let requests = Inventory(inventory).into_requests(
            &sleds,
            &mut rng,
            MIN_SLED_REPLICATION,
        );
        check_consistency(&requests);
        assert_eq!(requests.put.len(), 0);
        assert_eq!(requests.copy_after_put.len(), 0);
        assert_eq!(requests.recopy.len(), 1);
        assert_eq!(requests.other.len(), 0);
        assert_contents(
            "tests/tuf-replication/recopy.txt",
            &requests_to_string(&requests),
        );
    }

    #[test]
    fn nothing() {
        // 4 sleds each have two copies of an artifact; there's nothing to do.
        let mut rng = StdRng::from_seed(Default::default());
        let sleds = fake_sleds(4, &mut rng);
        let mut inventory = BTreeMap::new();
        inventory.insert(
            ArtifactHash(rng.gen()),
            ArtifactPresence {
                sleds: sleds.iter().map(|sled| (sled.id, 2)).collect(),
                local: None,
                wanted: true,
            },
        );
        let requests = Inventory(inventory).into_requests(
            &sleds,
            &mut rng,
            MIN_SLED_REPLICATION,
        );
        check_consistency(&requests);
        assert_eq!(requests.put.len(), 0);
        assert_eq!(requests.copy_after_put.len(), 0);
        assert_eq!(requests.recopy.len(), 0);
        assert_eq!(requests.other.len(), 0);
    }
}
