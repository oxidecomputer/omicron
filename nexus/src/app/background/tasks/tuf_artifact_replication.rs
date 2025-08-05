// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TUF Repo Depot: Artifact replication across sleds (RFD 424)
//!
//! See docs/tuf-artifact-replication.adoc for an architectural overview of the
//! TUF artifact replication system.
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
//! 2. The task fetches the artifact configuration (list of artifacts and
//!    generation number) from CockroachDB.
//! 3. The task puts the artifact configuration to each sled, and queries
//!    the list of artifacts stored on each sled. Sled artifact storage is
//!    content-addressed by SHA-256 checksum. Errors querying a sled are
//!    logged but otherwise ignored: the task proceeds as if that sled has no
//!    artifacts. (This means that the task will always be trying to replicate
//!    artifacts to that sled until it comes back or is pulled out of service.)
//! 4. The task builds a list of all artifacts and where they can be found
//!    (local `ArtifactsWithPlan` and/or sled agents).
//! 5. If all the artifacts belonging to an `ArtifactsWithPlan` object have
//!    been replicated to at least `MIN_SLED_REPLICATION` sleds, the task drops
//!    the object from its `Vec` (thus cleaning up the local storage of those
//!    files).
//! 6. The task generates a list of requests that need to be sent:
//!    - PUT each locally-stored artifact not present on any sleds to
//!      `MIN_SLED_REPLICATION` random sleds.
//!    - For each partially-replicated artifact, choose a sled that is missing
//!      the artifact, and tell it (via `artifact_copy_from_depot`) to fetch the
//!      artifact from a random sled that has it.
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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use http::StatusCode;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_networking::sled_client_from_address;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::{
    TufArtifactReplicationCounters, TufArtifactReplicationOperation,
    TufArtifactReplicationRequest, TufArtifactReplicationStatus,
};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::SledUuid;
use rand::seq::{IndexedRandom, SliceRandom};
use serde_json::json;
use sled_agent_client::types::ArtifactConfig;
use slog_error_chain::InlineErrorChain;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tufaceous_artifact::ArtifactHash;
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
            let (sleds_present, mut sleds_not_present) =
                sleds.iter().partition::<Vec<_>, _>(|sled| {
                    presence.sleds.get(&sled.id).copied().unwrap_or_default()
                        > 0
                });
            sleds_not_present.shuffle(rng);

            // If we have a local copy, PUT the artifact to more sleds until
            // we meet `MIN_SLED_REPLICATION`.
            let mut sled_puts = Vec::new();
            if let Some(handle) = presence.local {
                let count =
                    min_sled_replication.saturating_sub(sleds_present.len());
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
                if let Some(source_sled) = sleds_present.choose(rng).copied() {
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
                    if let Some(source_sled) = sled_puts.choose(rng).copied() {
                        requests.copy_after_put.push(Request::CopyFromDepot {
                            target_sled,
                            source_sled,
                            hash,
                        });
                    }
                }
            }

            // If there are sleds with 0 < n < `EXPECTED_COUNT` copies, tell
            // them to also fetch it from a random other sled.
            for target_sled in sleds {
                let Some(count) = presence.sleds.get(&target_sled.id) else {
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
        generation: Generation,
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
            .map(move |(request, permit)| {
                request.execute(log, generation, Some(permit))
            });
        let other =
            futures::stream::iter(self.other.into_iter().chain(self.recopy))
                .map(move |request| request.execute(log, generation, None));

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
                .map(move |request| request.execute(log, generation, None))
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
}

impl Request<'_> {
    async fn execute(
        self,
        log: &slog::Logger,
        generation: Generation,
        _permit: Option<OwnedSemaphorePermit>,
    ) -> TufArtifactReplicationRequest {
        let err: Option<Box<dyn std::error::Error>> = async {
            match &self {
                Request::Put { sled, handle, hash } => {
                    sled.client
                        .artifact_put(
                            &hash.to_string(),
                            &generation,
                            handle.file().await?,
                        )
                        .await?;
                }
                Request::CopyFromDepot { target_sled, source_sled, hash } => {
                    target_sled
                    .client
                    .artifact_copy_from_depot(
                        &hash.to_string(),
                        &generation,
                        &sled_agent_client::types::ArtifactCopyFromDepotBody {
                            depot_base_url: source_sled.depot_base_url.clone(),
                        },
                    )
                    .await?;
                }
            };
            Ok(())
        }
        .await
        .err();

        let time = Utc::now();
        let (target_sled, hash) = match &self {
            Request::Put { sled, hash, .. }
            | Request::CopyFromDepot { target_sled: sled, hash, .. } => {
                (sled, hash)
            }
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
            },
            error: err
                .map(|err| InlineErrorChain::new(err.as_ref()).to_string()),
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

            // List sleds and artifacts from the database. These are the only
            // parts of this task that can return a failure early.
            let sleds = match self
                .list_sleds(opctx)
                .await
                .context("failed to list sleds")
            {
                Ok(sleds) => sleds,
                Err(err) => return json!({"error": format!("{err:#}")}),
            };
            let (config, inventory) = match self
                .list_artifacts_from_database(opctx)
                .await
                .context("failed to list artifacts from database")
            {
                Ok(inventory) => inventory,
                Err(err) => return json!({"error": format!("{err:#}")}),
            };

            // Create a channel to receive request ringbuf entries.
            let (ringbuf_tx_owned, mut rx) =
                mpsc::channel(MAX_REQUEST_CONCURRENCY);
            let ringbuf_tx = &ringbuf_tx_owned;
            let log_task_handle = tokio::task::spawn(async move {
                let mut request_log = BTreeSet::new();
                let mut counters = TufArtifactReplicationCounters::default();
                while let Some(entry) = rx.recv().await {
                    counters.inc(&entry);
                    request_log.insert(entry);
                }
                (request_log, counters)
            });

            // Put the artifact configuration to and list the artifacts present
            // on each sled.
            let log = &opctx.log;
            let config = &config;
            let inventory = sleds
                .iter()
                .map(|sled| async move {
                    (
                        sled,
                        Self::sled_put_config_and_list(
                            log, sled, config, ringbuf_tx,
                        )
                        .await,
                    )
                })
                .collect::<FuturesUnordered<_>>()
                .fold(
                    ControlFlow::Continue(inventory),
                    |inventory, (sled, result)| async {
                        let mut inventory = inventory?;
                        for (hash, count) in result? {
                            if let Some(entry) = inventory.0.get_mut(&hash) {
                                entry.sleds.insert(sled.id, count);
                            }
                        }
                        ControlFlow::Continue(inventory)
                    },
                )
                .await;
            if let ControlFlow::Continue(mut inventory) = inventory {
                self.list_and_clean_up_local_artifacts(
                    &opctx.log,
                    &mut inventory,
                );
                let requests = inventory.into_requests(
                    &sleds,
                    &mut rand::rng(),
                    self.min_sled_replication,
                );
                requests
                    .into_stream(&opctx.log, config.generation)
                    .buffer_unordered(MAX_REQUEST_CONCURRENCY)
                    .for_each(|log_entry| async {
                        ringbuf_tx.send(log_entry).await.ok();
                    })
                    .await;
            }

            // Our work is done; prepare the status message.
            drop(ringbuf_tx_owned);
            let (request_log, counters) = log_task_handle.await.unwrap();
            {
                // `Arc::make_mut` will either directly provide a mutable
                // reference if there are no other references, or clone it if
                // there are. At this point there should never be any other
                // references; we only clone this Arc a few lines below when
                // serializing the ringbuf to a `serde_json::Value`.
                let ringbuf = Arc::make_mut(&mut self.request_debug_ringbuf);
                let to_delete = (ringbuf.len() + request_log.len())
                    .saturating_sub(MAX_REQUEST_DEBUG_BUFFER_LEN);
                ringbuf.drain(0..to_delete);
                ringbuf.extend(request_log);
            }
            self.lifetime_counters += counters;
            serde_json::to_value(TufArtifactReplicationStatus {
                generation: config.generation,
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

    async fn list_sleds(&self, opctx: &OpContext) -> Result<Vec<Sled>> {
        Ok(self
            .datastore
            .sled_list_all_batched(&opctx, SledFilter::TufArtifactReplication)
            .await?
            .into_iter()
            .map(|sled| Sled {
                id: sled.id(),
                client: sled_client_from_address(
                    sled.id(),
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
    ) -> Result<(ArtifactConfig, Inventory)> {
        let generation = self.datastore.tuf_get_generation(opctx).await?;
        let repos =
            self.datastore.tuf_list_repos_unpruned_batched(opctx).await?;
        // `tuf_list_repos_unpruned_batched` performs pagination internally,
        // so check that the generation hasn't changed during our pagination to
        // ensure we got a consistent read.
        {
            let generation_now =
                self.datastore.tuf_get_generation(opctx).await?;
            ensure!(
                generation == generation_now,
                "generation changed from {generation} \
                to {generation_now}, bailing"
            );
        }

        let mut inventory = Inventory::default();
        for repo in repos {
            for artifact in
                self.datastore.tuf_list_repo_artifacts(opctx, repo.id()).await?
            {
                inventory.0.entry(artifact.sha256.0).or_insert_with(|| {
                    ArtifactPresence { sleds: BTreeMap::new(), local: None }
                });
            }
        }
        let config = ArtifactConfig {
            generation,
            artifacts: inventory.0.keys().map(|h| h.to_string()).collect(),
        };
        Ok((config, inventory))
    }

    /// Put `config` to `sled`, then query `sled` for a list of its artifacts.
    ///
    /// Returns [`ControlFlow::Break`] if `config` was rejected by the sled due
    /// to an invalid generation number, or if the list response contained a
    /// different generation.
    async fn sled_put_config_and_list(
        log: &slog::Logger,
        sled: &Sled,
        config: &ArtifactConfig,
        ringbuf_tx: &mpsc::Sender<TufArtifactReplicationRequest>,
    ) -> ControlFlow<(), BTreeMap<ArtifactHash, u32>> {
        let response = sled.client.artifact_config_put(config).await;
        ringbuf_tx
            .send(TufArtifactReplicationRequest {
                time: Utc::now(),
                target_sled: sled.id,
                operation: TufArtifactReplicationOperation::PutConfig {
                    generation: config.generation,
                },
                error: response.as_ref().err().map(|err| {
                    error!(
                        log,
                        "Failed to put artifact config";
                        "error" => InlineErrorChain::new(err),
                        "sled" => sled.client.baseurl(),
                        "generation" => &config.generation,
                    );
                    InlineErrorChain::new(err).to_string()
                }),
            })
            .await
            .ok();
        // Bail without sending a list request if the config put failed.
        if let Err(err) = response {
            // If the request failed because the sled told us the
            // generation was invalid, return `Break`.
            if let sled_agent_client::Error::ErrorResponse(response) = err {
                if response.status() == StatusCode::CONFLICT
                    && response.error_code.as_deref()
                        == Some("CONFIG_GENERATION")
                {
                    return ControlFlow::Break(());
                }
            }
            return ControlFlow::Continue(BTreeMap::new());
        }

        let response = sled.client.artifact_list().await;
        let time = Utc::now();
        let (result, error) = match response {
            Ok(response) => {
                let response = response.into_inner();
                if response.generation == config.generation {
                    info!(
                        log,
                        "Successfully got artifact list";
                        "sled" => sled.client.baseurl(),
                    );
                    match response
                        .list
                        .into_iter()
                        .map(|(hash, count)| {
                            match ArtifactHash::from_str(&hash) {
                                Ok(hash) => Ok((hash, count)),
                                Err(_) => Err(hash),
                            }
                        })
                        .collect()
                    {
                        Ok(list) => (ControlFlow::Continue(list), None),
                        Err(bogus_hash) => {
                            error!(
                                log,
                                "Sled reported bogus artifact hash";
                                "sled" => sled.client.baseurl(),
                                "bogus_hash" => &bogus_hash,
                            );
                            (
                                ControlFlow::Continue(BTreeMap::new()),
                                Some(format!(
                                    "sled reported bogus artifact hash \
                                    {bogus_hash:?}"
                                )),
                            )
                        }
                    }
                } else {
                    error!(
                        log,
                        "Failed to get artifact list: \
                        sled reported different generation number";
                        "sled" => sled.client.baseurl(),
                        "sled_generation" => response.generation,
                        "config_generation" => config.generation,
                    );
                    (
                        ControlFlow::Break(()),
                        Some(format!(
                            "sled reported generation {}, expected {}",
                            response.generation, config.generation
                        )),
                    )
                }
            }
            Err(err) => {
                error!(
                    log,
                    "Failed to get artifact list";
                    "error" => InlineErrorChain::new(&err),
                    "sled" => sled.client.baseurl(),
                );
                (
                    ControlFlow::Continue(BTreeMap::new()),
                    Some(InlineErrorChain::new(&err).to_string()),
                )
            }
        };
        ringbuf_tx
            .send(TufArtifactReplicationRequest {
                time,
                target_sled: sled.id,
                operation: TufArtifactReplicationOperation::List,
                error,
            })
            .await
            .ok();
        result
    }

    /// Fill in the `local` field on the values of `inventory` with any local
    /// artifacts, while removing the locally-stored `ArtifactsWithPlan` objects
    /// once they reach the minimum requirement to be considered replicated.
    fn list_and_clean_up_local_artifacts(
        &mut self,
        log: &slog::Logger,
        inventory: &mut Inventory,
    ) {
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
            if !keep_plan {
                let version = &plan.description().repo.system_version;
                info!(
                    log,
                    "Cleaning up local repository";
                    "repo_system_version" => version.to_string(),
                );
            }
            keep_plan
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use expectorate::assert_contents;
    use omicron_uuid_kinds::GenericUuid;
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
                    uuid::Builder::from_random_bytes(rng.random()).into_uuid(),
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
        // Everything in `other` should be `Copy`.
        for request in &requests.other {
            assert!(
                matches!(request, Request::CopyFromDepot { .. }),
                "request in `other` is not `CopyFromDepot`: {request:?}"
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
                ArtifactHash(rng.random()),
                ArtifactPresence {
                    sleds: BTreeMap::new(),
                    local: Some(ArtifactHandle::Fake),
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
                ArtifactHash(rng.random()),
                ArtifactPresence { sleds: sled_presence.clone(), local: None },
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
    fn recopy() {
        // 3 sleds have two copies of an artifact; 1 has a single copy.
        let mut rng = StdRng::from_seed(Default::default());
        let sleds = fake_sleds(4, &mut rng);
        let mut inventory = BTreeMap::new();
        inventory.insert(
            ArtifactHash(rng.random()),
            ArtifactPresence {
                sleds: sleds
                    .iter()
                    .enumerate()
                    .map(|(i, sled)| (sled.id, if i == 0 { 1 } else { 2 }))
                    .collect(),
                local: None,
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
            ArtifactHash(rng.random()),
            ArtifactPresence {
                sleds: sleds.iter().map(|sled| (sled.id, 2)).collect(),
                local: None,
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
