// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TUF Repo Depot: Artifact replication across sleds (RFD 424)
//!
//! `Nexus::updates_put_repository` accepts a TUF repository, which Nexus
//! unpacks, verifies, and reasons about the artifacts in. This uses temporary
//! storage within the Nexus zone, so the update artifacts have to go somewhere.
//! We've settled for now on "everywhere": a copy of each artifact is stored on
//! each sled's M.2 devices.
//!
//! This background task is responsible for getting locally-stored artifacts
//! onto sleds, and ensuring all sleds have copies of all artifacts.
//! `Nexus::updates_put_repository` sends the [`ArtifactsWithPlan`] object to
//! this task via an [`mpsc`] channel and activates it.
//!
//! During each activation:
//!
//! 1. The task moves `ArtifactsWithPlan` objects of the `mpsc` channel and into
//!    a `Vec`.
//! 2. The task queries the list of artifacts stored on each sled, and compares
//!    it to the list of artifacts in CockroachDB. Sled artifact storage is
//!    content-addressed by SHA-256 checksum. Errors are logged but otherwise
//!    ignored (unless no sleds respond); the task proceeds as if that sled
//!    has no artifacts. (This means that the task will always be trying to
//!    replicate artifacts to that sled until it comes back or is pulled out
//!    of service.)
//! 3. The task compares the list of artifacts from the database to the list of
//!    artifacts available locally.
//! 4. If all the artifacts belonging to an `ArtifactsWithPlan` object have
//!    been replicated to at least `MIN_SLED_REPLICATION` sleds, the task drops
//!    the object from its `Vec` (thus cleaning up the local storage of those
//!    files).
//! 5. The task generates a list of requests that need to be sent:
//!    - PUT each locally-stored artifact not present on any sleds to a random
//!      sled.
//!    - For each partially-replicated artifact, choose a sled that is missing
//!      the artifact, and tell it (via `artifact_copy_from_depot`) to fetch the
//!      artifact from a random sled that has it.
//!    - DELETE all artifacts no longer tracked in CockroachDB from all sleds
//!      that have that artifact.
//! 6. The task randomly choose requests up to per-activation limits and
//!    sends them. Up to `MAX_REQUESTS` total requests are sent, with up to
//!    `MAX_PUT_REQUESTS` PUT requests. Successful and unsuccessful responses
//!    are logged.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::ensure;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::{
    datastore::SQL_BATCH_SIZE, pagination::Paginator, DataStore,
};
use nexus_networking::sled_client_from_address;
use nexus_types::deployment::SledFilter;
use nexus_types::internal_api::background::TufArtifactReplicationStatus;
use omicron_common::{address::REPO_DEPOT_PORT, update::ArtifactHash};
use omicron_uuid_kinds::{GenericUuid, SledUuid};
use rand::seq::SliceRandom;
use slog_error_chain::InlineErrorChain;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use update_common::artifacts::{
    ArtifactsWithPlan, ExtractedArtifactDataHandle,
};

use crate::app::background::BackgroundTask;

// The maximum number of PUT requests to send during each activation. This is
// relatively small to avoid significant bandwidth out from this sled.
const MAX_PUT_REQUESTS: usize = 8;
// The maximum total number of requests to send during each activation,
// including PUT requests capped by `MAX_PUT_REQUESTS`.
const MAX_REQUESTS: usize = 32;
// The number of sleds that artifacts must be present on before the local copy
// of artifacts is dropped. This is ignored if there are fewer than this many
// sleds in the system.
const MIN_SLED_REPLICATION: usize = 3;

pub struct ArtifactReplication {
    datastore: Arc<DataStore>,
    local: Vec<ArtifactsWithPlan>,
    local_rx: mpsc::Receiver<ArtifactsWithPlan>,
}

struct Sled {
    id: SledUuid,
    client: sled_agent_client::Client,
    depot_base_url: String,
}

#[derive(Default)]
struct ArtifactPresence<'a> {
    sleds: Vec<&'a Sled>,
    counts: HashMap<SledUuid, u32>,
    local: Option<ExtractedArtifactDataHandle>,
}

enum Request<'a> {
    Put {
        sled: &'a Sled,
        handle: ExtractedArtifactDataHandle,
        hash: ArtifactHash,
    },
    CopyFromDepot {
        target_sled: &'a Sled,
        depot_sled: &'a Sled,
        hash: ArtifactHash,
    },
    Delete {
        sled: &'a Sled,
        hash: String,
    },
}

impl BackgroundTask for ArtifactReplication {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            match self.activate_internal(opctx).await {
                Ok(status) => serde_json::to_value(status).unwrap(),
                Err(err) => {
                    let err_string = format!("{:#}", err);
                    error!(
                        &opctx.log,
                        "error during artifact replication";
                        "error" => &err_string,
                    );
                    serde_json::json!({
                        "error": err_string,
                    })
                }
            }
        }
        .boxed()
    }
}

impl ArtifactReplication {
    pub fn new(
        datastore: Arc<DataStore>,
        local_rx: mpsc::Receiver<ArtifactsWithPlan>,
    ) -> ArtifactReplication {
        ArtifactReplication { datastore, local: Vec::new(), local_rx }
    }

    async fn activate_internal<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> anyhow::Result<TufArtifactReplicationStatus> {
        let log = &opctx.log;
        let datastore = &self.datastore;

        // Move any received artifacts out of `local_rx` into `local`.
        loop {
            match self.local_rx.try_recv() {
                Ok(artifacts) => self.local.push(artifacts),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    // If the last sender for this channel is dropped, then the
                    // `Nexus` type has also been dropped. This is presumably
                    // a bug.
                    panic!("artifact replication receiver disconnected");
                }
            }
        }

        // Query the database for artifacts that we ought to have.
        let mut artifacts = HashMap::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = datastore
                .update_tuf_artifact_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|a| a.sha256);
            for artifact in batch {
                artifacts
                    .insert(artifact.sha256.0, ArtifactPresence::default());
            }
        }
        // Ask all sled agents to list the artifacts they have, and mark those
        // artifacts as present on those sleds.
        let sleds = datastore
            .sled_list_all_batched(&opctx, SledFilter::InService)
            .await?
            .into_iter()
            .map(|sled| Sled {
                id: SledUuid::from_untyped_uuid(sled.identity.id),
                client: sled_client_from_address(
                    sled.identity.id,
                    sled.address(),
                    log,
                ),
                depot_base_url: format!(
                    "http://{}",
                    sled.address_with_port(REPO_DEPOT_PORT)
                ),
            })
            .collect::<Vec<_>>();
        ensure!(!sleds.is_empty(), "no sleds");
        let responses =
            futures::future::join_all(sleds.iter().map(|sled| async move {
                let response = match sled.client.artifact_list().await {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        error!(
                            log,
                            "Failed to get artifact list";
                            "error" => InlineErrorChain::new(&err),
                            "sled" => sled.client.baseurl(),
                        );
                        HashMap::new()
                    }
                };
                (sled, response)
            }))
            .await;
        let mut delete_requests = Vec::new();
        for (sled, response) in responses {
            for (hash, count) in response {
                if let Some(presence) = ArtifactHash::from_str(&hash)
                    .ok()
                    .and_then(|hash| artifacts.get_mut(&hash))
                {
                    presence.counts.insert(sled.id, count);
                    presence.sleds.push(sled);
                } else {
                    delete_requests.push(Request::Delete { sled, hash });
                }
            }
        }

        // Mark all the artifacts found in `self.local` as locally available.
        // If all of the artifacts in an `ArtifactsWithPlan` are sufficiently
        // replicated, drop the `ArtifactsWithPlan`.
        let sufficient_sleds = MIN_SLED_REPLICATION.min(sleds.len());
        self.local.retain(|plan| {
            let mut keep = false;
            for hash_id in plan.by_id().values().flatten() {
                if let Some(handle) = plan.get_by_hash(hash_id) {
                    if let Some(presence) = artifacts.get_mut(&hash_id.hash) {
                        presence.local = Some(handle);
                        if presence.sleds.len() < sufficient_sleds {
                            keep = true;
                        }
                    }
                }
            }
            keep
        });

        // Generate and send a random set of requests up to our per-activation
        // limits.
        let (requests, requests_outstanding) =
            generate_requests(&sleds, artifacts, delete_requests);
        let futures = requests.iter().map(|request| request.execute(log));
        let result = futures::future::join_all(futures).await;

        // TODO: If there are any missing artifacts with no known copies,
        // check the status of the assigned Nexus for its repositories. If the
        // assigned Nexus is expunged, or if we are the assigned Nexus and we
        // don't have the artifact, mark the repository as failed.

        Ok(TufArtifactReplicationStatus {
            requests_ok: result.iter().filter(|x| **x).count(),
            requests_err: result.iter().filter(|x| !*x).count(),
            requests_outstanding,
            local_repos: self.local.len(),
        })
    }
}

impl<'a> Request<'a> {
    async fn execute(&self, log: &slog::Logger) -> bool {
        let (action, sled, hash) = match self {
            Request::Put { sled, handle, hash } => {
                let sha256 = hash.to_string();
                let file = match handle.file().await {
                    Ok(file) => file,
                    Err(err) => {
                        error!(
                            log,
                            "Failed to open artifact file";
                            "error" => &format!("{:#}", err),
                            "sha256" => &sha256,
                        );
                        return false;
                    }
                };
                if let Err(err) = sled.client.artifact_put(&sha256, file).await
                {
                    error!(
                        log,
                        "Failed to put artifact";
                        "error" => InlineErrorChain::new(&err),
                        "sled" => sled.client.baseurl(),
                        "sha256" => &sha256,
                    );
                    return false;
                }
                ("PUT".to_owned(), sled, sha256)
            }
            Request::CopyFromDepot { target_sled, depot_sled, hash } => {
                let sha256 = hash.to_string();
                if let Err(err) = target_sled
                    .client
                    .artifact_copy_from_depot(
                        &sha256,
                        &sled_agent_client::types::ArtifactCopyFromDepotBody {
                            depot_base_url: depot_sled.depot_base_url.clone(),
                        },
                    )
                    .await
                {
                    error!(
                        log,
                        "Failed to request artifact copy from depot";
                        "error" => InlineErrorChain::new(&err),
                        "sled" => target_sled.client.baseurl(),
                        "sha256" => &sha256,
                    );
                    return false;
                }
                (
                    format!("copy from depot {}", depot_sled.depot_base_url),
                    target_sled,
                    sha256,
                )
            }
            Request::Delete { sled, hash } => {
                if let Err(err) = sled.client.artifact_delete(&hash).await {
                    error!(
                        log,
                        "Failed to request artifact deletion";
                        "error" => InlineErrorChain::new(&err),
                        "sled" => sled.client.baseurl(),
                        "sha256" => &hash,
                    );
                    return false;
                }
                ("DELETE".to_owned(), sled, hash.clone())
            }
        };
        info!(
            log,
            "Request succeeded";
            "action" => &action,
            "sled" => sled.client.baseurl(),
            "sha256" => &hash,
        );
        true
    }
}

fn generate_requests<'a>(
    sleds: &'a [Sled],
    artifacts: HashMap<ArtifactHash, ArtifactPresence<'a>>,
    delete_requests: Vec<Request<'a>>,
) -> (Vec<Request<'a>>, usize) {
    let mut rng = rand::thread_rng();
    let mut put_requests = Vec::new();
    let mut other_requests = delete_requests;
    let mut low_priority_requests = Vec::new();
    for (hash, presence) in artifacts {
        if presence.sleds.is_empty() {
            if let Some(handle) = presence.local {
                // Randomly choose a sled to PUT the artifact to.
                let sled = sleds.choose(&mut rng).expect("sleds is not empty");
                put_requests.push(Request::Put { sled, handle, hash });
            }
        } else {
            // Randomly choose a sled where the artifact is not present.
            let missing_sleds = sleds
                .iter()
                .filter_map(|sled| {
                    let count = presence
                        .counts
                        .get(&sled.id)
                        .copied()
                        .unwrap_or_default();
                    // TODO: We should check on the number of non-expunged M.2s
                    // the sled is known to have instead of a hardcoded value.
                    (count < 2).then_some((sled, count))
                })
                .collect::<Vec<_>>();
            if let Some((target_sled, count)) = missing_sleds.choose(&mut rng) {
                if *count > 0 {
                    // This sled doesn't have a full set of copies, but it does
                    // have _a_ copy, so ensuring that both M.2s have a copy is
                    // a lower-priority concern.
                    &mut low_priority_requests
                } else {
                    &mut other_requests
                }
                .push(Request::CopyFromDepot {
                    target_sled,
                    depot_sled: presence
                        .sleds
                        .choose(&mut rng)
                        .expect("presence.sleds is not empty"),
                    hash,
                });
            }
        }
    }

    let total =
        put_requests.len() + other_requests.len() + low_priority_requests.len();

    let mut requests = put_requests;
    sample_vec(&mut rng, &mut requests, MAX_PUT_REQUESTS);
    sample_vec(
        &mut rng,
        &mut other_requests,
        MAX_REQUESTS.saturating_sub(requests.len()),
    );
    requests.extend(other_requests);
    sample_vec(
        &mut rng,
        &mut low_priority_requests,
        MAX_REQUESTS.saturating_sub(requests.len()),
    );
    requests.extend(low_priority_requests);
    let outstanding = total - requests.len();
    (requests, outstanding)
}

fn sample_vec<R, T>(rng: &mut R, vec: &mut Vec<T>, amount: usize)
where
    R: rand::Rng + ?Sized,
{
    let end = vec.len();
    if amount >= end {
        return;
    }
    for i in 0..amount {
        vec.swap(i, rng.gen_range((i + 1)..end));
    }
    vec.truncate(amount);
}
