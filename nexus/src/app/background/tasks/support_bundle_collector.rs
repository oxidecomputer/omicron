// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing Support Bundles

use crate::app::background::BackgroundTask;
use anyhow::Context;
use base64::Engine;
use camino::Utf8DirEntry;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use camino_tempfile::tempdir_in;
use camino_tempfile::tempfile_in;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use gateway_client::Client as MgsClient;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpIgnition;
use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::SupportBundleCleanupReport;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use parallel_task_set::ParallelTaskSet;
use serde_json::json;
use sha2::{Digest, Sha256};
use slog_error_chain::InlineErrorChain;
use std::future::Future;
use std::io::Write;
use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::SeekFrom;
use tufaceous_artifact::ArtifactHash;
use zip::ZipArchive;
use zip::ZipWriter;
use zip::write::FullFileOptions;

// We use "/var/tmp" to use Nexus' filesystem for temporary storage,
// rather than "/tmp", which would keep this collected data in-memory.
const TEMPDIR: &str = "/var/tmp";

// The size of piece of a support bundle to transfer to the sled agent
// within a single streaming request.
const CHUNK_SIZE: NonZeroU64 = NonZeroU64::new(1024 * 1024 * 1024).unwrap();

fn authz_support_bundle_from_id(id: SupportBundleUuid) -> authz::SupportBundle {
    authz::SupportBundle::new(
        authz::FLEET,
        id,
        LookupType::ById(id.into_untyped_uuid()),
    )
}

// Specifies the data to be collected within the Support Bundle.
#[derive(Clone)]
struct BundleRequest {
    // If "false": Skip collecting host-specific info from each sled.
    skip_sled_info: bool,

    // The size of chunks to use when transferring a bundle from Nexus
    // to a sled agent.
    //
    // Typically, this is CHUNK_SIZE, but can be modified for testing.
    transfer_chunk_size: NonZeroU64,
}

impl Default for BundleRequest {
    fn default() -> Self {
        Self { skip_sled_info: false, transfer_chunk_size: CHUNK_SIZE }
    }
}

// Result of asking a sled agent to clean up a bundle
enum SledAgentBundleCleanupResult {
    Deleted,
    NotFound,
    Failed,
}

// Result of asking the database to delete a bundle
enum DatabaseBundleCleanupResult {
    DestroyingBundleRemoved,
    FailingBundleUpdated,
    BadState,
}

/// The background task responsible for cleaning and collecting support bundles
pub struct SupportBundleCollector {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    disable: bool,
    nexus_id: OmicronZoneUuid,
}

impl SupportBundleCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        disable: bool,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        SupportBundleCollector { datastore, resolver, disable, nexus_id }
    }

    // Tells a sled agent to delete a support bundle
    async fn cleanup_bundle_from_sled_agent(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        bundle: &SupportBundle,
    ) -> anyhow::Result<SledAgentBundleCleanupResult> {
        let sled_client = nexus_networking::sled_client(
            &self.datastore,
            &opctx,
            sled_id.into_untyped_uuid(),
            &opctx.log,
        )
        .await?;

        let result = sled_client
            .support_bundle_delete(
                &ZpoolUuid::from(bundle.zpool_id),
                &DatasetUuid::from(bundle.dataset_id),
                &SupportBundleUuid::from(bundle.id),
            )
            .await;

        match result {
            Ok(_) => {
                info!(
                    &opctx.log,
                    "SupportBundleCollector deleted bundle";
                    "id" => %bundle.id
                );
                return Ok(SledAgentBundleCleanupResult::Deleted);
            }
            Err(progenitor_client::Error::ErrorResponse(err))
                if err.status() == http::StatusCode::NOT_FOUND =>
            {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector could not delete bundle (not found)";
                    "id" => %bundle.id
                );

                return Ok(SledAgentBundleCleanupResult::NotFound);
            }
            err => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector could not delete bundle";
                    "id" => %bundle.id,
                    "err" => ?err,
                );

                return Ok(SledAgentBundleCleanupResult::Failed);
            }
        }
    }

    async fn cleanup_bundle_from_database(
        &self,
        opctx: &OpContext,
        bundle: &SupportBundle,
    ) -> anyhow::Result<DatabaseBundleCleanupResult> {
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        match bundle.state {
            SupportBundleState::Destroying => {
                // Destroying is a terminal state; no one should be able to
                // change this state from underneath us.
                self.datastore.support_bundle_delete(
                    opctx,
                    &authz_bundle,
                ).await.map_err(|err| {
                    warn!(
                        &opctx.log,
                        "SupportBundleCollector: Could not delete 'destroying' bundle";
                        "err" => %err
                    );
                    anyhow::anyhow!("Could not delete 'destroying' bundle: {:#}", err)
                })?;

                return Ok(
                    DatabaseBundleCleanupResult::DestroyingBundleRemoved,
                );
            }
            SupportBundleState::Failing => {
                if let Err(err) = self
                    .datastore
                    .support_bundle_update(
                        &opctx,
                        &authz_bundle,
                        SupportBundleState::Failed,
                    )
                    .await
                {
                    if matches!(err, Error::InvalidRequest { .. }) {
                        // It's possible that the bundle is marked "destroying" by a
                        // user request, concurrently with our operation.
                        //
                        // In this case, we log that this happened, but do nothing.
                        // The next iteration of this background task should treat
                        // this as the "Destroying" case, and delete the bundle.
                        info!(
                            &opctx.log,
                            "SupportBundleCollector: Concurrent state change failing bundle";
                            "bundle" => %bundle.id,
                            "err" => ?err,
                        );
                        return Ok(DatabaseBundleCleanupResult::BadState);
                    } else {
                        warn!(
                            &opctx.log,
                            "Could not delete 'failing' bundle";
                            "err" => ?err,
                        );
                        anyhow::bail!(
                            "Could not delete 'failing' bundle: {:#}",
                            err
                        );
                    }
                }

                return Ok(DatabaseBundleCleanupResult::FailingBundleUpdated);
            }
            other => {
                // We should be filtering to only see "Destroying" and
                // "Failing" bundles in our database request above.
                error!(
                    &opctx.log,
                    "SupportBundleCollector: Cleaning bundle in unexpected state";
                    "id" => %bundle.id,
                    "state" => ?other
                );
                return Ok(DatabaseBundleCleanupResult::BadState);
            }
        }
    }

    // Monitors all bundles that are "destroying" or "failing" and assigned to
    // this Nexus, and attempts to clear their storage from Sled Agents.
    async fn cleanup_destroyed_bundles(
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<SupportBundleCleanupReport> {
        let pagparams = DataPageParams::max_page();
        let result = self
            .datastore
            .support_bundle_list_assigned_to_nexus(
                opctx,
                &pagparams,
                self.nexus_id,
                vec![
                    SupportBundleState::Destroying,
                    SupportBundleState::Failing,
                ],
            )
            .await;

        let bundles_to_destroy = match result {
            Ok(r) => r,
            Err(err) => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Failed to list terminating bundles";
                    "err" => %err
                );
                anyhow::bail!("failed to query database: {:#}", err);
            }
        };

        let mut report = SupportBundleCleanupReport::default();

        // NOTE: This could be concurrent, but the priority for that also seems low
        for bundle in bundles_to_destroy {
            info!(
                &opctx.log,
                "SupportBundleCollector starting bundle deletion";
                "id" => %bundle.id
            );

            // Find the sled where we're storing this bundle.
            let result = self
                .datastore
                .zpool_get_sled_if_in_service(&opctx, bundle.zpool_id.into())
                .await;

            let delete_from_db = match result {
                Ok(sled_id) => {
                    match self
                        .cleanup_bundle_from_sled_agent(
                            &opctx, sled_id, &bundle,
                        )
                        .await?
                    {
                        SledAgentBundleCleanupResult::Deleted => {
                            report.sled_bundles_deleted_ok += 1;
                            true
                        }
                        SledAgentBundleCleanupResult::NotFound => {
                            report.sled_bundles_deleted_not_found += 1;
                            true
                        }
                        SledAgentBundleCleanupResult::Failed => {
                            report.sled_bundles_delete_failed += 1;

                            // If the sled agent reports any other error, don't
                            // delete the bundle from the database. It might be
                            // transiently unavailable.
                            false
                        }
                    }
                }
                Err(Error::ObjectNotFound {
                    type_name: ResourceType::Zpool,
                    ..
                }) => {
                    // If the pool wasn't found in the database, it was
                    // expunged. Delete the support bundle, since there is no
                    // sled agent state to manage anymore.
                    true
                }
                Err(_) => false,
            };

            if delete_from_db {
                match self.cleanup_bundle_from_database(opctx, &bundle).await? {
                    DatabaseBundleCleanupResult::DestroyingBundleRemoved => {
                        report.db_destroying_bundles_removed += 1;
                    }
                    DatabaseBundleCleanupResult::FailingBundleUpdated => {
                        report.db_failing_bundles_updated += 1;
                    }
                    DatabaseBundleCleanupResult::BadState => {}
                }
            }
        }
        Ok(report)
    }

    async fn collect_bundle(
        &self,
        opctx: &OpContext,
        request: &BundleRequest,
    ) -> anyhow::Result<Option<SupportBundleCollectionReport>> {
        let pagparams = DataPageParams::max_page();
        let result = self
            .datastore
            .support_bundle_list_assigned_to_nexus(
                opctx,
                &pagparams,
                self.nexus_id,
                vec![SupportBundleState::Collecting],
            )
            .await;

        let bundle = match result {
            Ok(bundles) => {
                if let Some(bundle) = bundles.get(0) {
                    info!(
                        &opctx.log,
                        "SupportBundleCollector: Found bundle to collect";
                        "bundle" => %bundle.id,
                        "bundles_in_queue" => bundles.len()
                    );
                    bundle.clone()
                } else {
                    info!(&opctx.log, "No bundles to collect");
                    return Ok(None);
                }
            }
            Err(err) => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Failed to list collecting bundles";
                    "err" => %err
                );
                anyhow::bail!("failed to query database: {:#}", err);
            }
        };

        let collection = Arc::new(BundleCollection {
            datastore: self.datastore.clone(),
            resolver: self.resolver.clone(),
            log: opctx.log.new(slog::o!("bundle" => bundle.id.to_string())),
            opctx: opctx.child(std::collections::BTreeMap::new()),
            request: request.clone(),
            bundle: bundle.clone(),
            transfer_chunk_size: request.transfer_chunk_size,
        });

        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        let mut report = collection.collect_bundle_and_store_on_sled().await?;
        if let Err(err) = self
            .datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Active,
            )
            .await
        {
            if matches!(err, Error::InvalidRequest { .. }) {
                info!(
                    &opctx.log,
                    "SupportBundleCollector: Concurrent state change activating bundle";
                    "bundle" => %bundle.id,
                    "err" => ?err,
                );
                return Ok(Some(report));
            } else {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Unexpected error activating bundle";
                    "bundle" => %bundle.id,
                    "err" => ?err,
                );
                anyhow::bail!("failed to activate bundle: {:#}", err);
            }
        }
        report.activated_in_db_ok = true;
        Ok(Some(report))
    }
}

// Wraps up all arguments to perform a single support bundle collection
struct BundleCollection {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    log: slog::Logger,
    opctx: OpContext,
    request: BundleRequest,
    bundle: SupportBundle,
    transfer_chunk_size: NonZeroU64,
}

impl BundleCollection {
    // Collect the bundle within Nexus, and store it on a target sled.
    async fn collect_bundle_and_store_on_sled(
        self: &Arc<Self>,
    ) -> anyhow::Result<SupportBundleCollectionReport> {
        // Create a temporary directory where we'll store the support bundle
        // as it's being collected.
        let dir = tempdir_in(TEMPDIR)?;

        let report = self.collect_bundle_locally(&dir).await?;
        self.store_bundle_on_sled(dir).await?;
        Ok(report)
    }

    // Create the support bundle, placing the contents into a user-specified
    // directory.
    //
    // Does not attempt to convert the contents into a zipfile, nor send them
    // to any durable storage.
    async fn collect_bundle_locally(
        self: &Arc<Self>,
        dir: &Utf8TempDir,
    ) -> anyhow::Result<SupportBundleCollectionReport> {
        let mut collection = Box::pin(self.collect_bundle_as_file(&dir));

        // We periodically check the state of the support bundle - if a user
        // explicitly cancels it, we should stop the collection process and
        // return.
        let work_duration = tokio::time::Duration::from_secs(5);
        let mut yield_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + work_duration,
            work_duration,
        );

        loop {
            tokio::select! {
                // Timer fired mid-collection - let's check if we should stop.
                _ = yield_interval.tick() => {
                    trace!(
                        &self.log,
                        "Checking if Bundle Collection cancelled";
                        "bundle" => %self.bundle.id
                    );

                    let bundle = self.datastore.support_bundle_get(
                        &self.opctx,
                        self.bundle.id.into()
                    ).await?;
                    if !matches!(bundle.state, SupportBundleState::Collecting) {
                        warn!(
                            &self.log,
                            "Support Bundle cancelled - stopping collection";
                            "bundle" => %self.bundle.id,
                            "state" => ?self.bundle.state
                        );
                        anyhow::bail!("Support Bundle Cancelled");
                    }
                },
                // Otherwise, keep making progress on the collection itself.
                report = &mut collection => {
                    info!(
                        &self.log,
                        "Bundle Collection completed";
                        "bundle" => %self.bundle.id
                    );
                    return report;
                },
            }
        }
    }

    async fn store_bundle_on_sled(
        &self,
        dir: Utf8TempDir,
    ) -> anyhow::Result<()> {
        // Create the zipfile as a temporary file
        let mut zipfile = tokio::fs::File::from_std(bundle_to_zipfile(&dir)?);
        let total_len = zipfile.metadata().await?.len();

        // Collect the hash locally before we send it over the network
        //
        // We'll use this later during finalization to confirm the bundle
        // has been stored successfully.
        zipfile.seek(SeekFrom::Start(0)).await?;
        let hash = sha2_hash(&mut zipfile).await?;

        // Find the sled where we're storing this bundle.
        let sled_id = self
            .datastore
            .zpool_get_sled_if_in_service(
                &self.opctx,
                self.bundle.zpool_id.into(),
            )
            .await?;
        let sled_client = nexus_networking::sled_client(
            &self.datastore,
            &self.opctx,
            sled_id.into_untyped_uuid(),
            &self.log,
        )
        .await?;

        let zpool = ZpoolUuid::from(self.bundle.zpool_id);
        let dataset = DatasetUuid::from(self.bundle.dataset_id);
        let support_bundle = SupportBundleUuid::from(self.bundle.id);

        // Tell this sled to create the bundle.
        let creation_result = sled_client
            .support_bundle_start_creation(&zpool, &dataset, &support_bundle)
            .await
            .with_context(|| "Support bundle failed to start creation")?;

        if matches!(
            creation_result.state,
            sled_agent_client::types::SupportBundleState::Complete
        ) {
            // Early exit case: the bundle was already created -- we must have either
            // crashed or failed between "finalizing" and "writing to the database that we
            // finished".
            info!(&self.log, "Support bundle was already collected"; "bundle" => %self.bundle.id);
            return Ok(());
        }
        info!(&self.log, "Support bundle creation started"; "bundle" => %self.bundle.id);

        let mut offset = 0;
        while offset < total_len {
            // Stream the zipfile to the sled where it should be kept
            let mut file = zipfile
                .try_clone()
                .await
                .with_context(|| "Failed to clone zipfile")?;
            file.seek(SeekFrom::Start(offset)).await.with_context(|| {
                format!("Failed to seek to offset {offset} / {total_len} within zipfile")
            })?;

            // Only stream at most "transfer_chunk_size" bytes at once
            let chunk_size = std::cmp::min(
                self.transfer_chunk_size.get(),
                total_len - offset,
            );

            let limited_file = file.take(chunk_size);
            let stream = tokio_util::io::ReaderStream::new(limited_file);
            let body = reqwest::Body::wrap_stream(stream);

            info!(
                &self.log,
                "Streaming bundle chunk";
                "bundle" => %self.bundle.id,
                "offset" => offset,
                "length" => chunk_size,
            );

            sled_client.support_bundle_transfer(
                &zpool, &dataset, &support_bundle, offset, body
            ).await.with_context(|| {
                format!("Failed to transfer bundle: {chunk_size}@{offset} of {total_len} to sled")
            })?;

            offset += chunk_size;
        }

        sled_client
            .support_bundle_finalize(
                &zpool,
                &dataset,
                &support_bundle,
                &hash.to_string(),
            )
            .await
            .with_context(|| "Failed to finalize bundle")?;

        // Returning from this method should drop all temporary storage
        // allocated locally for this support bundle.
        Ok(())
    }

    // Perform the work of collecting the support bundle into a temporary directory
    //
    // - "dir" is a directory where data can be stored.
    // - "bundle" is metadata about the bundle being collected.
    //
    // If a partial bundle can be collected, it should be returned as
    // an Ok(SupportBundleCollectionReport). Any failures from this function
    // will prevent the support bundle from being collected altogether.
    //
    // NOTE: The background task infrastructure will periodically check to see
    // if the bundle has been cancelled by a user while it is being collected.
    // If that happens, this function will be CANCELLED at an await point.
    //
    // As a result, it is important that this function be implemented as
    // cancel-safe.
    async fn collect_bundle_as_file(
        self: &Arc<Self>,
        dir: &Utf8TempDir,
    ) -> anyhow::Result<SupportBundleCollectionReport> {
        let log = &self.log;

        info!(&log, "Collecting bundle as local file");
        let mut report =
            SupportBundleCollectionReport::new(self.bundle.id.into());

        tokio::fs::write(
            dir.path().join("bundle_id.txt"),
            self.bundle.id.to_string(),
        )
        .await?;

        let sp_dumps_dir = dir.path().join("sp_task_dumps");
        tokio::fs::create_dir_all(&sp_dumps_dir).await.with_context(|| {
            format!("failed to create SP task dump directory {sp_dumps_dir}")
        })?;
        if let Err(e) =
            save_all_sp_dumps(log, &self.resolver, &sp_dumps_dir).await
        {
            error!(log, "failed to capture SP task dumps"; "error" => InlineErrorChain::new(e.as_ref()));
        } else {
            report.listed_sps = true;
        };

        if let Ok(all_sleds) = self
            .datastore
            .sled_list_all_batched(&self.opctx, SledFilter::InService)
            .await
        {
            report.listed_in_service_sleds = true;

            const MAX_CONCURRENT_SLED_REQUESTS: usize = 16;
            const FAILURE_MESSAGE: &str =
                "Failed to fully collect support bundle info from sled";
            let mut set = ParallelTaskSet::new_with_parallelism(
                MAX_CONCURRENT_SLED_REQUESTS,
            );

            for sled in all_sleds {
                let prev_result = set
                    .spawn({
                        let collection: Arc<BundleCollection> = self.clone();
                        let dir = dir.path().to_path_buf();
                        async move {
                            collection.collect_data_from_sled(&sled, &dir).await
                        }
                    })
                    .await;
                if let Some(Err(err)) = prev_result {
                    warn!(&self.log, "{FAILURE_MESSAGE}"; "err" => ?err);
                }
            }
            while let Some(result) = set.join_next().await {
                if let Err(err) = result {
                    warn!(&self.log, "{FAILURE_MESSAGE}"; "err" => ?err);
                }
            }
        }

        Ok(report)
    }

    // Collect data from a sled, storing it into a directory that will
    // be turned into a support bundle.
    //
    // - "sled" is the sled from which we should collect data.
    // - "dir" is a directory where data can be stored, to be turned
    // into a bundle after collection completes.
    async fn collect_data_from_sled(
        &self,
        sled: &nexus_db_model::Sled,
        dir: &Utf8Path,
    ) -> anyhow::Result<()> {
        let log = &self.log;
        info!(&log, "Collecting bundle info from sled"; "sled" => %sled.id());
        let sled_path = dir
            .join("rack")
            .join(sled.rack_id.to_string())
            .join("sled")
            .join(sled.id().to_string());
        tokio::fs::create_dir_all(&sled_path).await?;
        tokio::fs::write(sled_path.join("sled.txt"), format!("{sled:?}"))
            .await?;

        if self.request.skip_sled_info {
            return Ok(());
        }

        let Ok(sled_client) = nexus_networking::sled_client(
            &self.datastore,
            &self.opctx,
            sled.id(),
            log,
        )
        .await
        else {
            tokio::fs::write(
                sled_path.join("error.txt"),
                "Could not contact sled",
            )
            .await?;
            return Ok(());
        };

        // NB: As new sled-diagnostic commands are added they should
        // be added to this array so that their output can be saved
        // within the support bundle.
        let mut diag_cmds = futures::stream::iter([
            save_diag_cmd_output_or_error(
                &sled_path,
                "zoneadm",
                sled_client.support_zoneadm_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "dladm",
                sled_client.support_dladm_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "ipadm",
                sled_client.support_ipadm_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "nvmeadm",
                sled_client.support_nvmeadm_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "pargs",
                sled_client.support_pargs_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "pfiles",
                sled_client.support_pfiles_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "pstack",
                sled_client.support_pstack_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "zfs",
                sled_client.support_zfs_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "zpool",
                sled_client.support_zpool_info(),
            )
            .boxed(),
            save_diag_cmd_output_or_error(
                &sled_path,
                "health-check",
                sled_client.support_health_check(),
            )
            .boxed(),
        ])
        // Currently we execute up to 10 commands concurrently which
        // might be doing their own concurrent work, for example
        // collectiong `pstack` output of every Oxide process that is
        // found on a sled.
        .buffer_unordered(10);

        while let Some(result) = diag_cmds.next().await {
            // Log that we failed to write the diag command output to a
            // file but don't return early as we wish to get as much
            // information as we can.
            if let Err(e) = result {
                error!(
                    &self.log,
                    "failed to write diagnostic command output to \
                    file: {e}"
                );
            }
        }

        // For each zone we concurrently fire off a request to its
        // sled-agent to collect its logs in a zip file and write the
        // result to the support bundle.
        let zones = sled_client.support_logs().await?.into_inner();
        let mut log_futs: FuturesUnordered<_> = zones
            .iter()
            .map(|zone| {
                save_zone_log_zip_or_error(log, &sled_client, zone, &sled_path)
            })
            .collect();

        while let Some(log_collection_result) = log_futs.next().await {
            // We log any errors saving the zip file to disk and
            // continue on.
            if let Err(e) = log_collection_result {
                error!(&self.log, "failed to write logs output: {e}");
            }
        }
        return Ok(());
    }
}

impl BackgroundTask for SupportBundleCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            if self.disable {
                return json!({ "error": "task disabled" });
            }

            let mut cleanup_report = None;
            let mut cleanup_err = None;
            let mut collection_report = None;
            let mut collection_err = None;

            match self.cleanup_destroyed_bundles(&opctx).await {
                Ok(report) => cleanup_report = Some(report),
                Err(err) => {
                    cleanup_err =
                        Some(json!({ "cleanup_error": err.to_string() }))
                }
            };

            let request = BundleRequest::default();
            match self.collect_bundle(&opctx, &request).await {
                Ok(report) => collection_report = Some(report),
                Err(err) => {
                    collection_err =
                        Some(json!({ "collect_error": InlineErrorChain::new(err.as_ref()).to_string() }))
                }
            };

            json!({
                "cleanup_report": cleanup_report,
                "cleanup_err": cleanup_err,
                "collection_report": collection_report,
                "collection_err": collection_err,
            })
        }
        .boxed()
    }
}

// Takes a directory "dir", and zips the contents into a single zipfile.
fn bundle_to_zipfile(dir: &Utf8TempDir) -> anyhow::Result<std::fs::File> {
    let tempfile = tempfile_in(TEMPDIR)?;
    let mut zip = ZipWriter::new(tempfile);

    recursively_add_directory_to_zipfile(&mut zip, dir.path(), dir.path())?;

    Ok(zip.finish()?)
}

fn recursively_add_directory_to_zipfile(
    zip: &mut ZipWriter<std::fs::File>,
    root_path: &Utf8Path,
    dir_path: &Utf8Path,
) -> anyhow::Result<()> {
    // Readdir might return entries in a non-deterministic order.
    // Let's sort it for the zipfile, to be nice.
    let mut entries = dir_path
        .read_dir_utf8()?
        .filter_map(Result::ok)
        .collect::<Vec<Utf8DirEntry>>();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    for entry in &entries {
        // Remove the "/tmp/..." prefix from the path when we're storing it in the
        // zipfile.
        let dst = entry.path().strip_prefix(root_path)?;

        let file_type = entry.file_type()?;
        if file_type.is_file() {
            let opts = FullFileOptions::default();
            let src = entry.path();

            zip.start_file_from_path(dst, opts)?;
            let mut file = std::fs::File::open(&src)?;
            std::io::copy(&mut file, zip)?;
        }
        if file_type.is_dir() {
            let opts = FullFileOptions::default();
            zip.add_directory_from_path(dst, opts)?;
            recursively_add_directory_to_zipfile(zip, root_path, entry.path())?;
        }
    }
    Ok(())
}

async fn sha2_hash(file: &mut tokio::fs::File) -> anyhow::Result<ArtifactHash> {
    let mut buf = vec![0u8; 65536];
    let mut ctx = Sha256::new();
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        ctx.write_all(&buf[0..n])?;
    }

    let digest = ctx.finalize();
    Ok(ArtifactHash(digest.as_slice().try_into()?))
}

/// For a given zone, save its service's logs into the provided destination
/// path. This path should be the location to a per-sled directory that will end
/// up in the final support bundle zip file.
async fn save_zone_log_zip_or_error(
    logger: &slog::Logger,
    client: &sled_agent_client::Client,
    zone: &str,
    path: &Utf8Path,
) -> anyhow::Result<()> {
    // In the future when support bundle collection exposes tuning parameters
    // this can turn into a collection parameter.
    const DEFAULT_MAX_ROTATED_LOGS: u32 = 5;

    match client.support_logs_download(zone, DEFAULT_MAX_ROTATED_LOGS).await {
        Ok(res) => {
            let bytestream = res.into_inner();
            let output_dir = path.join(format!("logs/{zone}"));
            let output_path = output_dir.join("logs.zip");

            // Ensure the logs output directory exists.
            tokio::fs::create_dir_all(&output_dir).await.with_context(
                || format!("failed to create output directory: {output_dir}"),
            )?;

            let mut file =
                tokio::fs::File::create(&output_path).await.with_context(
                    || format!("failed to create file: {output_path}"),
                )?;

            let stream = bytestream.into_inner().map(|chunk| {
                chunk.map_err(|e| std::io::Error::other(e.to_string()))
            });
            let mut reader = tokio_util::io::StreamReader::new(stream);
            let _nbytes = tokio::io::copy(&mut reader, &mut file).await?;
            file.flush().await?;

            // Unpack the zip so we don't end up with zip files inside of our
            // final zip
            let zipfile_path = output_path.clone();
            tokio::task::spawn_blocking(move || {
                extract_zip_file(&output_dir, &zipfile_path)
            })
            .await
            .map_err(|join_error| {
                anyhow::anyhow!(join_error)
                    .context("unzipping support bundle logs zip panicked")
            })??;

            // Cleanup the zip file since we no longer need it
            if let Err(e) = tokio::fs::remove_file(&output_path).await {
                error!(
                    logger,
                    "failed to cleanup temporary logs zip file";
                    "error" => %e,
                    "file" => %output_path,

                );
            }
        }
        Err(err) => {
            tokio::fs::write(
                path.join(format!("{zone}.logs.err")),
                err.to_string(),
            )
            .await?;
        }
    };

    Ok(())
}

fn extract_zip_file(
    output_dir: &Utf8Path,
    zip_file: &Utf8Path,
) -> Result<(), anyhow::Error> {
    let mut zip = std::fs::File::open(&zip_file)
        .with_context(|| format!("failed to open zip file: {zip_file}"))?;
    let mut archive = ZipArchive::new(&mut zip)?;
    archive.extract(&output_dir).with_context(|| {
        format!("failed to extract log zip file to: {output_dir}")
    })?;
    Ok(())
}

/// Run a `sled-dianostics` future and save its output to a corresponding file.
async fn save_diag_cmd_output_or_error<F, S: serde::Serialize>(
    path: &Utf8Path,
    command: &str,
    future: F,
) -> anyhow::Result<()>
where
    F: Future<
            Output = Result<
                sled_agent_client::ResponseValue<S>,
                sled_agent_client::Error<sled_agent_client::types::Error>,
            >,
        > + Send,
{
    let result = future.await;
    match result {
        Ok(result) => {
            let output = result.into_inner();
            let json = serde_json::to_string(&output).with_context(|| {
                format!("failed to serialize {command} output as json")
            })?;
            tokio::fs::write(path.join(format!("{command}.json")), json)
                .await
                .with_context(|| {
                    format!("failed to write output of {command} to file")
                })?;
        }
        Err(err) => {
            tokio::fs::write(
                path.join(format!("{command}_err.txt")),
                err.to_string(),
            )
            .await?;
        }
    }
    Ok(())
}

/// Collect task dumps from all SPs via MGS and save them to a directory.
async fn save_all_sp_dumps(
    log: &slog::Logger,
    resolver: &Resolver,
    sp_dumps_dir: &Utf8Path,
) -> anyhow::Result<()> {
    let mgs_client = resolver
        .lookup_socket_v6(ServiceName::ManagementGatewayService)
        .await
        .map(|sockaddr| {
            let url = format!("http://{}", sockaddr);
            gateway_client::Client::new(&url, log.clone())
        })
        .context("failed to resolve address of MGS")?;

    let available_sps = get_available_sps(&mgs_client).await?;

    let mut tasks = ParallelTaskSet::new();
    for sp in available_sps {
        let mgs_client = mgs_client.clone();
        let sp_dumps_dir = sp_dumps_dir.to_owned();

        tasks
            .spawn(async move {
                save_sp_dumps(mgs_client, sp, sp_dumps_dir)
                    .await
                    .with_context(|| format!("SP {} {}", sp.type_, sp.slot))
            })
            .await;
    }
    for result in tasks.join_all().await {
        if let Err(e) = result {
            error!(
                log,
                "failed to capture task dumps";
                "error" => InlineErrorChain::new(e.as_ref())
            );
        }
    }

    Ok(())
}

/// Use MGS ignition info to find active SPs.
async fn get_available_sps(
    mgs_client: &MgsClient,
) -> anyhow::Result<Vec<SpIdentifier>> {
    let ignition_info = mgs_client
        .ignition_list()
        .await
        .context("failed to get ignition info from MGS")?
        .into_inner();

    let mut active_sps = Vec::new();
    for info in ignition_info {
        if let SpIgnition::Yes { power, flt_sp, .. } = info.details {
            // Only return SPs that are powered on and are not in a faulted state.
            if power && !flt_sp {
                active_sps.push(info.id);
            }
        }
    }

    Ok(active_sps)
}

/// Fetch and save task dumps from a single SP.
async fn save_sp_dumps(
    mgs_client: MgsClient,
    sp: SpIdentifier,
    sp_dumps_dir: Utf8PathBuf,
) -> anyhow::Result<()> {
    let dump_count = mgs_client
        .sp_task_dump_count(sp.type_, sp.slot)
        .await
        .context("failed to get task dump count from SP")?
        .into_inner();

    let output_dir = sp_dumps_dir.join(format!("{}_{}", sp.type_, sp.slot));
    tokio::fs::create_dir_all(&output_dir).await?;

    for i in 0..dump_count {
        let task_dump = mgs_client
            .sp_task_dump_get(sp.type_, sp.slot, i)
            .await
            .with_context(|| format!("failed to get task dump {i} from SP"))?
            .into_inner();

        let zip_bytes = base64::engine::general_purpose::STANDARD
            .decode(task_dump.base64_zip)
            .context("failed to decode base64-encoded SP task dump zip")?;

        tokio::fs::write(output_dir.join(format!("dump-{i}.zip")), zip_bytes)
            .await
            .context("failed to write SP task dump zip to disk")?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::support_bundles::SupportBundleQueryType;
    use camino_tempfile::tempdir;
    use http_body_util::BodyExt;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::RendezvousDebugDataset;
    use nexus_db_model::Zpool;
    use nexus_test_utils::SLED_AGENT_UUID;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::DatasetsConfig;
    use omicron_common::disk::SharedDatasetConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::{
        BlueprintUuid, DatasetUuid, PhysicalDiskUuid, SledUuid,
    };
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // Ensure that we can convert a temporary directory into a zipfile
    #[test]
    fn test_zipfile_creation() {
        let dir = tempdir().unwrap();

        std::fs::create_dir_all(dir.path().join("dir-a")).unwrap();
        std::fs::create_dir_all(dir.path().join("dir-b")).unwrap();
        std::fs::write(dir.path().join("dir-a").join("file-a"), "some data")
            .unwrap();
        std::fs::write(dir.path().join("file-b"), "more data").unwrap();

        let zipfile = bundle_to_zipfile(&dir)
            .expect("Should have been able to bundle zipfile");
        let archive = zip::read::ZipArchive::new(zipfile).unwrap();

        // We expect the order to be deterministically alphabetical
        let mut names = archive.file_names();
        assert_eq!(names.next(), Some("dir-a/"));
        assert_eq!(names.next(), Some("dir-a/file-a"));
        assert_eq!(names.next(), Some("dir-b/"));
        assert_eq!(names.next(), Some("file-b"));
        assert_eq!(names.next(), None);
    }

    // If we have not populated any bundles needing cleanup, the cleanup
    // process should succeed with an empty cleanup report.
    #[nexus_test(server = crate::Server)]
    async fn test_cleanup_noop(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");

        assert_eq!(report, SupportBundleCleanupReport::default());
    }

    // If there are no bundles in need of collection, the collection task should
    // run without error, but return nothing.
    #[nexus_test(server = crate::Server)]
    async fn test_collect_noop(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let request = BundleRequest::default();
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should succeed with no bundles");
        assert!(report.is_none());
    }

    async fn add_zpool_and_debug_dataset(
        datastore: &DataStore,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
        sled_id: SledUuid,
        blueprint_id: BlueprintUuid,
    ) -> (ZpoolUuid, DatasetUuid) {
        let zpool = datastore
            .zpool_insert(
                opctx,
                Zpool::new(
                    Uuid::new_v4(),
                    sled_id.into_untyped_uuid(),
                    id,
                    ByteCount::from(0).into(),
                ),
            )
            .await
            .unwrap();
        let zpool_id = ZpoolUuid::from_untyped_uuid(zpool.id());

        let dataset = datastore
            .debug_dataset_insert_if_not_exists(
                opctx,
                RendezvousDebugDataset::new(
                    DatasetUuid::new_v4(),
                    zpool_id,
                    blueprint_id,
                ),
            )
            .await
            .unwrap()
            .expect("inserted new dataset");
        (zpool_id, dataset.id())
    }

    async fn make_disk_in_db(
        datastore: &DataStore,
        opctx: &OpContext,
        i: usize,
        sled_id: SledUuid,
    ) -> PhysicalDiskUuid {
        let id = PhysicalDiskUuid::new_v4();
        let physical_disk = PhysicalDisk::new(
            id,
            "v".into(),
            format!("s-{i})"),
            "m".into(),
            PhysicalDiskKind::U2,
            sled_id.into_untyped_uuid(),
        );
        datastore
            .physical_disk_insert(&opctx, physical_disk.clone())
            .await
            .unwrap();
        id
    }

    struct TestDataset {
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    }

    impl TestDataset {
        // For the purposes of this test, we're going to provision support
        // bundles to a single sled.
        async fn setup(
            cptestctx: &ControlPlaneTestContext,
            datastore: &Arc<DataStore>,
            opctx: &OpContext,
            count: usize,
        ) -> Vec<Self> {
            let sled_id = SledUuid::from_untyped_uuid(
                SLED_AGENT_UUID.parse::<Uuid>().unwrap(),
            );

            let mut disks = vec![];

            // The fake disks/datasets we create aren't really part of the test
            // blueprint, but that's fine for our purposes.
            let blueprint_id = cptestctx.initial_blueprint_id;

            for i in 0..count {
                // Create the (disk, zpool, dataset) tuple in Nexus
                let disk_id =
                    make_disk_in_db(datastore, opctx, i, sled_id).await;
                let (zpool_id, dataset_id) = add_zpool_and_debug_dataset(
                    &datastore,
                    &opctx,
                    disk_id,
                    sled_id,
                    blueprint_id,
                )
                .await;

                // Tell the simulated sled agent to create this storage.
                //
                // (We could do this via HTTP request, but it's in the test process,
                // so we can just directly call the method on the sled agent)
                cptestctx.first_sled_agent().create_zpool(
                    zpool_id,
                    disk_id,
                    1 << 40,
                );
                disks.push(Self { zpool_id, dataset_id })
            }

            // Create a configuration for the sled agent consisting of all these
            // debug datasets.
            let datasets = disks
                .iter()
                .map(|TestDataset { zpool_id, dataset_id }| {
                    (
                        *dataset_id,
                        DatasetConfig {
                            id: *dataset_id,
                            name: DatasetName::new(
                                ZpoolName::new_external(*zpool_id),
                                DatasetKind::Debug,
                            ),
                            inner: SharedDatasetConfig::default(),
                        },
                    )
                })
                .collect();

            // Read current sled config generation from zones (this will change
            // slightly once the simulator knows how to keep the unified config
            // and be a little less weird)
            let current_generation =
                cptestctx.first_sled_agent().omicron_zones_list().generation;

            let dataset_config = DatasetsConfig {
                generation: current_generation.next(),
                datasets,
            };

            let res = cptestctx
                .first_sled_agent()
                .datasets_ensure(dataset_config)
                .unwrap();
            assert!(!res.has_error());

            disks
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_collect_one(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // Assign a bundle to ourselves. We expect to collect it on
        // the next call to "collect_bundle".
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // The bundle collection should complete successfully.
        let request = BundleRequest {
            // NOTE: The support bundle querying interface isn't supported on
            // the simulated sled agent (yet?) so we're skipping this step.
            skip_sled_info: true,
            ..Default::default()
        };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        assert!(report.listed_in_service_sleds);
        assert!(report.listed_sps);
        assert!(report.activated_in_db_ok);

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Bundle should definitely be in db by this point");
        assert_eq!(observed_bundle.state, SupportBundleState::Active);

        // If we retry bundle collection, nothing should happen.
        // The bundle has already been collected.
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should be a no-op the second time");
        assert!(report.is_none());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_collect_chunked(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // The bundle collection should complete successfully.
        //
        // We're going to use a really small chunk size here to force the bundle
        // to get split up.
        let request = BundleRequest {
            skip_sled_info: true,
            transfer_chunk_size: NonZeroU64::new(16).unwrap(),
        };

        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        assert!(report.listed_in_service_sleds);
        assert!(report.listed_sps);
        assert!(report.activated_in_db_ok);

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Bundle should definitely be in db by this point");
        assert_eq!(observed_bundle.state, SupportBundleState::Active);

        // Download a file from the bundle, to verify that it was transferred
        // successfully.
        let head = false;
        let range = None;
        let response = nexus
            .support_bundle_download(
                &opctx,
                observed_bundle.id.into(),
                SupportBundleQueryType::Path {
                    file_path: "bundle_id.txt".to_string(),
                },
                head,
                range,
            )
            .await
            .unwrap();

        // Read the body to bytes, then convert to string
        let body_bytes =
            response.into_body().collect().await.unwrap().to_bytes();
        let body_string = String::from_utf8(body_bytes.to_vec()).unwrap();

        // Verify the content matches the bundle ID
        assert_eq!(body_string, observed_bundle.id.to_string());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_collect_many(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 2).await;

        // Assign two bundles to ourselves.
        let bundle1 = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        let bundle2 = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a second support bundle");

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        // Each time we call "collect_bundle", we collect a SINGLE bundle.
        let request =
            BundleRequest { skip_sled_info: true, ..Default::default() };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle1.id.into());
        assert!(report.listed_in_service_sleds);
        assert!(report.listed_sps);
        assert!(report.activated_in_db_ok);

        // This is observable by checking the state of bundle1 and bundle2:
        // the first is active, the second is still collecting.
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle1.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Active);
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle2.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Collecting);

        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle2.id.into());
        assert!(report.listed_in_service_sleds);
        assert!(report.listed_sps);
        assert!(report.activated_in_db_ok);

        // After another collection request, we'll see that both bundles have
        // been collected.
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle1.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Active);
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle2.id.into())
            .await
            .unwrap();
        assert_eq!(observed_bundle.state, SupportBundleState::Active);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_cancel_before_collect(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 2).await;

        // If we delete the bundle before we start collection, we can delete it
        // immediately.
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        // Cancel the bundle immediately
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .unwrap();

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // Nothing was provisioned on the sled, since we hadn't started
                // collection yet.
                sled_bundles_deleted_not_found: 1,
                // The database state was "destroying", and now it's gone.
                db_destroying_bundles_removed: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_cancel_after_collect(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle and collect it
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );
        let request =
            BundleRequest { skip_sled_info: true, ..Default::default() };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        assert!(report.listed_in_service_sleds);
        assert!(report.listed_sps);
        assert!(report.activated_in_db_ok);

        // Cancel the bundle after collection has completed
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .unwrap();

        // When we perform cleanup, we should see that it was removed from the
        // underlying sled.
        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // Nothing was provisioned on the sled, since we hadn't started
                // collection yet.
                sled_bundles_deleted_ok: 1,
                // The database state was "destroying", and now it's gone.
                db_destroying_bundles_removed: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_failed_bundle_before_collection(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle, though we'll fail it before it gets
        // collected.
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // Nothing was provisioned on the sled, since we hadn't started
                // collection yet.
                sled_bundles_deleted_not_found: 1,
                // The database state was "failing", and now it's updated.
                //
                // Note that it isn't immediately deleted, so the end-user
                // should have a chance to observe the new state.
                db_failing_bundles_updated: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_failed_bundle_after_collection(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle and collect it
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );
        let request =
            BundleRequest { skip_sled_info: true, ..Default::default() };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        // We can cleanup this bundle, even though it has already been
        // collected.
        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // The bundle was provisioned on the sled, so we should have
                // successfully removed it when we later talk to the sled.
                //
                // This won't always be the case for removal - if the entire
                // underlying sled was expunged, we won't get an affirmative
                // HTTP response from it. But in our simulated environment,
                // we have simply marked the bundle "failed" and kept the
                // simulated server running.
                sled_bundles_deleted_ok: 1,
                // The database state was "failing", and now it's updated.
                //
                // Note that it isn't immediately deleted, so the end-user
                // should have a chance to observe the new state.
                db_failing_bundles_updated: 1,
                ..Default::default()
            }
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_bundle_cleanup_after_zpool_deletion(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Before we can create any bundles, we need to create the
        // space for them to be provisioned.
        let _datasets =
            TestDataset::setup(cptestctx, &datastore, &opctx, 1).await;

        // We can allocate a support bundle and collect it
        let bundle = datastore
            .support_bundle_create(
                &opctx,
                "For collection testing",
                nexus.id(),
                None,
            )
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector = SupportBundleCollector::new(
            datastore.clone(),
            resolver.clone(),
            false,
            nexus.id(),
        );
        let request =
            BundleRequest { skip_sled_info: true, ..Default::default() };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        // Delete the zpool holding the bundle.
        //
        // This should call the "zpool_get_sled_if_in_service" call to fail!
        datastore
            .zpool_delete_self_and_all_datasets(&opctx, bundle.zpool_id.into())
            .await
            .unwrap();

        // We can cleanup this bundle, even though it has already been
        // collected.
        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            SupportBundleCleanupReport {
                // The database state was "failing", and now it's updated.
                //
                // Note that it isn't immediately deleted, so the end-user
                // should have a chance to observe the new state.
                db_failing_bundles_updated: 1,
                ..Default::default()
            }
        );
    }
}
