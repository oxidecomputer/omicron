// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing Support Bundles

use crate::app::background::BackgroundTask;
use camino::Utf8Path;
use camino_tempfile::tempdir;
use camino_tempfile::tempfile;
use camino_tempfile::Utf8TempDir;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::TryStreamExt;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::update::ArtifactHash;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::SeekFrom;
use zip::ZipWriter;

pub struct SupportBundleCollector {
    datastore: Arc<DataStore>,
    disable: bool,
    nexus_id: OmicronZoneUuid,
}

#[derive(Debug, Default, Serialize)]
struct CleanupReport {
    // Responses from Sled Agents
    sled_bundles_deleted_ok: usize,
    sled_bundles_deleted_not_found: usize,
    sled_bundles_delete_failed: usize,

    // Results from updating our database records
    db_destroying_bundles_removed: usize,
    db_failing_bundles_updated: usize,
}

impl SupportBundleCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        disable: bool,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        SupportBundleCollector { datastore, disable, nexus_id }
    }

    // Monitors all bundles that are "destroying" or "failing" and assigned to
    // this Nexus, and attempts to clear their storage from Sled Agents.
    async fn cleanup_destroyed_bundles(
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<CleanupReport> {
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

        let mut report = CleanupReport::default();

        // NOTE: This could be concurrent, but the priority for that also seems low
        for bundle in bundles_to_destroy {
            info!(
                &opctx.log,
                "SupportBundleCollector starting bundle deletion";
                "id" => %bundle.id
            );

            // Find the sled where we're storing this bundle.
            //
            // TODO: What happens if the zpool is concurrently expunged here?
            // TODO: Should we "continue" to other bundles rather than bailing
            // early?
            let sled_id = self
                .datastore
                .zpool_get_sled(&opctx, bundle.zpool_id.into())
                .await?;
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
                    report.sled_bundles_deleted_ok += 1;

                    // Safe to fall-through; we deleted sled-local state.
                }
                Err(progenitor_client::Error::ErrorResponse(err))
                    if err.status() == http::StatusCode::NOT_FOUND =>
                {
                    warn!(
                        &opctx.log,
                        "SupportBundleCollector could not delete bundle (not found)";
                        "id" => %bundle.id
                    );

                    report.sled_bundles_deleted_not_found += 1;

                    // Safe to fall-through; sled-local state does not exist
                }
                err => {
                    warn!(
                        &opctx.log,
                        "SupportBundleCollector could not delete bundle";
                        "id" => %bundle.id,
                        "err" => ?err,
                    );

                    report.sled_bundles_delete_failed += 1;

                    // We don't delete this bundle -- the sled storage may be
                    // experiencing a transient error.
                    continue;
                }
            }

            match bundle.state {
                SupportBundleState::Destroying => {
                    // Destroying is a terminal state; no one should be able to
                    // change this state from underneath us.
                    self.datastore.support_bundle_delete(
                        opctx,
                        bundle.id.into(),
                    ).await.map_err(|err| {
                        warn!(
                            &opctx.log,
                            "SupportBundleCollector: Could not delete 'destroying' bundle";
                            "err" => %err
                        );
                        anyhow::anyhow!("Could not delete 'destroying' bundle: {:#}", err)
                    })?;

                    report.db_destroying_bundles_removed += 1;
                }
                SupportBundleState::Failing => {
                    if let Err(err) = self
                        .datastore
                        .support_bundle_update(
                            &opctx,
                            bundle.id.into(),
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
                        } else {
                            anyhow::bail!(
                                "Could not delete 'failing' bundle: {:#}",
                                err
                            );
                        }
                    }

                    report.db_failing_bundles_updated += 1;
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
                }
            }
        }
        Ok(report)
    }

    async fn collect_bundle(
        &self,
        opctx: &OpContext,
    ) -> anyhow::Result<Option<CollectionReport>> {
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

        let mut report =
            self.collect_bundle_and_store_on_sled(&opctx, &bundle).await?;
        if let Err(err) = self
            .datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
                SupportBundleState::Active,
            )
            .await
        {
            info!(
                &opctx.log,
                "SupportBundleCollector: Concurrent state change activating bundle";
                "bundle" => %bundle.id,
                "err" => ?err,
            );
            return Ok(Some(report));
        }
        report.activated_in_db_ok = true;
        Ok(Some(report))
    }

    // Collect the bundle within Nexus, and store it on a target sled.
    async fn collect_bundle_and_store_on_sled(
        &self,
        opctx: &OpContext,
        bundle: &SupportBundle,
    ) -> anyhow::Result<CollectionReport> {
        // Create a temporary directory where we'll store the support bundle
        // as it's being collected.
        let dir = tempdir()?;
        let mut collection = Box::pin(collect_bundle_as_file(
            &self.datastore,
            opctx,
            &dir,
            bundle,
        ));

        // We periodically check the state of the support bundle - if a user
        // explicitly cancels it, we should stop the collection process and
        // return.
        let work_duration = tokio::time::Duration::from_secs(5);
        let mut yield_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + work_duration,
            work_duration,
        );

        let report = loop {
            tokio::select! {
                // Timer fired mid-collection - let's check if we should stop.
                _ = yield_interval.tick() => {
                    info!(
                        &opctx.log,
                        "Checking if Bundle Collection cancelled";
                        "bundle" => %bundle.id
                    );

                    let bundle = self.datastore.support_bundle_get(
                        &opctx,
                        bundle.id.into()
                    ).await?;
                    if !matches!(bundle.state, SupportBundleState::Collecting) {
                        warn!(
                            &opctx.log,
                            "Support Bundle cancelled - stopping collection";
                            "bundle" => %bundle.id,
                            "state" => ?bundle.state
                        );
                        anyhow::bail!("Support Bundle Cancelled");
                    }
                },
                // Otherwise, keep making progress on the collection itself.
                report = &mut collection => {
                    info!(
                        &opctx.log,
                        "Bundle Collection collected";
                        "bundle" => %bundle.id
                    );
                    break report?;
                },
            }
        };

        // Create the zipfile as a temporary file
        let mut zipfile = tokio::fs::File::from_std(bundle_to_zipfile(&dir)?);

        // Verify the hash locally before we send it over the network
        zipfile.seek(SeekFrom::Start(0)).await?;
        let hash = sha2_hash(&mut zipfile).await?;

        // Find the sled where we're storing this bundle.
        let sled_id = self
            .datastore
            .zpool_get_sled(&opctx, bundle.zpool_id.into())
            .await?;
        let sled_client = nexus_networking::sled_client(
            &self.datastore,
            &opctx,
            sled_id.into_untyped_uuid(),
            &opctx.log,
        )
        .await?;

        // Stream the zipfile to the sled where it should be kept
        zipfile.seek(SeekFrom::Start(0)).await?;
        let file_access = hyper_staticfile::vfs::TokioFileAccess::new(zipfile);
        let file_stream =
            hyper_staticfile::util::FileBytesStream::new(file_access);
        let body =
            reqwest::Body::wrap(hyper_staticfile::Body::Full(file_stream));

        sled_client
            .support_bundle_create(
                &ZpoolUuid::from(bundle.zpool_id),
                &DatasetUuid::from(bundle.dataset_id),
                &SupportBundleUuid::from(bundle.id),
                &hash.to_string(),
                body,
            )
            .await?;

        // Returning from this method should drop all temporary storage
        // allocated locally for this support bundle.
        Ok(report)
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

            match self.collect_bundle(&opctx).await {
                Ok(report) => collection_report = Some(report),
                Err(err) => {
                    collection_err =
                        Some(json!({ "collect_error": err.to_string() }))
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
    let tempfile = tempfile()?;
    let mut zip = ZipWriter::new(tempfile);

    recursively_add_directory_to_zipfile(&mut zip, dir.path())?;

    Ok(zip.finish()?)
}

fn recursively_add_directory_to_zipfile(
    zip: &mut ZipWriter<std::fs::File>,
    dir_path: &Utf8Path,
) -> anyhow::Result<()> {
    for entry in dir_path.read_dir_utf8()? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_file() {
            zip.deep_copy_file_from_path(
                entry.path(),
                entry.path().strip_prefix(dir_path)?,
            )?;
        }
        if file_type.is_dir() {
            recursively_add_directory_to_zipfile(zip, entry.path())?;
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

// Identifies what we could or could not store within this support bundle.
//
// This struct will get emitted as part of the background task infrastructure.
#[derive(Debug, Serialize)]
struct CollectionReport {
    bundle: SupportBundleUuid,

    // True iff we could list in-service sleds
    listed_in_service_sleds: bool,

    // True iff the bundle was successfully made 'active' in the database.
    activated_in_db_ok: bool,
}

impl CollectionReport {
    fn new(bundle: SupportBundleUuid) -> Self {
        Self {
            bundle,
            listed_in_service_sleds: false,
            activated_in_db_ok: false,
        }
    }
}

// Perform the work of collecting the support bundle into a temporary directory
//
// - "dir" is a directory where data can be stored.
// - "bundle" is metadata about the bundle being collected.
//
// If a partial bundle can be collected, it should be returned as
// an Ok(CollectionReport). Any failures from this function will prevent
// the support bundle from being collected altogether.
//
// NOTE: The background task infrastructure will periodically check to see
// if the bundle has been cancelled by a user while it is being collected.
// If that happens, this function will be CANCELLED at an await point.
//
// As a result, it is important that this function be implemented as
// cancel-safe.
async fn collect_bundle_as_file(
    datastore: &Arc<DataStore>,
    opctx: &OpContext,
    dir: &Utf8TempDir,
    bundle: &SupportBundle,
) -> anyhow::Result<CollectionReport> {
    let mut report = CollectionReport::new(bundle.id.into());

    tokio::fs::write(dir.path().join("bundle_id.txt"), bundle.id.to_string())
        .await?;

    if let Ok(all_sleds) =
        datastore.sled_list_all_batched(&opctx, SledFilter::InService).await
    {
        report.listed_in_service_sleds = true;

        // NOTE: This could be, and probably should be, done concurrently.
        for sled in &all_sleds {
            let sled_path = dir
                .path()
                .join("rack")
                .join(sled.rack_id.to_string())
                .join("sled")
                .join(sled.id().to_string());
            tokio::fs::create_dir_all(&sled_path).await?;
            tokio::fs::write(sled_path.join("sled.txt"), format!("{sled:?}"))
                .await?;

            let Ok(sled_client) = nexus_networking::sled_client(
                &datastore,
                &opctx,
                sled.id(),
                &opctx.log,
            )
            .await
            else {
                tokio::fs::write(
                    sled_path.join("error.txt"),
                    "Could not contact sled",
                )
                .await?;
                continue;
            };

            write_command_result_or_error(
                &sled_path,
                "dladm",
                sled_client.support_dladm_info().await,
            )
            .await?;
            write_command_result_or_error(
                &sled_path,
                "ipadm",
                sled_client.support_ipadm_info().await,
            )
            .await?;
            write_command_result_or_error(
                &sled_path,
                "zoneadm",
                sled_client.support_zoneadm_info().await,
            )
            .await?;
        }
    }

    Ok(report)
}

async fn write_command_result_or_error(
    path: &Utf8Path,
    command: &str,
    result: Result<
        sled_agent_client::ResponseValue<sled_agent_client::ByteStream>,
        sled_agent_client::Error<sled_agent_client::types::Error>,
    >,
) -> anyhow::Result<()> {
    match result {
        Ok(result) => {
            let output = result
                .into_inner_stream()
                .try_collect::<bytes::BytesMut>()
                .await?;
            tokio::fs::write(path.join(format!("{command}.txt")), output)
                .await?;
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
