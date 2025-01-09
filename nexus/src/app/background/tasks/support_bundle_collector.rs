// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing Support Bundles

use crate::app::background::BackgroundTask;
use camino::Utf8DirEntry;
use camino::Utf8Path;
use camino_tempfile::tempdir_in;
use camino_tempfile::tempfile_in;
use camino_tempfile::Utf8TempDir;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use omicron_common::update::ArtifactHash;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::SeekFrom;
use zip::write::FullFileOptions;
use zip::ZipWriter;

// We use "/var/tmp" to use Nexus' filesystem for temporary storage,
// rather than "/tmp", which would keep this collected data in-memory.
const TEMPDIR: &str = "/var/tmp";

// Specifies the data to be collected within the Support Bundle.
#[derive(Default)]
struct BundleRequest {
    // If "false": Skip collecting host-specific info from each sled.
    skip_sled_info: bool,
}

// Describes what happened while attempting to clean up Support Bundles.
#[derive(Debug, Default, Serialize, Eq, PartialEq)]
struct CleanupReport {
    // Responses from Sled Agents
    sled_bundles_deleted_ok: usize,
    sled_bundles_deleted_not_found: usize,
    sled_bundles_delete_failed: usize,

    // Results from updating our database records
    db_destroying_bundles_removed: usize,
    db_failing_bundles_updated: usize,
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
    disable: bool,
    nexus_id: OmicronZoneUuid,
}

impl SupportBundleCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        disable: bool,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        SupportBundleCollector { datastore, disable, nexus_id }
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

                return Ok(
                    DatabaseBundleCleanupResult::DestroyingBundleRemoved,
                );
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

        let collection = BundleCollection {
            collector: &self,
            log: opctx.log.new(slog::o!("bundle" => bundle.id.to_string())),
            opctx,
            request,
            bundle: &bundle,
        };

        let mut report = collection.collect_bundle_and_store_on_sled().await?;
        if let Err(err) = self
            .datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
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
struct BundleCollection<'a> {
    // The task responsible for this collection
    collector: &'a SupportBundleCollector,

    log: slog::Logger,
    opctx: &'a OpContext,
    request: &'a BundleRequest,
    bundle: &'a SupportBundle,
}

impl<'a> BundleCollection<'a> {
    // Collect the bundle within Nexus, and store it on a target sled.
    async fn collect_bundle_and_store_on_sled(
        &self,
    ) -> anyhow::Result<CollectionReport> {
        // Create a temporary directory where we'll store the support bundle
        // as it's being collected.
        let dir = tempdir_in(TEMPDIR)?;

        let mut collection = Box::pin(self.collect_bundle_as_file(&dir));

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
                    trace!(
                        &self.log,
                        "Checking if Bundle Collection cancelled";
                        "bundle" => %self.bundle.id
                    );

                    let bundle = self.collector.datastore.support_bundle_get(
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
            .collector
            .datastore
            .zpool_get_sled_if_in_service(
                &self.opctx,
                self.bundle.zpool_id.into(),
            )
            .await?;
        let sled_client = nexus_networking::sled_client(
            &self.collector.datastore,
            &self.opctx,
            sled_id.into_untyped_uuid(),
            &self.log,
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
                &ZpoolUuid::from(self.bundle.zpool_id),
                &DatasetUuid::from(self.bundle.dataset_id),
                &SupportBundleUuid::from(self.bundle.id),
                &hash.to_string(),
                body,
            )
            .await?;

        // Returning from this method should drop all temporary storage
        // allocated locally for this support bundle.
        Ok(report)
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
        &self,
        dir: &Utf8TempDir,
    ) -> anyhow::Result<CollectionReport> {
        let log = &self.log;

        info!(&log, "Collecting bundle as local file");
        let mut report = CollectionReport::new(self.bundle.id.into());

        tokio::fs::write(
            dir.path().join("bundle_id.txt"),
            self.bundle.id.to_string(),
        )
        .await?;

        if let Ok(all_sleds) = self
            .collector
            .datastore
            .sled_list_all_batched(&self.opctx, SledFilter::InService)
            .await
        {
            report.listed_in_service_sleds = true;

            // NOTE: This could be, and probably should be, done concurrently.
            for sled in &all_sleds {
                info!(&log, "Collecting bundle info from sled"; "sled" => %sled.id());
                let sled_path = dir
                    .path()
                    .join("rack")
                    .join(sled.rack_id.to_string())
                    .join("sled")
                    .join(sled.id().to_string());
                tokio::fs::create_dir_all(&sled_path).await?;
                tokio::fs::write(
                    sled_path.join("sled.txt"),
                    format!("{sled:?}"),
                )
                .await?;

                if self.request.skip_sled_info {
                    continue;
                }

                let Ok(sled_client) = nexus_networking::sled_client(
                    &self.collector.datastore,
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
            let mut reader = BufReader::new(std::fs::File::open(&src)?);

            loop {
                let buf = reader.fill_buf()?;
                let len = buf.len();
                if len == 0 {
                    break;
                }
                zip.write_all(&buf)?;
                reader.consume(len);
            }
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

// Identifies what we could or could not store within this support bundle.
//
// This struct will get emitted as part of the background task infrastructure.
#[derive(Debug, Serialize, PartialEq, Eq)]
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

async fn write_command_result_or_error<D: std::fmt::Debug>(
    path: &Utf8Path,
    command: &str,
    result: Result<
        sled_agent_client::ResponseValue<D>,
        sled_agent_client::Error<sled_agent_client::types::Error>,
    >,
) -> anyhow::Result<()> {
    match result {
        Ok(result) => {
            let output = result.into_inner();
            tokio::fs::write(
                path.join(format!("{command}.txt")),
                format!("{output:?}"),
            )
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

#[cfg(test)]
mod test {
    use super::*;

    use camino_tempfile::tempdir;
    use nexus_db_model::Dataset;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::Zpool;
    use nexus_test_utils::SLED_AGENT_UUID;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::shared::DatasetKind;
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::DatasetsConfig;
    use omicron_common::disk::SharedDatasetConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_uuid_kinds::{DatasetUuid, PhysicalDiskUuid, SledUuid};
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
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");

        assert_eq!(report, CleanupReport::default());
    }

    // If there are no bundles in need of collection, the collection task should
    // run without error, but return nothing.
    #[nexus_test(server = crate::Server)]
    async fn test_collect_noop(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());

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
    ) -> (ZpoolUuid, DatasetUuid) {
        let zpool = datastore
            .zpool_insert(
                opctx,
                Zpool::new(Uuid::new_v4(), sled_id.into_untyped_uuid(), id),
            )
            .await
            .unwrap();

        let dataset = datastore
            .dataset_upsert(Dataset::new(
                DatasetUuid::new_v4(),
                zpool.id(),
                None,
                DatasetKind::Debug,
            ))
            .await
            .unwrap();
        (ZpoolUuid::from_untyped_uuid(zpool.id()), dataset.id())
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

            for i in 0..count {
                // Create the (disk, zpool, dataset) tuple in Nexus
                let disk_id =
                    make_disk_in_db(datastore, opctx, i, sled_id).await;
                let (zpool_id, dataset_id) = add_zpool_and_debug_dataset(
                    &datastore, &opctx, disk_id, sled_id,
                )
                .await;

                // Tell the simulated sled agent to create this storage.
                //
                // (We could do this via HTTP request, but it's in the test process,
                // so we can just directly call the method on the sled agent)
                cptestctx.sled_agent.sled_agent.create_zpool(
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
            let dataset_config =
                DatasetsConfig { generation: Generation::new(), datasets };

            let res = cptestctx
                .sled_agent
                .sled_agent
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());

        // The bundle collection should complete successfully.
        let request = BundleRequest {
            // NOTE: The support bundle querying interface isn't supported on
            // the simulated sled agent (yet?) so we're skipping this step.
            skip_sled_info: true,
        };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        assert!(report.listed_in_service_sleds);
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
    async fn test_collect_many(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        let bundle2 = datastore
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a second support bundle");

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());

        // Each time we call "collect_bundle", we collect a SINGLE bundle.
        let request = BundleRequest { skip_sled_info: true };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle1.id.into());
        assert!(report.listed_in_service_sleds);
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        // Cancel the bundle immediately
        datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
                SupportBundleState::Destroying,
            )
            .await
            .unwrap();

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should succeed with no work to do");
        assert_eq!(
            report,
            CleanupReport {
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());
        let request = BundleRequest { skip_sled_info: true };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());
        assert!(report.listed_in_service_sleds);
        assert!(report.activated_in_db_ok);

        // Cancel the bundle after collection has completed
        datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
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
            CleanupReport {
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
                SupportBundleState::Failing,
            )
            .await
            .unwrap();

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());

        let report = collector
            .cleanup_destroyed_bundles(&opctx)
            .await
            .expect("Cleanup should delete failing bundle");
        assert_eq!(
            report,
            CleanupReport {
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());
        let request = BundleRequest { skip_sled_info: true };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
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
            CleanupReport {
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
            .support_bundle_create(&opctx, "For collection testing", nexus.id())
            .await
            .expect("Couldn't allocate a support bundle");
        assert_eq!(bundle.state, SupportBundleState::Collecting);

        let collector =
            SupportBundleCollector::new(datastore.clone(), false, nexus.id());
        let request = BundleRequest { skip_sled_info: true };
        let report = collector
            .collect_bundle(&opctx, &request)
            .await
            .expect("Collection should have succeeded under test")
            .expect("Collecting the bundle should have generated a report");
        assert_eq!(report.bundle, bundle.id.into());

        // Mark the bundle as "failing" - this should be triggered
        // automatically by the blueprint executor if the corresponding
        // storage has been expunged.
        datastore
            .support_bundle_update(
                &opctx,
                bundle.id.into(),
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
            CleanupReport {
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
