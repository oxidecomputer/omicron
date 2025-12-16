// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The entrypoint to all support bundle collection.
//!
//! These are the primitives used to look up everything else within the bundle.

use crate::app::background::tasks::support_bundle::cache::Cache;
use crate::app::background::tasks::support_bundle::perfetto;
use crate::app::background::tasks::support_bundle::request::BundleRequest;
use crate::app::background::tasks::support_bundle::request::TEMPDIR;
use crate::app::background::tasks::support_bundle::step::CollectionStep;
use crate::app::background::tasks::support_bundle::steps;

use anyhow::Context;
use camino::Utf8DirEntry;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use camino_tempfile::tempdir_in;
use camino_tempfile::tempfile_in;
use internal_dns_resolver::Resolver;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use parallel_task_set::ParallelTaskSet;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use slog_error_chain::InlineErrorChain;
use std::io::Write;
use std::num::NonZeroU64;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::SeekFrom;
use tufaceous_artifact::ArtifactHash;
use zip::ZipWriter;
use zip::write::FullFileOptions;

/// Wraps up all arguments to perform a single support bundle collection
pub struct BundleCollection {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    log: slog::Logger,
    opctx: OpContext,
    request: BundleRequest,
    bundle: SupportBundle,
    transfer_chunk_size: NonZeroU64,
}

impl BundleCollection {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        log: slog::Logger,
        opctx: OpContext,
        request: BundleRequest,
        bundle: SupportBundle,
        transfer_chunk_size: NonZeroU64,
    ) -> Self {
        Self {
            datastore,
            resolver,
            log,
            opctx,
            request,
            bundle,
            transfer_chunk_size,
        }
    }

    pub fn datastore(&self) -> &Arc<DataStore> {
        &self.datastore
    }

    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    pub fn log(&self) -> &slog::Logger {
        &self.log
    }

    pub fn opctx(&self) -> &OpContext {
        &self.opctx
    }

    pub fn request(&self) -> &BundleRequest {
        &self.request
    }

    pub fn bundle(&self) -> &SupportBundle {
        &self.bundle
    }

    /// Collect the bundle within Nexus, and store it on a target sled.
    pub async fn collect_bundle_and_store_on_sled(
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
        // TL;DR: This `tokio::select` is allowed to poll multiple futures, but
        // should not do any async work within the body of any chosen branch. A
        // previous iteration of this code polled the "collection" as "&mut
        // collection", and checked the status of the support bundle within a
        // branch of the "select" polling "yield_interval.tick()".
        //
        // We organize this work to "check for cancellation" as a whole future
        // for a critical, but subtle reason: After the tick timer yields,
        // we may then try to `await` a database function.
        //
        // This, at a surface-level glance seems innocent enough. However, there
        // is something potentially insidious here: if calling a datastore
        // function - such as "support_bundle_get" - awaits acquiring access
        // to a connection from the connection pool, while creating the
        // collection ALSO potentially awaits acquiring access to the
        // connection pool, it is possible for:
        //
        // 1. The `&mut collection` arm to have created a future, currently
        //    yielded, which wants access to this underlying resource.
        // 2. The current operation executing in `support_bundle_get` to
        //    be awaiting access to this same underlying resource.
        //
        // In this specific case, the connection pool would be attempting to
        // yield to the `&mut collection` arm, which cannot run, if we were
        // awaiting in the body of a different async select arm. This would
        // result in a deadlock.
        //
        // In the future, we may attempt to make access to the connection pool
        // safer from concurrent asynchronous access - it is unsettling that
        // multiple concurrent `.claim()` functions can cause this behavior -
        // but in the meantime, we perform this cancellation check in a single
        // future that always is polled concurrently with the collection work.
        // Because of this separation, each future is polled until one
        // completes, at which point we deterministically exit.
        //
        // For more details, see:
        // https://github.com/oxidecomputer/omicron/issues/9259

        tokio::select! {
            // Returns if the bundle should no longer be collected.
            why = self.check_for_cancellation() => {
                warn!(
                    &self.log,
                    "Support Bundle cancelled - stopping collection";
                    "bundle" => %self.bundle.id,
                    "state" => ?self.bundle.state
                );
                return Err(why);
            },
            // Otherwise, keep making progress on the collection itself.
            report = self.collect_bundle_as_file(&dir) => {
                info!(
                    &self.log,
                    "Bundle Collection completed";
                    "bundle" => %self.bundle.id
                );
                return report;
            },
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
            sled_id,
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

    // Indefinitely perform periodic checks about whether or not we should
    // cancel the bundle.
    //
    // Returns an error if:
    // - The bundle state is no longer SupportBundleState::Collecting
    // (which happens if the bundle has been explicitly cancelled, or
    // if the backing storage has been expunged).
    // - The bundle has been deleted
    //
    // Otherwise, keeps checking indefinitely while polled.
    async fn check_for_cancellation(&self) -> anyhow::Error {
        let work_duration = tokio::time::Duration::from_secs(5);
        let mut yield_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + work_duration,
            work_duration,
        );

        loop {
            // Timer fired mid-collection - check if we should stop.
            yield_interval.tick().await;
            trace!(
                self.log,
                "Checking if Bundle Collection cancelled";
                "bundle" => %self.bundle.id
            );

            match self
                .datastore
                .support_bundle_get(&self.opctx, self.bundle.id.into())
                .await
            {
                Ok(SupportBundle {
                    state: SupportBundleState::Collecting,
                    ..
                }) => {
                    // Bundle still collecting; continue...
                    continue;
                }
                Ok(_) => {
                    // Not collecting, for any reason: Time to exit
                    return anyhow::anyhow!("Support Bundle Cancelled");
                }
                Err(Error::ObjectNotFound { .. } | Error::NotFound { .. }) => {
                    return anyhow::anyhow!("Support Bundle Deleted");
                }
                Err(err) => {
                    warn!(
                        self.log,
                        "Database error checking bundle cancellation";
                        InlineErrorChain::new(&err)
                    );

                    // If we cannot contact the database, retry later
                    continue;
                }
            }
        }
    }

    async fn run_collect_bundle_steps(
        self: &Arc<Self>,
        output: &Utf8TempDir,
        mut steps: Vec<CollectionStep>,
    ) -> SupportBundleCollectionReport {
        let mut report =
            SupportBundleCollectionReport::new(self.bundle.id.into());

        const MAX_CONCURRENT_STEPS: usize = 16;
        let mut tasks =
            ParallelTaskSet::new_with_parallelism(MAX_CONCURRENT_STEPS);

        loop {
            // Process all the currently-planned steps
            while let Some(step) = steps.pop() {
                let previous_result = tasks
                    .spawn({
                        let collection = self.clone();
                        let dir = output.path().to_path_buf();
                        let log = self.log.clone();
                        async move {
                            debug!(log, "Running step"; "step" => &step.name);
                            step.run(&collection, dir.as_path(), &log).await
                        }
                    })
                    .await;

                if let Some(output) = previous_result {
                    output.process(&mut report, &mut steps);
                };
            }

            // If we've run out of tasks to spawn, join any of the previously
            // spawned tasks, if any exist.
            if let Some(output) = tasks.join_next().await {
                output.process(&mut report, &mut steps);

                // As soon as any task completes, see if we can spawn more work
                // immediately. This ensures that the ParallelTaskSet is
                // saturated as much as it can be.
                continue;
            }

            // Executing steps may create additional steps, as follow-up work.
            //
            // Only finish if we've exhausted all possible steps and joined all spawned work.
            if steps.is_empty() {
                // Write trace file before returning
                if let Err(err) = self.write_trace_file(output, &report).await {
                    warn!(
                        self.log,
                        "Failed to write trace file";
                        "error" => ?err
                    );
                }
                return report;
            }
        }
    }

    // Write a Perfetto Event format JSON file for visualization
    async fn write_trace_file(
        &self,
        output: &Utf8TempDir,
        report: &SupportBundleCollectionReport,
    ) -> anyhow::Result<()> {
        let meta_dir = output.path().join("meta");
        tokio::fs::create_dir_all(&meta_dir).await.with_context(|| {
            format!("Failed to create meta directory {meta_dir}")
        })?;

        let trace_path = meta_dir.join("trace.json");

        // Convert steps to Perfetto Trace Event format.
        // Sort steps by start time and assign each a unique sequential ID.
        //
        // This is necessary because the trace event format does not like
        // multiple slices to overlap - so we make each slice distinct.
        //
        // Ideally we'd be able to correlate these with actual tokio tasks,
        // but it's hard to convert tokio::task::Id to a u64 because
        // of https://github.com/tokio-rs/tokio/issues/7430
        let mut sorted_steps: Vec<_> = report.steps.iter().collect();
        sorted_steps.sort_by_key(|s| s.start);

        // Generate trace events - each step gets a unique ID (1, 2, 3, ...)
        // based on its start time order
        let trace_events: Vec<_> = sorted_steps
            .iter()
            .enumerate()
            .map(|(i, step)| {
                let start_us = step.start.timestamp_micros();
                let duration_us = (step.end - step.start)
                    .num_microseconds()
                    .unwrap_or(0)
                    .max(0);
                let step_id = i + 1;

                perfetto::TraceEvent {
                    name: step.name.clone(),
                    cat: "bundle_collection".to_string(),
                    ph: "X".to_string(),
                    ts: start_us,
                    dur: duration_us,
                    pid: 1,
                    tid: step_id,
                    args: json!({
                        "status": step.status.to_string(),
                    }),
                }
            })
            .collect();

        let trace = perfetto::Trace {
            trace_events,
            display_time_unit: "ms".to_string(),
        };

        let trace_content = serde_json::to_string_pretty(&trace)
            .context("Failed to serialize trace JSON")?;

        tokio::fs::write(&trace_path, trace_content).await.with_context(
            || format!("Failed to write trace file to {trace_path}"),
        )?;

        info!(
            self.log,
            "Wrote trace file";
            "path" => %trace_path,
            "num_events" => trace.trace_events.len()
        );

        Ok(())
    }

    // Perform the work of collecting the support bundle into a temporary directory
    //
    // "dir" is an output directory where data can be stored.
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
    //
    // The "steps" used within this function - passed to
    // [`Self::run_collect_bundle_steps`] - are run on a [`ParallelTaskSet`],
    // which automatically aborts tasks when it is dropped.
    async fn collect_bundle_as_file(
        self: &Arc<Self>,
        dir: &Utf8TempDir,
    ) -> anyhow::Result<SupportBundleCollectionReport> {
        let log = &self.log;

        info!(&log, "Collecting bundle as local file");

        let cache = Cache::new();
        let steps = steps::all(&cache);
        Ok(self.run_collect_bundle_steps(dir, steps).await)
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
            let src = entry.path();

            let zip_time = entry
                .path()
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|sys_time| jiff::Zoned::try_from(sys_time).ok())
                .and_then(|zoned| {
                    zip::DateTime::try_from(zoned.datetime()).ok()
                })
                .unwrap_or_else(zip::DateTime::default);

            let opts = FullFileOptions::default()
                .last_modified_time(zip_time)
                .compression_method(zip::CompressionMethod::Deflated)
                .large_file(true);

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

#[cfg(test)]
mod test {
    use super::*;

    use camino_tempfile::tempdir;

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
}
