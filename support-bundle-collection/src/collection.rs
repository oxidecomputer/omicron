// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The mechanism layer of support bundle collection.
//!
//! Given a datastore handle, internal-DNS resolver, [`OpContext`], a
//! [`BundleDataSelection`], a synthesized [`BundleInfo`] (id +
//! reason_for_creation), and an output directory, gather data into the
//! directory and produce a [`SupportBundleCollectionReport`].
//!
//! This layer never reads the `support_bundle` table, never transfers
//! data to a sled-agent's bundle storage endpoints, and never polls
//! bundle state. Those responsibilities belong to the caller.
//!
//! # Stopping early
//!
//! Collection can stop before every step has run, for one of two reasons:
//!
//! - The caller cancels it, through [`BundleCollection::cancellation_token`].
//!   The bundle is abandoned, and [`BundleCollection::collect_bundle_locally`]
//!   returns an error.
//! - The bundle reaches its size limit (see
//!   [`BundleCollection::with_size_limit`]). Collection stops, but the bundle
//!   keeps whatever was collected before then, along with a record of what was
//!   left out: in the collection report, and in the file
//!   [`SIZE_LIMIT_EXCEEDED_FILE`].
//!
//! Either way, steps observe the same signal: the
//! [`BundleCollection::stop_token`].

use crate::cache::Cache;
use crate::output::BundleOutput;
use crate::output::SizeBudget;
use crate::perfetto;
use crate::step::CollectionStep;
use crate::steps;
use nexus_types::support_bundle::BundleDataSelection;

use anyhow::Context;
use camino_tempfile::Utf8TempDir;
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use nexus_types::internal_api::background::SupportBundleSizeLimitExceeded;
use omicron_uuid_kinds::SupportBundleUuid;
use parallel_task_set::ParallelTaskSet;
use serde_json::json;
use slog::debug;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// The path of the file, relative to the root of a bundle, which describes what
/// was left out of a bundle that reached its size limit.
///
/// This file is only present in bundles that reached their limit.
pub const SIZE_LIMIT_EXCEEDED_FILE: &str = "meta/SIZE_LIMIT_EXCEEDED.txt";

/// Minimal bundle metadata needed by the mechanism layer.
///
/// Built by the caller — either drawn from an existing `support_bundle`
/// DB row or synthesized fresh.
#[derive(Clone, Debug)]
pub struct BundleInfo {
    pub id: SupportBundleUuid,
    pub reason_for_creation: String,
}

/// Wraps up all arguments to perform a single support bundle collection
pub struct BundleCollection {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    log: slog::Logger,
    opctx: OpContext,
    data_selection: BundleDataSelection,
    bundle: BundleInfo,
    // Cancelled by the caller, to abandon the bundle.
    cancellation_token: CancellationToken,
    // A child of `cancellation_token`, which is also cancelled when the bundle
    // reaches its size limit. This is what steps observe.
    stop_token: CancellationToken,
    size_budget: Arc<SizeBudget>,
}

impl BundleCollection {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        log: slog::Logger,
        opctx: OpContext,
        data_selection: BundleDataSelection,
        bundle: BundleInfo,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let stop_token = cancellation_token.child_token();
        let size_budget = Arc::new(SizeBudget::new(None, stop_token.clone()));
        Self {
            datastore,
            resolver,
            log,
            opctx,
            data_selection,
            bundle,
            cancellation_token,
            stop_token,
            size_budget,
        }
    }

    /// Limits the bundle to `limit` bytes, or removes the limit if `limit`
    /// is `None`. By default, bundles are unlimited.
    ///
    /// Once the bundle reaches its limit, collection stops, and the bundle
    /// keeps what was collected before then. See the [module
    /// documentation](self#stopping-early).
    pub fn with_size_limit(mut self, limit: Option<u64>) -> Self {
        self.size_budget =
            Arc::new(SizeBudget::new(limit, self.stop_token.clone()));
        self
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

    pub fn data_selection(&self) -> &BundleDataSelection {
        &self.data_selection
    }

    pub fn bundle(&self) -> &BundleInfo {
        &self.bundle
    }

    /// Returns the budget against which the bundle's size is tracked.
    pub fn size_budget(&self) -> &SizeBudget {
        &self.size_budget
    }

    /// Returns a reference to the cancellation token.
    ///
    /// Cancellation is driven by the caller, and abandons the bundle. Steps
    /// should observe [`BundleCollection::stop_token`] instead.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns the token which is cancelled when collection should stop:
    /// either because the caller cancelled it, or because the bundle
    /// reached its size limit.
    pub fn stop_token(&self) -> &CancellationToken {
        &self.stop_token
    }

    /// Returns true if collection should stop.
    ///
    /// Use for cooperative checks before cancel-unsafe operations (filesystem
    /// writes, `spawn_blocking`).
    pub fn is_stopped(&self) -> bool {
        self.stop_token.is_cancelled()
    }

    /// Returns a future that completes when collection should stop.
    ///
    /// Use in `tokio::select!` with cancel-safe operations (HTTP requests,
    /// DB queries) to stop immediately at await points.
    pub async fn stopped(&self) {
        self.stop_token.cancelled().await
    }

    /// Collect the bundle into the supplied temporary directory.
    ///
    /// The caller drives cancellation via `cancellation_token()`; this
    /// function drains in-flight work before returning. The caller is
    /// responsible for whatever happens to the directory afterward
    /// (zipping, transferring to durable storage, writing to stdout,
    /// etc.).
    pub async fn collect_bundle_locally(
        self: &Arc<Self>,
        dir: &Utf8TempDir,
    ) -> anyhow::Result<SupportBundleCollectionReport> {
        let report = self.collect_bundle_as_file(dir).await;

        if self.is_cancelled() {
            warn!(
                &self.log,
                "Support Bundle cancelled - stopping collection";
                "bundle" => %self.bundle.id,
            );
            return Err(anyhow::anyhow!("Support Bundle Cancelled"));
        }

        info!(
            &self.log,
            "Bundle Collection completed";
            "bundle" => %self.bundle.id
        );
        report
    }

    // Cancellation (and stopping at the size limit) is hybrid:
    //
    // - Cancel-safe operations (HTTP requests, DB queries) use
    //   `tokio::select!` with the token for immediate cancellation.
    //   Dropping these futures is safe — no local side effects.
    //
    // - Cancel-unsafe operations (filesystem writes via `tokio::fs`,
    //   `spawn_blocking`) use cooperative `is_stopped()` checks.
    //   These are never dropped mid-flight, ensuring all
    //   `spawn_blocking` work completes before the output directory
    //   is dropped.
    //
    // This loop checks the token before spawning new steps and drains
    // all in-flight tasks before returning. Failing to do so previously
    // raced `spawn_blocking` writes against `TempDir` cleanup; see
    // https://github.com/oxidecomputer/omicron/issues/10198.
    async fn run_collect_bundle_steps(
        self: &Arc<Self>,
        output: &Utf8TempDir,
        mut steps: Vec<CollectionStep>,
    ) -> SupportBundleCollectionReport {
        let mut report = SupportBundleCollectionReport::new(self.bundle.id);
        let bundle_output = BundleOutput::new(
            output.path(),
            self.size_budget.clone(),
            self.log.clone(),
        );
        let mut incomplete_steps = vec![];

        const MAX_CONCURRENT_STEPS: usize = 16;
        let mut tasks =
            ParallelTaskSet::new_with_parallelism(MAX_CONCURRENT_STEPS);

        loop {
            // Check for cancellation before spawning new work.
            if self.is_stopped() {
                info!(
                    &self.log,
                    "Collection stopped — draining in-flight steps";
                    "bundle" => %self.bundle.id,
                    "cancelled" => self.is_cancelled(),
                    "size_limit_exceeded" => self.size_budget.is_exceeded(),
                );
                break;
            }

            // Process all the currently-planned steps
            while !self.is_stopped()
                && let Some(step) = steps.pop()
            {
                let previous_result = tasks
                    .spawn({
                        let collection = self.clone();
                        let bundle_output = bundle_output.clone();
                        let log = self.log.clone();
                        async move {
                            debug!(log, "Running step"; "step" => &step.name);
                            step.run(&collection, &bundle_output, &log).await
                        }
                    })
                    .await;

                if let Some(output) = previous_result {
                    output.process(
                        &mut report,
                        &mut steps,
                        &mut incomplete_steps,
                    );
                };
            }

            // If we've run out of tasks to spawn, join any of the previously
            // spawned tasks, if any exist.
            if let Some(output) = tasks.join_next().await {
                output.process(&mut report, &mut steps, &mut incomplete_steps);

                // As soon as any task completes, see if we can spawn more work
                // immediately. This ensures that the ParallelTaskSet is
                // saturated as much as it can be.
                continue;
            }

            // Executing steps may create additional steps, as follow-up work.
            //
            // Only finish if we've exhausted all possible steps and joined
            // all spawned work.
            if steps.is_empty() {
                break;
            }
        }

        // Drain all in-flight tasks. This ensures any tokio::fs operations
        // (which internally use spawn_blocking) complete before we return,
        // so the TempDir can be safely dropped by our caller.
        while let Some(output) = tasks.join_next().await {
            output.process(&mut report, &mut steps, &mut incomplete_steps);
        }

        // If the caller cancelled collection, the bundle is being abandoned,
        // so there's no point in describing it.
        if self.is_cancelled() {
            return report;
        }

        // If the bundle reached its size limit, any steps left (including
        // those spawned by steps we drained) never ran.
        if let Some(exceeded) = self.size_budget.exceeded() {
            incomplete_steps.sort();
            let mut skipped_steps: Vec<String> =
                steps.into_iter().map(|step| step.name).collect();
            skipped_steps.sort();
            report.size_limit_exceeded = Some(SupportBundleSizeLimitExceeded {
                limit_bytes: self.size_budget.limit().unwrap_or(u64::MAX),
                used_bytes: self.size_budget.used(),
                exceeded_at: exceeded.at,
                exceeded_by_path: exceeded.path.to_string(),
                incomplete_steps,
                skipped_steps,
            });
        }

        // Write trace and report files before returning.
        //
        // These, like the size limit marker, are not charged against the
        // bundle's size limit: they're small, and describe the rest of the
        // bundle, so they must be present even if it reached its limit.
        if let Some(exceeded) = &report.size_limit_exceeded {
            if let Err(err) =
                self.write_size_limit_exceeded_file(output, exceeded).await
            {
                warn!(
                    self.log,
                    "Failed to write size limit marker file";
                    InlineErrorChain::new(err.as_ref())
                );
            }
        }
        if let Err(err) = self.write_trace_file(output, &report).await {
            warn!(
                self.log,
                "Failed to write trace file";
                InlineErrorChain::new(err.as_ref())
            );
        }
        if let Err(err) = self.write_report_file(output, &report).await {
            warn!(
                self.log,
                "Failed to write report file";
                InlineErrorChain::new(err.as_ref())
            );
        }
        report
    }

    // Write a human-readable description of what was left out of a bundle
    // that reached its size limit.
    async fn write_size_limit_exceeded_file(
        &self,
        output: &Utf8TempDir,
        exceeded: &SupportBundleSizeLimitExceeded,
    ) -> anyhow::Result<()> {
        let path = output.path().join(SIZE_LIMIT_EXCEEDED_FILE);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create directory {parent}")
            })?;
        }
        let mut contents = format!(
            "This support bundle is incomplete. Collection stopped when the \
             bundle reached\n\
             its size limit of {} bytes.\n\
             \n\
             Limit reached at: {}\n\
             Left out:         {}\n",
            exceeded.limit_bytes,
            exceeded.exceeded_at,
            exceeded.exceeded_by_path,
        );
        for (title, steps) in [
            (
                "Steps which were running, and may be incomplete:",
                &exceeded.incomplete_steps,
            ),
            ("Steps which never started:", &exceeded.skipped_steps),
        ] {
            if steps.is_empty() {
                continue;
            }
            contents.push_str(&format!("\n{title}\n"));
            for step in steps {
                contents.push_str(&format!("  - {step}\n"));
            }
        }
        contents.push_str("\nSee meta/report.json for details.\n");

        tokio::fs::write(&path, contents)
            .await
            .with_context(|| format!("failed to write {path}"))?;

        info!(
            self.log,
            "Bundle reached its size limit";
            "limit_bytes" => exceeded.limit_bytes,
            "used_bytes" => exceeded.used_bytes,
            "exceeded_by_path" => &exceeded.exceeded_by_path,
            "incomplete_steps" => exceeded.incomplete_steps.len(),
            "skipped_steps" => exceeded.skipped_steps.len(),
        );
        Ok(())
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

    // Write the collection report as JSON into the bundle so anyone who
    // unzips it later can see what was collected, including any per-step
    // partial-success details.
    async fn write_report_file(
        &self,
        output: &Utf8TempDir,
        report: &SupportBundleCollectionReport,
    ) -> anyhow::Result<()> {
        let meta_dir = output.path().join("meta");
        tokio::fs::create_dir_all(&meta_dir).await.with_context(|| {
            format!("failed to create meta directory {meta_dir}")
        })?;

        let report_path = meta_dir.join("report.json");
        let report_content = serde_json::to_string_pretty(report)
            .context("failed to serialize collection report")?;

        tokio::fs::write(&report_path, report_content).await.with_context(
            || format!("failed to write report file to {report_path}"),
        )?;

        info!(
            self.log,
            "Wrote report file";
            "path" => %report_path,
            "num_steps" => report.steps.len(),
        );

        Ok(())
    }

    // Perform the work of collecting the support bundle into a temporary
    // directory.
    //
    // "dir" is an output directory where data can be stored.
    //
    // If a partial bundle can be collected, it should be returned as
    // an Ok(SupportBundleCollectionReport). Any failures from this function
    // will prevent the support bundle from being collected altogether.
    //
    // Cancellation is hybrid: cancel-safe operations (HTTP, DB) use
    // `select!` with the stop token for immediate cancellation; cancel-unsafe
    // operations (filesystem writes) use cooperative `is_stopped()`.
    // In-flight work is drained before returning.
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
